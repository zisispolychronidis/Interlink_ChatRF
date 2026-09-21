"""
ChatRF Interlink Module
=======================
Links multiple ChatRF repeaters together over the internet,
like a global linked repeater network (e.g. Echolink/AllStar but native to ChatRF).

Configuration (config.ini):
    [Interlink]
    enabled = true
    hub_url = ws://yourserver.com:6769
    network_id = main
    output_device_index = -1
    opus_bitrate = 24000
    jitter_buffer_size = 10
"""

import threading
import queue
import time
import struct
import asyncio
import json
import logging
import numpy as np

from modules.base import BackgroundServiceModule

# Ensure dependencies
try:
    import pyaudio
    PYAUDIO_AVAILABLE = True
except ImportError:
    PYAUDIO_AVAILABLE = False

try:
    import opuslib
    OPUSLIB_AVAILABLE = True
except ImportError:
    OPUSLIB_AVAILABLE = False

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

# Audio settings
SAMPLE_RATE = 48000 # Opus works best at 48kHz
CHANNELS = 1 # Mono
SAMPLE_WIDTH = 2 # 16-bit PCM (2 bytes per sample)
FRAME_DURATION = 20 # ms per Opus frame (20ms is standard)
FRAME_SIZE = int(SAMPLE_RATE * FRAME_DURATION / 1000) # samples per frame


class InterlinkModule(BackgroundServiceModule):
    # ------------------------------------------------------------------
    # Module metadata
    # ------------------------------------------------------------------
    name        = "ChatRF Interlink"
    version     = "1.0.0"
    description = "Links ChatRF repeaters together over the internet via a central hub"
    enabled     = True

    def initialize(self):
        """Load config, check dependencies, set up state."""
        # dependency check
        missing = []
        if not PYAUDIO_AVAILABLE: missing.append("pyaudio")
        if not OPUSLIB_AVAILABLE: missing.append("opuslib")
        if not WEBSOCKETS_AVAILABLE: missing.append("websockets")
        if missing:
            self.logger.error(
                f"Interlink disabled - missing packages: {', '.join(missing)}. "
                f"Run: pip install {' '.join(missing)}"
            )
            self.enabled = False
            return

        # config
        cfg = self.config.config
        self.callsign      = cfg.get    ('CW', 'callsign',                   fallback='N0CALL')
        self.hub_url       = cfg.get    ('Interlink', 'hub_url',             fallback='ws://localhost:6769')
        self.network_id    = cfg.get    ('Interlink', 'network_id',          fallback='main')
        self.out_dev_idx   = cfg.getint ('Interlink', 'output_device_index', fallback=-1)
        self.opus_bitrate  = cfg.getint ('Interlink', 'opus_bitrate',        fallback=24000)
        self.jitter_buf_sz = cfg.getint ('Interlink', 'jitter_buffer_size',  fallback=10)

        # runtime state
        self.websocket = None # active websocket connection
        self.ws_loop = None # asyncio event loop for websocket thread
        self.connected = False
        self.receiving_network_audio = False # TX lockout flag

        # Jitter buffer: decoded PCM frames waiting to be played
        self.jitter_buffer = queue.Queue(maxsize=self.jitter_buf_sz * 4)

        # Connected peers (callsign list from hub)
        self.connected_peers: list[str] = []

        # Opus codec instances (created in run() after thread starts)
        self.encoder = None
        self.decoder = None

        # PyAudio instance (shared)
        self.pa = None

        self.logger.info(
            f"Interlink initialized - hub: {self.hub_url}, "
            f"callsign: {self.callsign}, network: {self.network_id}"
        )

    # ----------------------------------------------------------------
    # Main entry point
    # ----------------------------------------------------------------
    def run(self):
        if not self.enabled:
            return

        # Build Opus codec instances
        self.encoder = opuslib.Encoder(SAMPLE_RATE, CHANNELS, opuslib.APPLICATION_VOIP)
        self.encoder.bitrate = self.opus_bitrate
        self.decoder = opuslib.Decoder(SAMPLE_RATE, CHANNELS)

        # Start PyAudio
        self.pa = pyaudio.PyAudio()

        # Spin up sub-threads
        threads = [
            threading.Thread(target=self._ws_thread,       daemon=True, name="interlink-ws"),
            threading.Thread(target=self._tx_thread,       daemon=True, name="interlink-tx"),
            threading.Thread(target=self._rx_audio_thread, daemon=True, name="interlink-rx-audio"),
        ]
        for t in threads:
            t.start()

        # Keep alive - just wait for stop signal
        self._stop_event.wait()

        # Cleanup
        self.logger.info("Interlink shutting down…")
        if self.pa:
            self.pa.terminate()

    # WebSocket thread
    def _ws_thread(self):
        """Runs an asyncio event loop dedicated to the WebSocket connection."""
        self.ws_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.ws_loop)
        self.ws_loop.run_until_complete(self._ws_connect_loop())

    async def _ws_connect_loop(self):
        """Reconnects to the hub whenever the connection drops."""
        RETRY_DELAY = 5  # seconds between attempts

        while not self._stop_event.is_set():
            try:
                self.logger.info(f"Connecting to hub at {self.hub_url}…")
                async with websockets.connect(self.hub_url, ping_interval=20, ping_timeout=10) as ws:
                    self.websocket = ws
                    self.connected = True
                    self.logger.info("Connected to hub!")

                    # Identify ourselves to the hub
                    await ws.send(json.dumps({
                        "type":       "join",
                        "callsign":   self.callsign,
                        "network_id": self.network_id,
                    }))

                    # Receive loop
                    async for raw_message in ws:
                        if self._stop_event.is_set():
                            break
                        await self._handle_ws_message(raw_message)

            except Exception as e:
                self.logger.warning(f"Hub connection lost: {e}. Retrying in {RETRY_DELAY}s…")
            finally:
                self.connected = False
                self.websocket = None
                self.receiving_network_audio = False

            # Wait before retrying
            self._stop_event.wait(RETRY_DELAY)

    async def _handle_ws_message(self, raw):
        """Dispatch incoming WebSocket messages."""
        # Binary message, Opus audio frame
        if isinstance(raw, bytes):
            self._handle_audio_packet(raw)
            return

        # JSON control message
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        msg_type = msg.get("type")

        if msg_type == "peers":
            # Hub sends updated peer list whenever someone joins/leaves
            self.connected_peers = msg.get("callsigns", [])
            # Remove ourselves from the display list
            display = [c for c in self.connected_peers if c != self.callsign]
            self.logger.info(f"Linked peers: {', '.join(display) if display else 'none'}")

        elif msg_type == "tx_start":
            # Another node started transmitting
            sender = msg.get("callsign", "unknown")
            self.logger.info(f"RX from {sender} on network")
            self.receiving_network_audio = True

        elif msg_type == "tx_end":
            # Other node finished transmitting - drain jitter buffer before clearing flag
            sender = msg.get("callsign", "unknown")
            self.logger.info(f"End of TX from {sender}")
            # Give the audio thread a moment to drain
            threading.Timer(0.5, self._clear_receiving_flag).start()

        elif msg_type == "announce":
            # Hub-originated text announcement (e.g. who joined)
            text = msg.get("text", "")
            if text:
                self.logger.info(f"Μήνυμα Hub: {text}")
                # Speak announcement
                self.repeater.speak_with_piper(text)

    def _clear_receiving_flag(self):
        self.receiving_network_audio = False

    # Audio packet handling
    def _handle_audio_packet(self, data: bytes):
        """
        Wire format: 2-byte little-endian length prefix + Opus payload.
        Decode and push PCM into the jitter buffer.
        """
        if len(data) < 2:
            return
        payload_len = struct.unpack_from('<H', data, 0)[0]
        opus_payload = data[2:2 + payload_len]

        try:
            pcm_bytes = self.decoder.decode(opus_payload, FRAME_SIZE)
            # Drop frames if buffer is full
            if not self.jitter_buffer.full():
                self.jitter_buffer.put_nowait(pcm_bytes)
        except Exception as e:
            self.logger.debug(f"Opus decode error: {e}")

    # ---------------------------------------------------------------------
    # TX thread - reads mic audio from repeater's shared audio buffer and
    # streams it to the hub when someone is talking
    # ---------------------------------------------------------------------
    def _tx_thread(self):
        """
        Watches repeater.talking. When True (and we're not receiving network
        audio), pulls the raw PCM chunks from self.repeater.shared_data['audio_buffer'].
        Chunks are downmixed to mono, resampled to 48kHz, sliced into 20ms Opus frames
        and sent to the hub.
        """
        was_talking = False

        # Index into shared_data['audio_buffer'] we've already consumed.
        # We only ever read from this list, never mutate it.
        self._buf_pos = 0

        # The list object that index belongs to. repeater.py starts a fresh list
        # for every transmission, so a different object means "buffer was reset".
        self._buf_ref = None

        # Resampled mono 48kHz PCM waiting to be sliced into Opus frames
        self._pcm48k = np.array([], dtype=np.int16)

        src_rate        = self.repeater.config.RATE
        src_channels    = self.repeater.config.CHANNELS
        src_channel_sel = self.repeater.config.INPUT_CHANNEL

        while not self._stop_event.is_set():
            talking = getattr(self.repeater, 'talking', False)

            # Don't TX if we're playing received network audio
            if self.receiving_network_audio:
                if was_talking:
                    self._send_control({"type": "tx_end", "callsign": self.callsign})
                    was_talking = False
                    self._pcm48k = np.array([], dtype=np.int16)
                time.sleep(0.02)
                continue

            buf = getattr(self.repeater, 'shared_data', {}).get('audio_buffer', [])

            # A different list object means the buffer was reset (new transmission),
            # so start from its beginning. The length check remains as a fallback
            # for modules that empty the same list in place.
            if buf is not self._buf_ref or len(buf) < self._buf_pos:
                self._buf_ref = buf
                self._buf_pos = 0

            new_chunks = buf[self._buf_pos:]
            self._buf_pos = len(buf)

            if talking and not was_talking:
                self._send_control({"type": "tx_start", "callsign": self.callsign})
                was_talking = True
                self.logger.debug("TX started (shared audio buffer)")
            elif not talking and was_talking:
                self._send_control({"type": "tx_end", "callsign": self.callsign})
                was_talking = False
                # Drop any leftover unframed samples so the next transmission starts clean
                self._pcm48k = np.array([], dtype=np.int16)
                self.logger.debug("TX ended")

            if new_chunks:
                mono = self._downmix_chunks(new_chunks, src_channels, src_channel_sel)
                resampled = self._resample(mono, src_rate, SAMPLE_RATE)
                if resampled.size:
                    self._pcm48k = np.concatenate([self._pcm48k, resampled])

                # Slice off complete 20ms Opus frames
                while self._pcm48k.size >= FRAME_SIZE:
                    frame, self._pcm48k = self._pcm48k[:FRAME_SIZE], self._pcm48k[FRAME_SIZE:]
                    try:
                        opus_payload = self.encoder.encode(frame.tobytes(), FRAME_SIZE)
                        # Prepend 2-byte length header
                        packet = struct.pack('<H', len(opus_payload)) + opus_payload
                        self._send_audio(packet)
                    except Exception as e:
                        self.logger.debug(f"TX frame error: {e}")

            time.sleep(0.01)

    @staticmethod
    def _downmix_chunks(chunks, channels, channel_sel):
        """Concatenate raw PCM chunks from repeater.py and downmix to mono."""
        raw = b''.join(chunks)
        if not raw:
            return np.array([], dtype=np.int16)

        samples = np.frombuffer(raw, dtype=np.int16)

        if channels == 2:
            samples = samples.reshape(-1, 2)
            if channel_sel == 'left':
                samples = samples[:, 0]
            elif channel_sel == 'right':
                samples = samples[:, 1]
            else:  # 'mono' or 'both' -> average down to mono
                samples = samples.mean(axis=1).astype(np.int16)

        return samples

    @staticmethod
    def _resample(samples: np.ndarray, orig_rate: int, target_rate: int) -> np.ndarray:
        """Linear-interpolation resample (no audioop dependency, since it's
        gone in Python 3.13+). Good enough for narrowband voice over RF."""
        if samples.size == 0 or orig_rate == target_rate:
            return samples.astype(np.int16)

        duration = samples.size / orig_rate
        target_len = max(1, int(round(duration * target_rate)))
        x_old = np.linspace(0, duration, samples.size, endpoint=False)
        x_new = np.linspace(0, duration, target_len, endpoint=False)
        resampled = np.interp(x_new, x_old, samples.astype(np.float32))
        return np.clip(resampled, -32768, 32767).astype(np.int16)

    # ------------------------------------------------------------------
    # RX audio thread - plays contents via PyAudio output
    # ------------------------------------------------------------------
    def _rx_audio_thread(self):
        """
        Pulls decoded PCM frames from the jitter buffer and writes them
        to a PyAudio output stream for real-time playback.
        """
        out_idx = self.out_dev_idx if self.out_dev_idx >= 0 else None
        stream  = None

        while not self._stop_event.is_set():
            if not self.receiving_network_audio:
                # Nothing to play - close stream if open and idle
                if stream:
                    try:
                        stream.stop_stream()
                        stream.close()
                    except Exception:
                        pass
                    stream = None
                time.sleep(0.02)
                continue

            # Ensure output stream is open
            if stream is None or not stream.is_active():
                try:
                    stream = self.pa.open(
                        format=pyaudio.paInt16,
                        channels=CHANNELS,
                        rate=SAMPLE_RATE,
                        output=True,
                        output_device_index=out_idx,
                        frames_per_buffer=FRAME_SIZE,
                    )
                except Exception as e:
                    self.logger.error(f"Could not open output stream: {e}")
                    time.sleep(1)
                    continue

            # Drain one frame from the jitter buffer
            try:
                pcm_bytes = self.jitter_buffer.get(timeout=0.1)
                stream.write(pcm_bytes)
            except queue.Empty:
                # Underrun - write silence to keep stream alive
                silence = b'\x00' * (FRAME_SIZE * CHANNELS * SAMPLE_WIDTH)
                stream.write(silence)

    # Helpers for sending over the WebSocket from non-async threads
    def _send_audio(self, packet: bytes):
        """Schedule a binary send on the ws event loop."""
        if self.connected and self.websocket and self.ws_loop:
            asyncio.run_coroutine_threadsafe(
                self.websocket.send(packet),
                self.ws_loop
            )

    def _send_control(self, msg: dict):
        """Schedule a JSON control message on the ws event loop."""
        if self.connected and self.websocket and self.ws_loop:
            asyncio.run_coroutine_threadsafe(
                self.websocket.send(json.dumps(msg)),
                self.ws_loop
            )

    # ------------------------------------------------------------------
    # DTMF function: speak connected peers over the air
    # (this is called from a seperate DTMF module)
    # ------------------------------------------------------------------
    def announce_peers(self):
        peers = [c for c in self.connected_peers if c != self.callsign]
        if not self.connected:
            self.repeater.speak_with_piper("Το Interlink δεν είναι συνδεδεμένο.")
        elif not peers:
            self.repeater.speak_with_piper("Δεν υπάρχουν άλλοι σταθμοί μέσω Interlink.")
        else:
            names = ", ".join(peers)
            self.repeater.speak_with_piper(f"Συνδεδεμένοι: {names}")

    def cleanup(self):
        self.logger.info("Interlink module cleanup.")
        if self.pa:
            try:
                self.pa.terminate()
            except Exception:
                pass
