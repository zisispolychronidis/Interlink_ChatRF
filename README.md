# 📡 ChatRF Interlink

A module for [ChatRF](https://github.com/zisispolychronidis/ChatRF) that links multiple ChatRF repeaters together over the internet. Like Echolink or AllStar, but built natively into ChatRF.

When any linked node keys up, their audio is streamed in real-time to every other node on the network, and played out over their radio. It's plug-and-play: install the module, point it at a hub, and you're linked.

*Note: Some parts of this module were written using Claude because I don't have free time to write code these days. Any suggestions and contributions for improvement are welcome!*
---

## 🤔How It Works

```
Node A (e.g. SV2TMT)       Hub Server            Node B (e.g. SY7ETE)
────────────────────     ──────────────          ────────────────────
PTT keys up
  → capture audio   ──►  fan out to all    ──►   decode audio
  → Opus encode          connected nodes           → play via PyAudio
  → send to hub                                    → out over radio
```

- Audio is captured from the shared audio buffer, compressed with **Opus**, and streamed over a **WebSocket** to a central hub
- The hub fans the audio out to all other nodes in the same network in real-time
- Receiving nodes decode the audio and play it through a **jitter buffer** to smooth out network hiccups
- A TX lockout flag prevents a node from echoing received network audio back to the hub

---

## 📂Files

| File | Description |
|---|---|
| `background_interlink.py` | The ChatRF module - goes in your `modules/` directory |
| `dtmf_interlink_announce.py` | Optional DTMF module to announce all linked peers |
| `interlink_hub.py` | The hub server - runs on any machine with a public IP (no ChatRF needed) |

---

## ⏬Installation

### For a ChatRF node:

### 1. Install dependencies

On each ChatRF node:

```bash
pip install websockets pyaudio opuslib numpy
```

### 2. Clone the repository

```
git clone https://github.com/zisispolychronidis/Interlink_ChatRF.git
```

### 3. Copy the module files

Copy `background_interlink.py` (and optionally `dtmf_interlink_announce.py`) into your ChatRF `modules/` directory:

```
modules/
├── background_interlink.py
├── dtmf_interlink_announce.py  # optional
├── opus.dll # usually required on Windows
├── ...
```

And copy `opus.dll` to your Python installation directory (usually something like `C:\Users\User\AppData\Local\Programs\Python\Python313` on Windows).

### 4. Add configuration

Add the following section to your `config/settings/config.ini`:

```ini
[Interlink]
enabled = true
hub_url = ws://localhost:6769
network_id = main
# Optional settings (uncomment to change)
# output_device_index = -1
# opus_bitrate = 24000
# jitter_buffer_size = 10
```

### 5. Restart ChatRF

### For a hub server (doesn't need a ChatRF installation):

### 1. Install dependencies

```bash
pip install websockets
```

### 2. Start the hub server

On your server (needs a public IP or port forwarding):

```bash
python interlink_hub.py
```

With a custom port and an allowlist:

```bash
python interlink_hub.py --host 0.0.0.0 --port 6769 --allowlist callsigns.txt
```

That's it! Start ChatRF on each node and they'll link up automatically.

---

## 🔧Configuration Reference

All options go under `[Interlink]` in `config.ini`.

| Option | Default | Description |
|---|---|---|
| `enabled` | `true` | Enable or disable the module |
| `hub_url` | `ws://localhost:6769` | WebSocket URL of the hub server |
| `network_id` | `main` | Network name - only nodes with the same ID are linked |
| `output_device_index` | `-1` | PyAudio output device index (`-1` = system default) |
| `opus_bitrate` | `24000` | Opus encoder bitrate in bps (higher = better quality, more bandwidth) |
| `jitter_buffer_size` | `10` | Jitter buffer depth in frames |

---

## 🖥️Hub Server

The hub is a lightweight asyncio WebSocket server. It handles:

- **Node registration** - nodes identify themselves with their callsign and network ID on connect
- **TX arbitration** - only one node can transmit at a time per network; others receive a "channel busy" response
- **Audio fanout** - binary audio packets from the active transmitter are forwarded to all other nodes instantly
- **Peer announcements** - every node is notified when someone joins or leaves
- **Automatic cleanup** - disconnected nodes are removed and TX locks are released cleanly

### Hub command line options

```
python interlink_hub.py [--host HOST] [--port PORT] [--allowlist FILE]
```

| Option | Default | Description |
|---|---|---|
| `--host` | `0.0.0.0` | Address to bind to |
| `--port` | `6769` | Port to listen on |
| `--allowlist` | *(none)* | Path to a text file of permitted callsigns |

### Allowlist format

One callsign per line. Lines starting with `#` are ignored:

```
# ChatRF Interlink allowlist
SV2TMT
SY7ETE
SV2TCB
SV3TRX
```

If no allowlist is provided, any callsign can connect.

---

## 📢DTMF Peer Announce

The optional `dtmf_interlink_announce.py` module adds a DTMF command that speaks the currently linked stations over the air.

**Default trigger:** `9`

Example output:
> *"Currently linked: SY2EJG, SV7CFA"*

To change the trigger key, edit `dtmf_command` in the file:

```python
dtmf_command = '8'   # Change to any free DTMF key
```

---

## 🌐Multiple Networks

The `network_id` config value acts like a room name. Nodes only hear each other if they share the same ID. You can run multiple isolated networks on a single hub:

```ini
; Node A and B
network_id = main

; Node C - isolated, won't hear A or B
network_id = tyropita
```

---

## 🔊Audio Pipeline

```
TX path:
  Repeater shared audio buffer → Opus encode (24kbps) → 2-byte length header → WebSocket → Hub → all peers

RX path:
  WebSocket → strip header → Opus decode → Queue (jitter buffer) → PyAudio output
```

The Opus codec is optimised for voice at low bitrates and handles packet loss gracefully, well suited to real-world internet links. The jitter buffer absorbs network timing variations so playback stays smooth.

---

## 🆘Troubleshooting

**Module not loading**

Check `logs/repeater.log` for errors. Most likely cause is a missing dependency:
```bash
pip install websockets pyaudio opuslib numpy
```

**Not connecting to hub**

- Check `hub_url` in config, it must be `ws://` not `http://`
- Make sure port 6769 (or your chosen port) is open in your firewall/router
- Check the hub server terminal for connection attempts

**Audio is choppy or has gaps**

Increase the jitter buffer:
```ini
jitter_buffer_size = 20
```

**Wrong audio device being used**

List available devices with:
```python
import pyaudio
p = pyaudio.PyAudio()
for i in range(p.get_device_count()):
    print(i, p.get_device_info_by_index(i)['name'])
```

Then set `output_device_index` in config accordingly.

**TX lockout not releasing**

If a node disconnects mid-transmission, the hub releases the TX lock automatically. If it seems stuck, restarting the affected node might resolve it.

---

## 🤝Contributing

Found a bug or have an improvement? Open an issue or pull request on the repository. Feature ideas and module contributions are always welcome!

---

*73 de SV2TMT 🎙️📡*
