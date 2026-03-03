"""
ChatRF Interlink Hub Server
============================
The central relay server that ChatRF Interlink nodes connect to.
Acts like a CoD lobby, nodes join the network and any audio
one node transmits is fanned out to all other nodes in the same network.

Features:
  - Multiple isolated networks, like private rooms (via network_id)
  - Callsign-based authentication (optional allowlist)
  - TX arbitration: only one node can transmit at a time per network
  - Peer list broadcast on join/leave
  - Simple console status display

Usage:
    pip install websockets
    python interlink_hub.py [--host 0.0.0.0] [--port 6769] [--allowlist callsigns.txt]

Protocol (all text messages are JSON):
    Client -> Hub:
        {"type": "join",     "callsign": "SV2TMT", "network_id": "main"}
        {"type": "tx_start", "callsign": "SV2TMT"}
        {"type": "tx_end",   "callsign": "SV2TMT"}

    Hub -> Client:
        {"type": "peers",    "callsigns": ["SV2TMT", "SY7ETE", ...]}
        {"type": "tx_start", "callsign": "SV7CFA"}
        {"type": "tx_end",   "callsign": "SV7CFA"}
        {"type": "announce", "text": "Ο σταθμός SV2TCB συνδέθηκε στο δίκτυο."}
        {"type": "error",    "text": "...reason..."}

    Audio frames are binary messages (2-byte LE length prefix + Opus payload).
"""

import asyncio
import json
import logging
import argparse
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import websockets
from websockets.server import WebSocketServerProtocol

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger("interlink-hub")

# -------------------------------------------------------------------------
# Data structures
# -------------------------------------------------------------------------
@dataclass
class Node:
    """Represents a connected ChatRF repeater node."""
    callsign:   str
    network_id: str
    ws:         WebSocketServerProtocol
    joined_at:  float = field(default_factory=time.time)
    is_txing:   bool  = False

    def __str__(self):
        return f"{self.callsign}@{self.network_id}"

class Network:
    """A group of nodes that share audio with each other."""

    def __init__(self, network_id: str):
        self.network_id = network_id
        self.nodes: dict[str, Node] = {} # callsign -> Node
        self.current_tx: Optional[str] = None # callsign of active transmitter

    def add(self, node: Node):
        self.nodes[node.callsign] = node

    def remove(self, callsign: str):
        self.nodes.pop(callsign, None)
        if self.current_tx == callsign:
            self.current_tx = None

    def peers(self) -> list[str]:
        return list(self.nodes.keys())

    def is_empty(self) -> bool:
        return len(self.nodes) == 0

# Hub state
networks: dict[str, Network] = {}          # network_id → Network
ws_to_node: dict[WebSocketServerProtocol, Node] = {}  # ws → Node
allowlist: Optional[set[str]] = None       # None = open, set = restricted


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------
async def broadcast_json(network: Network, msg: dict, exclude: str = None):
    """Send a JSON message to all nodes in a network, optionally skipping one."""
    payload = json.dumps(msg)
    tasks = [
        node.ws.send(payload)
        for cs, node in network.nodes.items()
        if cs != exclude
    ]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def broadcast_peers(network: Network):
    """Tell every node in the network who else is connected."""
    msg = {"type": "peers", "callsigns": network.peers()}
    await broadcast_json(network, msg)

async def broadcast_audio(network: Network, data: bytes, sender_callsign: str):
    """Fan out a binary audio packet to all nodes except the sender."""
    tasks = [
        node.ws.send(data)
        for cs, node in network.nodes.items()
        if cs != sender_callsign
    ]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# ---------------------------------------------------------------------------
# Connection handler
# ---------------------------------------------------------------------------
async def handle_connection(ws: WebSocketServerProtocol):
    node: Optional[Node] = None

    try:
        async for raw in ws:

            # Binary audio frame
            if isinstance(raw, bytes):
                if node and node.is_txing:
                    net = networks.get(node.network_id)
                    if net:
                        await broadcast_audio(net, raw, node.callsign)
                continue

            # JSON control message
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await ws.send(json.dumps({"type": "error", "text": "Invalid JSON"}))
                continue

            msg_type = msg.get("type")

            # -- JOIN --
            if msg_type == "join":
                callsign   = str(msg.get("callsign",   "UNKNOWN")).upper().strip()
                network_id = str(msg.get("network_id", "main")).strip()

                # Allowlist check
                if allowlist is not None and callsign not in allowlist:
                    await ws.send(json.dumps({
                        "type": "error",
                        "text": f"{callsign} is not on the allowlist for this hub."
                    }))
                    log.warning(f"Rejected {callsign} - not on allowlist")
                    return

                # Create node
                node = Node(callsign=callsign, network_id=network_id, ws=ws)
                ws_to_node[ws] = node

                # Create network
                if network_id not in networks:
                    networks[network_id] = Network(network_id)
                net = networks[network_id]
                net.add(node)

                log.info(f"+ {node}  ({len(net.nodes)} nodes in '{network_id}')")

                # Tell everyone the updated peer list
                await broadcast_peers(net)

                # Announce the join to existing peers
                await broadcast_json(net, {
                    "type": "announce",
                    "text": f"Ο σταθμός {callsign} συνδέθηκε στο δίκτυο."
                }, exclude=callsign)

            # -- TX START --
            elif msg_type == "tx_start":
                if not node:
                    continue
                net = networks.get(node.network_id)
                if not net:
                    continue

                if net.current_tx is not None and net.current_tx != node.callsign:
                    # Someone else is already transmitting - reject politely
                    await ws.send(json.dumps({
                        "type": "error",
                        "text": f"Channel busy ({net.current_tx} is transmitting)"
                    }))
                    log.debug(f"TX blocked for {node.callsign} - {net.current_tx} is on air")
                    continue

                net.current_tx = node.callsign
                node.is_txing  = True
                log.info(f"▶  TX start: {node.callsign} on '{node.network_id}'")

                # Notify all other nodes
                await broadcast_json(net, {
                    "type":     "tx_start",
                    "callsign": node.callsign,
                }, exclude=node.callsign)

            # -- TX END --
            elif msg_type == "tx_end":
                if not node:
                    continue
                net = networks.get(node.network_id)
                if not net:
                    continue

                node.is_txing = False
                if net.current_tx == node.callsign:
                    net.current_tx = None

                log.info(f"■  TX end: {node.callsign} on '{node.network_id}'")

                await broadcast_json(net, {
                    "type":     "tx_end",
                    "callsign": node.callsign,
                }, exclude=node.callsign)

            else:
                await ws.send(json.dumps({"type": "error", "text": f"Unknown message type: {msg_type}"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        log.error(f"Unexpected error for {node or ws.remote_address}: {e}")

    finally:
        # Clean up on disconnect
        if node:
            net = networks.get(node.network_id)
            if net:
                # Clear TX lock if this node was transmitting
                if net.current_tx == node.callsign:
                    net.current_tx = None
                    await broadcast_json(net, {
                        "type":     "tx_end",
                        "callsign": node.callsign,
                    }, exclude=node.callsign)

                net.remove(node.callsign)
                log.info(f"- {node}  ({len(net.nodes)} nodes in '{node.network_id}')")

                if net.is_empty():
                    del networks[node.network_id]
                    log.info(f"  Network '{node.network_id}' is now empty, removed.")
                else:
                    # Announce departure and update peer list
                    await broadcast_peers(net)
                    await broadcast_json(net, {
                        "type": "announce",
                        "text": f"Ο σταθμός {node.callsign} έφυγε από το δίκτυο."
                    })

        ws_to_node.pop(ws, None)

# Status printer (prints a summary every 60s)
async def status_printer():
    while True:
        await asyncio.sleep(60)
        if not networks:
            log.info("Status: no active networks.")
            continue
        for nid, net in networks.items():
            txing = f", TX: {net.current_tx}" if net.current_tx else ""
            log.info(f"Status [{nid}]: {', '.join(net.peers())}{txing}")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main(host: str, port: int, allowlist_file: Optional[str]):
    global allowlist

    if allowlist_file:
        try:
            with open(allowlist_file) as f:
                allowlist = {line.strip().upper() for line in f if line.strip() and not line.startswith('#')}
            log.info(f"Allowlist loaded: {len(allowlist)} callsigns")
        except FileNotFoundError:
            log.error(f"Allowlist file not found: {allowlist_file}")
            return

    log.info(f"ChatRF Interlink Hub starting on ws://{host}:{port}")
    log.info("Press Ctrl+C to stop.\n")

    asyncio.create_task(status_printer())

    async with websockets.serve(handle_connection, host, port):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    # Launch arguments
    parser = argparse.ArgumentParser(description="ChatRF Interlink Hub Server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    parser.add_argument("--port", default=6769, type=int, help="Port (default: 6769)")
    parser.add_argument("--allowlist", default=None, help="Path to allowlist file (one callsign per line)")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.host, args.port, args.allowlist))
    except KeyboardInterrupt:
        log.info("Hub stopped.")
