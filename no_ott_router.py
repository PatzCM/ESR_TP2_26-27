#!/usr/bin/env python3
"""Routing demo: minimal UDP node (manual neighbours) + João's Router.

Tests the routing module alone, without Pedro's bootstrapper or Patz's video:
the server sends one text "frame" per second. For the full node use ott.py.

Usage: python3 no_ott_router.py --id NAME [--port 5000] [--neighbors ID=IP:PORT ...]
                                [--serve VIDEO] [--watch SERVER/VIDEO] [--ping]
  --serve VIDEO        this node serves stream "<id>/VIDEO" (repeatable)
  --watch STREAM       this node wants to receive STREAM (repeatable)
  --ping               (server) sends a PING every 5 s
  --bind IP            default 0.0.0.0 (only change for local tests)
  --metric delay|hops  routing metric (default: measured delay + 1 ms per hop)

Console:
  routes        show this node's routing table
  ping          server: send a PING along the active routes
  down <id>     simulate a neighbour going down
  quit

Example (Cenario 1 in CORE; the router "Node" also runs the program):
  Server : python3 no_ott_router.py --id Server  --neighbors Node=10.0.0.1:5000 --serve video1
  Node   : python3 no_ott_router.py --id Node    --neighbors Server=10.0.0.20:5000 \\
                                     Client1=10.0.1.20:5000 Client2=10.0.2.20:5000
  Client1: python3 no_ott_router.py --id Client1 --neighbors Node=10.0.1.1:5000 --watch Server/video1
  Client2: python3 no_ott_router.py --id Client2 --neighbors Node=10.0.2.1:5000 --watch Server/video1
"""
import argparse
import logging
import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "routing"))
from router import Router  # noqa: E402

ap = argparse.ArgumentParser(usage=__doc__)
ap.add_argument("--id", required=True)
ap.add_argument("--port", type=int, default=5000)
ap.add_argument("--neighbors", nargs="*", default=[], metavar="ID=IP:PORT")
ap.add_argument("--serve", action="append", default=[], metavar="VIDEO")
ap.add_argument("--watch", action="append", default=[], metavar="STREAM")
ap.add_argument("--ping", action="store_true")
ap.add_argument("--bind", default="0.0.0.0")
ap.add_argument("--metric", choices=("delay", "hops"), default="delay")
args = ap.parse_args()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s",
                    datefmt="%H:%M:%S")

ADDRESS = {}                          # neighbour ID -> (ip, port): the "address book"
for item in args.neighbors:
    nid, addr = item.split("=", 1)
    ip, port = addr.rsplit(":", 1)
    ADDRESS[nid] = (ip, int(port))

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((args.bind, args.port))
lock = threading.RLock()              # the Router is not thread-safe
running = True


def send(to, data):
    if to not in ADDRESS:
        return
    try:
        sock.sendto(data, ADDRESS[to])
    except OSError as e:
        logging.warning("send to %s failed: %s", to, e)


def on_data(stream, payload):
    print(f"\n[CLIENT {args.id}] {stream} <- {payload!r}", flush=True)


STREAMS = [f"{args.id}/{v}" for v in args.serve]
router = Router(args.id, send, lambda: list(ADDRESS),
                server_streams=STREAMS, on_data=on_data, metric=args.metric)


def receive():
    while running:
        try:
            data, _ = sock.recvfrom(65535)
        except OSError:
            break
        with lock:
            router.on_receive(data)


def timers():
    """Router timers, one text frame per second (server) and PINGs."""
    next_frame = next_ping = 0.0
    n = 0
    while running:
        with lock:
            router.tick()
            now = time.monotonic()
            if STREAMS and now >= next_frame:
                next_frame, n = now + 1.0, n + 1
                for s in STREAMS:
                    router.send_data(s, f"frame {n}".encode())
            if args.ping and STREAMS and now >= next_ping:
                next_ping = now + 5
                for s in STREAMS:
                    router.ping(s)
        time.sleep(0.2)


def main():
    global running
    threading.Thread(target=receive, daemon=True).start()
    threading.Thread(target=timers, daemon=True).start()
    with lock:
        for s in args.watch:
            router.request_stream(s)
    print(f"[{args.id}] port {args.port} neighbours={ADDRESS} serve={STREAMS} watch={args.watch}")
    try:
        while True:
            line = input("> ").strip()
            if line == "quit":
                break
            elif line == "ping":
                with lock:
                    for s in STREAMS:
                        router.ping(s)
            elif line == "routes":
                with lock:
                    print(router.dump() or "(empty)")
            elif line.startswith("down "):
                nid = line.split()[1]
                if ADDRESS.pop(nid, None):
                    with lock:
                        router.on_neighbor_down(nid)
            elif line:
                print("commands: routes | ping | down <id> | quit")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        running = False
        sock.close()


if __name__ == "__main__":
    main()
