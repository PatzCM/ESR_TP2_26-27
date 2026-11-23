#!/usr/bin/env python3
"""ott.py - one overlay node: Pedro's overlay + Joao's routing + Patz's streaming.

Every machine in the overlay runs this program. Options decide its role:

  bootstrapper   --config FILE            reads the topology, tells nodes their neighbours
  normal node    --bootstrapper IP:PORT   asks the bootstrapper for neighbours
  fallback       --neighbors ID=IP:PORT   used if the bootstrapper does not answer
                                          (or alone: fully manual mode)
  video server   --serve NAME=FILE        offers stream "<id>/<NAME>" (repeatable)
  video client   --watch STREAM           plays that stream (window, or --no-gui)

EXAMPLES (CORE, Cenario 1; names and IPs from Mixed_testing/cenario1.json)
  Server : python3 ott.py --id Server --config Mixed_testing/cenario1.json \\
                          --serve video1=streaming/videos/movie.Mjpeg
  Node   : python3 ott.py --id Node --bootstrapper 10.0.0.20:5000
  Client1: python3 ott.py --id Client1 --bootstrapper 10.0.0.20:5000 \\
                          --neighbors Node=10.0.0.1:5000 --watch Server/video1

CONSOLE (nodes that are not playing video): neighbors | routes | ping | quit

How the pieces are wired is described in INTERFACES.md. The router calls use
the English names from INTERFACES.md section 3; Pedro's overlay still uses its
current names (estado, ativo, ...) until his renames land.
"""
import argparse
import json
import logging
import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
for sub in ("Mixed_testing", "routing", "streaming"):
    sys.path.insert(0, os.path.join(HERE, sub))

import no_ott_mixed as overlay          # noqa: E402  Pedro
from router import ROUTING_TYPES, Router  # noqa: E402  Joao
from server import StreamSender         # noqa: E402  Patz
from transport import OverlayTransport  # noqa: E402  Patz
import client as player                 # noqa: E402  Patz



class OttNode:
    def __init__(self, args):
        self.args = args
        self.node = overlay.Node(args.id, args.port)
        self.lock = threading.RLock()        # one lock for every router call
        self.receiver = None                 # set on client nodes

        self.served = {}                     # stream ID -> video file
        for item in args.serve:
            name, path = item.split("=", 1)
            self.served["%s/%s" % (args.id, name)] = path

        self.router = Router(args.id, self.send, self.neighbors,
                             server_streams=list(self.served),
                             on_data=self.on_data, metric=args.metric)

    # ---- what the overlay gives the router (INTERFACES.md section 3)
    def send(self, to, data):
        """Send to the neighbour with ID `to`; the overlay knows its address."""
        n = self.node.neighbors.get(to)
        if n is None:
            return
        try:
            self.node.sock.sendto(data, (n["ip"], n["port"]))
        except OSError as e:
            self.node.log("ERRO", "send to %s failed: %s" % (to, e))

    def neighbors(self):
        """IDs of the neighbours that are currently alive."""
        with self.node.lock:
            return [i for i, n in self.node.neighbors.items() if n["estado"] == "ativo"]

    # ---- video arriving for this node's own client
    def on_data(self, stream, payload):
        if self.receiver is not None:
            self.receiver.on_data(payload)

    # ---- one receive loop for the whole node
    def receive_loop(self):
        n = self.node
        handlers = {"REGISTER": n.on_register, "NEED_NEIGHBOR": n.on_need_neighbor,
                    "NEIGHBORS": n.on_neighbors, "ERROR": n.on_error,
                    "HELLO": n.on_hello, "LEAVE": n.on_leave, "MSG": n.on_msg}
        while not n.parar.is_set():
            try:
                data, addr = n.sock.recvfrom(65535)
            except OSError:
                break                                   # socket closed on exit
            try:
                if data.startswith(b"DATA|"):
                    with self.lock:
                        self.router.on_receive(data)
                    continue
                msg = json.loads(data)
                if msg.get("type") in ROUTING_TYPES:
                    with self.lock:
                        self.router.on_receive(data)
                elif msg.get("type") in handlers:
                    handlers[msg["type"]](msg, addr)
            except Exception as e:                      # one bad packet must not kill the node
                n.log("AVISO", "bad datagram from %s: %r" % (addr[0], e))

    # ---- timers: router tick + tell the router when a neighbour goes down
    def tick_loop(self):
        alive = set()
        while not self.node.parar.is_set():
            now = set(self.neighbors())
            with self.lock:
                for gone in alive - now:
                    self.router.on_neighbor_down(gone)
                self.router.tick()
            alive = now
            self.node.parar.wait(0.2)

    # ---- start-up, same order as Pedro's main()
    def start(self):
        a, n = self.args, self.node
        if a.config:
            n.carregar_config(a.config)
        threading.Thread(target=self.receive_loop, daemon=True).start()

        if a.config:
            n.adicionar_manuais(a.neighbors)
        elif a.bootstrapper:
            n.boot_addr = overlay.parse_addr(a.bootstrapper)
            if not n.registo_inicial():
                n.log("REGISTO", "bootstrapper did not answer; using manual neighbours")
                n.adicionar_manuais(a.neighbors)
        else:
            n.adicionar_manuais(a.neighbors)

        threading.Thread(target=n.manutencao, daemon=True).start()
        threading.Thread(target=self.tick_loop, daemon=True).start()

        for i, (stream, path) in enumerate(sorted(self.served.items()), start=1):
            StreamSender(a.id, i, path, OverlayTransport(self.router, self.lock, stream),
                         a.fps, loop=True, verbose=False).start()
            n.log("STREAM", "serving %s from %s" % (stream, path))

        if a.watch:
            self.receiver = player.Receiver(None, None, a.buffer)
            with self.lock:
                self.router.request_stream(a.watch)
            n.log("STREAM", "requested %s" % a.watch)

    def console(self):
        print("Commands: neighbors | routes | ping | quit")
        while True:
            try:
                line = input("> ").strip()
            except EOFError:                        # no terminal: keep running
                self.node.parar.wait()
                return
            if line == "quit":
                return
            elif line == "neighbors":
                self.node.mostrar_vizinhos()
            elif line == "routes":
                with self.lock:
                    print(self.router.dump() or "(no routes)")
            elif line == "ping":
                with self.lock:
                    for s in self.served:
                        self.router.ping(s)
                print("ping sent" if self.served else "only a server node can ping")
            elif line:
                print("unknown command")

    def stop(self):
        if self.args.watch:
            with self.lock:
                self.router.stop_stream(self.args.watch)
        self.node.terminar()                        # sends LEAVE, closes the socket


def main():
    ap = argparse.ArgumentParser(description="OTT overlay node",
                                 formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog=__doc__.split("EXAMPLES", 1)[1])
    ap.add_argument("--id", required=True, help="node name, e.g. Node1 (letters/digits)")
    ap.add_argument("--port", type=int, default=5000, help="UDP port (default 5000)")
    ap.add_argument("--config", metavar="FILE", help="topology file: this node is the bootstrapper")
    ap.add_argument("--bootstrapper", metavar="IP:PORT", help="bootstrapper address")
    ap.add_argument("--neighbors", nargs="*", default=[], metavar="ID=IP:PORT",
                    help="manual neighbours (fallback, or manual mode)")
    ap.add_argument("--serve", action="append", default=[], metavar="NAME=FILE",
                    help="offer a video as stream <id>/<NAME> (repeatable)")
    ap.add_argument("--watch", metavar="STREAM", help="play this stream, e.g. Server/video1")
    ap.add_argument("--no-gui", action="store_true", help="client: print frames instead of a window")
    ap.add_argument("--metric", choices=("delay", "hops"), default="delay",
                    help="routing metric: measured link delay + 1 ms/hop (default) or hop count")
    ap.add_argument("--fps", type=float, default=20, help="frames per second (default 20)")
    ap.add_argument("--buffer", type=int, default=5, help="client: frames to buffer before playing")
    args = ap.parse_args()
    if args.config and args.bootstrapper:
        ap.error("--config and --bootstrapper cannot be used together")
    if "|" in args.id or "/" in args.id:
        ap.error("--id cannot contain '|' or '/'")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s",
                        datefmt="%H:%M:%S")
    ott = OttNode(args)
    ott.start()
    try:
        if args.watch and args.no_gui:
            player.run_headless(ott.receiver)
        elif args.watch:
            player.run_gui(ott.receiver, args.fps, args.buffer, "OTT %s - %s" % (args.id, args.watch))
        else:
            ott.console()
    except KeyboardInterrupt:
        pass
    finally:
        ott.stop()


if __name__ == "__main__":
    main()
