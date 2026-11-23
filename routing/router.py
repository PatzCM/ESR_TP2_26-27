"""Overlay router (Module 2 - João): announces, activation, replication, failover.

Needs two functions from the overlay module (Pedro), see INTERFACES.md section 3:
    send(to: str, data: bytes) -> None     # send to the neighbour with ID `to`
    neighbors() -> list[str]               # IDs of the neighbours that are alive
and the overlay calls these on the Router:
    on_receive(data: bytes)                # every routing/data datagram received
    on_neighbor_down(id: str)
    tick()                                 # about once per second

Control messages: one JSON object, always with "type" and "from" (sender ID).
Data: b"DATA|<stream>|<from>|<payload>"  (payload is opaque, never re-encoded).

The Router is NOT thread-safe: hold the node's single lock around every call.
"""
import json
import logging
import time

from tabela import LOCAL, TabelaRotas

ROUTING_TYPES = {"ANNOUNCE", "ACTIVATE", "DEACTIVATE", "PING"}


class Router:
    def __init__(self, node_id, send, neighbors, server_streams=(), on_data=None,
                 clock=time.monotonic, announce_period=5.0, refresh_period=2.0):
        self.id = node_id
        self._send = send
        self._neighbors = neighbors
        self.on_data = on_data                # fn(stream, payload) -> this node's client
        self.now = clock
        self.announce_period = announce_period
        self.refresh_period = refresh_period
        # route expires after 3 missed ANNOUNCEs (15 s), destination after 3 missed ACTIVATEs (6 s)
        self.t = TabelaRotas(3 * announce_period, 3 * refresh_period, clock)
        self.log = logging.getLogger(f"router[{node_id}]")
        self.server_streams = list(server_streams)
        for s in self.server_streams:
            self.t.criar_local(s, node_id)
        self._seq = int(time.time())          # does not restart at 0 if the server restarts
        self._last_announce = float("-inf")
        self._last_refresh = float("-inf")
        self.stats = {"data_in": 0, "data_out": 0, "data_dropped": 0}

    # ---------- API for the streaming module (Patz) ----------
    def request_stream(self, stream):
        """Client: "I want to receive this stream"."""
        self.t.ativar(stream, LOCAL)
        self._reconcile(stream)

    def stop_stream(self, stream):
        """Client: "stop sending it to me"."""
        self.t.desativar(stream, LOCAL)
        self._reconcile(stream)

    def send_data(self, stream, payload):
        """Server: inject one video packet into the overlay."""
        self._replicate(stream, None, payload)

    def ping(self, stream):
        """Server: path test along the active routes (Etapa 3)."""
        self._propagate_ping(stream, None, [])

    def dump(self):
        """Human-readable routing table (for the node console)."""
        return self.t.dump()

    # ---------- events from the overlay (Pedro) ----------
    def on_receive(self, data):
        if data.startswith(b"DATA|"):
            parts = data.split(b"|", 3)
            if len(parts) == 4:
                self.stats["data_in"] += 1
                self._replicate(parts[1].decode(), parts[2].decode(), parts[3])
            return
        try:
            m = json.loads(data)
            kind, sender = m["type"], m["from"]
            stream = m["stream"]
        except (ValueError, KeyError, TypeError):
            return  # UDP: garbage is ignored
        try:
            if kind == "ANNOUNCE":
                self._on_announce(sender, m)
            elif kind == "ACTIVATE":
                if self.t.ativar(stream, sender):
                    self.log.info("%s: %s activated it", stream, sender)
                self._reconcile(stream)
            elif kind == "DEACTIVATE":
                self.t.desativar(stream, sender)
                self.log.info("%s: %s deactivated it", stream, sender)
                self._reconcile(stream)
            elif kind == "PING":
                self._propagate_ping(stream, sender, list(m.get("path", [])))
        except (KeyError, TypeError, ValueError) as e:
            self.log.warning("bad %s from %s: %r", kind, sender, e)

    def on_neighbor_down(self, node_id):
        self.log.info("neighbour %s is down", node_id)
        for s in self.t.remover_vizinho(node_id):
            self._reconcile(s)

    def tick(self):
        now = self.now()
        if self.server_streams and now - self._last_announce >= self.announce_period:
            self._last_announce = now
            self._seq += 1
            for s in self.server_streams:
                self._flood({"type": "ANNOUNCE", "from": self.id, "stream": s,
                             "server": self.id, "metric": 0, "seq": self._seq})
        if now - self._last_refresh >= self.refresh_period:
            self._last_refresh = now
            for r in self.t.rotas.values():       # soft state: refresh the upstream activation
                if r.ativa and r.upstream:
                    self._ctrl(r.upstream, "ACTIVATE", stream=r.stream)
        for s in self.t.expirar():
            self._reconcile(s)

    # ---------- internals ----------
    @staticmethod
    def _encode(msg):
        return json.dumps(msg, separators=(",", ":")).encode()

    def _ctrl(self, to, kind, **fields):
        self._send(to, self._encode({"type": kind, "from": self.id, **fields}))

    def _flood(self, msg, except_=None):
        data = self._encode(msg)
        for n in self._neighbors():
            if n != except_:                      # never back through the link it came from
                self._send(n, data)

    def _on_announce(self, sender, m):
        before = self.t.obter(m["stream"])
        old_origin = before.origem if before else None
        forward, cost = self.t.anuncio(m["stream"], m["server"], sender,
                                       int(m["metric"]), int(m["seq"]))
        if forward:
            self._flood({"type": "ANNOUNCE", "from": self.id, "stream": m["stream"],
                         "server": m["server"], "metric": cost, "seq": m["seq"]},
                        except_=sender)
        new_origin = self.t.obter(m["stream"]).origem
        if new_origin != old_origin:
            self.log.info("%s: best route now via %s (metric %s)", m["stream"], new_origin, cost)
        self._reconcile(m["stream"])

    def _reconcile(self, stream):
        """Make the upstream activation match the current best origin."""
        r = self.t.obter(stream)
        if not r or r.local:
            return
        if r.ativa:
            if r.origem is not None and r.origem != r.upstream:
                if r.upstream:
                    self._ctrl(r.upstream, "DEACTIVATE", stream=stream)
                self._ctrl(r.origem, "ACTIVATE", stream=stream)
                self.log.info("%s activated via %s", stream, r.origem)
                r.upstream = r.origem
        elif r.upstream:
            self._ctrl(r.upstream, "DEACTIVATE", stream=stream)
            r.upstream = None

    def _replicate(self, stream, sender, payload):
        dests = self.t.destinos_dados(stream, sender)
        if not dests:
            if sender is not None:
                self.stats["data_dropped"] += 1   # inactive route or not from our origin
            return
        packet = b"DATA|" + stream.encode() + b"|" + self.id.encode() + b"|" + payload
        for d in dests:
            if d == LOCAL:
                if self.on_data:
                    self.on_data(stream, payload)
            else:
                self._send(d, packet)
                self.stats["data_out"] += 1
        self.log.debug("DATA %s from %s -> %s", stream, sender or "(local server)", dests)

    def _propagate_ping(self, stream, sender, path):
        path = path + [self.id]
        dests = self.t.destinos_dados(stream, sender)
        self.log.info("PING %s path=%s -> %s", stream, path, dests)
        for d in dests:
            if d != LOCAL:
                self._ctrl(d, "PING", stream=stream, path=path)
