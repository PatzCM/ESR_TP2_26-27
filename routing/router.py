"""Overlay router (Module 2 - João): announces, activation, replication, failover.

Needs two functions from the overlay module (Pedro), see INTERFACES.md section 3:
    send(to: str, data: bytes) -> None     # send to the neighbour with ID `to`
    neighbors() -> list[str]               # IDs of the neighbours that are alive
and the overlay calls these on the Router:
    on_receive(data: bytes)                # every routing/data datagram received
    on_neighbor_down(id: str)
    tick()                                 # about once per second

Control messages: one JSON object, always with "type" and "from" (sender ID).

Metric (metric="delay", the default): cost of a link = measured one-way delay
in ms (half the smoothed RTT of PROBE/PROBE_REPLY) + HOP_PENALTY_MS per hop.
With no delay on the links this is the hop count; with delay, delay dominates.
metric="hops" uses the plain hop count.
Data: b"DATA|<stream>|<from>|<payload>"  (payload is opaque, never re-encoded).

The Router is NOT thread-safe: hold the node's single lock around every call.
"""
import json
import logging
import time

from tabela import LOCAL, TabelaRotas

ROUTING_TYPES = {"ANNOUNCE", "ACTIVATE", "DEACTIVATE", "PING", "PROBE", "PROBE_REPLY"}

HOP_PENALTY_MS = 1.0     # added per hop, so a shorter path wins when delays are equal
DEFAULT_LINK_MS = 0.0    # delay assumed for a link not measured yet
RTT_ALPHA = 0.3          # EWMA weight of a new RTT sample
HYSTERESIS = 0.1         # delay mode: switch origin only if >10% better


class Router:
    def __init__(self, node_id, send, neighbors, server_streams=(), on_data=None,
                 clock=time.monotonic, announce_period=5.0, refresh_period=2.0,
                 metric="delay", probe_period=1.0):
        if metric not in ("delay", "hops"):
            raise ValueError("metric must be 'delay' or 'hops'")
        self.id = node_id
        self._send = send
        self._neighbors = neighbors
        self.on_data = on_data                # fn(stream, payload) -> this node's client
        self.now = clock
        self.announce_period = announce_period
        self.refresh_period = refresh_period
        # route expires after 3 missed ANNOUNCEs (15 s), destination after 3 missed ACTIVATEs (6 s)
        self.metric = metric
        self.probe_period = probe_period
        self.t = TabelaRotas(3 * announce_period, 3 * refresh_period, clock,
                             histerese=HYSTERESIS if metric == "delay" else 0.0)
        self.srtt = {}                        # neighbour -> smoothed RTT (ms)
        self._last_probe = float("-inf")
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
        out = self.t.dump()
        if self.metric == "delay" and self.srtt:
            links = ", ".join(f"{n}={self.link_delay(n):.2f}ms" for n in sorted(self.srtt))
            out = (out + "\n" if out else "") + f"link delay (RTT/2): {links}"
        return out

    def link_delay(self, neighbor):
        """Estimated one-way delay to a neighbour, in ms."""
        rtt = self.srtt.get(neighbor)
        return DEFAULT_LINK_MS if rtt is None else rtt / 2

    def link_cost(self, neighbor):
        if self.metric == "hops":
            return 1
        return HOP_PENALTY_MS + self.link_delay(neighbor)

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
            if kind == "PROBE":
                self._ctrl(sender, "PROBE_REPLY", t=m["t"])
                return
            if kind == "PROBE_REPLY":
                self._on_probe_reply(sender, float(m["t"]))
                return
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
        self.srtt.pop(node_id, None)
        for s in self.t.remover_vizinho(node_id):
            self._reconcile(s)

    def tick(self):
        now = self.now()
        if self.metric == "delay" and now - self._last_probe >= self.probe_period:
            self._last_probe = now
            for n in self._neighbors():           # our own clock is echoed back: no sync needed
                self._ctrl(n, "PROBE", t=round(now * 1000, 3))
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

    def _on_probe_reply(self, sender, sent_ms):
        sample = self.now() * 1000 - sent_ms
        if sample < -1:                           # not a timestamp of ours: ignore
            return
        sample = max(sample, 0.0)                 # rounding of the timestamp can give -0.0
        old = self.srtt.get(sender)
        self.srtt[sender] = sample if old is None else (1 - RTT_ALPHA) * old + RTT_ALPHA * sample

    def _on_announce(self, sender, m):
        before = self.t.obter(m["stream"])
        old_origin = before.origem if before else None
        forward, cost = self.t.anuncio(m["stream"], m["server"], sender,
                                       int(m["metric"]) if self.metric == "hops" else float(m["metric"]),
                                       int(m["seq"]),
                                       custo_link=self.link_cost(sender))
        if cost is not None:
            cost = int(cost) if self.metric == "hops" else round(cost, 3)
        if forward:
            self._flood({"type": "ANNOUNCE", "from": self.id, "stream": m["stream"],
                         "server": m["server"], "metric": cost, "seq": m["seq"]},
                        except_=sender)
        new_origin = self.t.obter(m["stream"]).origem
        if new_origin != old_origin:
            self.log.info("%s: best route now via %s (metric %s %s)", m["stream"], new_origin,
                          cost, "hops" if self.metric == "hops" else "ms")
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
