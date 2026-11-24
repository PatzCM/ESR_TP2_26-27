# Interfaces: Overlay ↔ Routing ↔ Streaming

This file describes how the three modules talk to each other. If you change a message or a shared function, update this file in the same commit.

Based on João's first version (`INTERFACES.md` on the `Sousa` branch).

| Module | Owner | Folder |
|---|---|---|
| 1. Overlay: neighbours, bootstrapper | Pedro | `Mixed_testing/` |
| 2. Routing: routes, forwarding, replication | João | `routing/` |
| 3. Streaming: video server and client | Patz | `streaming/` |

## 1. Decisions

| Topic | Decision |
|---|---|
| Language | **English** for everything shared: message types, field names, shared function names. |
| Transport | **UDP**, one socket per node, port **5000** by default. |
| Node ID | A **name** from the config file, e.g. `Server1`, `Node1`, `Client1`. Not an IP. Letters and digits only (no `|`, no `/`). |
| Addresses | Only in Pedro's config file (the address book). The rest of the code says "send to `Node1`", and the overlay turns that into an IP and port. |
| Sender | Every message says who sent it in the `from` field. Never use the source address from `recvfrom` as the sender, because CORE routers have several interfaces and so several IPs. |
| Stream ID | `"<server name>/<video name>"`, e.g. `Server1/video1`. No `|`. |

## 2. Message format

### Control messages
One JSON object per UDP datagram. **Every message has `type` and `from`.**

**Overlay (Pedro)**

| type | Direction | Fields |
|---|---|---|
| `REGISTER` | node → bootstrapper | `type, from, port`. Also sent periodically as "I'm alive". |
| `NEIGHBORS` | bootstrapper → node | `type, from, reason, neighbors, min_degree` |
| `NEED_NEIGHBOR` | node → bootstrapper | `type, from, exclude` (list of node IDs it already has) |
| `ERROR` | bootstrapper → node | `type, from, reason` |
| `HELLO` | node ↔ neighbour | `type, from, port` (keep-alive) |
| `LEAVE` | node → all | `type, from` |
| `MSG` | node → neighbours | `type, from, text` (testing only) |

- `reason` in `NEIGHBORS` is `"register"` (answer to REGISTER) or `"replacement"` (answer to NEED_NEIGHBOR).
- Each entry of `neighbors` is `{"id": "Node2", "ip": "10.0.1.2", "port": 5000}`. Here `id` means "this neighbour's name", not the sender.

**Routing (João)**

| type | Direction | Fields |
|---|---|---|
| `ANNOUNCE` | server → flood | `type, from, stream, server, delay, hops, path, seq` |
| `ACTIVATE` | client → towards server | `type, from, stream` |
| `DEACTIVATE` | client → towards server | `type, from, stream` |
| `PING` | server → along active routes | `type, from, stream, path` (list of node IDs it went through) |
| `PROBE` | node → each neighbour | `type, from, t` (sender's own clock, ms) |
| `PROBE_REPLY` | neighbour → node | `type, from, t` (the same `t`, echoed back) |

- Route metric (agreed with the teacher): **1st criterion `delay`** (ms from the server, lower is better), **2nd criterion `hops`** (tie-break only). If both are equal, either route is fine (the current one is kept). Two delays count as equal when they differ by at most 2 ms or 10%. The delay of a link is half the smoothed RTT of `PROBE`/`PROBE_REPLY`.
- `path` is the list of node IDs the ANNOUNCE went through, starting at the server. A node ignores an ANNOUNCE whose `path` already contains it (that route would be a loop).

Examples:
```json
{"type": "REGISTER", "from": "Node1", "port": 5000}
{"type": "ANNOUNCE", "from": "Node1", "stream": "Server1/video1", "server": "Server1", "delay": 2.31, "hops": 1, "path": ["Server1", "Node1"], "seq": 17}
{"type": "ACTIVATE", "from": "Client1", "stream": "Server1/video1"}
```

### Data (video) packets
Not JSON. Bytes:
```
DATA|<stream>|<from>|<payload>
e.g. b"DATA|Server1/video1|Node1|" + <video packet bytes>
```
The router never looks inside `payload`. The streaming module puts its video packet there (see section 5).

### How a node sorts what it receives
```
datagram starts with b"DATA|"                     -> router.on_receive(data)
JSON with type in router.ROUTING_TYPES            -> router.on_receive(data)
  (ANNOUNCE, ACTIVATE, DEACTIVATE, PING, PROBE, PROBE_REPLY)
any other JSON type                               -> overlay handlers (Pedro)
```
Receive buffer: at least **2048 bytes** (recommended 65535). A video datagram is about 1.4 KB.

## 3. Shared functions

### Overlay → Routing (Pedro gives these to João's `Router`)
| Function | What it does |
|---|---|
| `send(to: str, data: bytes)` | Send to the neighbour with ID `to`. The overlay finds its IP and port. |
| `neighbors() -> list[str]` | IDs of the neighbours that are currently alive (not `down`). |

### Routing ← Overlay (Pedro calls these on the `Router`)
| Function | When |
|---|---|
| `router.on_receive(data: bytes)` | For every routing or data datagram received (see the sorting rules above). |
| `router.on_neighbor_down(id: str)` | When a neighbour stops answering HELLO, or sends LEAVE. |
| `router.tick()` | About once per second. |

### Routing → Streaming (Patz uses these)
| Function | Who | What it does |
|---|---|---|
| `router.send_data(stream, payload: bytes)` | server | Inject one video packet into the overlay. |
| `router.request_stream(stream)` | client | "I want to receive this video." |
| `router.stop_stream(stream)` | client | "Stop sending it to me." |
| `Router(..., on_data=fn)` → `fn(stream, payload)` | client | Called for every video packet that reaches this node. |
| `router.dump() -> str` | any | Routing table as text (console `routes` command). |

Constructor: `Router(node_id, send, neighbors, server_streams=[...], on_data=fn)`.

### One lock per node
The `Router` is **not thread-safe**: it can't safely handle two calls at the same moment. Each node program creates **one** `threading.RLock()` and holds it around **every** call to the router: the receive thread, the `tick()` thread, and the streaming server's sending threads.

## 4. Timers

| Timer | Value | Module |
|---|---|---|
| HELLO interval | 2 s | overlay |
| Neighbour considered down after no HELLO | 6 s | overlay |
| Re-register with bootstrapper | 5 s | overlay |
| Bootstrapper forgets a node after no REGISTER | 15 s | overlay |
| REGISTER attempts at start-up | 3 × 2 s, then use the manual neighbours | overlay |
| Server ANNOUNCE | 5 s | routing |
| Link delay PROBE | 1 s | routing |
| ACTIVATE refresh | 2 s | routing |
| Destination expires without refresh | 6 s | routing |
| Route expires without ANNOUNCE | 15 s | routing |
| Video frame rate | 20 frames/s | streaming |

## 5. Video packet (inside `payload`)
Only the streaming client reads this. The router treats it as opaque bytes.

42-byte header followed by at most 1300 bytes of JPEG data. Numbers are in network byte order.

| Bytes | Field | Notes |
|---|---|---|
| 4 | magic | `OTTV` |
| 1 | version | 1 |
| 1 | type | 1 = video |
| 16 | server_id | server name, zero-padded |
| 2 | stream_id | video number on that server |
| 4 | seq | frame number |
| 2 | frag_index | piece of the frame (0, 1, …) |
| 2 | frag_count | pieces in this frame |
| 8 | timestamp_ms | send time, used to measure delay |
| 2 | payload_len | ≤ 1300 |

Size check: 1300 + 42 + about 32 (`DATA|…|…|`) + 28 (UDP/IP headers) ≈ 1400 bytes, under the 1500-byte limit of a link, so the network never has to split the packet. Details are in `streaming/README.md`.

## 6. What each module has to change

| Who | Change |
|---|---|
| **Pedro** | Rename `id` → `from` and `porta` → `port`, `motivo` → `reason`, `vizinhos` → `neighbors`, `excluir` → `exclude`, `texto` → `text`. Value `"substituto"` → `"replacement"`. Add `from` to NEIGHBORS and ERROR. Pass `DATA|…` and the routing message types to the router instead of rejecting them. Provide `send()` and `neighbors()`, and call `on_neighbor_down()` when a neighbour goes down. Merge `cenario1.json` and `cenario2.json` into `main`. |
| **João** | ✅ Wire fields renamed (`type`, `from`, `server`, `path`). ✅ Metric: delay first, hops as tie-break (`delay`, `hops`, `path` in ANNOUNCE). ✅ Shared functions renamed as in section 3 (`ott.py` and `OverlayTransport` updated). ✅ Names instead of IPs in the tests and in `no_ott_router.py`. ✅ `Sousa` merged into `main`. |
| **Patz** | Nothing to rename. ✅ `OverlayTransport` added to `streaming/transport.py`. ✅ `ott.py` combines the three modules. ✅ Router calls in `ott.py` and `OverlayTransport` updated (by João). Update the overlay calls once Pedro's renames are in. |

## 7. Running the combined node: `ott.py`

`ott.py` (repo root) is the program every overlay machine runs. It implements sections 2 and 3: one receive loop that sorts datagrams, `send()`/`neighbors()` on top of Pedro's neighbour table, a `tick()` thread that also calls `on_neighbor_down()`, and the streaming server/client on top of the router. It uses the English router names; Pedro's overlay code still has its pre-rename names.

```bash
# Cenário 1 in CORE (names and IPs from Mixed_testing/cenario1.json)
python3 ott.py --id Server  --config Mixed_testing/cenario1.json --serve video1=streaming/videos/movie.Mjpeg
python3 ott.py --id Node    --bootstrapper 10.0.0.20:5000
python3 ott.py --id Client1 --bootstrapper 10.0.0.20:5000 --neighbors Node=10.0.0.1:5000 --watch Server/video1
```
`python3 ott.py --help` lists every option. Nodes that aren't playing video get a console: `neighbors`, `routes`, `ping`, `quit`.

**Test on one machine, no CORE needed** (about 35 s): `python3 tests/test_integration_local.py`. It starts 6 nodes, checks both clients receive 20 frames/s, kills Node2, and checks Client1 gets the video back through Node3. The video freezes about 5 s after a node dies, because a neighbour is only declared down after 6 s without HELLO.
