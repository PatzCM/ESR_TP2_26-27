# Módulo 3: Streaming (Patz)

The server reads a video, cuts it into packets, and sends them. The client receives the packets, glues them back into pictures, and shows them in a window.

## The big picture

```
 SERVER                                               CLIENT
 movie.Mjpeg                                          window
    │ video_stream.py: read 1 picture (frame)            ▲
    ▼                                                    │ client.py: show 20 per second
 frame (~8 KB)                                        frame
    │ packet.py: cut into ~1.3 KB pieces                 ▲ packet.py: glue the pieces back
    ▼                                                    │
 packets ── transport.py ──► network / overlay ──► transport.py
```

- **Frame:** one picture. The video is 500 pictures shown one after another.
- **Packet:** one small envelope sent over the network. A frame is too big for one envelope, so it's split into about 4 to 10 of them.
- **Sequence number (seq):** the frame number written on every envelope. It lets the client put pieces in order, notice missing frames, and throw away late ones.
- **Stream:** one video flow, identified by *server node ID + video name*, for example `n16/video2`. The router uses that name. Inside our packets the same stream is `server_id` + a number (`n16` + `2`), because the videos of a server are numbered in name order: `video1` → 1, `video2` → 2.

## Files

| File | What it does | Checklist |
|---|---|---|
| `video_stream.py` | Opens the `.Mjpeg` file and returns one frame at a time | Fase 3 |
| `packet.py` | Packet format; splits frames and rebuilds them (`Reassembler`) | Fase 4 |
| `transport.py` | The "plug" to the network: `UdpTransport` (tests) and `OverlayTransport` (through João's router) | Fase 5/8 |
| `server.py` | Sends one or more streams at 20 frames/s | Fase 5 |
| `client.py` | Receives, buffers, and shows the video (or prints it with `--no-gui`) | Fase 6/7 |
| `test_local.py` | Tests everything without a network (shuffled pieces, 2% losses) | Fase 3/4 |
| `videos/movie.Mjpeg` | The teachers' video (500 frames) | |
| `videos/movie2.Mjpeg` | Same clip mirrored, grey, with a red "VIDEO 2" label, so two streams look different. Rebuild with `python3 tools/make_video2.py` | |

Outside this folder: `ott.py` (one program per node: overlay + routing + streaming), `tests/test_two_servers_local.py` and `tools/core_final_2x2.sh` (see below).

## How to run

### Streaming alone (no overlay)

```bash
cd streaming
python3 test_local.py                       # 1. offline tests

# 2. same machine, two terminals (direct UDP, no overlay)
python3 client.py --stream 1                # terminal A (window)
python3 server.py --id S1                   # terminal B

# 3. two streams, client picks one
python3 server.py --id S1 --loop --stream 1:videos/movie.Mjpeg --stream 2:videos/movie2.Mjpeg
python3 client.py --server S1 --stream 2
```

### Through the overlay (`ott.py`, from the `Codebase` folder)

```bash
# server: --serve NAME=FILE, repeatable -> streams <id>/<NAME>
python3 ott.py --id n16 --config Mixed_testing/cenarioF.json \
    --serve video1=streaming/videos/movie.Mjpeg --serve video2=streaming/videos/movie2.Mjpeg
# client: --watch <server id>/<video name>   (add --no-gui inside CORE without a display)
python3 ott.py --id n17 --bootstrapper 10.1.5.20:5000 --watch n16/video1
```

### 2 servers × 2 streams

Local, one machine (9 nodes on 127.0.0.1, ~35 s). Each of 4 clients watches a different stream, then a node is killed and the video must come back:

```bash
python3 tests/test_two_servers_local.py
```

Result on 24/11/2026: all 4 clients at 20 frames/s, each received only its own stream, all recovered after the node died.

In CORE, final scenario (start `topology/Cenario_final.xml`, then on the host, as root):

```bash
sudo bash tools/core_final_2x2.sh test        # start all, measure, kill n9, time the outage, stop
sudo bash tools/core_final_2x2.sh test n5     # same, killing another node
sudo bash tools/core_final_2x2.sh start | status | kill nX | stop    # by hand
```

Roles used by the script (still to be confirmed by the group):

| Node | Role |
|---|---|
| n16 | S1 + bootstrapper: `n16/video1` = movie, `n16/video2` = movie2 |
| n20 | S2: `n20/video1` = movie2, `n20/video2` = movie |
| n17, n23 | watch `n16/video1` (same stream twice: tests replication) |
| n18 / n19 / n22 | watch `n16/video2` / `n20/video1` / `n20/video2` |
| all others | relay |

⚠ `core_final_2x2.sh` has not run in CORE yet (only a dry run). Logs go to `tools/logs_2x2/`.

Options of `server.py` / `client.py`: `--fps` (speed), `--buffer` (frames to wait for before playing), `-v` (log every frame), `--port` (default 25000).

## Packet format (for the report and for João)

42-byte header followed by the video data (all numbers in network byte order):

| Bytes | Field | Example |
|---|---|---|
| 4 | magic `OTTV` | marks "this is a video packet" |
| 1 | version | 1 |
| 1 | type | 1 = video |
| 16 | server_id | `n16` (server node ID, zero-padded, as in `INTERFACES.md`) |
| 2 | stream_id | 2 |
| 4 | seq (frame number) | 137 |
| 2 | frag_index | 0, 1, 2… |
| 2 | frag_count | 6 |
| 8 | timestamp (ms) | used to measure delay |
| 2 | payload_len | ≤ 1300 |

João's router **never opens this packet**. It treats the whole thing as an opaque `payload` and wraps it in its own envelope, `DATA|<stream>|<from>|<payload>` (see `INTERFACES.md`). Our header is only read by our client.

## How it plugs into the overlay

- **Transport:** UDP for video. For a live video it's better to lose a frame than to wait for it.
- **Server side:** `ott.py` gives each `StreamSender` an `OverlayTransport`; its `send(data)` calls the router's `enviar_dados(stream, data)` (`send_data` after the rename), holding the node's lock (the router is not thread-safe).
- **Client side:** `ott.py` asks the router for the stream (`pedir_stream`) and passes every packet the router delivers (`on_dados`) to `client.Receiver.on_data`.
- **Choosing a stream:** the stream name is `<server id>/<video name>`, the same string in `--watch`, in the router's ANNOUNCE/ACTIVATE and in `routes`.

## What's done / what's left

- ✅ Fases 1–7: reading, packets, splitting/rebuilding, sending, receiving, losses/out-of-order detection, playback with buffer.
- ✅ Fase 8: sending through the overlay (`OverlayTransport`, `ott.py`); 6-node local test with failover.
- ✅ 2 servers × 2 streams working locally (`tests/test_two_servers_local.py`).
- ⏳ Run `tools/core_final_2x2.sh` in CORE and keep the numbers for the report.
- ⏳ Client log of frames/s, losses and delay every 1 s (for the report).
