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
- **Stream:** one video flow, identified by *server name + stream number* (for example `S1/2`).

## Files

| File | What it does | Checklist |
|---|---|---|
| `video_stream.py` | Opens the `.Mjpeg` file and returns one frame at a time | Fase 3 |
| `packet.py` | Packet format; splits frames and rebuilds them (`Reassembler`) | Fase 4 |
| `transport.py` | The "plug" to the network. Today plain UDP, later the overlay | Fase 5/8 |
| `server.py` | Sends one or more streams at 20 frames/s | Fase 5 |
| `client.py` | Receives, buffers, and shows the video (or prints it with `--no-gui`) | Fase 6/7 |
| `test_local.py` | Tests everything without a network (shuffled pieces, 2% losses) | Fase 3/4 |

## How to run

```bash
cd streaming
python3 test_local.py                       # 1. offline tests

# 2. same machine, two terminals
python3 client.py --stream 1                # terminal A (window)
python3 server.py --id S1                   # terminal B

# 3. two streams, client picks one
python3 server.py --id S1 --loop --stream 1:videos/movie.Mjpeg --stream 2:videos/movie.Mjpeg
python3 client.py --server S1 --stream 2

# 4. in CORE: server on one node, client on another (use the client's IP)
python3 server.py --id S1 --loop --to 10.0.0.20      # on the server node
python3 client.py --no-gui                           # on the client node
```

Options: `--fps` (speed), `--buffer` (frames to wait for before playing), `-v` (log every frame), `--port` (default 25000).

## Packet format (for the report and for João)

42-byte header followed by the video data (all numbers in network byte order):

| Bytes | Field | Example |
|---|---|---|
| 4 | magic `OTTV` | marks "this is a video packet" |
| 1 | version | 1 |
| 1 | type | 1 = video |
| 16 | server_id | `10.0.0.20` (server IP, as in `INTERFACES.md`) |
| 2 | stream_id | 2 |
| 4 | seq (frame number) | 137 |
| 2 | frag_index | 0, 1, 2… |
| 2 | frag_count | 6 |
| 8 | timestamp (ms) | used to measure delay |
| 2 | payload_len | ≤ 1300 |

João's router **never opens this packet**. It treats the whole thing as an opaque `payload` and wraps it in its own envelope, `DATA|<stream>|<from>|<payload>` (see `INTERFACES.md` on the `Sousa` branch). Our header is only read by our client.

## What to agree with Pedro and João

1. **Language:** this code is Python 3, the same as the teachers' example (ProgEx). Confirm it with the group.
2. **Transport:** UDP for video. For a live video it's better to lose a frame than to wait for it.
3. **The plug** (`transport.py`): the streaming code only needs two functions from the overlay:
   - `send(data)`: the server hands a packet to the overlay, which forwards it along the route (João) and over the neighbour connections (Pedro).
   - `start_receiving(callback)`: the client asks the local node to call `callback(data)` for each video packet that arrives.
   Make an `OverlayTransport` class with these two methods. `server.py` and `client.py` stay the same.
4. **Choosing a stream:** the client asks for `(server_id, stream_id)`, for example `S1/2`. João's route activation message should use the same two identifiers.

## What's done / what's left

- ✅ Fases 1–7: reading, packets, splitting/rebuilding, sending, receiving, losses/out-of-order detection, playback with buffer.
- ⏳ Fase 8: replace `UdpTransport` with `OverlayTransport` once Pedro's and João's modules are ready; test in the 3 CORE scenarios.
