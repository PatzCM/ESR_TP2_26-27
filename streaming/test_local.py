"""
test_local.py - Checks the module without any network (checklist Fase 3/4).

  python3 test_local.py

1. Reads every frame of the video.
2. Splits each frame into packets, encodes them to bytes and decodes them back.
3. Rebuilds the frames and checks they are byte-for-byte identical.
4. Repeats with packets shuffled (out of order) and with some packets lost.
"""

import os
import random

from video_stream import VideoStream
from packet import split_frame, VideoPacket, Reassembler, HEADER_SIZE, MAX_PAYLOAD

VIDEO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "videos", "movie.Mjpeg")


def load_frames():
    vs = VideoStream(VIDEO)
    frames = []
    while True:
        f = vs.next_frame()
        if f is None:
            break
        frames.append(f)
    vs.close()
    return frames


def wire(frames):
    """All packets of all frames, as bytes, in sending order."""
    out = []
    for f in frames:
        out.append([p.encode() for p in split_frame("S1", 1, f)])
    return out


def test_perfect(frames):
    r = Reassembler()
    rebuilt = {}
    for pkts in wire(frames):
        for raw in pkts:
            done = r.add(VideoPacket.decode(raw))
            if done:
                rebuilt[done[0]] = done[1]
    assert len(rebuilt) == len(frames), "some frames missing"
    for f in frames:
        assert rebuilt[f.number] == f.data, "frame %d differs" % f.number
    print("OK  perfect network: %d/%d frames identical" % (len(rebuilt), len(frames)))


def test_shuffled_inside_frame(frames):
    r = Reassembler()
    count = 0
    for pkts in wire(frames):
        random.shuffle(pkts)                    # pieces of a frame in random order
        for raw in pkts:
            if r.add(VideoPacket.decode(raw)):
                count += 1
    assert count == len(frames)
    print("OK  pieces out of order: %d/%d frames rebuilt" % (count, len(frames)))


def test_losses(frames, loss=0.02):
    random.seed(1)
    r = Reassembler()
    count = 0
    sent = 0
    for pkts in wire(frames):
        for raw in pkts:
            sent += 1
            if random.random() < loss:
                continue                        # this packet got "lost"
            if r.add(VideoPacket.decode(raw)):
                count += 1
    s = r.stats
    assert s["frames"] == count and 0 < count < len(frames)
    print("OK  %.0f%% packet loss: %d frames shown, %d lost (of %d), %d packets sent"
          % (loss * 100, count, len(frames) - count, len(frames), sent))


def test_garbage():
    assert VideoPacket.decode(b"HELLO") is None
    assert VideoPacket.decode(b"X" * 100) is None
    print("OK  non-video messages are ignored")


if __name__ == "__main__":
    frames = load_frames()
    sizes = [len(f.data) for f in frames]
    print("Video: %d frames, %d-%d bytes; header %d B, max payload %d B"
          % (len(frames), min(sizes), max(sizes), HEADER_SIZE, MAX_PAYLOAD))
    test_perfect(frames)
    test_shuffled_inside_frame(frames)
    test_losses(frames)
    test_garbage()
    print("All tests passed.")
