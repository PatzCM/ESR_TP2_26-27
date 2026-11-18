"""
server.py - Streaming server: reads videos and sends them as packets.

One server process can offer several streams (the statement asks for
2 servers x 2 streams each). Each stream runs in its own thread and sends
`fps` frames per second.

EXAMPLES
--------
  # one stream, to a client on the same machine
  python3 server.py --id S1 --to 127.0.0.1:25000

  # two streams, looping forever, to two clients
  python3 server.py --id S1 --loop \
      --stream 1:videos/movie.Mjpeg --stream 2:videos/movie.Mjpeg \
      --to 10.0.0.20:25000 --to 10.0.1.20:25000

(Checklist Fase 5: test packet, send frames, sending interval, logs.)
"""

import argparse
import os
import threading
import time

from video_stream import VideoStream
from packet import split_frame
from transport import UdpTransport, parse_address

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_VIDEO = os.path.join(HERE, "videos", "movie.Mjpeg")
DEFAULT_PORT = 25000


class StreamSender(threading.Thread):
    """Sends one video stream, frame after frame, at a fixed rate."""

    def __init__(self, server_id, stream_id, video_path, transport, fps,
                 loop, verbose):
        super().__init__(daemon=True)
        self.server_id = server_id
        self.stream_id = stream_id
        self.video = VideoStream(video_path, loop=loop)
        self.transport = transport
        self.interval = 1.0 / fps
        self.verbose = verbose
        self.stop_event = threading.Event()
        self.frames_sent = 0
        self.packets_sent = 0

    def run(self):
        tag = "[%s/%d]" % (self.server_id, self.stream_id)
        print("%s started (%.0f fps)" % (tag, 1 / self.interval))
        next_time = time.monotonic()
        while not self.stop_event.is_set():
            frame = self.video.next_frame()
            if frame is None:
                print("%s video finished" % tag)
                break
            packets = split_frame(self.server_id, self.stream_id, frame)
            for p in packets:
                self.transport.send(p.encode())
            self.frames_sent += 1
            self.packets_sent += len(packets)
            if self.verbose:
                print("%s sent frame %d (%d bytes in %d packets)"
                      % (tag, frame.number, len(frame.data), len(packets)))
            elif self.frames_sent % 100 == 0:
                print("%s %d frames sent" % (tag, self.frames_sent))

            # wait until it is time for the next frame (keeps a steady rate)
            next_time += self.interval
            delay = next_time - time.monotonic()
            if delay > 0:
                self.stop_event.wait(delay)
            else:
                next_time = time.monotonic()   # we are late; don't try to catch up
        self.video.close()


def parse_stream(text):
    """'2:videos/movie.Mjpeg' -> (2, 'videos/movie.Mjpeg')"""
    sid, path = text.split(":", 1)
    return int(sid), path


def main():
    ap = argparse.ArgumentParser(description="OTT streaming server")
    ap.add_argument("--id", default="S1", help="server ID, max 16 chars; use the server IP, e.g. 10.0.0.20 (default S1)")
    ap.add_argument("--stream", action="append", type=parse_stream,
                    metavar="ID:FILE", help="stream to offer (repeatable)")
    ap.add_argument("--to", action="append", metavar="IP[:PORT]",
                    help="where to send (repeatable). Test mode only.")
    ap.add_argument("--fps", type=float, default=20, help="frames per second (default 20)")
    ap.add_argument("--loop", action="store_true", help="restart the video when it ends")
    ap.add_argument("-v", "--verbose", action="store_true", help="log every frame")
    args = ap.parse_args()

    streams = args.stream or [(1, DEFAULT_VIDEO)]
    dests = [parse_address(d, DEFAULT_PORT) for d in (args.to or ["127.0.0.1"])]
    transport = UdpTransport(destinations=dests)
    print("Server %s -> %s" % (args.id, ", ".join("%s:%d" % d for d in dests)))

    senders = [StreamSender(args.id, sid, path, transport, args.fps,
                            args.loop, args.verbose) for sid, path in streams]
    for s in senders:
        s.start()
    try:
        while any(s.is_alive() for s in senders):
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping...")
        for s in senders:
            s.stop_event.set()
    for s in senders:
        s.join(timeout=2)
        print("[%s/%d] total: %d frames, %d packets"
              % (s.server_id, s.stream_id, s.frames_sent, s.packets_sent))
    transport.close()


if __name__ == "__main__":
    main()
