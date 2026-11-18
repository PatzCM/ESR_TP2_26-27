"""
client.py - Streaming client: receives packets, rebuilds frames, plays them.

HOW IT WORKS
------------
 network --> [receiver thread] --> Reassembler --> buffer --> [window] shows
                                    (glues pieces)   (queue)    one frame every
                                                                 1/fps seconds

The small buffer (a few frames) absorbs irregular arrival times, so the
video plays at a steady pace instead of jumping (checklist Fase 7).

EXAMPLES
--------
  python3 client.py                         # window, listen on port 25000
  python3 client.py --server S1 --stream 2  # only play stream 2 of S1
  python3 client.py --no-gui                # just print what arrives (Fase 6 test)
"""

import argparse
import io
import queue
import sys
import time

from packet import VideoPacket, Reassembler
from transport import UdpTransport

DEFAULT_PORT = 25000


class Receiver:
    """Turns raw bytes into complete frames for the chosen stream."""

    def __init__(self, want_server, want_stream, buffer_size):
        self.want_server = want_server
        self.want_stream = want_stream
        self.reassemblers = {}                     # (server, stream) -> Reassembler
        self.frames = queue.Queue(maxsize=buffer_size * 4)
        self.ignored = 0

    def on_data(self, raw):
        pkt = VideoPacket.decode(raw)
        if pkt is None:
            self.ignored += 1
            return
        if self.want_server and pkt.server_id != self.want_server:
            return
        if self.want_stream and pkt.stream_id != self.want_stream:
            return
        r = self.reassemblers.setdefault(pkt.stream_key(), Reassembler())
        done = r.add(pkt)
        if done:
            seq, jpeg = done
            delay_ms = int(time.time() * 1000) - pkt.timestamp_ms
            try:
                self.frames.put_nowait((pkt.stream_key(), seq, jpeg, delay_ms))
            except queue.Full:                      # player too slow: drop oldest
                try:
                    self.frames.get_nowait()
                except queue.Empty:
                    pass
                self.frames.put_nowait((pkt.stream_key(), seq, jpeg, delay_ms))

    def stats_text(self):
        parts = []
        for (srv, st), r in self.reassemblers.items():
            s = r.stats
            parts.append("%s/%d  frames %d  lost %d  late %d  pkts %d"
                         % (srv, st, s["frames"], s["missing"], s["late"], s["packets"]))
        return " | ".join(parts) or "waiting for packets..."


def run_headless(receiver):
    """No window: print each frame that arrives (useful inside CORE without display)."""
    print("Listening... (Ctrl+C to stop)")
    try:
        while True:
            try:
                key, seq, jpeg, delay = receiver.frames.get(timeout=2)
                print("%s/%d frame %d  %d bytes  delay %d ms"
                      % (key[0], key[1], seq, len(jpeg), delay))
            except queue.Empty:
                print(receiver.stats_text())
    except KeyboardInterrupt:
        print("\n" + receiver.stats_text())


def run_gui(receiver, fps, buffer_size, title):
    from tkinter import Tk, Label, Button
    from PIL import Image, ImageTk

    root = Tk()
    root.title(title)
    video = Label(root, text="waiting for video...", width=48, height=18)
    video.grid(row=0, column=0, columnspan=2, padx=5, pady=5)
    info = Label(root, text="", anchor="w", font=("monospace", 9))
    info.grid(row=1, column=0, sticky="we", padx=5)
    Button(root, text="Quit", command=root.destroy).grid(row=1, column=1, padx=5, pady=5)

    state = {"started": False}
    interval_ms = int(1000 / fps)

    def tick():
        # wait until a few frames are buffered before starting playback
        if not state["started"]:
            if receiver.frames.qsize() >= buffer_size:
                state["started"] = True
        if state["started"]:
            try:
                key, seq, jpeg, delay = receiver.frames.get_nowait()
                try:
                    img = ImageTk.PhotoImage(Image.open(io.BytesIO(jpeg)))
                    video.configure(image=img, width=img.width(), height=img.height())
                    video.image = img        # keep a reference or Tk forgets it
                except Exception:
                    pass                     # damaged JPEG: skip this frame
                info.configure(text="frame %d  delay %d ms  buffer %d   %s"
                               % (seq, delay, receiver.frames.qsize(),
                                  receiver.stats_text()))
            except queue.Empty:
                state["started"] = False     # buffer ran dry: re-buffer
        root.after(interval_ms, tick)

    root.after(interval_ms, tick)
    root.mainloop()


def main():
    ap = argparse.ArgumentParser(description="OTT streaming client")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT, help="UDP port to listen on")
    ap.add_argument("--server", default=None, help="only play this server (e.g. S1)")
    ap.add_argument("--stream", type=int, default=None, help="only play this stream id")
    ap.add_argument("--fps", type=float, default=20, help="playback rate (default 20)")
    ap.add_argument("--buffer", type=int, default=5, help="frames to buffer before playing")
    ap.add_argument("--no-gui", action="store_true", help="print frames instead of showing them")
    args = ap.parse_args()

    if not args.no_gui and args.stream is None:
        print("Tip: with several streams arriving, choose one with --stream N", file=sys.stderr)

    receiver = Receiver(args.server, args.stream, args.buffer)
    transport = UdpTransport(listen_port=args.port)
    transport.start_receiving(receiver.on_data)
    print("Client listening on UDP port %d" % args.port)

    try:
        if args.no_gui:
            run_headless(receiver)
        else:
            title = "OTT client - %s stream %s" % (args.server or "any", args.stream or "any")
            run_gui(receiver, args.fps, args.buffer, title)
    finally:
        transport.close()


if __name__ == "__main__":
    main()
