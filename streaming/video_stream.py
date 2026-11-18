"""
video_stream.py - Reads a .Mjpeg video file one frame at a time.

HOW THE .Mjpeg FILE IS ORGANISED
--------------------------------
An MJPEG video is just a sequence of JPEG pictures glued together.
Before each picture there are 5 ASCII digits saying how big the picture is:

    "06014" <6014 bytes of JPEG>  "05873" <5873 bytes of JPEG>  ...

So reading a frame = read 5 bytes -> turn into a number N -> read N bytes.

(Checklist Fase 3: open file, check it exists, read incrementally,
 find frame boundaries, data structure per frame, test without sending.)
"""

import os


class Frame:
    """One picture of the video."""

    def __init__(self, number, data):
        self.number = number   # 1, 2, 3, ... (keeps growing when the video loops)
        self.data = data       # the raw JPEG bytes

    def __repr__(self):
        return "Frame(#%d, %d bytes)" % (self.number, len(self.data))


class VideoStream:
    HEADER_LEN = 5  # the 5 digits before each frame

    def __init__(self, filename, loop=False):
        if not os.path.isfile(filename):
            raise FileNotFoundError("Video file not found: %s" % filename)
        if not os.access(filename, os.R_OK):
            raise PermissionError("Video file cannot be read: %s" % filename)
        self.filename = filename
        self.loop = loop          # start again from the beginning when it ends
        self.file = open(filename, "rb")
        self.frame_count = 0      # how many frames were handed out so far

    def next_frame(self):
        """Return the next Frame, or None when the video is over (and loop=False)."""
        header = self.file.read(self.HEADER_LEN)

        if len(header) < self.HEADER_LEN:          # reached the end of the file
            if not self.loop:
                return None
            self.file.seek(0)                       # rewind and try again
            header = self.file.read(self.HEADER_LEN)
            if len(header) < self.HEADER_LEN:
                return None                         # empty file

        try:
            size = int(header)
        except ValueError:
            raise ValueError("Corrupted video file: bad frame header %r" % header)

        data = self.file.read(size)
        if len(data) < size:
            raise ValueError("Corrupted video file: frame cut short")

        self.frame_count += 1
        return Frame(self.frame_count, data)

    def close(self):
        self.file.close()


if __name__ == "__main__":
    # Quick test: read the whole video without sending anything.
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "videos", "movie.Mjpeg")
    vs = VideoStream(path)
    sizes = []
    while True:
        f = vs.next_frame()
        if f is None:
            break
        sizes.append(len(f.data))
    vs.close()
    print("Read %d frames. Smallest %d bytes, largest %d bytes."
          % (len(sizes), min(sizes), max(sizes)))
