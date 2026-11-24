#!/usr/bin/env python3
"""make_video2.py - Build a second test video from movie.Mjpeg.

The final scenario needs 2 servers x 2 streams, and a demo is only convincing
if you can SEE that two clients are watching different videos. This script
writes movie2.Mjpeg: the same frames, mirrored, in grey, with a "VIDEO 2"
label, in the same format (5 ASCII digits = JPEG size, then the JPEG).

    python3 tools/make_video2.py                       (needs Pillow)
    python3 tools/make_video2.py IN.Mjpeg OUT.Mjpeg
"""
import io
import os
import sys

from PIL import Image, ImageDraw, ImageOps

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VIDEOS = os.path.join(ROOT, "streaming", "videos")


def frames(path):
    with open(path, "rb") as f:
        while True:
            header = f.read(5)
            if len(header) < 5:
                return
            yield f.read(int(header))


def convert(jpeg):
    img = ImageOps.mirror(ImageOps.grayscale(Image.open(io.BytesIO(jpeg)))).convert("RGB")
    draw = ImageDraw.Draw(img)
    draw.rectangle((4, 4, 80, 20), fill=(200, 0, 0))
    draw.text((10, 7), "VIDEO 2", fill=(255, 255, 255))
    out = io.BytesIO()
    img.save(out, "JPEG", quality=80)
    return out.getvalue()


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else os.path.join(VIDEOS, "movie.Mjpeg")
    dst = sys.argv[2] if len(sys.argv) > 2 else os.path.join(VIDEOS, "movie2.Mjpeg")
    n = 0
    with open(dst, "wb") as out:
        for jpeg in frames(src):
            data = convert(jpeg)
            if len(data) > 99999:
                raise ValueError("frame %d too big for the 5-digit header" % (n + 1))
            out.write(b"%05d" % len(data) + data)
            n += 1
    print("wrote %d frames to %s (%d bytes)" % (n, dst, os.path.getsize(dst)))


if __name__ == "__main__":
    main()
