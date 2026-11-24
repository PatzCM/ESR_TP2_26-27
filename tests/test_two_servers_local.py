#!/usr/bin/env python3
"""2 servers x 2 streams, end to end, on ONE machine (no CORE needed).

A small copy of the final scenario: two servers, each offering two videos,
and four clients that each watch a DIFFERENT stream.

        S1 ---- A ------ B ---- C1  (watches S1/video1)
                |\\      /|\\
                | \\    / | \\--- C2 (watches S1/video2)
                |  \\  /  |
        S2 ---- C ------ +      C1 and C2 also hang off A / C
                |  \\
               C3   C4          C3 watches S2/video1, C4 watches S2/video2

After 15 s, B is killed (no LEAVE, like a crash). C1 and C2 must get their
video back through the other paths.

Checks: every client plays its own stream, never a stream it didn't ask for,
and C1/C2 recover after B dies.

    python3 tests/test_two_servers_local.py     (takes about 35 s)
"""
import json
import os
import re
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE = 5200
NAMES = ["S1", "S2", "A", "B", "C", "C1", "C2", "C3", "C4"]
PORT = {n: BASE + i for i, n in enumerate(NAMES, start=1)}
LINKS = {"S1": ["A"], "S2": ["C"],
         "A": ["B", "C", "C2"], "B": ["C", "C1", "C2"],
         "C": ["C1", "C3", "C4"]}
VIDEOS = os.path.join(ROOT, "streaming", "videos")
SERVE = {"S1": ["video1=" + os.path.join(VIDEOS, "movie.Mjpeg"),
                "video2=" + os.path.join(VIDEOS, "movie2.Mjpeg")],
         "S2": ["video1=" + os.path.join(VIDEOS, "movie2.Mjpeg"),
                "video2=" + os.path.join(VIDEOS, "movie.Mjpeg")]}
WATCH = {"C1": "S1/video1", "C2": "S1/video2", "C3": "S2/video1", "C4": "S2/video2"}
# ott.py numbers a server's streams in name order: video1 -> 1, video2 -> 2,
# and the client prints them as "<server>/<number> frame ..."
EXPECTED = {c: "%s/%s" % (w.split("/")[0], w[-1]) for c, w in WATCH.items()}
KILL, KILL_AT, END_AT = "B", 15, 32
FRAME_LINE = re.compile(r"^(\S+/\d+) frame ")


def count(path):
    """{"S1/1": frames, ...} seen in a client's log so far."""
    seen = {}
    with open(path, errors="replace") as f:
        for line in f:
            m = FRAME_LINE.match(line)
            if m:
                seen[m.group(1)] = seen.get(m.group(1), 0) + 1
    return seen


def main():
    if not os.path.isfile(os.path.join(VIDEOS, "movie2.Mjpeg")):
        sys.exit("streaming/videos/movie2.Mjpeg missing: run python3 tools/make_video2.py")
    tmp = tempfile.mkdtemp(prefix="ott-2x2-")
    topo = os.path.join(tmp, "topo.json")
    with open(topo, "w") as f:
        json.dump({"min_degree": 1, "links": LINKS,
                   "nodes": {n: {"ip": "127.0.0.1", "port": p} for n, p in PORT.items()}}, f)
    boot = "127.0.0.1:%d" % PORT["S1"]

    procs, logs = {}, {}
    for n in NAMES:
        cmd = [sys.executable, "-u", os.path.join(ROOT, "ott.py"), "--id", n, "--port", str(PORT[n])]
        cmd += ["--config", topo] if n == "S1" else ["--bootstrapper", boot]
        for item in SERVE.get(n, []):
            cmd += ["--serve", item]
        if n in WATCH:
            cmd += ["--watch", WATCH[n], "--no-gui"]
        logs[n] = os.path.join(tmp, n + ".log")
        procs[n] = subprocess.Popen(cmd, stdout=open(logs[n], "w"), stderr=subprocess.STDOUT,
                                    stdin=subprocess.DEVNULL)
        if n == "S1":
            time.sleep(0.5)                         # bootstrapper first

    t0 = time.time()
    snap = {}
    try:
        def at(t):
            time.sleep(max(0, t0 + t - time.time()))
            return {c: count(logs[c]) for c in WATCH}
        snap[5] = at(5)
        snap[KILL_AT] = at(KILL_AT)
        procs[KILL].kill()
        snap[KILL_AT + 10] = at(KILL_AT + 10)
        snap[END_AT] = at(END_AT)
    finally:
        for p in procs.values():
            p.terminate()
        for p in procs.values():
            try:
                p.wait(3)
            except subprocess.TimeoutExpired:
                p.kill()

    def got(c, a, b):
        return snap[b][c].get(EXPECTED[c], 0) - snap[a][c].get(EXPECTED[c], 0)

    print("client  watches     frames %2d-%2ds  frames %2d-%2ds (after %s died)  streams seen"
          % (5, KILL_AT, KILL_AT + 10, END_AT, KILL))
    for c in WATCH:
        print("%-6s  %-10s  %13d  %30d  %s" % (c, WATCH[c], got(c, 5, KILL_AT),
                                              got(c, KILL_AT + 10, END_AT),
                                              ", ".join(sorted(snap[END_AT][c])) or "-"))

    fps_ok = 20 * 0.7                               # allow 30% below 20 fps
    window = END_AT - KILL_AT - 10
    checks = {
        "every client plays its own stream":
            all(got(c, 5, KILL_AT) > fps_ok * (KILL_AT - 5) for c in WATCH),
        "no client receives a stream it did not ask for":
            all(set(snap[END_AT][c]) <= {EXPECTED[c]} for c in WATCH),
        "all clients play after %s dies (C1, C2 rerouted)" % KILL:
            all(got(c, KILL_AT + 10, END_AT) > fps_ok * window for c in WATCH),
    }
    for name, ok in checks.items():
        print("%s  %s" % ("OK  " if ok else "FAIL", name))
    print("logs: %s" % tmp)
    sys.exit(0 if all(checks.values()) else 1)


if __name__ == "__main__":
    main()
