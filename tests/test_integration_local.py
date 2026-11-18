#!/usr/bin/env python3
"""End-to-end test of ott.py on ONE machine (no CORE needed).

Starts 6 ott.py nodes on 127.0.0.1 (different ports), based on Cenario 2 plus
one extra link Node3-Client1 so Client1 has a second path:

    Server - Node1 - Node2 - Client1
               \\     |    /
                 - Node3 - Client2

Both clients watch Server/video1. After 15 s Node2 is killed (no LEAVE, like
a crash); Client1 must get the video back through Node3.

    python3 tests/test_integration_local.py      (takes about 35 s)
"""
import json
import os
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE = 5100
NAMES = ["Server", "Node1", "Node2", "Node3", "Client1", "Client2"]
PORT = {n: BASE + i for i, n in enumerate(NAMES, start=1)}
LINKS = {"Server": ["Node1"], "Node1": ["Node2", "Node3"],
         "Node2": ["Node3", "Client1"], "Node3": ["Client2", "Client1"]}
KILL_AT, END_AT = 15, 32


def frames(path):
    with open(path, errors="replace") as f:
        return sum(1 for line in f if " frame " in line)


def main():
    tmp = tempfile.mkdtemp(prefix="ott-test-")
    topo = os.path.join(tmp, "topo.json")
    with open(topo, "w") as f:
        json.dump({"min_degree": 1, "links": LINKS,
                   "nodes": {n: {"ip": "127.0.0.1", "port": p} for n, p in PORT.items()}}, f)
    video = os.path.join(ROOT, "streaming", "videos", "movie.Mjpeg")
    boot = "127.0.0.1:%d" % PORT["Server"]

    procs, logs = {}, {}
    for n in NAMES:
        cmd = [sys.executable, "-u", os.path.join(ROOT, "ott.py"), "--id", n, "--port", str(PORT[n])]
        if n == "Server":
            cmd += ["--config", topo, "--serve", "video1=" + video]
        else:
            cmd += ["--bootstrapper", boot]
        if n.startswith("Client"):
            cmd += ["--watch", "Server/video1", "--no-gui"]
        logs[n] = os.path.join(tmp, n + ".log")
        procs[n] = subprocess.Popen(cmd, stdout=open(logs[n], "w"), stderr=subprocess.STDOUT,
                                    stdin=subprocess.DEVNULL)
        if n == "Server":
            time.sleep(0.5)

    t0 = time.time()
    counts = {}
    try:
        def at(t):
            time.sleep(max(0, t0 + t - time.time()))
            return {c: frames(logs[c]) for c in ("Client1", "Client2")}
        counts[5] = at(5)
        counts[KILL_AT] = at(KILL_AT)
        procs["Node2"].kill()
        counts[KILL_AT + 10] = at(KILL_AT + 10)
        counts[END_AT] = at(END_AT)
    finally:
        for p in procs.values():
            p.terminate()
        for p in procs.values():
            try:
                p.wait(3)
            except subprocess.TimeoutExpired:
                p.kill()

    window = END_AT - KILL_AT - 10
    print("frames received      Client1  Client2")
    print("  %2d-%2ds (normal)    %6d   %6d" % (5, KILL_AT, counts[KILL_AT]["Client1"] - counts[5]["Client1"],
                                               counts[KILL_AT]["Client2"] - counts[5]["Client2"]))
    print("  %2d-%2ds (recovered) %6d   %6d" % (KILL_AT + 10, END_AT,
          counts[END_AT]["Client1"] - counts[KILL_AT + 10]["Client1"],
          counts[END_AT]["Client2"] - counts[KILL_AT + 10]["Client2"]))

    fps_ok = 20 * 0.7                                     # allow 30% below 20 fps
    checks = {
        "both clients receive video":
            all(counts[KILL_AT][c] - counts[5][c] > fps_ok * (KILL_AT - 5) for c in ("Client1", "Client2")),
        "Client1 recovers through Node3 after Node2 dies":
            counts[END_AT]["Client1"] - counts[KILL_AT + 10]["Client1"] > fps_ok * window,
        "Client2 keeps playing":
            counts[END_AT]["Client2"] - counts[KILL_AT + 10]["Client2"] > fps_ok * window,
    }
    for name, ok in checks.items():
        print("%s  %s" % ("OK  " if ok else "FAIL", name))
    print("logs: %s" % tmp)
    sys.exit(0 if all(checks.values()) else 1)


if __name__ == "__main__":
    main()
