"""
transport.py - The "plug" between the streaming module and the network.

WHY THIS FILE EXISTS
--------------------
The streaming server/client should not care HOW bytes travel. Today, while
Pedro (overlay) and Joao (routing) are still building their modules, we send
packets directly over UDP to test. Later, we swap in the overlay without
touching server.py or client.py.

Anything that implements these two methods works as a transport:

    send(data: bytes)                 -> push one packet into the network
    start_receiving(callback)         -> call callback(data: bytes) for
                                         every packet that arrives

HOW TO PLUG IN THE OVERLAY LATER
--------------------------------
Write a class OverlayTransport with the same two methods:
  - send(data)  -> hand the packet to Joao's forwarding function, which
                   replicates it to the right neighbours (Pedro's links).
  - start_receiving(callback) -> register `callback` with the local ott node
                   so it is called when a video packet for this client arrives.
Agree the exact function names with Pedro and Joao (checklist "Integracao").
"""

import socket
import threading


class UdpTransport:
    """Direct UDP: server -> one or more addresses. Only for testing."""

    def __init__(self, listen_port=None, destinations=None, bind_addr="0.0.0.0"):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if listen_port is not None:
            self.sock.bind((bind_addr, listen_port))
        self.destinations = destinations or []   # list of (ip, port)
        self._running = False

    def send(self, data):
        for dest in self.destinations:
            self.sock.sendto(data, dest)

    def start_receiving(self, callback):
        self._running = True
        self.sock.settimeout(0.5)

        def loop():
            while self._running:
                try:
                    data, _addr = self.sock.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    break
                callback(data)

        t = threading.Thread(target=loop, daemon=True)
        t.start()
        return t

    def close(self):
        self._running = False
        try:
            self.sock.close()
        except OSError:
            pass


class OverlayTransport:
    """Sends one stream's packets through the overlay (Joao's Router).

    Used by ott.py on a server node: StreamSender calls send(data) and the
    router replicates the packet along the active routes. Receiving on a
    client node doesn't need this class: ott.py passes the router's on_data
    callback straight to client.Receiver.on_data.

    `lock` must be the same lock the node uses for every router call
    (the Router is not thread-safe, see INTERFACES.md section 3).
    """

    def __init__(self, router, lock, stream):
        self.router = router
        self.lock = lock
        self.stream = stream      # e.g. "Server1/video1"

    def send(self, data):
        with self.lock:
            self.router.send_data(self.stream, data)

    def close(self):
        pass


def parse_address(text, default_port):
    """'10.0.0.2:25000' -> ('10.0.0.2', 25000);  '10.0.0.2' -> ('10.0.0.2', default)."""
    if ":" in text:
        host, port = text.rsplit(":", 1)
        return (host, int(port))
    return (text, default_port)
