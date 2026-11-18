"""
packet.py - Format of a video packet, and how frames are split / rebuilt.

WHY WE SPLIT FRAMES
-------------------
A frame of movie.Mjpeg is 5 000 - 14 000 bytes, but one network packet can
only carry about 1 300 bytes safely. So each frame is cut into pieces
("fragments"), each piece is sent in its own packet, and the client glues the
pieces back together.

PACKET FORMAT  (42-byte header + data)
--------------------------------------
 offset size  field         meaning
   0     4    magic         always b"OTTV" -> "this is one of our video packets"
   4     1    version       format version (1)
   5     1    type          1 = video data
   6    16    server_id     who sent it: the server's IP, e.g. "10.0.0.20"
                            (16 chars fits any IPv4 address; padded with zeros)
  22     2    stream_id     which video of that server, e.g. 1 or 2
  24     4    seq           frame number (1, 2, 3, ...)
  28     2    frag_index    which piece of the frame this is (0, 1, 2, ...)
  30     2    frag_count    how many pieces this frame has in total
  32     8    timestamp_ms  when the server sent it (milliseconds)
  40     2    payload_len   how many bytes of data follow
  42     ...  payload       a piece of the JPEG picture

(Checklist Fase 4: server id, stream id, sequence number, data,
 split when needed, rebuild at the destination.)
"""

import struct
import time

MAGIC = b"OTTV"
VERSION = 1
TYPE_VIDEO = 1

HEADER_FORMAT = "!4sBB16sHIHHQH"           # "!" = network byte order
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 42
MAX_PAYLOAD = 1300   # bytes of video per packet. 1300 + our 42 + router "DATA|..." ~32
                     # + UDP/IP 28 stays under the 1500-byte limit of a link


class VideoPacket:
    def __init__(self, server_id, stream_id, seq, frag_index, frag_count,
                 payload, timestamp_ms=None):
        self.server_id = server_id
        self.stream_id = stream_id
        self.seq = seq
        self.frag_index = frag_index
        self.frag_count = frag_count
        self.payload = payload
        self.timestamp_ms = (int(time.time() * 1000)
                             if timestamp_ms is None else timestamp_ms)

    # -- turn the object into bytes (to send) ------------------------------
    def encode(self):
        sid = self.server_id.encode("ascii")
        if len(sid) > 16:
            raise ValueError("server_id too long (max 16 chars): %r" % self.server_id)
        header = struct.pack(HEADER_FORMAT, MAGIC, VERSION, TYPE_VIDEO, sid,
                             self.stream_id, self.seq, self.frag_index,
                             self.frag_count, self.timestamp_ms,
                             len(self.payload))
        return header + self.payload

    # -- turn received bytes back into an object ---------------------------
    @staticmethod
    def decode(raw):
        """Return a VideoPacket, or None if the bytes are not one of ours."""
        if len(raw) < HEADER_SIZE:
            return None
        (magic, version, ptype, sid, stream_id, seq, frag_index, frag_count,
         ts, plen) = struct.unpack(HEADER_FORMAT, raw[:HEADER_SIZE])
        if magic != MAGIC or version != VERSION or ptype != TYPE_VIDEO:
            return None
        payload = raw[HEADER_SIZE:HEADER_SIZE + plen]
        if len(payload) != plen:
            return None
        return VideoPacket(sid.rstrip(b"\x00").decode("ascii"), stream_id,
                           seq, frag_index, frag_count, payload, ts)

    def stream_key(self):
        """(server, stream) pair that identifies one video flow."""
        return (self.server_id, self.stream_id)

    def __repr__(self):
        return ("VideoPacket(%s/%d seq=%d frag=%d/%d %dB)"
                % (self.server_id, self.stream_id, self.seq,
                   self.frag_index + 1, self.frag_count, len(self.payload)))


def split_frame(server_id, stream_id, frame):
    """Cut one Frame into a list of VideoPackets."""
    data = frame.data
    pieces = [data[i:i + MAX_PAYLOAD] for i in range(0, len(data), MAX_PAYLOAD)]
    if not pieces:
        pieces = [b""]
    ts = int(time.time() * 1000)
    return [VideoPacket(server_id, stream_id, frame.number, i, len(pieces), p, ts)
            for i, p in enumerate(pieces)]


class Reassembler:
    """
    Collects pieces and hands back complete frames.

    - Pieces can arrive in any order: they are stored until all are present.
    - If a frame never completes (a piece was lost), it is thrown away once
      we are `max_pending` frames past it.
    - Frames older than the last one delivered are "late" and discarded.
    Keeps statistics so we can show losses in the client / report.
    """

    def __init__(self, max_pending=10):
        self.max_pending = max_pending
        self.pending = {}            # seq -> {frag_index: payload}
        self.frag_counts = {}        # seq -> frag_count
        self.last_delivered = 0      # highest seq handed out
        self.stats = {"packets": 0, "frames": 0, "late": 0,
                      "incomplete": 0, "missing": 0, "out_of_order": 0}
        self._highest_seen = 0

    def add(self, pkt):
        """Give it a packet. Returns (seq, jpeg_bytes) when a frame is complete, else None."""
        self.stats["packets"] += 1

        if pkt.seq < self._highest_seen:
            self.stats["out_of_order"] += 1
        self._highest_seen = max(self._highest_seen, pkt.seq)

        if pkt.seq <= self.last_delivered:
            self.stats["late"] += 1
            return None

        parts = self.pending.setdefault(pkt.seq, {})
        parts[pkt.frag_index] = pkt.payload
        self.frag_counts[pkt.seq] = pkt.frag_count

        self._drop_old(pkt.seq)

        if len(parts) == pkt.frag_count:
            data = b"".join(parts[i] for i in range(pkt.frag_count))
            del self.pending[pkt.seq]
            del self.frag_counts[pkt.seq]
            # frames between the last one and this one never arrived
            if self.last_delivered and pkt.seq > self.last_delivered + 1:
                self.stats["missing"] += pkt.seq - self.last_delivered - 1
            self.last_delivered = pkt.seq
            # anything older than this frame can never be shown now
            for old in [s for s in self.pending if s < pkt.seq]:
                del self.pending[old]
                del self.frag_counts[old]
                self.stats["incomplete"] += 1
            self.stats["frames"] += 1
            return (pkt.seq, data)
        return None

    def _drop_old(self, newest):
        for s in [s for s in self.pending if s < newest - self.max_pending]:
            del self.pending[s]
            del self.frag_counts[s]
            self.stats["incomplete"] += 1
