#!/usr/bin/env python3
"""Minimal Snapcast server using plain synchronous sockets.

Implements the Snapcast "SnapStream" binary wire protocol (the framing used
by snapcast >= 0.28, i.e. without protobuf and without encryption) and serves
raw PCM samples.  No audio codec is involved: the stream is advertised with
the "pcm" encoder, exactly like winsnapserver's built-in pcm stream reader does
(codec header = 44 byte WAV header).

Two sources are available:

* live capture (the default) - capture what this machine is playing right now
  from its playback device, through the Windows WASAPI loopback endpoint.
  That needs the WASAPI-patched PyAudio fork, "pyaudiowpatch" (the only pip
  dependency, and only used in capture mode).  One capture stream fans its
  blocks out to every client, so all clients hear the same moment of audio.
* a WAV file, looped forever (--wav FILE).

Live capture runs the Windows device callback in its own thread (Windows hands
out loopback packets from a callback, and a blocking read on an idle endpoint
never returns); the sockets themselves stay plain and synchronous.  When an
endpoint delivers nothing, the server sends silence instead of stalling, so
the client's playout timeline never breaks.

It also provides the JSON-RPC control interface, on the two dialects real
control clients use:

* port 1705 - plain TCP, one JSON-RPC object per line ("\\r\\n" terminated).
  This is the "control port" the Android snapcast app (snapdroid) asks for.
* port 1780 - HTTP POST /jsonrpc plus a minimal WebSocket upgrade at
  /jsonrpc, for snapweb-style clients.

The status JSON carries both the modern (flat clients/streams) and the older
nested server/groups/streams shape, because the Android app parses the
latter.

Usage:
    python winsnapserver.py                          # stream this machine's audio
    python winsnapserver.py --list-capture-devices   # see the playback devices
    python winsnapserver.py --capture-device 16      # capture that device
    python winsnapserver.py --wav song.wav           # loop a file instead
    python winsnapserver.py --silence                # serve silence
    python winsnapserver.py --wav song.wav --chunk-ms 40 --port 1704

Requires Python 3.8+ (stdlib only, plus pyaudiowpatch for live capture).
"""

import argparse
import array
import base64
import collections
import hashlib
import json
import platform
import socket
import struct
import sys
import threading
import time
import traceback
import wave

try:
    # Only needed for live capture: the WASAPI-patched PyAudio fork, whose
    # devices include the loopback endpoint of each playback device.
    import pyaudiowpatch as pyaudio
except ImportError:
    pyaudio = None

# --------------------------------------------------------------------------
# SnapStream wire protocol constants (see snapcast develop:
# common/message/message.hpp, codec_header.hpp, wire_chunk.hpp, time.hpp,
# server_settings.hpp, server/encoder/pcm_encoder.cpp)
# --------------------------------------------------------------------------

MSG_BASE = 0
MSG_CODEC_HEADER = 1
MSG_WIRE_CHUNK = 2
MSG_SERVER_SETTINGS = 3
MSG_TIME = 4
MSG_HELLO = 5
MSG_CODEC_REQUEST = 6
MSG_CLIENT_INFO = 7
MSG_ERROR = 8

MSG_NAMES = {MSG_BASE: "Base", MSG_CODEC_HEADER: "CodecHeader",
             MSG_WIRE_CHUNK: "WireChunk", MSG_SERVER_SETTINGS: "ServerSettings",
             MSG_TIME: "Time", MSG_HELLO: "Hello",
             MSG_CODEC_REQUEST: "CodecRequest", MSG_CLIENT_INFO: "ClientInfo",
             MSG_ERROR: "Error"}

# Enabled by --verbose.  Logs every wire frame in both directions, which is
# what you need to debug a client that reconnects in a loop: snapclient sends
# each request with an id and matches the answer by refersTo, so a reply that
# does not carry the right refersTo makes the client sit in its 2 s timeout,
# print "Operation canceled" and reconnect.
VERBOSE = False


def _now_str():
    t = time.time()
    return "%s.%03d" % (time.strftime("%H:%M:%S", time.localtime(t)),
                        int((t - int(t)) * 1000))


def log_frame(direction, peer, msg_type, msg_id, refers_to, size):
    if VERBOSE:
        print("%s %s %s %s: id %d, refersTo %d, size %d" %
              (_now_str(), peer, direction, MSG_NAMES.get(msg_type, "type?%d" % msg_type),
               msg_id, refers_to, size))


def log_line(text):
    """one status line, with a timestamp when --verbose is on"""
    print("%s %s" % (_now_str(), text) if VERBOSE else text)

# type u16, id u16, refersTo u16, sent sec/usec i32/i32,
# received sec/usec i32/i32, payload size u32   (little endian, 26 bytes;
# matches BaseMessage::getSize() = 3*sizeof(u16) + 2*sizeof(tv) + sizeof(u32))
HEADER_FMT = "<HHHIIIII"
HEADER_SIZE = struct.calcsize(HEADER_FMT)
assert HEADER_SIZE == 26

MAX_MSG_SIZE = 1000000  # same limit as winsnapserver


class StreamGone(Exception):
    """peer closed / broke the connection"""


def now_tv():
    """wall clock as (sec, usec), same idea as snapcast's steadytimeofday()."""
    t = time.time()
    sec = int(t)
    usec = int(round((t - sec) * 1000000))
    if usec >= 1000000:
        sec, usec = sec + 1, usec - 1000000
    return sec, usec


def pack_header(msg_type, msg_id, refers_to, payload_len, sent=None, received=None):
    ss, us = sent if sent else now_tv()
    rs, us2 = received if received else (0, 0)
    return struct.pack(HEADER_FMT, msg_type, msg_id & 0xFFFF, refers_to & 0xFFFF,
                       ss, us, rs, us2, payload_len)


def pack_json_message(msg_type, msg_id, obj, refers_to=0):
    data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    payload = struct.pack("<I", len(data)) + data
    return pack_header(msg_type, msg_id, refers_to, len(payload)) + payload


def recv_exactly(conn, n):
    """Read exactly n bytes or raise StreamGone."""
    if n <= 0:
        return b""
    chunks = []
    remaining = n
    while remaining:
        try:
            part = conn.recv(min(remaining, 65536))
        except OSError:
            raise StreamGone()
        if not part:
            raise StreamGone()
        chunks.append(part)
        remaining -= len(part)
    return b"".join(chunks)


def read_message(conn):
    """Return (msg_type, msg_id, refers_to, sent, received, payload)."""
    header = recv_exactly(conn, HEADER_SIZE)
    mtype, mid, refers, ss, us, rs, us2, size = struct.unpack(HEADER_FMT, header)
    if size > MAX_MSG_SIZE:
        raise StreamGone()
    payload = recv_exactly(conn, size)
    return mtype, mid, refers, (ss, us), (rs, us2), payload


# --------------------------------------------------------------------------
# PCM sources: a looped WAV file, or live capture from a playback device
# --------------------------------------------------------------------------

def build_wav_header(rate, channels, bits):
    # byte-for-byte what winsnapserver's PcmEncoder::initEncoder() emits
    byte_rate = rate * bits * channels // 8
    block_align = channels * ((bits + 7) // 8)
    return (b"RIFF" + struct.pack("<I", 36) + b"WAVE" + b"fmt " +
            struct.pack("<IHHIIHH", 16, 1, channels, rate, byte_rate,
                        block_align, bits) +
            b"data" + struct.pack("<I", 0))


class FileCursor:
    """One client's read position inside the looped WAV samples."""

    def __init__(self, stream):
        self.stream = stream
        self.pos = 0

    def take(self, nbytes, timeout):
        """Return (data, lost_bytes, starved) - a file never starves."""
        samples = self.stream.samples
        total = len(samples)
        data = samples[self.pos:self.pos + nbytes]
        if len(data) < nbytes:  # wrap around -> infinite loop
            data += samples[:nbytes - len(data)]
        self.pos = (self.pos + nbytes) % total
        return data, 0, False


class PcmStream:
    """Raw PCM samples from a WAV file, plus the snapcast 'pcm' codec header."""

    live = False

    def __init__(self, rate, channels, bits, samples):
        if not (1 <= channels <= 8 and 8 <= bits <= 32 and 1 <= rate <= 192000):
            raise ValueError("implausible sample format %d/%d bit/%d Hz"
                             % (rate, channels, bits))
        self.rate = rate
        self.channels = channels
        self.bits = bits
        self.frame_bytes = channels * (bits // 8)
        self.samples = samples  # concatenated raw little endian frames
        if len(samples) == 0:
            raise ValueError("stream has no samples")
        self.codec_header = self._build_wav_header()
        self.uri_name = "wav"

    def _build_wav_header(self):
        return build_wav_header(self.rate, self.channels, self.bits)

    def open_cursor(self):
        return FileCursor(self)

    @classmethod
    def from_wave(cls, path):
        with wave.open(path, "rb") as w:
            if w.getcomptype() != "wpcm" and w.getcomptype() != "NONE":
                raise ValueError("%s: unsupported WAV coding %r (only PCM)"
                                 % (path, w.getcomptype()))
            fmt = w.getparams()
            frames = w.readframes(fmt.nframes)
        # snapcast sampleformat string, e.g. "44100:16:2"
        print("stream: %s  sampleformat %d:%d:%d, %d frames"
              % (path, fmt.framerate, fmt.sampwidth * 8, fmt.nchannels, fmt.nframes))
        return cls(fmt.framerate, fmt.nchannels, fmt.sampwidth * 8, frames)

    @classmethod
    def silence(cls, rate=44100, channels=2, bits=16):
        frame = b"\x00" * (channels * bits // 8)
        return cls(rate, channels, bits, frame * rate)  # 1 s loop

    def duration_ms(self):
        return len(self.samples) * 1000.0 // (self.rate * self.frame_bytes)


# --------------------------------------------------------------------------
# Live capture: the sound this machine is playing right now
# --------------------------------------------------------------------------

def float_to_int16(data):
    """Fallback conversion, only used if the device refuses 16 bit capture."""
    src = array.array("f")
    src.frombytes(data)
    dst = array.array("h", (int(max(-1.0, min(1.0, x)) * 32767.0) for x in src))
    return dst.tobytes()


def list_capture_devices():
    """Print the loopback endpoints Windows offers, one per playback device."""
    if pyaudio is None:
        print("live capture needs the WASAPI patched PyAudio:\n"
              "    python -m pip install pyaudiowpatch")
        return []
    pa = pyaudio.PyAudio()
    try:
        out = None
        try:
            out = pa.get_default_output_device_info()
            print("default playback device: %r  %d Hz, %d ch (host api %d)"
                  % (out["name"], out["defaultSampleRate"], out["maxOutputChannels"],
                     out["hostApi"]))
        except (IOError, OSError, ValueError):
            print("no default playback device visible")
        devices = []
        try:
            default = pa.get_default_wasapi_loopback()
        except (IOError, OSError, ValueError):
            default = None
        for d in pa.get_loopback_device_info_generator():
            devices.append(d)
            mark = "  <- captured by default" if default and d["index"] == default["index"] else ""
            print("  device %-3d %-52r %6d Hz  %d ch%s"
                  % (d["index"], d["name"], d["defaultSampleRate"],
                     d["maxInputChannels"], mark))
        return devices
    finally:
        pa.terminate()


def select_capture_device(spec):
    """Pick the loopback endpoint to capture: None = the default one.

    Returns (device_info, PyAudio instance); the instance stays open because
    the capture stream is created from it.  On a bad selection this exits
    before any port is bound.
    """
    if pyaudio is None:
        raise SystemExit("live capture needs the WASAPI patched PyAudio:\n"
                         "    python -m pip install pyaudiowpatch\n"
                         "or pick a file source instead:  --wav FILE")
    pa = pyaudio.PyAudio()
    devices = list(pa.get_loopback_device_info_generator())
    if spec is None:
        try:
            return pa.get_default_wasapi_loopback(), pa
        except (IOError, OSError, ValueError):
            if devices:
                return devices[0], pa
            pa.terminate()
            raise SystemExit("no WASAPI loopback endpoint found "
                             "(is a playback device connected?)")
    text = str(spec)
    if text.isdigit():
        for d in devices:
            if d["index"] == int(text):
                return d, pa
        pa.terminate()
        raise SystemExit("no loopback device with index %s "
                         "(see --list-capture-devices)" % text)
    hits = [d for d in devices if text.lower() in d["name"].lower()]
    if len(hits) == 1:
        return hits[0], pa
    pa.terminate()
    if not hits:
        raise SystemExit("no loopback device matching %r "
                         "(see --list-capture-devices)" % text)
    raise SystemExit("%r matches several devices: %s"
                     % (text, ", ".join("%d %s" % (d["index"], d["name"]) for d in hits)))


class LoopCursor:
    """One client's position in the captured ring: a live stream, never a loop."""

    def __init__(self, source):
        self.source = source
        with source._cond:
            self.pos = source._written          # join at the live edge
        self.lost_reported = 0
        self.lost_total = 0

    def take(self, nbytes, timeout):
        """Return (data, bytes_lost_by_resync, starved).

        Resync: the client fell so far behind that its audio left the retained
        ring, so it resumes at the oldest sample still held.  Starved: the
        device delivered nothing within timeout, so the caller gets silence -
        the playout timeline must keep moving either way.
        """
        src = self.source
        deadline = time.time() + timeout
        while True:
            with src._cond:
                if src._blocks:
                    oldest = src._blocks[0][0]
                    if self.pos < oldest:
                        self.lost_total += oldest - self.pos
                        self.pos = oldest
                if self.pos + nbytes <= src._written:
                    data = src._slice(self.pos, nbytes)
                    self.pos += nbytes
                    lost = self.lost_total - self.lost_reported
                    self.lost_reported = self.lost_total
                    return data, lost, False
                remaining = deadline - time.time()
                if remaining <= 0:
                    lost = self.lost_total - self.lost_reported
                    self.lost_reported = self.lost_total
                    return b"\x00" * nbytes, lost, True
                src._cond.wait(remaining)


class LoopStream:
    """PCM captured live from a playback device's WASAPI loopback endpoint.

    Windows hands out the loopback packets from a device callback, so that
    callback only appends blocks to one ring buffer; every client reads the
    same ring through its own cursor, and the client threads do all the socket
    work synchronously.  The ring keeps keep_ms of audio, which both bounds the
    memory and decides when a client that fell behind has to resync.
    """

    live = True

    def __init__(self, device, pa, frames_per_chunk, keep_ms=4000):
        self.device = device
        self.name = device["name"]
        self.index = int(device["index"])
        self.rate = int(round(device["defaultSampleRate"]))
        self.channels = max(1, int(device["maxInputChannels"]))
        self.bits = 16
        self.frame_bytes = self.channels * (self.bits // 8)
        self.codec_header = build_wav_header(self.rate, self.channels, self.bits)
        self.uri_name = "loopback"
        self.frames_per_chunk = max(1, frames_per_chunk)
        self.keep_bytes = max(self.keep_min(), int(self.rate * self.frame_bytes * keep_ms / 1000.0))
        self._blocks = collections.deque()   # (absolute byte offset, data)
        self._written = 0                    # absolute byte offset of the write head
        self._cond = threading.Condition()
        self._converted = 0
        self._pa = pa
        self._stream = None
        self._open()

    def keep_min(self):
        # never trim so aggressively that a client's own chunk cannot be served
        return 64 * self.frames_per_chunk * self.frame_bytes

    def _open(self):
        fmt = pyaudio.paInt16
        self._convert = False
        try:
            self._stream = self._pa.open(
                format=fmt, channels=self.channels, rate=self.rate, input=True,
                input_device_index=self.index, frames_per_buffer=self.frames_per_chunk,
                stream_callback=self._callback)
        except (IOError, OSError):
            # some endpoints only hand out their native float mix format
            self._stream = self._pa.open(
                format=pyaudio.paFloat32, channels=self.channels, rate=self.rate,
                input=True, input_device_index=self.index,
                frames_per_buffer=self.frames_per_chunk, stream_callback=self._callback)
            self._convert = True
        self._stream.start_stream()

    def _callback(self, in_data, frame_count, time_info, status_flags):
        """Runs on the Windows audio thread: buffer the block, nothing else."""
        try:
            if self._convert:
                in_data = float_to_int16(in_data)
                self._converted += 1
            with self._cond:
                self._blocks.append((self._written, in_data))
                self._written += len(in_data)
                limit = self._written - self.keep_bytes
                while len(self._blocks) > 1 and self._blocks[0][0] + len(self._blocks[0][1]) < limit:
                    self._blocks.popleft()
                self._cond.notify_all()
        except Exception:
            # never let an exception escape into PortAudio: it would kill the
            # stream silently and the clients would only see silence
            traceback.print_exc()
        return (None, pyaudio.paContinue)

    def _slice(self, pos, nbytes):
        """Copy [pos, pos+nbytes) out of the ring.  Caller holds _cond."""
        out = bytearray()
        end = pos + nbytes
        for start, block in self._blocks:
            if start >= end:
                break
            a = max(pos, start)
            b = min(end, start + len(block))
            if b > a:
                out += block[a - start:b - start]
        return bytes(out)

    def open_cursor(self):
        return LoopCursor(self)

    def stats(self):
        with self._cond:
            buffered = self._written
        return buffered

    def close(self):
        try:
            self._stream.stop_stream()
            self._stream.close()
        except (IOError, OSError, AttributeError):
            pass


# --------------------------------------------------------------------------
# Stream server (TCP, one thread per client)
# --------------------------------------------------------------------------

class StreamServer:
    def __init__(self, stream, chunk_ms=20, buffer_ms=500):
        self.stream = stream
        self.chunk_ms = chunk_ms
        self.buffer_ms = buffer_ms  # playout lead so clients are never starved
        self.clients = {}           # client_id -> info dict (for the RPC status)
        self.clients_lock = threading.Lock()
        self.on_client_event = None  # callback(kind, client_id) for RPC notifications
        self.frame_bytes = stream.frame_bytes
        self.frames_per_chunk = max(1, int(stream.rate * chunk_ms / 1000))
        self.chunk_bytes = self.frames_per_chunk * self.frame_bytes

    def serve(self, host, port):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((host, port))
        srv.listen(16)
        print("SnapStream listening on %s:%d" % (host, port))
        while True:
            conn, addr = srv.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            conn.settimeout(60)
            log_line("%s:%d: accepted (stream)" % addr)
            t = threading.Thread(target=self._client_loop, args=(conn, addr),
                                 daemon=True)
            t.start()

    def _client_loop(self, conn, addr):
        cid = "%s:%d" % addr
        state = {"id": cid, "name": "", "volume": 100, "muted": False,
                 "latency": 0, "instance": 1, "ip": addr[0], "mac": "",
                 "host_name": "", "os": "", "arch": "", "version": "",
                 "protocol": 2, "last_seen": now_tv(), "push": [], "conn": conn,
                 "rbuf": bytearray()}
        try:
            self._handshake(conn, state)
            with self.clients_lock:
                self.clients[state["id"]] = state
            # fired outside the lock: the handler reads the client table itself
            if self.on_client_event:
                self.on_client_event("Connect", state["id"])
            self._stream_loop(conn, state)
        except (StreamGone, OSError) as e:
            # StreamGone = clean EOF from the client.  WinError 10053/10054 =
            # the client (or its network stack) aborted/resetted the socket,
            # which is what snapclient does when it gives up on a request and
            # reconnects.
            log_line("%s: connection ended: %s" % (cid, e or type(e).__name__))
        except Exception as e:
            log_line("%s: server error: %s" % (cid, e))
            traceback.print_exc()
        finally:
            try:
                conn.close()
            except OSError:
                pass
            with self.clients_lock:
                self.clients.pop(state["id"], None)
            if self.on_client_event:
                self.on_client_event("Disconnect", state["id"])
            print("%s: disconnected" % state["id"])

    def _handshake(self, conn, state):
        """Wait for Hello, then answer it and send the CodecHeader.

        snapclient sends the Hello as a request (ClientConnection::sendRequest)
        and matches the answer by refersTo, so the ServerSettings reply has to
        carry the id of the Hello it answers - the real winsnapserver does exactly
        that (server.cpp: "serverSettings->refersTo = helloMsg.id").  If the
        refersTo does not match, the client never sees its answer, waits for
        the 2 s request timeout, logs "Failed to send hello request" and
        reconnects - on the client side that is exactly the
        "Error reading message header of length 0: Operation canceled" you get
        when the cancelled socket read reports back.
        """
        while True:
            mtype, mid, refers, sent, received, payload = read_message(conn)
            log_frame("<-", state["id"], mtype, mid, refers, len(payload))
            if mtype == MSG_HELLO:
                jlen = struct.unpack("<I", payload[:4])[0]
                hello = json.loads(payload[4:4 + jlen])
                cid = hello.get("ID") or hello.get("MAC") or state["id"]
                inst = int(hello.get("Instance", 1) or 1)
                if inst != 1:
                    cid = "%s#%d" % (cid, inst)
                state["id"] = cid
                state["name"] = hello.get("ClientName", "")
                state["host_name"] = hello.get("HostName", "")
                state["os"] = hello.get("OS", "")
                state["arch"] = hello.get("Arch", "")
                state["mac"] = hello.get("MAC") or cid.split("#")[0]
                state["version"] = hello.get("Version", "")
                state["instance"] = inst
                state["last_seen"] = now_tv()
                hello_id = mid
                log_line("%s: hello from %s (%s %s), id %d" %
                         (cid, hello.get("ClientName", "?"),
                          hello.get("HostName", "?"), hello.get("Version", "?"),
                          mid))
                if hello.get("SnapStreamProtocolVersion", 1) != 2:
                    print("%s: warning: protocol version %s" %
                          (cid, hello.get("SnapStreamProtocolVersion")))
                break
            elif mtype == MSG_TIME:
                self._time_reply(conn, state["id"], mid, sent)
            elif mtype in (MSG_CODEC_HEADER, MSG_CODEC_REQUEST):
                pass  # CodecHeader / CodecRequest sent by a client: ignore
            else:
                print("%s: ignoring pre-hello message type %d" % (state["id"], mtype))

        sid = next_id()
        settings = pack_json_message(MSG_SERVER_SETTINGS, sid,
                                     {"bufferMs": self.buffer_ms,
                                      "latency": 0,
                                      "volume": state["volume"],
                                      "muted": state["muted"]},
                                     refers_to=hello_id)
        conn.sendall(settings)
        log_frame("->", state["id"], MSG_SERVER_SETTINGS, sid, hello_id,
                  len(settings) - HEADER_SIZE)
        hdr = struct.pack("<I", 3) + b"pcm" + \
            struct.pack("<I", len(self.stream.codec_header)) + self.stream.codec_header
        cid_hdr = next_id()
        hdr_msg = pack_header(MSG_CODEC_HEADER, cid_hdr, 0, len(hdr)) + hdr
        conn.sendall(hdr_msg)
        log_frame("->", state["id"], MSG_CODEC_HEADER, cid_hdr, 0,
                  len(hdr_msg) - HEADER_SIZE)

    @staticmethod
    def _time_reply(conn, peer, msg_id, client_sent):
        """Answer a Time request the way winsnapserver does (server.cpp).

        latency  = our arrival time - the client's "sent" stamp (this carries
                   the clock offset between the two machines, which the client
                   needs to schedule the chunks we timestamp with OUR clock)
        refersTo = the request id, so the client's pending request matches
        id, sent, received = as in winsnapserver: id stays the request id, sent
                   is the moment we send the reply, received the arrival stamp.
        """
        arrival = now_tv()
        lat_s = arrival[0] - client_sent[0]
        lat_u = arrival[1] - client_sent[1]
        while lat_u < 0:
            lat_s, lat_u = lat_s - 1, lat_u + 1000000
        while lat_u >= 1000000:
            lat_s, lat_u = lat_s + 1, lat_u - 1000000
        payload = struct.pack("<ii", lat_s, lat_u)
        msg = pack_header(MSG_TIME, msg_id, msg_id, len(payload),
                          sent=now_tv(), received=arrival) + payload
        conn.sendall(msg)
        log_frame("->", peer, MSG_TIME, msg_id, msg_id, len(payload))
        if VERBOSE:
            print("%s %s: time sync: client sent %d.%06d, our arrival "
                  "%d.%06d, latency %d.%06d" %
                  (_now_str(), peer, client_sent[0], client_sent[1],
                   arrival[0], arrival[1], lat_s, lat_u))

    def _drain_input(self, conn, state):
        """Read every message the client has already put on the wire.

        Called with the socket in non-blocking mode, so a silent or slow client
        can never stall the audio.  Partial messages are kept in state["rbuf"]
        (a 26-byte header and its payload do not always arrive in one recv) and
        completed on the next round.  Answers are not sent here - the caller
        puts the socket back into blocking mode first, because a reply that
        would block must not be swallowed.
        """
        buf = state.get("rbuf")
        if buf is None:
            buf = state["rbuf"] = bytearray()
        pending = []
        while True:
            if len(buf) < HEADER_SIZE:
                try:
                    chunk = conn.recv(HEADER_SIZE - len(buf))
                except BlockingIOError:
                    break
                if not chunk:
                    raise StreamGone("connection closed by the client")
                buf += chunk
                continue
            mtype, mid, refers, ss, us, rs, us2, size = \
                struct.unpack(HEADER_FMT, buf[:HEADER_SIZE])
            if size > MAX_MSG_SIZE:
                raise StreamGone("oversized message (%d bytes)" % size)
            if len(buf) < HEADER_SIZE + size:
                try:
                    chunk = conn.recv(HEADER_SIZE + size - len(buf))
                except BlockingIOError:
                    break
                if not chunk:
                    raise StreamGone("connection closed by the client")
                buf += chunk
                continue
            payload = bytes(buf[HEADER_SIZE:HEADER_SIZE + size])
            del buf[:HEADER_SIZE + size]
            state["last_seen"] = now_tv()
            log_frame("<-", state["id"], mtype, mid, refers, size)
            if mtype == MSG_TIME:
                # answer with the client's own "sent" stamp, like winsnapserver
                pending.append((mid, (ss, us)))
            elif mtype in (MSG_CLIENT_INFO, MSG_HELLO):
                self._handle_json(state, mtype, payload)
            # CodecHeader / CodecRequest from a client need no answer
        return pending

    def _stream_loop(self, conn, state):
        stream = self.stream
        cursor = stream.open_cursor()
        chunk_us = int(self.frames_per_chunk * 1e6 / stream.rate)
        # Playout timestamp of the next chunk. The first one is scheduled
        # buffer_ms in the future so the client always has buffered audio.
        lead_us = self.buffer_ms * 1000
        play = _tv_add(now_tv(), 0, lead_us)
        starved = 0
        while True:
            # drain everything the client sent without blocking
            conn.setblocking(False)
            try:
                pending = self._drain_input(conn, state)
            finally:
                conn.setblocking(True)
            for mid, client_sent in pending:
                self._time_reply(conn, state["id"], mid, client_sent)

            # flush settings the control interface queued for this client
            # (Client.SetVolume / SetLatency -> ServerSettings message)
            queue = state.get("push") or []
            while queue:
                conn.sendall(queue.pop(0))

            # Next chunk of audio.  A WAV loop always has one; live capture
            # answers with silence when the device has delivered nothing, so
            # the playout timeline below keeps advancing either way.
            data, chunk_lost, silent = cursor.take(self.chunk_bytes,
                                                   2.0 * self.chunk_ms / 1000.0)
            if chunk_lost and VERBOSE:
                print("%s %s: capture fell behind, %d byte(s) dropped (resync)"
                      % (_now_str(), state["id"], chunk_lost))
            if silent:
                starved += 1
                if starved == 1 or starved % 50 == 0:
                    print("%s %s: capture idle, %d silent chunk(s) sent "
                          "(device %r delivers no packets right now)"
                          % (_now_str(), state["id"], starved, getattr(stream, "name", "")))

            sec, usec = play
            payload = struct.pack("<iiI", sec, usec, len(data)) + data
            msg_id = next_id()
            conn.sendall(pack_header(MSG_WIRE_CHUNK, msg_id, 0, len(payload)) + payload)
            state["chunks"] = state.get("chunks", 0) + 1
            if VERBOSE and (state["chunks"] == 1 or state["chunks"] % 50 == 0):
                log_frame("->", state["id"], MSG_WIRE_CHUNK, msg_id, 0, len(payload))
                print("%s %s: chunk %d, play at %d.%06d (lead %.0f ms)" %
                      (_now_str(), state["id"], state["chunks"], sec, usec,
                       (sec - now_tv()[0]) * 1000 + (usec - now_tv()[1]) / 1000.0))

            # pace: the next chunk must be on the wire lead_us before it plays
            play = _tv_add(play, 0, chunk_us)
            wait = _tv_sub(_tv_add(play, 0, -lead_us), now_tv())
            if wait > 0:
                time.sleep(wait)
            elif wait < -2 * chunk_us / 1e6:
                # fell behind (slow client, or waiting on a capture block):
                # re-anchor instead of flooding the client with backlogged
                # chunks
                play = _tv_add(now_tv(), 0, lead_us + chunk_us)

    def _handle_json(self, state, mtype, payload):
        try:
            jlen = struct.unpack("<I", payload[:4])[0]
            obj = json.loads(payload[4:4 + jlen])
        except (ValueError, struct.error):
            return
        state["last_seen"] = now_tv()
        if mtype == MSG_CLIENT_INFO and ("Volume" in obj or "volume" in obj):
            vol = obj.get("Volume")
            if isinstance(vol, dict):       # pre-0.28: {"Volume":{"Percent","Muted"}}
                state["volume"] = int(vol.get("Percent", state["volume"]))
                state["muted"] = bool(vol.get("Muted", state["muted"]))
            else:                          # 0.28+: {"volume","muted"}
                state["volume"] = int(obj.get("volume", state["volume"]))
                state["muted"] = bool(obj.get("muted", state["muted"]))
            if "latency" in obj:
                state["latency"] = int(obj.get("latency", state["latency"]))
            if self.on_client_event:
                self.on_client_event("Volume", state["id"])

    def update_client(self, client_id, **fields):
        """Change stored client settings; return the new state or None."""
        with self.clients_lock:
            state = self.clients.get(client_id)
            if state is None:
                return None
            state.update(fields)
            return state

    def push_settings(self, client_id):
        """Queue a ServerSettings message for one stream connection.

        This is how a volume/latency change made through the control port
        reaches the player: snapclient applies ServerSettings immediately.
        """
        with self.clients_lock:
            state = self.clients.get(client_id)
            if state is None:
                return False
            blob = pack_json_message(MSG_SERVER_SETTINGS, next_id(),
                                     {"bufferMs": self.buffer_ms,
                                      "latency": state.get("latency", 0),
                                      "volume": state.get("volume", 100),
                                      "muted": state.get("muted", False)})
        state.setdefault("push", []).append(blob)
        return True

    def drop_client(self, client_id):
        """Forget a client and close its stream connection (DeleteClient)."""
        with self.clients_lock:
            state = self.clients.pop(client_id, None)
        if state is None:
            return None
        try:
            state["conn"].shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        return state


def _tv_add(tv, ds, dus):
    s, u = tv[0] + ds, tv[1] + dus
    while u < 0:
        s, u = s - 1, u + 1000000
    while u >= 1000000:
        s, u = s + 1, u - 1000000
    return s, u


def _tv_sub(a, b):
    return (a[0] - b[0]) + (a[1] - b[1]) / 1e6


# 16 bit message id generator (shared, like winsnapserver's global reference)
_id_lock = threading.Lock()
_id_counter = [0]


def next_id():
    with _id_lock:
        _id_counter[0] += 1
        return _id_counter[0] % 0xFFFF


# --------------------------------------------------------------------------
# JSON-RPC control interface (port 1780): HTTP POST /jsonrpc + WebSocket
# --------------------------------------------------------------------------

WS_MAGIC = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class ControlServer:
    def __init__(self, stream_server, stream):
        self.ssrv = stream_server
        self.stream = stream
        self.notifs_on = set()
        self.notif_lock = threading.Lock()
        self.ws_senders = []
        self.ws_lock = threading.Lock()
        self.tcp_senders = []
        self.tcp_lock = threading.Lock()
        stream_server.on_client_event = self._client_event

    def serve(self, host, port):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((host, port))
        srv.listen(16)
        print("JSON-RPC (snapweb) listening on %s:%d" % (host, port))
        while True:
            conn, addr = srv.accept()
            conn.settimeout(120)
            threading.Thread(target=self._conn_loop, args=(conn,), daemon=True).start()

    # ---------------- transport ----------------

    def _conn_loop(self, conn):
        try:
            while True:
                req, body = self._read_http(conn)
                if req is None:
                    break
                method, target, headers = req
                if method == "POST" and target.split("?")[0] == "/jsonrpc":
                    answers = [self.handle_request(line)
                               for line in self._rpc_lines(body)]
                    for i, ans in enumerate(answers):
                        if ans is None:
                            continue
                        if len(answers) == 1:
                            out = json.dumps(ans)
                        else:
                            out = json.dumps([a for a in answers if a is not None])
                        self._http_reply(conn, out)
                    if headers.get("connection", "").lower() == "close":
                        break
                elif headers.get("upgrade", "").lower() == "websocket" and \
                        target.split("?")[0] == "/jsonrpc":
                    self._websocket_loop(conn, headers)
                    break
                elif target in ("/", "/jsonrpc"):
                    self._http_reply(conn, json.dumps({
                        "snapcast": "jsonrpc endpoint: POST or websocket on /jsonrpc"}))
                    break
                else:
                    self._http_reply(conn, "Not found", status=404)
                    break
        except (StreamGone, OSError, ValueError):
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    @staticmethod
    def _read_http(conn):
        line_buf = b""
        while b"\r\n\r\n" not in line_buf:
            try:
                part = conn.recv(4096)
            except OSError:
                raise StreamGone()
            if not part:
                return None, None
            line_buf += part
            if len(line_buf) > 65536:
                raise StreamGone()
        head, _, rest = line_buf.partition(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        parts = lines[0].split(" ")
        if len(parts) < 2:
            return None, None
        headers = {}
        for line in lines[1:]:
            k, _, v = line.partition(":")
            headers[k.strip().lower()] = v.strip()
        body = rest
        clen = int(headers.get("content-length", 0))
        while len(body) < clen:
            try:
                part = conn.recv(min(clen - len(body), 65536))
            except OSError:
                raise StreamGone()
            if not part:
                raise StreamGone()
            body += part
        return (parts[0], parts[1], headers), body

    @staticmethod
    def _http_reply(conn, text, status=200):
        data = text.encode("utf-8")
        conn.sendall(("HTTP/1.1 %d OK\r\nContent-Type: application/json\r\n"
                      "Content-Length: %d\r\nConnection: keep-alive\r\n\r\n"
                      % (status, len(data))).encode("latin-1") + data)

    # ---------------- websocket ----------------

    def _websocket_loop(self, conn, headers):
        key = headers.get("sec-websocket-key")
        if not key:
            conn.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return
        accept = base64.b64encode(
            hashlib.sha1(key.encode() + WS_MAGIC).digest()).decode()
        conn.sendall(("HTTP/1.1 101 Switching Protocols\r\n"
                      "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                      "Sec-WebSocket-Accept: %s\r\n\r\n" % accept).encode())
        sender = (conn, threading.Lock())
        with self.ws_lock:
            self.ws_senders.append(sender)
        try:
            frags = b""
            while True:
                b0 = recv_exactly(conn, 1)[0]
                fin, opcode = b0 >> 7, b0 & 0x0F
                b1 = recv_exactly(conn, 1)[0]
                masked, ln = b1 & 0x80, b1 & 0x7F
                if ln == 126:
                    ln = struct.unpack(">H", recv_exactly(conn, 2))[0]
                elif ln == 127:
                    ln = struct.unpack(">Q", recv_exactly(conn, 8))[0]
                mask = recv_exactly(conn, 4) if masked else b""
                data = recv_exactly(conn, ln)
                if masked:
                    data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
                if opcode == 0x8:  # close
                    break
                if opcode == 0x9:  # ping -> pong
                    self._ws_send(sender, 0xA, data)
                    continue
                if opcode == 0x1:
                    frags = data if fin else frags + data
                    if fin:
                        for line in self._rpc_lines(frags):
                            ans = self.handle_request(line)
                            if ans is not None:
                                self._ws_send(sender, 0x1,
                                              json.dumps(ans).encode())
                        frags = b""
        except (StreamGone, OSError):
            pass
        finally:
            with self.ws_lock:
                if sender in self.ws_senders:
                    self.ws_senders.remove(sender)

    @staticmethod
    def _ws_send(sender, opcode, data):
        sock, lock = sender
        n = len(data)
        if n < 126:
            head = struct.pack(">BB", 0x80 | opcode, n)
        elif n < 65536:
            head = struct.pack(">BBH", 0x80 | opcode, 126, n)
        else:
            head = struct.pack(">BBQ", 0x80 | opcode, 127, n)
        try:
            with lock:
                sock.sendall(head + data)
        except OSError:
            pass

    # ---------------- notifications ----------------

    def _client_event(self, kind, client_id):
        # push notifications to every connected control client.  snapweb
        # subscribes explicitly (Client.Subscribe); the Android snapdroid
        # app does not, so notify whoever is connected.
        if not (self.ws_senders or self.tcp_senders):
            return
        with self.ssrv.clients_lock:
            info = dict(self.ssrv.clients.get(client_id, {"id": client_id}))
        method = {"Connect": "Client.OnConnect",
                  "Disconnect": "Client.OnDisconnect",
                  "Deleted": "Client.OnDisconnect",
                  "Volume": "Client.OnVolumeChanged",
                  "Name": "Client.OnNameChanged",
                  "Latency": "Client.OnLatencyChanged"}.get(kind)
        if method is None:
            return
        if method.endswith("VolumeChanged"):
            params = {"id": client_id,
                      "volume": {"percent": info.get("volume", 100),
                                 "muted": info.get("muted", False)}}
        elif method.endswith("LatencyChanged"):
            params = {"id": client_id, "latency": info.get("latency", 0)}
        elif method.endswith("NameChanged"):
            params = {"id": client_id, "name": info.get("name", "")}
        elif method.endswith("OnConnect"):
            params = {"client": self._old_client(info)}
        else:
            params = {"id": client_id}
        msgs = [json.dumps({"jsonrpc": "2.0", "method": method, "params": params})]
        if kind in ("Connect", "Disconnect", "Deleted"):
            # the Android app rebuilds its whole view from this one
            msgs.append(json.dumps({"jsonrpc": "2.0", "method": "Server.OnUpdate",
                                    "params": {"server":
                                               self._status_result()["server"]}}))
        for msg in msgs:
            for sender in list(self.ws_senders):
                self._ws_send(sender, 0x1, msg.encode())
            for sender in list(self.tcp_senders):
                try:
                    sender.write(msg.encode() + b"\r\n")
                    sender.flush()
                except OSError:
                    pass

    # ---------------- plain TCP (line delimited JSON-RPC) ----------------
    #
    # snapcast's native control transport: one JSON-RPC object per line on
    # port 1705, replies terminated with "\r\n".  The snapdroid (Android)
    # app's "control port" field talks exactly this dialect.

    def serve_tcp(self, host, port):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((host, port))
        srv.listen(16)
        print("JSON-RPC (raw TCP) listening on %s:%d" % (host, port))
        while True:
            conn, addr = srv.accept()
            conn.settimeout(None)
            threading.Thread(target=self._tcp_conn_loop, args=(conn,),
                             daemon=True).start()

    def _tcp_conn_loop(self, conn):
        sender = conn.makefile("wb")
        peer = "%s:%d" % conn.getpeername()
        with self.tcp_lock:
            self.tcp_senders.append(sender)
        print("control tcp: %s connected" % peer)
        buf = b""
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue  # snapdroid sends a blank line after each request
                    ans = self.handle_request(line.decode("utf-8", "replace"))
                    if ans is not None:
                        sender.write(json.dumps(ans).encode() + b"\r\n")
                        sender.flush()
        except (OSError, StreamGone):
            pass
        finally:
            with self.tcp_lock:
                if sender in self.tcp_senders:
                    self.tcp_senders.remove(sender)
            try:
                conn.close()
            except OSError:
                pass
            print("control tcp: %s disconnected" % peer)

    # ---------------- jsonrpc ----------------

    @staticmethod
    def _rpc_lines(body):
        if not body:
            return []
        try:
            parsed = json.loads(body)
        except ValueError:
            return []
        if isinstance(parsed, list):
            return [json.dumps(p) for p in parsed]
        return [json.dumps(parsed)]

    def _client_infos(self):
        with self.ssrv.clients_lock:
            ids = sorted(self.ssrv.clients)
            infos = [dict(self.ssrv.clients[c]) for c in ids]
        out = []
        for info in infos:
            # every key the snapdroid Client class asks for, plus the
            # "settings" object snapweb (>= 0.28) asks for
            entry = self._old_client(info)
            entry["settings"] = {"id": info["id"],
                                 "name": entry["config"]["name"],
                                 "latencyOffset": info.get("latency", 0),
                                 "volume": entry["config"]["volume"]}
            out.append(entry)
        return out

    def _stream_infos(self):
        s = self.stream
        label = getattr(s, "uri_name", "winsnapserver.py")
        return {"id": "default",
                "uri": {"raw": label, "scheme": label,
                        "host": "", "port": "", "path": "", "query": {}},
                "baseVolume": 100,
                "settings": {"id": "default", "name": label,
                             "sampleformat": "%d:%d:%d" % (s.rate, s.bits, s.channels),
                             "codec": "pcm", "bufferMs": self.ssrv.buffer_ms},
                "status": {"bufferMs": 0.0, "codec": "pcm",
                           "identity": {"logical": "", "physical": ""},
                           "sampleformat": "%d:%d:%d" % (s.rate, s.bits, s.channels),
                           "uri": label}}

    # ---- old (pre-0.28) JSON shapes -------------------------------------
    #
    # The snapdroid Android app hands "result.server" to its ServerStatus
    # class, which insists on the nested {"server": ..., "groups": [...],
    # "streams": [...]} layout with these exact key names (see snapdroid
    # control/json/{ServerStatus,Server,winsnapserver,Group,Client,Host,
    # ClientConfig,Volume,Time_t,Stream,StreamUri}.java).  A winsnapserver of
    # that era answers the same way, so we keep the flat keys for modern
    # clients and merge the nested ones into result["server"].

    NAME = "winsnapserver.py"
    VERSION = "0.1"

    def _host_info(self):
        try:
            host_name = socket.gethostname()
        except OSError:
            host_name = "winsnapserver.py"
        return {"name": host_name, "mac": "00:00:00:00:00:00",
                "os": sys.platform, "arch": platform.machine(), "ip": ""}

    def _snapserver_info(self):
        return {"name": "winsnapserver", "version": self.VERSION,
                "protocolVersion": 2, "controlProtocolVersion": 1}

    def _old_client(self, info):
        sec, usec = info.get("last_seen") or now_tv()
        name = info.get("name") or info.get("host_name") or "snapclient"
        return {
            "id": info["id"],
            "host": {"name": info.get("host_name") or name,
                     "mac": info.get("mac") or info["id"],
                     "os": info.get("os") or "",
                     "arch": info.get("arch") or "",
                     "ip": info.get("ip") or ""},
            "snapclient": {"name": "Snapclient",
                           "version": info.get("version") or "",
                           "protocolVersion": info.get("protocol", 2)},
            "config": {"name": name,
                       "volume": {"percent": info.get("volume", 100),
                                  "muted": info.get("muted", False)},
                       "latency": info.get("latency", 0),
                       "instance": info.get("instance", 1)},
            "lastSeen": {"sec": sec, "usec": usec},
            "connected": True,
        }

    def _old_client_for(self, client_id):
        with self.ssrv.clients_lock:
            info = dict(self.ssrv.clients.get(client_id, {"id": client_id}))
        return self._old_client(info)

    def _old_client_infos(self):
        with self.ssrv.clients_lock:
            ids = sorted(self.ssrv.clients)
            infos = [dict(self.ssrv.clients[c]) for c in ids]
        return [self._old_client(info) for info in infos]

    def _group_infos(self):
        clients = self._old_client_infos()
        percents = [c["config"]["volume"]["percent"] for c in clients] or [100]
        muted = bool(clients) and all(c["config"]["volume"]["muted"]
                                      for c in clients)
        return [{"id": "default", "name": "default", "stream_id": "default",
                 "muted": muted,
                 # keys a newer control client looks at as well
                 "selected": True, "mute": muted,
                 "volume": sum(percents) // len(percents),
                 "clients": clients}]

    def _old_stream_infos(self):
        s = self.stream
        fmt = "%d:%d:%d" % (s.rate, s.bits, s.channels)
        return [{"id": "default", "status": "playing",
                 "uri": {"raw": "pipe:///default?name=default&sampleformat=" + fmt,
                         "scheme": "pipe", "host": "", "port": "",
                         "path": "/default", "fragment": "",
                         "query": {"name": "default", "sampleformat": fmt,
                                   "mode": "push"}}}]

    def _status_result(self):
        groups = self._group_infos()
        server = {"server": {"host": self._host_info(),
                             "winsnapserver": self._snapserver_info()},
                  "groups": groups,
                  "streams": self._old_stream_infos(),
                  # modern (>= 0.30) keys, kept side by side
                  "hosts": {"_": self.NAME},
                  "version": self.VERSION,
                  "snapcast": {"arch": platform.machine(), "branchName": "",
                               "buildType": "", "commit": "", "name": self.NAME,
                               "protocol": "2.0.0", "tags": [""],
                               "version": self.VERSION}}
        return {"clients": self._client_infos(),
                "streams": [self._stream_infos()],
                "server": server}

    # ---- setters (Client.Set* / Group.Set* / Server.DeleteClient) --------

    def _client_ids(self):
        with self.ssrv.clients_lock:
            return sorted(self.ssrv.clients)

    def _apply(self, client_id, fields, event, ret):
        """Store new settings, push them to the player, notify control clients."""
        if self.ssrv.update_client(client_id, **fields) is None:
            raise ValueError("unknown client id: %r" % (client_id,))
        self.ssrv.push_settings(client_id)
        if self.ssrv.on_client_event:
            self.ssrv.on_client_event(event, client_id)
        return ret

    def _volume_fields(self, params):
        vol = params.get("volume")
        if not isinstance(vol, dict):     # some clients send them flat
            vol = params
        fields = {}
        if vol.get("percent") is not None:
            fields["volume"] = max(0, min(100, int(vol["percent"])))
        if vol.get("muted") is not None:
            fields["muted"] = bool(vol["muted"])
        return fields

    def _volume_result(self, client_id):
        vol = self._old_client_for(client_id)["config"]["volume"]
        return {"volume": {"percent": vol["percent"], "muted": vol["muted"]}}

    def _set_volume(self, params):
        fields = self._volume_fields(params)
        cid = _pid(params)
        self._apply(cid, fields, "Volume", None)
        return self._volume_result(cid)

    def _set_name(self, params):
        name = str(params.get("name", "")) if isinstance(params, dict) else ""
        if self.ssrv.update_client(_pid(params), name=name) is None:
            raise ValueError("unknown client id: %r" % (_pid(params),))
        if self.ssrv.on_client_event:
            self.ssrv.on_client_event("Name", _pid(params))
        return {"name": name}

    def _set_latency(self, params):
        lat = int(params.get("latency", 0)) if isinstance(params, dict) else 0
        return self._apply(_pid(params), {"latency": lat}, "Latency",
                           {"latency": lat})

    def _set_group_volume(self, params):
        fields = self._volume_fields(params)
        ids = self._client_ids()
        for cid in ids:
            self._apply(cid, dict(fields), "Volume", None)
        if ids:
            return self._volume_result(ids[0])
        return {"volume": {"percent": fields.get("volume", 100),
                           "muted": fields.get("muted", False)}}

    def _set_group_mute(self, params):
        muted = bool(params.get("mute", params.get("muted", False)))
        for cid in self._client_ids():
            self._apply(cid, {"muted": muted}, "Volume", None)
        return {"mute": muted}

    def _set_group_stream(self, params):
        return {"id": _pid(params) or "default",
                "stream_id": params.get("stream_id", "default")}

    def _set_group_name(self, params):
        return {"id": _pid(params) or "default", "name": params.get("name", "")}

    def _set_group_clients(self, params):
        # snapdroid re-reads result.server after moving clients between groups;
        # with a single group there is nothing to move, but the shape matters.
        return self._status_result()

    def _delete_client(self, params):
        cid = _pid(params)
        if self.ssrv.drop_client(cid) is None:
            raise ValueError("unknown client id: %r" % (cid,))
        if self.ssrv.on_client_event:
            self.ssrv.on_client_event("Deleted", cid)
        return self._status_result()

    def handle_request(self, line):
        try:
            req = json.loads(line)
        except ValueError:
            return None
        rid = req.get("id")
        method = req.get("method", "")
        params = req.get("params") or {}
        if not isinstance(params, dict):
            params = {}
        notify = "id" not in req

        def result(value):
            return None if notify else {"jsonrpc": "2.0", "result": value, "id": rid}

        def error(code, message):
            if notify:
                return None
            # "data" is not optional here: snapdroid's error path reads it
            # unconditionally (RemoteControl.processJson).
            return {"jsonrpc": "2.0",
                    "error": {"code": code, "message": message, "data": message},
                    "id": rid}

        status = self._status_result

        handlers = {
            "GetStatus": status,
            "Server.GetStatus": status,
            "GetServerSettings": lambda: {"attachClientToDefaultGroup": True,
                                          "auto_rescan": False, "serverVersion": "0.0.0"},
            "Server.GetSettings": lambda: {"version": "unknown"},
            "GetClients": lambda: self._client_infos(),
            "Client.GetStatus": lambda: {"client": next(
                (c for c in self._client_infos() if c["id"] == _pid(params)), None)},
            "Client.GetSettings": lambda: {"id": _pid(params)},
            "Stream.GetStatus": lambda: {"stream": self._stream_infos()},
            "Stream.GetSettings": lambda: {"id": _pid(params)},
            "GetGroups": lambda: {"groups": self._group_infos()},
            "Group.GetStatus": lambda: {"group": next(
                (g for g in self._group_infos() if g["id"] == _pid(params)),
                self._group_infos()[0])},
            "GetServerVersion": lambda: {"version": self.NAME},
            "Server.GetRPCVersion": lambda: {"major": 2, "minor": 0, "patch": 0},
            "Client.SetVolume": lambda: self._set_volume(params),
            "Client.SetName": lambda: self._set_name(params),
            "Client.SetLatency": lambda: self._set_latency(params),
            "Client.SetMeta": lambda: True,
            "Client.SetConnected": lambda: True,
            "Group.SetVolume": lambda: self._set_group_volume(params),
            "Group.SetMute": lambda: self._set_group_mute(params),
            "Group.SetStream": lambda: self._set_group_stream(params),
            "Group.SetName": lambda: self._set_group_name(params),
            "Group.SetClients": lambda: self._set_group_clients(params),
            "Server.DeleteClient": lambda: self._delete_client(params),
            "Client.Subscribe": lambda: self._subscribe("Client.%s" % _method(params)),
            "Client.Unsubscribe": lambda: self._unsubscribe("Client.%s" % _method(params)),
            "Stream.Subscribe": lambda: self._subscribe("Stream.%s" % _method(params)),
            "Stream.Unsubscribe": lambda: self._unsubscribe("Stream.%s" % _method(params)),
        }
        if method in handlers:
            try:
                return result(handlers[method]())
            except Exception as exc:  # keep the endpoint resilient
                return error(-32000, str(exc))
        if method.startswith("Client.Set") or method.startswith("Stream.Set"):
            return error(-32601, "not supported by this simple server")
        return error(-32601, "Method not found: %s" % method)

    def _subscribe(self, key):
        with self.notif_lock:
            self.notifs_on.add(key)
        return True

    def _unsubscribe(self, key):
        with self.notif_lock:
            self.notifs_on.discard(key)
        return True


def _pid(params):
    return params.get("id") if isinstance(params, dict) else None


def _method(params):
    return params.get("method", "") if isinstance(params, dict) else ""


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("wav", nargs="?",
                    help="WAV file to loop instead of capturing live audio")
    ap.add_argument("--wav", dest="wav_file", metavar="FILE",
                    help="loop this WAV file instead of capturing live audio")
    ap.add_argument("--silence", action="store_true", help="loop 1 s of silence")
    ap.add_argument("--list-capture-devices", action="store_true",
                    help="list the playback devices that can be captured, then exit")
    ap.add_argument("--capture-device", metavar="SPEC",
                    help="live capture source: device index or a fragment of "
                         "the device name (default: the loopback endpoint of "
                         "the default playback device)")
    ap.add_argument("--keep-ms", type=int, default=4000,
                    help="live capture: how much audio to retain, so a client "
                         "that stalls can catch up before it has to resync")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=1704, help="SnapStream port")
    ap.add_argument("--tcp-control-port", type=int, default=1705,
                    help='line delimited JSON-RPC port (the "control port" '
                         'the Android app asks for, snapcast default 1705)')
    ap.add_argument("--control-port", type=int, default=1780,
                    help="HTTP + WebSocket JSON-RPC port (snapweb)")
    ap.add_argument("--chunk-ms", type=int, default=20)
    ap.add_argument("--buffer-ms", type=int, default=500,
                    help="playout lead advertised to clients "
                         "(the Android player subtracts its own output latency, "
                         "so a larger value is safer over Wi-Fi)")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="log every SnapStream frame (type, id, refersTo) and "
                         "every connect/disconnect with a timestamp")
    args = ap.parse_args()

    global VERBOSE
    VERBOSE = args.verbose

    if args.list_capture_devices:
        list_capture_devices()
        return 0

    if args.wav_file or args.wav:
        path = args.wav_file or args.wav
        stream = PcmStream.from_wave(path)
        stream.uri_name = path
        print("source: WAV loop, %.1f s" % (stream.duration_ms() / 1000.0))
    elif args.silence:
        stream = PcmStream.silence()
        stream.uri_name = "silence"
        print("source: 1 s of silence, looped")
    else:
        device, pa = select_capture_device(args.capture_device)
        frames = max(1, int(round(float(device["defaultSampleRate"])
                                  * args.chunk_ms / 1000.0)))
        stream = LoopStream(device, pa, frames, args.keep_ms)
        print("source: live capture from %r  sampleformat %d:%d:%d, %d ms blocks"
              % (stream.name, stream.rate, stream.bits, stream.channels, args.chunk_ms))

    sserver = StreamServer(stream, args.chunk_ms, args.buffer_ms)
    cserver = ControlServer(sserver, stream)

    def listen(target, port, what):
        try:
            target(args.host, port)
            return True
        except OSError as exc:
            print("cannot listen on %s port %d: %s" % (what, port, exc))
            return False

    threading.Thread(target=listen, args=(cserver.serve, args.control_port,
                                          "http/websocket"), daemon=True).start()
    if args.tcp_control_port > 0:
        threading.Thread(target=listen,
                         args=(cserver.serve_tcp, args.tcp_control_port,
                                "control"), daemon=True).start()
    try:
        ok = listen(sserver.serve, args.port, "stream")
    except KeyboardInterrupt:
        print("\nbye")
        sys.exit(0)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
