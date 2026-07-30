"""Probe a Tyr (UE 5.6) .replay file: decode the header + walk the chunk list.

Tyr uses stock UE FLocalFileNetworkReplayStreaming (.replay files under
Saved\\Demos). The on-disk layout, confirmed byte-for-byte against real Tyr
replays (see docs/replay-format.md), is:

    ReplayInfo header (magic, file version, length, engine/network version,
    friendly name, timestamp, compression/encryption flags)
    then a flat sequence of chunks:
        uint32 chunkType   (0=Header, 1=ReplayData, 2=Checkpoint, 3=Event)
        int32  chunkSize   (bytes, payload only)
        <payload>

The embedded Header chunk (type 0) additionally carries engine version,
branch, and the level/map name(s) -- all cheaply decodable without any
net-serialization work. Event chunks (type 3, if present) carry
id/group/metadata FStrings + a time range, generally used for scoreboard/
killfeed bookmarks. ReplayData and Checkpoint chunks hold the bit-packed
net stream (actor spawns/property replication/RPCs) and are NOT decoded by
this tool -- they are only logged by type/size/offset.

Reference: this format (and the exact conditional-field layout used below)
matches Shiqan/FortniteReplayDecompressor's Unreal.Core library, which
implements the same generic UE ReplayReader.ReadReplayInfo /
ReadReplayHeader / ReadReplayChunks logic Epic ships in
LocalFileNetworkReplayStreaming + NetworkReplayStreaming.

Usage:
    python -m tools.replay_probe <path.replay>
    python -m tools.replay_probe <path.replay> --strings
"""
import argparse
import re
import struct
from collections import Counter
from pathlib import Path

FILE_MAGIC = 0x1CA2E27F          # NETWORK_DEMO_MAGIC
HEADER_MAGIC = 0x2CF5A13D        # NETWORK_DEMO_HEADER_MAGIC (embedded Header chunk)

CHUNK_TYPE_NAMES = {0: "Header", 1: "ReplayData", 2: "Checkpoint", 3: "Event"}

# NetworkVersionHistory thresholds (embedded Header chunk), per UE / Fortnite
# replay-decompressor reference implementation.
NV_HEADER_FLAGS = 9
NV_SAVE_FULL_ENGINE_VERSION = 11
NV_HEADER_GUID = 12
NV_SAVE_PACKAGE_VERSION_UE = 17
NV_RECORDING_METADATA = 18
NV_USE_CUSTOM_VERSION = 19

# ReplayVersionHistory thresholds (top-level ReplayInfo)
RV_RECORDED_TIMESTAMP = 3
RV_COMPRESSION = 2
RV_ENCRYPTION = 6
RV_CUSTOM_VERSIONS = 7


class Cursor:
    """Small defensive byte-cursor. Every read can raise; callers catch."""

    def __init__(self, data):
        self.data = data
        self.off = 0

    def remaining(self):
        return len(self.data) - self.off

    def rd(self, n):
        if n < 0 or self.off + n > len(self.data):
            raise EOFError(f"want {n} bytes at offset {self.off}, have {self.remaining()}")
        b = self.data[self.off:self.off + n]
        self.off += n
        return b

    def u8(self):
        return self.rd(1)[0]

    def u16(self):
        return struct.unpack("<H", self.rd(2))[0]

    def u32(self):
        return struct.unpack("<I", self.rd(4))[0]

    def i32(self):
        return struct.unpack("<i", self.rd(4))[0]

    def i64(self):
        return struct.unpack("<q", self.rd(8))[0]

    def f32(self):
        return struct.unpack("<f", self.rd(4))[0]

    def guid(self):
        return self.rd(16).hex()

    def fstring(self):
        """UE FString: int32 length prefix. Positive => ANSI/UTF-8, count
        includes the trailing NUL. Negative => UTF-16LE, abs(count) chars
        (in UTF-16 code units) also includes the trailing NUL."""
        n = self.i32()
        if n == 0:
            return ""
        if n > 0:
            if n > 1 << 20:
                raise ValueError(f"implausible ANSI FString length {n}")
            s = self.rd(n)
            return s.decode("utf-8", errors="replace").rstrip("\x00")
        n = -n
        if n > 1 << 20:
            raise ValueError(f"implausible UTF-16 FString length {n}")
        s = self.rd(n * 2)
        return s.decode("utf-16-le", errors="replace").rstrip("\x00")


def _try(label, fn, log):
    """Run fn(), returning its result, or None while logging the failure."""
    try:
        return fn()
    except Exception as e:  # noqa: BLE001 - deliberately broad, this is a probe
        log(f"  ! failed to read {label}: {e}")
        return None


def parse_replay_info(c, log):
    """Top-level ReplayInfo header. Returns a dict of whatever it could read."""
    info = {}
    start = c.off
    magic = c.u32()
    info["magic"] = f"0x{magic:08X}"
    if magic != FILE_MAGIC:
        log(f"  ! unexpected magic 0x{magic:08X} (expected 0x{FILE_MAGIC:08X}) -- "
            f"not a Tyr/UE local replay file, or format changed")
    file_version = c.u32()
    info["fileVersion"] = file_version

    if file_version >= RV_CUSTOM_VERSIONS:
        cvc = _try("customVersionCount", c.i32, log)
        if cvc is not None:
            info["customVersionCount"] = cvc
            if 0 <= cvc <= 10_000:
                _try("custom version entries", lambda: c.rd(cvc * 20), log)

    info["lengthInMs"] = _try("lengthInMs", c.u32, log)
    info["networkVersion"] = _try("networkVersion", c.u32, log)
    info["changelist"] = _try("changelist", c.u32, log)
    info["friendlyName"] = _try("friendlyName", c.fstring, log)
    is_live = _try("isLive", c.u32, log)
    info["isLive"] = bool(is_live) if is_live is not None else None

    if file_version >= RV_RECORDED_TIMESTAMP:
        ticks = _try("timestamp", c.i64, log)
        info["timestampRawTicks"] = ticks
        if ticks is not None:
            info["timestampIso"] = _ticks_to_iso(ticks)

    if file_version >= RV_COMPRESSION:
        v = _try("isCompressed", c.u32, log)
        info["isCompressed"] = bool(v) if v is not None else None

    if file_version >= RV_ENCRYPTION:
        v = _try("isEncrypted", c.u32, log)
        info["isEncrypted"] = bool(v) if v is not None else None
        # size is read unconditionally once we're in this version branch
        # (it's simply 0 when not encrypted) - only the key bytes are
        # conditional on size > 0.
        size = _try("encryptionKeySize", c.u32, log)
        info["encryptionKeySize"] = size
        if size and 0 < size < 1 << 20:
            _try("encryptionKey", lambda: c.rd(size), log)

    info["_headerBytes"] = c.off - start
    return info


def _ticks_to_iso(ticks):
    """.NET-style DateTime.FromBinary ticks (100ns since 0001-01-01), masking
    off the 2-bit DateTimeKind flag UE/the reference parser also strips."""
    import datetime
    try:
        raw_ticks = ticks & 0x3FFFFFFFFFFFFFFF
        dt = datetime.datetime(1, 1, 1) + datetime.timedelta(microseconds=raw_ticks / 10)
        return dt.isoformat()
    except Exception:
        return None


def parse_header_chunk(payload, log):
    """Decode the embedded Header chunk (chunkType 0) payload."""
    c = Cursor(payload)
    h = {}
    magic = _try("header magic", c.u32, log)
    if magic is not None:
        h["magic"] = f"0x{magic:08X}"
        if magic != HEADER_MAGIC:
            log(f"  ! header chunk magic 0x{magic:08X} != expected 0x{HEADER_MAGIC:08X}")
    nv = _try("header networkVersion", c.u32, log)
    h["networkVersion"] = nv
    if nv is None:
        return h

    if nv >= NV_USE_CUSTOM_VERSION:
        cvc = _try("header customVersionCount", c.i32, log)
        if cvc is not None and 0 <= cvc <= 10_000:
            _try("header custom version entries", lambda: c.rd(cvc * 20), log)

    h["networkChecksum"] = _try("networkChecksum", c.u32, log)
    h["engineNetworkVersion"] = _try("engineNetworkVersion", c.u32, log)
    h["gameNetworkProtocolVersion"] = _try("gameNetworkProtocolVersion", c.u32, log)

    if nv >= NV_HEADER_GUID:
        h["guid"] = _try("header guid", c.guid, log)

    if nv >= NV_SAVE_FULL_ENGINE_VERSION:
        maj = _try("engineMajor", c.u16, log)
        minr = _try("engineMinor", c.u16, log)
        patch = _try("enginePatch", c.u16, log)
        cl = _try("engineChangelist", c.u32, log)
        branch = _try("engineBranch", c.fstring, log)
        h["engineVersion"] = f"{maj}.{minr}.{patch}" if None not in (maj, minr, patch) else None
        h["engineChangelist"] = cl
        h["engineBranch"] = branch
    else:
        h["engineChangelist"] = _try("changelist(legacy)", c.u32, log)

    if nv >= NV_RECORDING_METADATA:
        h["ue4Version"] = _try("ue4Version", c.u32, log)
        h["ue5Version"] = _try("ue5Version", c.u32, log)
        h["packageVersionLicenseeUE"] = _try("packageVersionLicenseeUE", c.u32, log)

    level_count = _try("levelNamesAndTimes count", c.i32, log)
    levels = []
    if level_count is not None and 0 <= level_count <= 1000:
        for _ in range(level_count):
            name = _try("level name", c.fstring, log)
            t = _try("level time", c.u32, log)
            if name is None:
                break
            levels.append({"name": name, "timeMs": t})
    h["levels"] = levels

    if nv >= NV_HEADER_FLAGS:
        flags = _try("flags", c.u32, log)
        h["flags"] = f"0x{flags:X}" if flags is not None else None

    gs_count = _try("gameSpecificData count", c.i32, log)
    game_specific = []
    if gs_count is not None and 0 <= gs_count <= 1000:
        for _ in range(gs_count):
            s = _try("gameSpecificData entry", c.fstring, log)
            if s is None:
                break
            game_specific.append(s)
    h["gameSpecificData"] = game_specific

    if nv >= NV_SAVE_PACKAGE_VERSION_UE:
        h["minRecordHz"] = _try("minRecordHz", c.f32, log)
        h["maxRecordHz"] = _try("maxRecordHz", c.f32, log)
        h["frameLimitInMS"] = _try("frameLimitInMS", c.f32, log)
        h["checkpointLimitInMS"] = _try("checkpointLimitInMS", c.f32, log)
        h["platform"] = _try("platform", c.fstring, log)
        h["buildConfig"] = _try("buildConfig", c.u8, log)
        h["buildTargetType"] = _try("buildTargetType", c.u8, log)

    h["_consumedBytes"] = c.off
    h["_payloadBytes"] = len(payload)
    return h


def parse_event_chunk(payload, log):
    c = Cursor(payload)
    ev = {}
    ev["id"] = _try("event id", c.fstring, log)
    ev["group"] = _try("event group", c.fstring, log)
    ev["metadata"] = _try("event metadata", c.fstring, log)
    ev["startTime"] = _try("event startTime", c.u32, log)
    ev["endTime"] = _try("event endTime", c.u32, log)
    ev["sizeInBytes"] = _try("event sizeInBytes", c.i32, log)
    return ev


def probe(path, verbose=True):
    """Parse header + walk chunks. Returns a result dict; never raises."""
    logs = []

    def log(msg):
        logs.append(msg)
        if verbose:
            print(msg)

    data = Path(path).read_bytes()
    log(f"file: {path}")
    log(f"size: {len(data)} bytes")

    c = Cursor(data)
    info = parse_replay_info(c, log)
    log("")
    log("=== ReplayInfo ===")
    for k, v in info.items():
        if k == "_headerBytes":
            continue
        log(f"  {k}: {v!r}")
    log(f"  (header consumed {info.get('_headerBytes')} bytes, chunk loop starts at offset {c.off})")

    chunk_hist = Counter()
    chunks = []
    events = []
    header_chunk = None
    n = 0
    while c.remaining() > 0:
        if c.remaining() < 8:
            log(f"\n! {c.remaining()} trailing bytes (< 8) at offset {c.off}, stopping")
            break
        chunk_off = c.off
        try:
            ctype = c.u32()
            csize = c.i32()
        except Exception as e:
            log(f"\n! failed to read chunk header at offset {chunk_off}: {e}")
            break
        type_name = CHUNK_TYPE_NAMES.get(ctype, f"Unknown({ctype})")
        if csize < 0 or c.off + csize > len(data):
            log(f"\n! chunk #{n} type={type_name} at offset {chunk_off} has implausible "
                f"size {csize} (only {c.remaining()} bytes left) -- stopping chunk walk")
            break
        payload = data[c.off:c.off + csize]
        chunk_hist[type_name] += 1
        chunks.append({"index": n, "type": type_name, "offset": chunk_off, "size": csize})
        n += 1

        if ctype == 0 and header_chunk is None:
            log(f"\n=== chunk #{n - 1}: Header (offset {chunk_off}, {csize} bytes) ===")
            header_chunk = parse_header_chunk(payload, log)
            for k, v in header_chunk.items():
                if k.startswith("_"):
                    continue
                log(f"  {k}: {v!r}")
        elif ctype == 3:
            log(f"\n=== chunk #{n - 1}: Event (offset {chunk_off}, {csize} bytes) ===")
            ev = parse_event_chunk(payload, log)
            for k, v in ev.items():
                log(f"  {k}: {v!r}")
            events.append(ev)

        c.off = chunk_off + 8 + csize

    log("")
    log("=== chunk histogram ===")
    for k, v in chunk_hist.items():
        log(f"  {k}: {v}")
    log(f"  total chunks: {n}")
    log(f"  total events (type=Event, decoded): {len(events)}")

    return {
        "path": str(path),
        "size": len(data),
        "info": info,
        "header": header_chunk,
        "chunkHistogram": dict(chunk_hist),
        "chunks": chunks,
        "events": events,
        "logs": logs,
    }


_ASCII_RE = re.compile(rb"[\x20-\x7e]{4,}")
_UTF16_RE = re.compile(rb"(?:[\x20-\x7e]\x00){4,}")


def extract_strings(path, limit=200):
    """Grep printable ASCII and UTF-16LE strings (>=4 chars) from the whole
    file. Deduped, first `limit` in file order. This is intentionally dumb
    (no bit-level net-stream parsing) -- it exists to reveal map/class/
    ability/property NAMES that live in the NetGUID export table and Header
    chunk, which are byte-aligned even though replicated property VALUES
    are not."""
    data = Path(path).read_bytes()
    seen = set()
    out = []
    for m in _ASCII_RE.finditer(data):
        s = m.group().decode("ascii")
        if s not in seen:
            seen.add(s)
            out.append(s)
            if len(out) >= limit:
                break
    if len(out) < limit:
        for m in _UTF16_RE.finditer(data):
            try:
                s = m.group().decode("utf-16-le")
            except UnicodeDecodeError:
                continue
            if s not in seen:
                seen.add(s)
                out.append(s)
                if len(out) >= limit:
                    break
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", help="path to a .replay file")
    ap.add_argument("--strings", action="store_true",
                     help="also dump printable ASCII+UTF16 strings (>=4 chars, first ~200)")
    ap.add_argument("--limit", type=int, default=200, help="max strings for --strings (default 200)")
    args = ap.parse_args(argv)

    p = Path(args.path)
    if not p.is_file():
        print(f"not a file: {p}")
        return 1

    probe(p)

    if args.strings:
        print("")
        print("=== strings (ascii + utf16le, >=4 chars, deduped, file order) ===")
        for s in extract_strings(p, args.limit):
            print(f"  {s}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
