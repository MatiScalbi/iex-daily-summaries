"""IEX Exchange HIST (TOPS feed) reader: free, licensed for redistribution with
attribution ("Data provided for free by IEX").

Files are gzipped packet captures (pcap or pcapng) of the IEX-TP protocol over
UDP. We stream them and keep, per symbol and session, a daily summary of
regular-session, round-lot trades on IEX: first/high/low/last price and IEX
volume. The last regular trade on IEX approximates (does not equal) the
consolidated close.

Formats (all little-endian):
- IEX-TP header (40 bytes): version u8, reserved u8, protocol id u16
  (0x8003 = TOPS), channel u32, session u32, payload length u16, message count
  u16, stream offset i64, first seq i64, send time i64. Then `message count`
  blocks of [length u16][message].
- TOPS Trade Report 'T' (0x54), 38 bytes: type u8, sale condition flags u8,
  timestamp i64 (ns since epoch), symbol 8 ASCII (space padded), size u32,
  price i64 (1e-4 dollars), trade id i64.
- Sale condition flags: 0x80 ISO, 0x40 extended hours, 0x20 odd lot,
  0x10 trade-through exempt, 0x08 single-price cross.
"""

from __future__ import annotations

import gzip
import io
import struct
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import pandas as pd
import requests

HIST_URL = "https://iextrading.com/api/1.0/hist?date={date}"
ATTRIBUTION = "Data provided for free by IEX. View IEX's Terms of Use: https://iextrading.com/api-exhibit-a/"

TOPS_PROTOCOLS = {0x8002, 0x8003}  # 0x8002 seen in TOPS 1.5 captures (2017)
FLAG_EXTENDED = 0x40
FLAG_ODD_LOT = 0x20
_TP_HEADER = struct.Struct("<BBHIIHHqqq")  # 40 bytes
_TRADE = struct.Struct("<BBq8sIqq")  # 38 bytes
_U16 = struct.Struct("<H")

PCAP_MAGIC = {b"\xd4\xc3\xb2\xa1": "<", b"\xa1\xb2\xc3\xd4": ">", b"\x4d\x3c\xb2\xa1": "<", b"\xa1\xb2\x3c\x4d": ">"}
PCAPNG_SHB = b"\x0a\x0d\x0d\x0a"


@dataclass
class Trade:
    symbol: str
    price: float
    size: int
    flags: int
    ts: int


def _udp_payload(frame: bytes) -> bytes | None:
    """Ethernet (optionally VLAN-tagged) / IPv4 / UDP payload, or None."""
    if len(frame) < 42:
        return None
    off = 12
    ethertype = int.from_bytes(frame[off : off + 2], "big")
    off += 2
    while ethertype in (0x8100, 0x88A8):
        ethertype = int.from_bytes(frame[off + 2 : off + 4], "big")
        off += 4
    if ethertype != 0x0800:
        return None
    ihl = (frame[off] & 0x0F) * 4
    if frame[off + 9] != 17:  # UDP
        return None
    udp = off + ihl
    return frame[udp + 8 :]


def _pcap_frames(fh: BinaryIO, endian: str) -> Iterator[bytes]:
    fh.read(20)  # rest of the 24-byte global header (magic already consumed)
    rec = struct.Struct(endian + "IIII")
    while True:
        head = fh.read(16)
        if len(head) < 16:
            return
        _, _, incl, _ = rec.unpack(head)
        data = fh.read(incl)
        if len(data) < incl:
            return
        yield data


def _pcapng_frames(fh: BinaryIO) -> Iterator[bytes]:
    endian = "<"
    first = True
    while True:
        head = fh.read(8)
        if len(head) < 8:
            return
        if first:
            # Section header: byte-order magic follows the length.
            bom = fh.read(4)
            endian = "<" if bom == b"\x4d\x3c\x2b\x1a" else ">"
            (length,) = struct.unpack(endian + "I", head[4:8])
            fh.read(length - 12)
            first = False
            continue
        btype, length = struct.unpack(endian + "II", head)
        body = fh.read(length - 8)
        if len(body) < length - 8:
            return
        if btype == 0x0A0D0D0A:  # another section header
            continue
        if btype == 6:  # Enhanced Packet Block
            caplen = struct.unpack(endian + "I", body[12:16])[0]
            yield body[20 : 20 + caplen]
        elif btype == 3:  # Simple Packet Block
            yield body[4:]


def frames(fh: BinaryIO) -> Iterator[bytes]:
    magic = fh.read(4)
    if magic == PCAPNG_SHB:
        # Re-feed: the block type was the magic.
        yield from _pcapng_frames(_Prefixed(magic, fh))
    elif magic in PCAP_MAGIC:
        yield from _pcap_frames(fh, PCAP_MAGIC[magic])
    else:
        raise ValueError(f"not a pcap/pcapng stream (magic {magic!r})")


class _Prefixed:
    """File-like wrapper that replays a few already-consumed bytes."""

    def __init__(self, prefix: bytes, fh: BinaryIO):
        self.prefix = prefix
        self.fh = fh

    def read(self, n: int) -> bytes:
        if self.prefix:
            out, self.prefix = self.prefix[:n], self.prefix[n:]
            if len(out) < n:
                out += self.fh.read(n - len(out))
            return out
        return self.fh.read(n)


def trades(fh: BinaryIO) -> Iterator[Trade]:
    """All TOPS trade reports in a pcap stream."""
    for frame in frames(fh):
        payload = _udp_payload(frame)
        if payload is None or len(payload) < 40:
            continue
        (_, _, proto, _, _, plen, count, _, _, _) = _TP_HEADER.unpack_from(payload, 0)
        if proto not in TOPS_PROTOCOLS:
            continue
        off = 40
        end = min(len(payload), 40 + plen)
        for _ in range(count):
            if off + 2 > end:
                break
            (mlen,) = _U16.unpack_from(payload, off)
            off += 2
            if payload[off] == 0x54 and mlen >= 38:
                _, flags, ts, sym, size, price, _ = _TRADE.unpack_from(payload, off)
                yield Trade(sym.decode("ascii").rstrip(), price / 10_000.0, size, flags, ts)
            off += mlen


def daily_summary(fh: BinaryIO) -> pd.DataFrame:
    """Per-symbol regular-session, round-lot trade summary for one session."""
    acc: dict[str, list] = {}
    for t in trades(fh):
        if t.flags & (FLAG_EXTENDED | FLAG_ODD_LOT):
            continue
        a = acc.get(t.symbol)
        if a is None:
            acc[t.symbol] = [t.price, t.price, t.price, t.price, t.size, t.ts, t.ts]
        else:
            if t.ts >= a[6]:
                a[3], a[6] = t.price, t.ts
            if t.ts < a[5]:
                a[0], a[5] = t.price, t.ts
            a[1] = max(a[1], t.price)
            a[2] = min(a[2], t.price)
            a[4] += t.size
    rows = [
        {"symbol": s, "open": v[0], "high": v[1], "low": v[2], "close": v[3], "volume_iex": v[4], "last_ts": v[6]}
        for s, v in acc.items()
    ]
    return pd.DataFrame(rows, columns=["symbol", "open", "high", "low", "close", "volume_iex", "last_ts"])


def list_files(date: str, session: requests.Session | None = None) -> list[dict]:
    """HIST files for YYYYMMDD (empty list on non-trading days)."""
    s = session or requests.Session()
    resp = s.get(HIST_URL.format(date=date), timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def summarize_day(date: str, cache_dir: Path, session: requests.Session | None = None) -> pd.DataFrame | None:
    """Download-and-stream one day's TOPS file into a cached daily summary.
    The multi-GB capture is never stored; only the small summary is."""
    out = cache_dir / f"{date}.parquet"
    if out.exists():
        cached = pd.read_parquet(out)
        if len(cached):
            return cached
    s = session or requests.Session()
    tops = [f for f in list_files(date, s) if f.get("feed") == "TOPS"]
    if not tops:
        return None
    with s.get(tops[0]["link"], stream=True, timeout=120) as resp:
        resp.raise_for_status()
        resp.raw.decode_content = False
        with gzip.GzipFile(fileobj=resp.raw) as gz:
            df = daily_summary(io.BufferedReader(gz, buffer_size=1 << 22))
    if df.empty:
        raise RuntimeError(f"no TOPS trades parsed for {date}; unknown capture format?")
    df.insert(0, "date", pd.Timestamp(date))
    cache_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    return df
