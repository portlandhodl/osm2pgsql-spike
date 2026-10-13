#!/usr/bin/env python3
"""Generate the .osm.pbf test fixtures consumed by the tests in src/pbf.rs.

Run with the project venv:  /tmp/opencode/bddvenv/bin/python3 gen_fixtures.py

Produces (next to this script):
  fixture_nodes.osm.pbf  3 dense nodes: tagged (unicode), untagged, 1-tag
                         (1 OSMData block)
  fixture_ways.osm.pbf   2 ways (tagged, untagged) + 1 relation (skipped);
                         libosmium flushes ways vs relations into separate
                         blocks, so this file has 2 OSMData blocks (the second
                         decodes to zero entities)
  fixture_all.osm.pbf    binary concatenation of the two: header/data/header/
                         data+data — used to test cross-block emit ordering.
"""

import os
import struct
import sys
import zlib

import osmium
import osmium.osm

HERE = os.path.dirname(os.path.abspath(__file__))


def write_nodes(path):
    w = osmium.SimpleWriter(path)
    # Tagged node, unicode tags. libosmium keeps dict insertion order in the
    # tag list: highway, name, shop.
    w.add_node(
        osmium.osm.mutable.Node(
            id=1001,
            version=3,
            location=osmium.osm.Location(6.1234567, 49.654321),
            tags={"highway": "bus_stop", "name": "Café Münchën ☕", "shop": "bäckerei"},
        )
    )
    # Untagged node, negative coordinates.
    w.add_node(
        osmium.osm.mutable.Node(
            id=1002,
            version=1,
            location=osmium.osm.Location(-1.2345678, -33.3),
        )
    )
    # Single-tag node exactly on the equator / greenwich origin.
    w.add_node(
        osmium.osm.mutable.Node(
            id=1005,
            version=1,
            location=osmium.osm.Location(0.0, 0.0),
            tags={"natural": "peak"},
        )
    )
    w.close()


def write_ways(path):
    w = osmium.SimpleWriter(path)
    w.add_way(
        osmium.osm.mutable.Way(
            id=90001,
            version=2,
            nodes=[1001, 1002, 1005],
            tags={"highway": "residential", "name": "Grande-Rue"},
        )
    )
    w.add_way(
        osmium.osm.mutable.Way(
            id=90002,
            version=1,
            nodes=[1005, 1001],
        )
    )
    # A relation, which the Rust decoder must skip entirely.
    w.add_relation(
        osmium.osm.mutable.Relation(
            id=77,
            version=1,
            members=[("w", 90001, "outer"), ("w", 90002, "")],
            tags={"type": "multipolygon"},
        )
    )
    w.close()


def read_frame(data, off):
    """Return (blob_type, header_bytes, payload_bytes, next_offset)."""
    (hlen,) = struct.unpack(">I", data[off : off + 4])
    hstart = off + 4
    hdr = data[hstart : hstart + hlen]
    j = 0
    typ = None
    ds = None
    while j < len(hdr):
        tag, j = _varint(hdr, j)
        fn, wt = tag >> 3, tag & 7
        if wt == 2:
            ln, j = _varint(hdr, j)
            if fn == 1:
                typ = hdr[j : j + ln].decode()
            j += ln
        elif wt == 0:
            v, j = _varint(hdr, j)
            if fn == 3:
                ds = v
        else:
            raise ValueError(f"unexpected wire type {wt}")
    pstart = hstart + hlen
    end = pstart + ds
    return typ, data[off:end], data[pstart:end], end


def _varint(b, j):
    v = 0
    s = 0
    while True:
        x = b[j]
        j += 1
        v |= (x & 0x7F) << s
        if not (x & 0x80):
            return v, j
        s += 7


def _varint_bytes(v):
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def data_frame(payload):
    """Wrap a Blob payload in an OSMData BlobHeader + 4-byte length prefix."""
    hdr = b"\x0a\x07OSMData\x18" + _varint_bytes(len(payload))
    return struct.pack(">I", len(hdr)) + hdr + payload


def write_raw_variant(src_nodes, dst):
    """Rewrite the nodes fixture into an uncompressed (raw blob) PBF."""
    with open(src_nodes, "rb") as f:
        data = f.read()
    typ, header_frame, _, off = read_frame(data, 0)
    assert typ == "OSMHeader"
    typ, _, blob, off = read_frame(data, off)
    assert typ == "OSMData" and off == len(data)
    # Blob wire: field2 raw_size (0x10, varint), field3 zlib_data (0x1a, bytes)
    j = 0
    raw_size = None
    zdata = None
    while j < len(blob):
        tag, j = _varint(blob, j)
        fn, wt = tag >> 3, tag & 7
        if wt == 0:
            v, j = _varint(blob, j)
            if fn == 2:
                raw_size = v
        elif wt == 2:
            ln, j = _varint(blob, j)
            if fn == 3:
                zdata = blob[j : j + ln]
            j += ln
        else:
            raise ValueError(f"unexpected wire type {wt} in blob")
    assert zdata is not None and raw_size is not None
    raw = zlib.decompress(zdata)
    assert len(raw) == raw_size
    new_blob = b"\x0a" + _varint_bytes(len(raw)) + raw + b"\x10" + _varint_bytes(raw_size)
    with open(dst, "wb") as f:
        f.write(header_frame)
        f.write(data_frame(new_blob))


def write_lz4_variant(src_nodes, dst):
    """PBF whose data blob claims lz4 compression (decoder must reject it)."""
    with open(src_nodes, "rb") as f:
        data = f.read()
    typ, header_frame, _, _ = read_frame(data, 0)
    assert typ == "OSMHeader"
    blob = b"\x32\x07garbage"  # field 6 (lz4_data, tag 0x32) with junk
    with open(dst, "wb") as f:
        f.write(header_frame)
        f.write(data_frame(blob))


def splice_frames(*srcs):
    """Collect complete framed blobs from valid PBF files; returns
    (header_frame, [data_frames])."""
    header = None
    frames = []
    for path in srcs:
        with open(path, "rb") as f:
            data = f.read()
        off = 0
        while off < len(data):
            typ, frame, _, off = read_frame(data, off)
            if typ == "OSMHeader":
                if header is None:
                    header = frame
            else:
                frames.append(frame)
    return header, frames


def main():
    nodes = os.path.join(HERE, "fixture_nodes.osm.pbf")
    ways = os.path.join(HERE, "fixture_ways.osm.pbf")
    allf = os.path.join(HERE, "fixture_all.osm.pbf")
    write_nodes(nodes)
    write_ways(ways)
    write_raw_variant(nodes, os.path.join(HERE, "fixture_raw.osm.pbf"))
    write_lz4_variant(nodes, os.path.join(HERE, "fixture_lz4.osm.pbf"))
    # Multi-OSMData-block file for cross-block emit-ordering tests: single
    # header + node block + way block + relation-only block (valid framing,
    # acceptable to strict readers such as osmium itself).
    header, frames = splice_frames(nodes, ways)
    with open(allf, "wb") as out:
        out.write(header)
        for fr in frames:
            out.write(fr)
    for p in (nodes, ways, allf):
        print(f"wrote {p} ({os.path.getsize(p)} bytes)")

    # Verify with an independent reader and dump the contents so the
    # expectations in src/pbf.rs can be cross-checked.
    for p in (nodes, ways):
        print(f"--- verifying {p}")
        for o in osmium.FileProcessor(p):
            if o.is_node():
                print(f"node {o.id} lon={o.location.lon} lat={o.location.lat} tags={dict(o.tags)}")
            elif o.is_way():
                print(f"way {o.id} refs={[n.ref for n in o.nodes]} tags={dict(o.tags)}")
            elif o.is_relation():
                print(f"relation {o.id} tags={dict(o.tags)}")


if __name__ == "__main__":
    sys.exit(main())
