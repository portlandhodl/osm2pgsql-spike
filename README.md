# osm2pgsql Rust speed-spike

A standalone Rust prototype that re-implements the hot import path of
[osm2pgsql](https://osm2pgsql.org/) (PBF decode → mercator projection →
EWKB/hex serialization → PostgreSQL COPY) as a
[parallel pipeline](#architecture). Built to answer one question: *how much
faster could this be?*

**Status:** experiment / benchmark harness, not a product. It handles nodes
and ways only (no relations, no expire, no indexes, one fixed schema).
License: GPL-2.0-or-later (row formatting logic is ported from osm2pgsql;
geometry projection constants from libosmium, which is Boost Software
License).

## Results

Same rows in, same bytes out, same database — a 48 MB Luxembourg extract
(4.85M objects, 860k written rows), PostgreSQL 18, 12-core machine:

| Configuration | Wall time (min) |
|---|---|
| osm2pgsql 2.3.0 (flex output, RAM middle) | 4.6 s |
| the above + [our optimization branch][opt] | 4.4 s |
| **this spike** | **1.9 s** |

The interesting part: the spike is just as fast with a single decode
thread — the ~2 s floor is PostgreSQL's COPY ingest; parsing, projecting,
and serializing 4.85M objects costs almost nothing extra on top of it.
osm2pgsql's stage-1 processing is single-threaded by design; this prototype
formats COPY rows in parallel with rayon, which osm2pgsql's architecture
cannot do.

[opt]: https://github.com/portlandhodl/osm2pgsql

## Correctness

The comparison harness validated that the spike's database contents are
**identical** to osm2pgsql's for Liechtenstein (byte-for-byte, 0.1 mm
sense) and Luxembourg (860,334 rows; required replicating two subtle
behaviors: flex output routes untagged objects to separate callbacks, and
osmium's `Location` is fixed-precision 10⁻⁷°).

- Mercator projection is bit-identical to libosmium's fast polynomial
  (verified against a C++ oracle on a 10,301-point grid).
- COPY escaping / hstore literals fuzz-compared against the C++
  implementation over all byte values.

## Architecture

```
PBF file ──read──> [N decode workers: zlib+prost] ──ordered──> emit(block)
read thread         (parallel decompress+parse)                  (in file order)
                                                        |
                                      [rayon: parallel row formatting]
                                      node store (DashMap), mercator,
                                      escape + EWKB hex into COPY rows
                                                        |
                            8 MB chunks ──┬─────────────┴─────────────┐
                                          v                           v
                                 COPY spike_nodes thread       COPY spike_ways thread
                                 (own connection)              (own connection)
```

## Running it

Needs a PostgreSQL with PostGIS + hstore (env vars PGHOST/PGPORT/PGUSER/
PGPASSWORD are honored, like libpq):

```sh
cargo build --release
createdb spike_test
./target/release/spike spike_test input.osm.pbf [threads]
```

Tables `spike_nodes(node_id, tags, geom)` and `spike_ways(way_id, tags,
geom)` are dropped and recreated on each run.

`cargo test` runs the module test suites (fixture PBF files generated with
pyosmium are included under `testdata/`).
