#!/bin/bash
# Compare spike (Rust) vs osm2pgsql (C++) table contents on a given input.
set -e
INPUT=$1
NODES_SQL="COPY (SELECT node_id, (SELECT COALESCE(string_agg(k||'=®'||v, ',' ORDER BY k), '') FROM each(n.tags) AS e(k,v)) AS t, ST_AsText(ST_SnapToGrid(n.geom, 0.001), 9) AS g FROM spike_nodes n ORDER BY node_id) TO STDOUT"
WAYS_SQL="COPY (SELECT way_id, (SELECT COALESCE(string_agg(k||'=®'||v, ',' ORDER BY k), '') FROM each(w.tags) AS e(k,v)) AS t, ST_NPoints(w.geom), ST_AsText(ST_SnapToGrid(w.geom, 0.001), 9) AS g FROM spike_ways w ORDER BY way_id) TO STDOUT"

mkdir -p /tmp/opencode/cmp
pg_virtualenv bash -c "
set -e
createdb cpp; createdb rs
psql -d cpp -qc 'CREATE EXTENSION postgis; CREATE EXTENSION hstore'
/home/qrsnap/git/osm2pgsql/build/osm2pgsql -c -d cpp -O flex -S /tmp/opencode/spike.lua --slim '$INPUT' >/dev/null 2>&1
/tmp/opencode/spike/target/release/spike rs '$INPUT' >/dev/null 2>&1
psql -X -d cpp -c \"$NODES_SQL\" > /tmp/opencode/cmp/cpp.nodes
psql -X -d cpp -c \"$WAYS_SQL\"  > /tmp/opencode/cmp/cpp.ways
psql -X -d rs -c \"$NODES_SQL\" > /tmp/opencode/cmp/rs.nodes
psql -X -d rs -c \"$WAYS_SQL\"  > /tmp/opencode/cmp/rs.ways
"
mkdir -p /tmp/opencode/cmp
echo "row counts: cpp=$(wc -l < /tmp/opencode/cmp/cpp.nodes)/$(wc -l < /tmp/opencode/cmp/cpp.ways) rs=$(wc -l < /tmp/opencode/cmp/rs.nodes)/$(wc -l < /tmp/opencode/cmp/rs.ways)"
if cmp -s /tmp/opencode/cmp/cpp.nodes /tmp/opencode/cmp/rs.nodes; then echo "NODES IDENTICAL"; else echo "NODES DIFFER:"; diff /tmp/opencode/cmp/cpp.nodes /tmp/opencode/cmp/rs.nodes | head -10; echo "(diff lines: $(diff /tmp/opencode/cmp/cpp.nodes /tmp/opencode/cmp/rs.nodes | grep -c '^<'))"; fi
if cmp -s /tmp/opencode/cmp/cpp.ways /tmp/opencode/cmp/rs.ways; then echo "WAYS IDENTICAL"; else echo "WAYS DIFFER:"; diff /tmp/opencode/cmp/cpp.ways /tmp/opencode/cmp/rs.ways | head -10; echo "(diff lines: $(diff /tmp/opencode/cmp/cpp.ways /tmp/opencode/cmp/rs.ways | grep -c '^<'))"; fi
