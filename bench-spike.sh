#!/bin/bash
# Head-to-head: C++ osm2pgsql (spike.lua, slim) vs Rust spike.
set -u
INPUT=/tmp/opencode/luxembourg.osm.pbf
CPP=/home/qrsnap/git/osm2pgsql/build/osm2pgsql
RS=/tmp/opencode/spike/target/release/spike
REPS=${1:-3}

run_case() { # name, command...
    local name=$1; shift
    local best=999999 total=0
    for i in $(seq 1 $REPS); do
        local t
        t=$(pg_virtualenv bash -c "
            createdb osm 2>/dev/null
            psql -d osm -qc 'CREATE EXTENSION postgis; CREATE EXTENSION hstore' 2>/dev/null
            t0=\$(date +%s%N)
            $* >/dev/null 2>&1
            t1=\$(date +%s%N)
            echo \$(( (t1-t0)/1000000 ))
        " | grep -E '^[0-9]+$')
        echo "  $name run$i: ${t} ms"
        [ "$t" -lt "$best" ] && best=$t
        total=$((total + t))
    done
    echo ">> $name: min=${best} ms, avg=$((total / REPS)) ms"
}

echo "=== input: $INPUT ($(du -h $INPUT | cut -f1)) ==="
run_case "cpp-rammiddle-procs12" "$CPP -c -d osm -O flex -S /tmp/opencode/spike.lua $INPUT --log-level=error"
run_case "cpp-rammiddle-procs4" "$CPP -c -d osm -O flex -S /tmp/opencode/spike.lua --number-processes=4 $INPUT --log-level=error"
run_case "rust-threads-12"   "$RS osm $INPUT 12"
run_case "rust-threads-4"    "$RS osm $INPUT 4"
run_case "rust-threads-1"    "$RS osm $INPUT 1"
