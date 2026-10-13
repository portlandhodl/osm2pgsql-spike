mod db;
mod pbf;
mod rowfmt;

use dashmap::DashMap;
use pbf::Entity;
use rayon::prelude::*;
use std::env;
use std::path::Path;
use std::time::Instant;

const FLUSH_LIMIT: usize = 8 * 1024 * 1024;

enum Row {
    Node(String),
    Way(String),
}

/// osmium::Location stores coordinates as int32 with 1e-7 degree
/// precision (location.hpp), rounding with std::round (half away from
/// zero, same as f64::round). Mirror that quantization so coordinates
/// match osm2pgsql exactly.
fn quantize(deg: f64) -> f64 {
    (deg * 1e7).round() as i32 as f64 / 1e7
}

fn format_node(id: i64, lat: f64, lon: f64, tags: &[(String, String)]) -> String {
    let (x, y) = rowfmt::to_mercator(lat, lon);
    let mut row = String::with_capacity(128);
    row.push_str(&id.to_string());
    row.push('\t');
    if !tags.is_empty() {
        rowfmt::append_hstore(&mut row, tags);
    }
    row.push('\t');
    rowfmt::append_point_hex(&mut row, x, y);
    row.push('\n');
    row
}

fn format_way(
    w: &pbf::RawWay,
    store: &DashMap<i64, (f64, f64)>,
) -> Option<String> {
    let mut pts: Vec<(f64, f64)> = Vec::with_capacity(w.refs.len());
    for r in &w.refs {
        if let Some(loc) = store.get(r) {
            let (lat, lon) = *loc;
            pts.push(rowfmt::to_mercator(lat, lon));
        } else {
            // osm2pgsql drops ways whose node locations are unknown.
            return None;
        }
    }
    let mut row = String::with_capacity(128 + pts.len() * 32);
    row.push_str(&w.id.to_string());
    row.push('\t');
    if !w.tags.is_empty() {
        rowfmt::append_hstore(&mut row, &w.tags);
    }
    row.push('\t');
    rowfmt::append_linestring_hex(&mut row, &pts);
    row.push('\n');
    Some(row)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: spike <dbname> <input.osm.pbf> [threads]");
        std::process::exit(2);
    }
    let dbname = &args[1];
    let input = &args[2];
    let threads: usize = if args.len() > 3 {
        args[3].parse().expect("threads must be a number")
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    };
    println!("spike: db={} input={} threads={}", dbname, input, threads);

    db::setup_db(dbname);
    let workers = db::start_copy_workers(dbname);

    let store: DashMap<i64, (f64, f64)> = DashMap::with_capacity(8 << 20);
    let mut counts = (0u64, 0u64);

    let t0 = Instant::now();

    let mut buf_nodes = String::with_capacity(FLUSH_LIMIT + (1 << 20));
    let mut buf_ways = String::with_capacity(FLUSH_LIMIT + (1 << 20));

    pbf::read_pbf_ordered(Path::new(input), threads, &mut |entities| {
        // Insert node locations and format rows in parallel within the block.
        let rows: Vec<Row> = entities
            .par_iter()
            .filter_map(|e| match e {
                Entity::Node(n) => {
                    let lat = quantize(n.lat);
                    let lon = quantize(n.lon);
                    store.insert(n.id, (lat, lon));
                    // osm2pgsql flex only emits rows for tagged objects
                    if n.tags.is_empty() {
                        None
                    } else {
                        Some(Row::Node(format_node(n.id, lat, lon, &n.tags)))
                    }
                }
                Entity::Way(w) => {
                    if w.tags.is_empty() {
                        None // osm2pgsql flex only emits rows for tagged objects
                    } else {
                        format_way(w, &store).map(Row::Way)
                    }
                }
            })
            .collect();

        for row in rows {
            match row {
                Row::Node(r) => {
                    buf_nodes.push_str(&r);
                    counts.0 += 1;
                }
                Row::Way(r) => {
                    buf_ways.push_str(&r);
                    counts.1 += 1;
                }
            }
        }
        if buf_nodes.len() >= FLUSH_LIMIT {
            workers
                .nodes
                .send(std::mem::take(&mut buf_nodes))
                .expect("node copy worker died");
            buf_nodes = String::with_capacity(FLUSH_LIMIT + (1 << 20));
        }
        if buf_ways.len() >= FLUSH_LIMIT {
            workers
                .ways
                .send(std::mem::take(&mut buf_ways))
                .expect("way copy worker died");
            buf_ways = String::with_capacity(FLUSH_LIMIT + (1 << 20));
        }
    });

    if !buf_nodes.is_empty() {
        workers.nodes.send(buf_nodes).expect("node worker died");
    }
    if !buf_ways.is_empty() {
        workers.ways.send(buf_ways).expect("way worker died");
    }
    drop(workers.nodes);
    drop(workers.ways);
    for h in workers.join {
        h.join().expect("copy worker panicked");
    }

    let elapsed = t0.elapsed();
    let total = counts.0 + counts.1;
    println!(
        "spike: {} nodes + {} ways = {} rows in {:.2}s ({:.0} rows/s)",
        counts.0,
        counts.1,
        total,
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64()
    );
}
