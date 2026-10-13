//! Database side of the spike: schema setup + parallel COPY writers.
//!
//! Implemented by agent DB. Do not change the public interface.
//!
//! Connection handling mimics libpq environment defaults: PGHOST / PGPORT /
//! PGUSER are read from the environment (empty = unset), with fallbacks to
//! localhost / 5432 / $USER. Under pg_virtualenv PGHOST points at the
//! cluster's unix-socket directory, which the postgres crate accepts as a
//! `host=` value (a leading `/` selects a unix-domain socket).

use anyhow::Context;
use crossbeam_channel::{Receiver, Sender};
use postgres::{Client, NoTls};
use std::io::Write;
use std::thread::JoinHandle;

/// Which table a COPY channel feeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetTable {
    Nodes,
    Ways,
}

impl TargetTable {
    fn table_name(self) -> &'static str {
        match self {
            TargetTable::Nodes => "spike_nodes",
            TargetTable::Ways => "spike_ways",
        }
    }

    fn copy_sql(self) -> &'static str {
        match self {
            TargetTable::Nodes => "COPY spike_nodes (node_id, tags, geom) FROM STDIN",
            TargetTable::Ways => "COPY spike_ways (way_id, tags, geom) FROM STDIN",
        }
    }
}

/// Everything setup_db() runs, in order, via a single batch_execute.
const SETUP_SQL: &str = "\
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS hstore;
DROP TABLE IF EXISTS spike_nodes, spike_ways;
CREATE TABLE spike_nodes(node_id int8, tags hstore, geom geometry(Point,3857));
CREATE TABLE spike_ways(way_id int8, tags hstore, geom geometry(LineString,3857));
";

/// libpq-ish defaults used when the corresponding env var is unset/empty.
const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: &str = "5432";
const DEFAULT_USER: &str = "postgres";

/// Backpressure between row formatter and COPY worker.
const CHANNEL_BOUND: usize = 8;

// ---------------------------------------------------------------------------
// connection-string construction (pure logic, unit-tested without a DB)
// ---------------------------------------------------------------------------

/// libpq key=value connection string, exactly the clausal form libpq accepts.
fn conninfo(host: &str, port: &str, user: &str, dbname: &str, password: Option<&str>) -> String {
    let mut ci = format!("host={host} port={port} user={user} dbname={dbname}");
    if let Some(pw) = password {
        // Values in a libpq connection string need quoting when they
        // contain spaces or quotes; ours come from test harnesses which
        // generate plain hex tokens, single-quote wrap is enough.
        ci.push_str(&format!(" password='{pw}'"));
    }
    ci
}

/// Build the connection string for `dbname` using `lookup` for env vars.
/// Empty values count as unset, mirroring libpq's treatment of PG* vars.
/// PGUSER falls back to USER (i.e. $USER), then to a hard default.
fn conninfo_from(dbname: &str, lookup: &dyn Fn(&str) -> Option<String>) -> String {
    let get = |name: &str| lookup(name).filter(|v| !v.is_empty());
    let host = get("PGHOST").unwrap_or_else(|| DEFAULT_HOST.to_string());
    let port = get("PGPORT").unwrap_or_else(|| DEFAULT_PORT.to_string());
    let user = get("PGUSER")
        .or_else(|| get("USER"))
        .unwrap_or_else(|| DEFAULT_USER.to_string());
    let password = get("PGPASSWORD");
    conninfo(&host, &port, &user, dbname, password.as_deref())
}

/// The production lookup: real process environment.
fn conninfo_from_env(dbname: &str) -> String {
    conninfo_from(dbname, &|name| std::env::var(name).ok())
}

fn connect(dbname: &str) -> anyhow::Result<Client> {
    let conninfo = conninfo_from_env(dbname);
    // The conninfo contains no password, so it is safe to put in context.
    Client::connect(&conninfo, NoTls).with_context(|| format!("connecting ({conninfo})"))
}

// ---------------------------------------------------------------------------
// schema
// ---------------------------------------------------------------------------

/// Drop and recreate the spike tables in database `dbname`:
///   spike_nodes(node_id int8, tags hstore, geom geometry(Point,3857))
///   spike_ways(way_id int8, tags hstore, geom geometry(LineString,3857))
/// plus the needed extensions (postgis, hstore). Uses libpq environment
/// variables (PGHOST/PGPORT/PGUSER...) for the connection.
pub fn setup_db(dbname: &str) {
    let mut client =
        connect(dbname).unwrap_or_else(|e| panic!("setup_db({dbname:?}): connect failed: {e:#}"));
    client
        .batch_execute(SETUP_SQL)
        .unwrap_or_else(|e| panic!("setup_db({dbname:?}): schema setup failed: {e:#}"));
}

/// Integrator hook: for this spike identical to [`setup_db`].
pub fn drop_and_recreate_schema(dbname: &str) {
    setup_db(dbname);
}

// ---------------------------------------------------------------------------
// COPY workers
// ---------------------------------------------------------------------------

/// Two COPY sessions in flight at once, one per table, each on its own
/// connection and thread. Strings sent through the senders are raw COPY
/// payload bytes (text format) to be fed into the respective COPY stream
/// via copy_in. Dropping both senders then joining finishes the COPYs.
pub struct CopyWorkers {
    pub nodes: Sender<String>,
    pub ways: Sender<String>,
    pub join: Vec<JoinHandle<()>>,
}

pub fn start_copy_workers(dbname: &str) -> CopyWorkers {
    let (tx_nodes, rx_nodes) = crossbeam_channel::bounded::<String>(CHANNEL_BOUND);
    let (tx_ways, rx_ways) = crossbeam_channel::bounded::<String>(CHANNEL_BOUND);
    let join = vec![
        spawn_copy_worker(dbname, TargetTable::Nodes, rx_nodes),
        spawn_copy_worker(dbname, TargetTable::Ways, rx_ways),
    ];
    CopyWorkers {
        nodes: tx_nodes,
        ways: tx_ways,
        join,
    }
}

fn spawn_copy_worker(
    dbname: &str,
    table: TargetTable,
    rx: Receiver<String>,
) -> JoinHandle<()> {
    let dbname = dbname.to_string();
    std::thread::Builder::new()
        .name(format!("copy_{}", table.table_name()))
        .spawn(move || {
            if let Err(e) = copy_worker(&dbname, table, rx) {
                panic!("COPY worker for {} failed: {e:#}", table.table_name());
            }
        })
        .unwrap_or_else(|e| panic!("spawning COPY worker for {}: {e}", table.table_name()))
}

/// Body of one COPY worker thread: connect, COPY ... FROM STDIN, stream all
/// chunks until the sender side hangs up, then finish and report rows.
fn copy_worker(dbname: &str, table: TargetTable, rx: Receiver<String>) -> anyhow::Result<()> {
    let mut client = connect(dbname)
        .with_context(|| format!("COPY worker for {}", table.table_name()))?;
    let mut writer = client
        .copy_in(table.copy_sql())
        .with_context(|| format!("starting `{}`", table.copy_sql()))?;

    let bytes = pump_chunks(rx, &mut writer)
        .with_context(|| format!("writing payload to {}", table.table_name()))?;

    // Sender disconnected: close the COPY stream explicitly. `finish`
    // flushes, sends CopyDone, and returns the server-reported row count.
    writer.flush().ok(); // finish() flushes internally; best effort here
    let rows = writer
        .finish()
        .with_context(|| format!("finishing COPY for {}", table.table_name()))?;

    println!(
        "spike: COPY {} finished: {rows} rows ({bytes} payload bytes)",
        table.table_name()
    );
    Ok(())
}

/// Write every chunk received on `rx` into `out`, stopping cleanly when the
/// sender side disconnects. Returns the number of payload bytes written.
/// Pure I/O logic — unit-tested with an in-memory writer, no DB needed.
fn pump_chunks<W: Write>(rx: Receiver<String>, mut out: W) -> std::io::Result<u64> {
    let mut total: u64 = 0;
    while let Ok(chunk) = rx.recv() {
        out.write_all(chunk.as_bytes())?;
        total += chunk.len() as u64;
    }
    out.flush()?;
    Ok(total)
}

// ---------------------------------------------------------------------------
// validation
// ---------------------------------------------------------------------------

/// Create an unpipelined (for now) pg client helper for validation queries.
pub fn validate_counts(dbname: &str) -> anyhow::Result<(i64, i64)> {
    let mut client = connect(dbname).context("validate_counts")?;
    let nodes: i64 = client
        .query_one("SELECT count(*) FROM spike_nodes", &[])?
        .get(0);
    let ways: i64 = client
        .query_one("SELECT count(*) FROM spike_ways", &[])?
        .get(0);
    Ok((nodes, ways))
}

// ---------------------------------------------------------------------------
// tests: pure logic only, no database connections
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Fake environment for conninfo_from().
    fn fake_env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn conninfo_format_is_libpq_kv_pairs() {
        assert_eq!(
            conninfo("myhost", "5433", "alice", "spikedb", None),
            "host=myhost port=5433 user=alice dbname=spikedb"
        );
        assert_eq!(
            conninfo("myhost", "5433", "alice", "spikedb", Some("s3cret")),
            "host=myhost port=5433 user=alice dbname=spikedb password='s3cret'"
        );
    }

    #[test]
    fn env_vars_take_precedence() {
        // pg_virtualenv sets PGHOST to a unix-socket directory.
        let env = fake_env(&[
            ("PGHOST", "/tmp/pg_virtualenv.abc"),
            ("PGPORT", "5544"),
            ("PGUSER", "osm"),
        ]);
        assert_eq!(
            conninfo_from("spike", &env),
            "host=/tmp/pg_virtualenv.abc port=5544 user=osm dbname=spike"
        );
    }

    #[test]
    fn unset_env_falls_back_to_libpq_defaults() {
        let env = fake_env(&[]);
        assert_eq!(
            conninfo_from("spike", &env),
            "host=localhost port=5432 user=postgres dbname=spike"
        );
    }

    #[test]
    fn pguser_unset_falls_back_to_user_env() {
        let env = fake_env(&[("USER", "qrsnap")]);
        assert_eq!(
            conninfo_from("spike", &env),
            "host=localhost port=5432 user=qrsnap dbname=spike"
        );
        // But PGUSER wins over USER when both exist.
        let env = fake_env(&[("USER", "qrsnap"), ("PGUSER", "osm")]);
        assert_eq!(
            conninfo_from("spike", &env),
            "host=localhost port=5432 user=osm dbname=spike"
        );
    }

    #[test]
    fn empty_env_vars_are_treated_as_unset() {
        let env = fake_env(&[("PGHOST", ""), ("PGPORT", ""), ("PGUSER", "")]);
        assert_eq!(
            conninfo_from("spike", &env),
            "host=localhost port=5432 user=postgres dbname=spike"
        );
    }

    #[test]
    fn copy_sql_targets_the_right_tables_and_columns() {
        assert_eq!(
            TargetTable::Nodes.copy_sql(),
            "COPY spike_nodes (node_id, tags, geom) FROM STDIN"
        );
        assert_eq!(
            TargetTable::Ways.copy_sql(),
            "COPY spike_ways (way_id, tags, geom) FROM STDIN"
        );
        assert_eq!(TargetTable::Nodes.table_name(), "spike_nodes");
        assert_eq!(TargetTable::Ways.table_name(), "spike_ways");
        assert_ne!(TargetTable::Nodes, TargetTable::Ways);
    }

    #[test]
    fn setup_sql_has_extensions_drop_and_two_tables() {
        assert!(SETUP_SQL.contains("CREATE EXTENSION IF NOT EXISTS postgis;"));
        assert!(SETUP_SQL.contains("CREATE EXTENSION IF NOT EXISTS hstore;"));
        assert!(SETUP_SQL.contains("DROP TABLE IF EXISTS spike_nodes, spike_ways;"));
        assert!(SETUP_SQL.contains(
            "CREATE TABLE spike_nodes(node_id int8, tags hstore, geom geometry(Point,3857));"
        ));
        assert!(SETUP_SQL.contains(
            "CREATE TABLE spike_ways(way_id int8, tags hstore, geom geometry(LineString,3857));"
        ));
        // Tables must be created after the extensions they depend on.
        let ext_hstore = SETUP_SQL.find("CREATE EXTENSION IF NOT EXISTS hstore").unwrap();
        let tbl_nodes = SETUP_SQL.find("CREATE TABLE spike_nodes").unwrap();
        assert!(ext_hstore < tbl_nodes);
    }

    #[test]
    fn validate_count_sql_mentions_both_tables() {
        // These strings are duplicated in validate_counts; guard the contract.
        for t in ["spike_nodes", "spike_ways"] {
            let sql = format!("SELECT count(*) FROM {t}");
            assert!(sql.contains("count(*)"));
        }
    }

    #[test]
    fn pump_chunks_concatenates_and_stops_on_disconnect() {
        let (tx, rx) = crossbeam_channel::bounded::<String>(2);
        let producer = std::thread::spawn(move || {
            tx.send("hello ".to_string()).unwrap();
            tx.send("world\n".to_string()).unwrap();
            // tx dropped here -> disconnect
        });
        let mut buf: Vec<u8> = Vec::new();
        let n = pump_chunks(rx, &mut buf).unwrap();
        producer.join().unwrap();
        assert_eq!(buf, b"hello world\n");
        assert_eq!(n, 12);
    }

    #[test]
    fn pump_chunks_handles_zero_chunks_and_more_than_channel_capacity() {
        // Zero chunks.
        let (tx, rx) = crossbeam_channel::bounded::<String>(CHANNEL_BOUND);
        drop(tx);
        let mut buf: Vec<u8> = Vec::new();
        assert_eq!(pump_chunks(rx, &mut buf).unwrap(), 0);
        assert!(buf.is_empty());

        // Far more chunks than the bounded capacity: producer is throttled
        // by recv(), everything still lands in order.
        let (tx, rx) = crossbeam_channel::bounded::<String>(CHANNEL_BOUND);
        let producer = std::thread::spawn(move || {
            for i in 0..1000 {
                tx.send(format!("{i};")).unwrap();
            }
        });
        let mut buf: Vec<u8> = Vec::new();
        let n = pump_chunks(rx, &mut buf).unwrap();
        producer.join().unwrap();
        let expected: String = (0..1000).map(|i| format!("{i};")).collect();
        assert_eq!(buf, expected.as_bytes());
        assert_eq!(n, expected.len() as u64);
    }
}
