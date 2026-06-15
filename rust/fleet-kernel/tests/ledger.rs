//! Parity gate for the PS-D ledger write path vs live Python
//! `fleet_watch.events.log_event` + `fleet_watch.referee.claim_port`.
//!
//! Vectors in `ledger_vectors.json` are generated from Python with a FIXED
//! timestamp (monkeypatched `_now_iso`) so `claim_port` hashes are
//! deterministic and comparable. Every test runs against an in-memory temp DB —
//! it NEVER touches the live `~/.fleet-watch/registry.db`.

use fleet_kernel::events::{verify_chain, Event};
use fleet_kernel::ledger::{claim_port, get_last_hash, log_event, LedgerError};
use rusqlite::Connection;
use serde_json::Value;
use std::fs;

const PROCESSES_DDL: &str = "CREATE TABLE processes (
    pid INTEGER PRIMARY KEY, session_id TEXT NOT NULL, workstream TEXT NOT NULL,
    name TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 3, port INTEGER,
    gpu_mb INTEGER DEFAULT 0, repo_dir TEXT, model TEXT,
    restart_policy TEXT NOT NULL DEFAULT 'ALERT_ONLY', start_cmd TEXT,
    start_time TEXT NOT NULL, last_heartbeat TEXT NOT NULL,
    expected_duration_min INTEGER, UNIQUE(port), UNIQUE(repo_dir))";

const EVENTS_DDL: &str = "CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
    event_type TEXT NOT NULL, pid INTEGER, workstream TEXT, detail TEXT,
    prev_hash TEXT, hash TEXT NOT NULL)";

fn build_db() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    conn.execute_batch(PROCESSES_DDL).expect("create processes");
    conn.execute_batch(EVENTS_DDL).expect("create events");
    conn
}

fn vectors() -> Value {
    let p = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/ledger_vectors.json");
    serde_json::from_str(&fs::read_to_string(p).expect("ledger_vectors.json must exist"))
        .expect("ledger_vectors.json must be valid JSON")
}

#[test]
fn claim_port_matches_python_chain() {
    let v = vectors();
    let ts = v["fixed_ts"].as_str().unwrap();
    let conn = build_db();

    // 1. Claim a free port → CLAIM event.
    let d1 = claim_port(&conn, ts, 5000);
    assert_eq!(d1.allowed, v["claim_free"]["allowed"].as_bool().unwrap());
    assert_eq!(d1.reason, v["claim_free"]["reason"].as_str().unwrap());

    // 2. Insert a holder, then claim the taken port → CONFLICT event.
    conn.execute(
        "INSERT INTO processes (pid, session_id, workstream, name, port, start_time, last_heartbeat) \
         VALUES (999,'s1','ws','demo',4242,?1,?2)",
        rusqlite::params![ts, ts],
    )
    .unwrap();
    let d2 = claim_port(&conn, ts, 4242);
    assert_eq!(d2.allowed, v["claim_taken"]["allowed"].as_bool().unwrap());
    assert_eq!(d2.reason, v["claim_taken"]["reason"].as_str().unwrap());

    // 3. The persisted event rows must match Python byte-for-byte.
    let mut stmt = conn
        .prepare("SELECT timestamp,event_type,detail,prev_hash,hash FROM events ORDER BY id")
        .unwrap();
    let got: Vec<(String, String, String, String, String)> = stmt
        .query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .unwrap()
        .map(|x| x.unwrap())
        .collect();
    let want = v["events"].as_array().unwrap();
    assert_eq!(got.len(), want.len(), "event count");
    for (g, w) in got.iter().zip(want) {
        assert_eq!(g.0, w["timestamp"].as_str().unwrap(), "timestamp");
        assert_eq!(g.1, w["event_type"].as_str().unwrap(), "event_type");
        assert_eq!(g.2, w["detail"].as_str().unwrap(), "detail");
        assert_eq!(g.3, w["prev_hash"].as_str().unwrap(), "prev_hash");
        assert_eq!(g.4, w["hash"].as_str().unwrap(), "hash vs Python");
    }

    // 4. The resulting chain verifies (PS-C verifier over the written rows).
    let events: Vec<Event> = got
        .iter()
        .map(|g| Event {
            timestamp: g.0.clone(),
            event_type: g.1.clone(),
            detail: g.2.clone(),
            prev_hash: g.3.clone(),
            hash: g.4.clone(),
        })
        .collect();
    assert_eq!(verify_chain(&events), (true, 2));
}

#[test]
fn get_last_hash_empty_is_genesis() {
    let conn = build_db();
    assert_eq!(get_last_hash(&conn).unwrap(), "genesis");
}

#[test]
fn log_event_rejects_unknown_type_and_writes_nothing() {
    let conn = build_db();
    let r = log_event(
        &conn,
        "2026-01-01T00:00:00+00:00",
        "NOT_A_TYPE",
        None,
        None,
        "{}",
    );
    assert!(matches!(r, Err(LedgerError::UnknownEventType(_))));
    // fail-closed: nothing written
    assert_eq!(get_last_hash(&conn).unwrap(), "genesis");
}

#[test]
fn log_event_extends_chain() {
    let conn = build_db();
    let id1 = log_event(
        &conn,
        "2026-01-01T00:00:00+00:00",
        "HEARTBEAT",
        Some(7),
        Some("ws"),
        "{}",
    )
    .unwrap();
    assert_eq!(id1, 1);
    let h1 = get_last_hash(&conn).unwrap();
    assert_ne!(h1, "genesis");
    log_event(
        &conn,
        "2026-01-01T00:00:01+00:00",
        "RELEASE",
        None,
        None,
        "{}",
    )
    .unwrap();
    let h2 = get_last_hash(&conn).unwrap();
    assert_ne!(h2, h1);
}
