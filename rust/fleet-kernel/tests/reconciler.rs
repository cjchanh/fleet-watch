//! Parity gate: Rust `reconciler::check_repo_with_session` vs the Python
//! reference output captured in `reconciler_vectors.json`.
//!
//! Each test vector contains:
//!   - scenario id + description
//!   - call args (repo_dir, current_session_id, write_scopes, exclusive)
//!   - alive_map: {str(pid): bool}  (which PIDs are considered alive)
//!   - expected decision (allowed, reason, safe_mode, holders, stale_holders, overlap_paths)
//!   - expected post-state (events rows, lease status, process table)
//!
//! The test builds an in-memory SQLite with the same fixture data as the Python
//! generator, injects a per-scenario MockSignaller (scripted alive/dead), and
//! verifies that the Rust output matches Python byte-for-byte.

use fleet_kernel::reconciler;
use fleet_kernel::Decision;
use rusqlite::Connection;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;

// ── Full schema for the test DB ───────────────────────────────────────────────

const FULL_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS processes (
    pid INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL,
    workstream TEXT NOT NULL,
    name TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 3,
    port INTEGER,
    gpu_mb INTEGER DEFAULT 0,
    repo_dir TEXT,
    model TEXT,
    restart_policy TEXT NOT NULL DEFAULT 'ALERT_ONLY',
    start_cmd TEXT,
    start_time TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL,
    expected_duration_min INTEGER,
    UNIQUE(port),
    UNIQUE(repo_dir)
);
CREATE TABLE IF NOT EXISTS session_leases (
    session_id TEXT PRIMARY KEY,
    owner_pid INTEGER,
    owner_ppid INTEGER,
    owner_pgid INTEGER,
    owner_tty TEXT,
    repo_dir TEXT,
    repo_lock_mode TEXT NOT NULL DEFAULT 'cooperative',
    write_scopes TEXT,
    started_at TEXT NOT NULL,
    last_heartbeat_at TEXT NOT NULL,
    shutdown_at TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE'
);
CREATE TABLE IF NOT EXISTS external_resources (
    provider TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    external_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    workstream TEXT NOT NULL,
    name TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 3,
    gpu_mb INTEGER DEFAULT 0,
    repo_dir TEXT,
    model TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    started_by TEXT,
    owner_tool TEXT,
    endpoint TEXT,
    cleanup_cmd TEXT,
    safe_to_delete INTEGER NOT NULL DEFAULT 0,
    metadata TEXT,
    start_time TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    PRIMARY KEY(provider, external_id)
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    event_type TEXT NOT NULL,
    pid INTEGER,
    workstream TEXT,
    detail TEXT,
    prev_hash TEXT,
    hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gpu_budget (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total_mb INTEGER NOT NULL DEFAULT 131072,
    reserve_mb INTEGER NOT NULL DEFAULT 16384,
    allocated_mb INTEGER NOT NULL DEFAULT 0
);
";

fn build_db() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    conn.execute_batch(FULL_SCHEMA).expect("create schema");
    // GPU budget singleton.
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id,total_mb,reserve_mb,allocated_mb) \
         VALUES (1,131072,16384,0)",
        [],
    )
    .expect("init gpu_budget");
    conn
}

// ── MockSignaller ─────────────────────────────────────────────────────────────

/// Scripted alive/dead signaller — issues NO real signals; deterministic.
struct MockSignaller {
    alive_map: HashMap<i64, bool>,
}

impl MockSignaller {
    fn from_value(map: &Value) -> Self {
        let mut alive_map = HashMap::new();
        if let Some(obj) = map.as_object() {
            for (k, v) in obj {
                if let (Ok(pid), Some(alive)) = (k.parse::<i64>(), v.as_bool()) {
                    alive_map.insert(pid, alive);
                }
            }
        }
        MockSignaller { alive_map }
    }
}

impl fleet_kernel::preempt::Signaller for MockSignaller {
    fn terminate(&self, _pid: i64) -> bool {
        false // never called by reconciler
    }
    fn is_alive(&self, pid: i64) -> bool {
        *self.alive_map.get(&pid).unwrap_or(&false)
    }
    fn wait_for_exit(&self, _pid: i64, _grace: u64) {}
}

// ── Vector loader ─────────────────────────────────────────────────────────────

fn vectors() -> Value {
    let p = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/reconciler_vectors.json");
    serde_json::from_str(&fs::read_to_string(p).expect("reconciler_vectors.json must exist"))
        .expect("reconciler_vectors.json must be valid JSON")
}

// ── Fixture helpers ───────────────────────────────────────────────────────────

const FIXED_TS: &str = "2026-06-13T12:00:00+00:00";
/// 300s before FIXED_TS (stale — > 180s).
const OLD_TS: &str = "2026-06-13T11:55:00+00:00";
/// 60s before FIXED_TS (fresh — < 180s).
const RECENT_TS: &str = "2026-06-13T11:59:00+00:00";
const REPO: &str = "/Users/cj/tmp/fleet-watch-test-repo";

fn insert_lease(
    conn: &Connection,
    sid: &str,
    owner_pid: Option<i64>,
    lock_mode: &str,
    write_scopes_json: Option<&str>,
    hb: &str,
) {
    conn.execute(
        "INSERT INTO session_leases \
         (session_id,owner_pid,repo_dir,repo_lock_mode,write_scopes,started_at,last_heartbeat_at,shutdown_at,status) \
         VALUES (?,?,?,?,?,?,?,NULL,'ACTIVE')",
        rusqlite::params![sid, owner_pid, REPO, lock_mode, write_scopes_json, hb, hb],
    )
    .expect("insert lease");
}

fn insert_process(conn: &Connection, pid: i64, sid: &str) {
    conn.execute(
        "INSERT INTO processes (pid,session_id,workstream,name,priority,gpu_mb,repo_dir,start_time,last_heartbeat) \
         VALUES (?,?,'ws','proc-holder',3,0,?,?,?)",
        rusqlite::params![pid, sid, REPO, FIXED_TS, FIXED_TS],
    )
    .expect("insert process");
}

// ── Setup for each scenario (mirrors Python gen_reconciler_vectors.py) ────────

fn setup_for_scenario(conn: &Connection, scenario: &str) {
    match scenario {
        "a_free_repo" => {} // nothing
        "b_owned_by_current_session" => {
            insert_lease(
                conn,
                "sess-current",
                Some(101),
                "cooperative",
                None,
                FIXED_TS,
            );
        }
        "c_foreign_live_cooperative_no_overlap" => {
            let scopes = r#"["/Users/cj/tmp/fleet-watch-test-repo/subdir-a"]"#;
            insert_lease(
                conn,
                "sess-foreign",
                Some(201),
                "cooperative",
                Some(scopes),
                FIXED_TS,
            );
        }
        "d_foreign_live_cooperative_overlap" => {
            let scopes = r#"["/Users/cj/tmp/fleet-watch-test-repo/src"]"#;
            insert_lease(
                conn,
                "sess-foreign",
                Some(201),
                "cooperative",
                Some(scopes),
                FIXED_TS,
            );
        }
        "e_foreign_exclusive" => {
            insert_lease(
                conn,
                "sess-exclusive",
                Some(201),
                "exclusive",
                None,
                FIXED_TS,
            );
        }
        "f_dead_stale_lease_gc" => {
            insert_lease(
                conn,
                "sess-dead-stale",
                Some(201),
                "cooperative",
                None,
                OLD_TS,
            );
        }
        "g_dead_recent_lease_held" => {
            insert_lease(
                conn,
                "sess-dead-recent",
                Some(201),
                "cooperative",
                None,
                RECENT_TS,
            );
        }
        "h_process_holder_dead_pid" => {
            insert_process(conn, 301, "sess-proc");
        }
        other => panic!("unknown scenario: {other}"),
    }
}

// ── Per-scenario call args ────────────────────────────────────────────────────

fn call_args(scenario: &str) -> (Option<String>, Vec<String>, bool) {
    match scenario {
        "a_free_repo" => (None, vec![], false),
        "b_owned_by_current_session" => (Some("sess-current".to_owned()), vec![], false),
        "c_foreign_live_cooperative_no_overlap" => (Some("sess-mine".to_owned()), vec![], false),
        "d_foreign_live_cooperative_overlap" => (
            Some("sess-mine".to_owned()),
            vec!["/Users/cj/tmp/fleet-watch-test-repo/src".to_owned()],
            false,
        ),
        "e_foreign_exclusive" => (Some("sess-mine".to_owned()), vec![], false),
        "f_dead_stale_lease_gc" => (Some("sess-mine".to_owned()), vec![], false),
        "g_dead_recent_lease_held" => (Some("sess-mine".to_owned()), vec![], false),
        "h_process_holder_dead_pid" => (None, vec![], false),
        other => panic!("unknown scenario: {other}"),
    }
}

// ── Assert helpers ────────────────────────────────────────────────────────────

fn assert_decision_matches(got: &Decision, want: &Value, scenario: &str) {
    let ctx = |field: &str| format!("[{scenario}] decision.{field}");

    assert_eq!(
        got.allowed,
        want["allowed"].as_bool().expect("allowed bool"),
        "{}",
        ctx("allowed")
    );
    assert_eq!(
        got.reason,
        want["reason"].as_str().expect("reason str"),
        "{}",
        ctx("reason")
    );
    // safe_mode
    let want_sm = match &want["safe_mode"] {
        Value::Null => None,
        v => Some(v.as_str().expect("safe_mode str").to_owned()),
    };
    assert_eq!(got.safe_mode, want_sm, "{}", ctx("safe_mode"));

    // holders count
    let want_holders = want["holders"].as_array().expect("holders array");
    assert_eq!(
        got.holders.len(),
        want_holders.len(),
        "{} holders count",
        ctx("holders")
    );

    // stale_holders count
    let want_stale = want["stale_holders"]
        .as_array()
        .expect("stale_holders array");
    assert_eq!(
        got.stale_holders.len(),
        want_stale.len(),
        "{} stale_holders count",
        ctx("stale_holders")
    );

    // overlap_paths
    let want_overlaps: Vec<&str> = want["overlap_paths"]
        .as_array()
        .expect("overlap_paths")
        .iter()
        .map(|v| v.as_str().expect("overlap str"))
        .collect();
    assert_eq!(
        got.overlap_paths,
        want_overlaps
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        "{}",
        ctx("overlap_paths")
    );

    // For holder/holders: check key fields (session_id, name, repo_lock_mode, write_scopes).
    for (i, (gh, wh)) in got.holders.iter().zip(want_holders.iter()).enumerate() {
        assert_eq!(
            gh["session_id"], wh["session_id"],
            "[{scenario}] holders[{i}].session_id"
        );
        assert_eq!(gh["name"], wh["name"], "[{scenario}] holders[{i}].name");
        assert_eq!(
            gh["repo_lock_mode"], wh["repo_lock_mode"],
            "[{scenario}] holders[{i}].repo_lock_mode"
        );
        assert_eq!(
            gh["write_scopes"], wh["write_scopes"],
            "[{scenario}] holders[{i}].write_scopes"
        );
    }

    // stale_holders: check session_id.
    for (i, (gs, ws)) in got.stale_holders.iter().zip(want_stale.iter()).enumerate() {
        assert_eq!(
            gs["session_id"], ws["session_id"],
            "[{scenario}] stale_holders[{i}].session_id"
        );
    }
}

fn assert_events_match(conn: &Connection, want_events: &[Value], scenario: &str) {
    let rows: Vec<(String, Option<i64>, Option<String>, String)> = {
        let mut stmt = conn
            .prepare("SELECT event_type,pid,workstream,detail FROM events ORDER BY id")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .map(|x| x.unwrap())
            .collect()
    };

    assert_eq!(rows.len(), want_events.len(), "[{scenario}] event count");

    for (i, (got_row, want_ev)) in rows.iter().zip(want_events.iter()).enumerate() {
        let ctx = |field: &str| format!("[{scenario}] events[{i}].{field}");
        assert_eq!(
            got_row.0,
            want_ev["event_type"].as_str().unwrap(),
            "{}",
            ctx("event_type")
        );
        // detail must match byte-for-byte (Python uses json.dumps with separators=(",",":"))
        assert_eq!(
            got_row.3,
            want_ev["detail"].as_str().unwrap(),
            "{}",
            ctx("detail")
        );
    }
}

fn assert_lease_status(conn: &Connection, want_leases: &[Value], scenario: &str) {
    let rows: Vec<(String, String)> = {
        let mut stmt = conn
            .prepare("SELECT session_id, status FROM session_leases ORDER BY session_id")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|x| x.unwrap())
            .collect()
    };
    assert_eq!(
        rows.len(),
        want_leases.len(),
        "[{scenario}] lease row count"
    );
    for (i, (got_row, want_lease)) in rows.iter().zip(want_leases.iter()).enumerate() {
        assert_eq!(
            got_row.0,
            want_lease["session_id"].as_str().unwrap(),
            "[{scenario}] leases[{i}].session_id"
        );
        assert_eq!(
            got_row.1,
            want_lease["status"].as_str().unwrap(),
            "[{scenario}] leases[{i}].status"
        );
    }
}

fn assert_process_count(conn: &Connection, want_processes: &[Value], scenario: &str) {
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM processes", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        count,
        want_processes.len() as i64,
        "[{scenario}] process count"
    );
}

// ── Main parity test ──────────────────────────────────────────────────────────

#[test]
fn reconciler_parity_all_scenarios() {
    let v = vectors();
    let scenarios = v.as_array().expect("vectors array");

    for sv in scenarios {
        let scenario = sv["scenario"].as_str().expect("scenario id");
        let want_decision = &sv["decision"];
        let want_events = sv["post_events"].as_array().expect("post_events");
        let want_leases = sv["post_leases"].as_array().expect("post_leases");
        let want_procs = sv["post_processes"].as_array().expect("post_processes");

        let mut conn = build_db();
        setup_for_scenario(&conn, scenario);

        let sig = MockSignaller::from_value(&sv["alive_map"]);
        let (current_sid, write_scopes, exclusive) = call_args(scenario);

        let got = reconciler::check_repo_with_session(
            &mut conn,
            FIXED_TS,
            REPO,
            current_sid.as_deref(),
            &write_scopes,
            exclusive,
            &sig,
        );

        assert_decision_matches(&got, want_decision, scenario);
        assert_events_match(&conn, want_events, scenario);
        assert_lease_status(&conn, want_leases, scenario);
        assert_process_count(&conn, want_procs, scenario);
    }
}

// ── Specific scenario tests (also serve as documentation) ─────────────────────

#[test]
fn scenario_a_free_repo_allows() {
    let mut conn = build_db();
    let sig = MockSignaller {
        alive_map: HashMap::new(),
    };
    let d = reconciler::check_repo(&mut conn, FIXED_TS, REPO, &sig);
    assert!(d.allowed, "free repo must allow");
    assert_eq!(d.reason, "repo available");
    assert!(d.holders.is_empty());
    assert!(d.stale_holders.is_empty());
}

#[test]
fn scenario_b_same_session_bypass() {
    let mut conn = build_db();
    insert_lease(&conn, "sess-mine", Some(100), "cooperative", None, FIXED_TS);
    let sig = MockSignaller {
        alive_map: [(100, true)].into_iter().collect(),
    };
    let d = reconciler::check_repo_with_session(
        &mut conn,
        FIXED_TS,
        REPO,
        Some("sess-mine"),
        &[],
        false,
        &sig,
    );
    assert!(d.allowed, "own lease must allow");
    assert_eq!(d.reason, "repo available (owned by current session)");
    assert_eq!(d.safe_mode.as_deref(), Some("same-session"));
}

#[test]
fn scenario_e_exclusive_blocks() {
    let mut conn = build_db();
    insert_lease(&conn, "sess-excl", Some(201), "exclusive", None, FIXED_TS);
    let sig = MockSignaller {
        alive_map: [(201, true)].into_iter().collect(),
    };
    let d = reconciler::check_repo_with_session(
        &mut conn,
        FIXED_TS,
        REPO,
        Some("sess-mine"),
        &[],
        false,
        &sig,
    );
    assert!(!d.allowed);
    assert!(d.reason.contains("exclusive session"));
}

#[test]
fn scenario_f_gc_dead_stale_lease() {
    let mut conn = build_db();
    insert_lease(&conn, "sess-dead", Some(201), "cooperative", None, OLD_TS);
    let sig = MockSignaller {
        alive_map: [(201, false)].into_iter().collect(),
    };
    let d = reconciler::check_repo_with_session(
        &mut conn,
        FIXED_TS,
        REPO,
        Some("sess-mine"),
        &[],
        false,
        &sig,
    );
    assert!(d.allowed, "dead+stale lease must be GC'd and allow");
    assert_eq!(d.reason, "repo available (stale session lease cleared)");
    assert_eq!(d.stale_holders.len(), 1);
    // Verify CLEAN event was written.
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE event_type='CLEAN'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 1, "one CLEAN event must be logged");
    // Verify lease is now CLOSED.
    let status: String = conn
        .query_row(
            "SELECT status FROM session_leases WHERE session_id='sess-dead'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(status, "CLOSED");
}

#[test]
fn scenario_h_dead_process_auto_released() {
    let mut conn = build_db();
    insert_process(&conn, 301, "sess-proc");
    let sig = MockSignaller {
        alive_map: [(301, false)].into_iter().collect(),
    };
    let d = reconciler::check_repo(&mut conn, FIXED_TS, REPO, &sig);
    assert!(d.allowed);
    assert_eq!(d.reason, "repo available (stale lock cleared)");
    // Process must be gone.
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM processes WHERE pid=301", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(n, 0, "dead process must be released");
    // CLEAN event logged.
    let ev_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE event_type='CLEAN'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(ev_count, 1);
}

#[test]
fn fail_closed_db_error_returns_deny() {
    // Open a DB with no schema at all — every query will fail.
    let mut conn = Connection::open_in_memory().unwrap();
    let sig = MockSignaller {
        alive_map: HashMap::new(),
    };
    let d = reconciler::check_repo(&mut conn, FIXED_TS, REPO, &sig);
    assert!(!d.allowed, "DB error must produce DENY (fail-closed)");
    assert!(
        d.reason.starts_with("registry unreadable:"),
        "deny reason must start with 'registry unreadable:' — got: {:?}",
        d.reason
    );
}

#[test]
fn gc_event_detail_matches_python_byte_for_byte() {
    // This is the load-bearing single-writer correctness test for the CLEAN
    // event detail format.  Python builds:
    //   json.dumps({"reason":"dead_session_owner","repo_dir":"...","session_id":"..."},
    //              separators=(",",":"))
    // serde_json with preserve_order produces the same compact form.
    let mut conn = build_db();
    insert_lease(
        &conn,
        "sess-dead-stale",
        Some(201),
        "cooperative",
        None,
        OLD_TS,
    );
    let sig = MockSignaller {
        alive_map: [(201, false)].into_iter().collect(),
    };
    let _d = reconciler::check_repo_with_session(
        &mut conn,
        FIXED_TS,
        REPO,
        Some("sess-mine"),
        &[],
        false,
        &sig,
    );
    let detail: String = conn
        .query_row(
            "SELECT detail FROM events WHERE event_type='CLEAN'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    // Must match Python byte-for-byte.
    let want = format!(
        r#"{{"reason":"dead_session_owner","repo_dir":"{REPO}","session_id":"sess-dead-stale"}}"#
    );
    assert_eq!(detail, want, "CLEAN detail must match Python compact-JSON");
}
