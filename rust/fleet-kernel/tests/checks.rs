//! Parity tests for `check_port` and `check_gpu_budget`.
//!
//! Builds the same SQLite fixture as `gen_checks_vectors.py`, then asserts
//! that Rust `allowed` + `reason` match the JSON vectors byte-for-byte.

use fleet_kernel::checks::{check_gpu_budget, check_port};
use rusqlite::Connection;
use std::path::Path;

// ── fixture constants (must match gen_checks_vectors.py) ─────────────────────
const FIXTURE_PID: i64 = 999;
const FIXTURE_NAME: &str = "demo";
const FIXTURE_PORT: i64 = 4242;
const FIXTURE_TOTAL_MB: i64 = 131_072;
const FIXTURE_RESERVE_MB: i64 = 16_384;
const FIXTURE_ALLOCATED_MB: i64 = 40_960;
// available = 131072 - 16384 - 40960 = 73728
const FIXTURE_AVAILABLE_MB: i64 = FIXTURE_TOTAL_MB - FIXTURE_RESERVE_MB - FIXTURE_ALLOCATED_MB;

// ── schema DDL (inline, matching SCHEMA in registry.py) ──────────────────────
const PROCESSES_DDL: &str = "
CREATE TABLE IF NOT EXISTS processes (
    pid         INTEGER PRIMARY KEY,
    session_id  TEXT NOT NULL,
    workstream  TEXT NOT NULL,
    name        TEXT NOT NULL,
    priority    INTEGER NOT NULL DEFAULT 3,
    port        INTEGER,
    gpu_mb      INTEGER DEFAULT 0,
    repo_dir    TEXT,
    model       TEXT,
    restart_policy TEXT NOT NULL DEFAULT 'ALERT_ONLY',
    start_cmd      TEXT,
    start_time     TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL,
    expected_duration_min INTEGER,
    UNIQUE(port),
    UNIQUE(repo_dir)
);
";

const GPU_BUDGET_DDL: &str = "
CREATE TABLE IF NOT EXISTS gpu_budget (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    total_mb        INTEGER NOT NULL DEFAULT 131072,
    reserve_mb      INTEGER NOT NULL DEFAULT 16384,
    allocated_mb    INTEGER NOT NULL DEFAULT 0
);
";

// ── helper ────────────────────────────────────────────────────────────────────

fn build_fixture_db() -> (tempfile::NamedTempFile, Connection) {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let conn = Connection::open(tmp.path()).expect("open db");

    conn.execute_batch(PROCESSES_DDL).expect("create processes");
    conn.execute_batch(GPU_BUDGET_DDL)
        .expect("create gpu_budget");

    conn.execute(
        "INSERT INTO processes \
         (pid, session_id, workstream, name, priority, port, gpu_mb, \
          repo_dir, model, restart_policy, start_cmd, start_time, last_heartbeat) \
         VALUES (?1, 'sess-1', 'test', ?2, 3, ?3, 0, NULL, NULL, \
                 'ALERT_ONLY', NULL, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')",
        rusqlite::params![FIXTURE_PID, FIXTURE_NAME, FIXTURE_PORT],
    )
    .expect("insert process");

    conn.execute(
        "INSERT INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) \
         VALUES (1, ?1, ?2, ?3)",
        rusqlite::params![FIXTURE_TOTAL_MB, FIXTURE_RESERVE_MB, FIXTURE_ALLOCATED_MB],
    )
    .expect("insert gpu_budget");

    (tmp, conn)
}

// ── load vectors ─────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct Vector {
    scenario: String,
    expected_allowed: bool,
    expected_reason: String,
}

fn load_vectors() -> Vec<Vector> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("checks_vectors.json");
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("cannot parse {}: {e}", path.display()))
}

// ── parity tests ──────────────────────────────────────────────────────────────

#[test]
fn parity_port_claimed() {
    let (_tmp, conn) = build_fixture_db();
    let vectors = load_vectors();
    let v = vectors
        .iter()
        .find(|v| v.scenario == "port_claimed")
        .expect("port_claimed vector");

    let d = check_port(&conn, FIXTURE_PORT);
    assert_eq!(
        d.allowed, v.expected_allowed,
        "port_claimed: allowed mismatch"
    );
    assert_eq!(d.reason, v.expected_reason, "port_claimed: reason mismatch");
}

#[test]
fn parity_port_free() {
    let (_tmp, conn) = build_fixture_db();
    let vectors = load_vectors();
    let v = vectors
        .iter()
        .find(|v| v.scenario == "port_free")
        .expect("port_free vector");

    let d = check_port(&conn, 5000);
    assert_eq!(d.allowed, v.expected_allowed, "port_free: allowed mismatch");
    assert_eq!(d.reason, v.expected_reason, "port_free: reason mismatch");
}

#[test]
fn parity_gpu_zero() {
    let (_tmp, conn) = build_fixture_db();
    let vectors = load_vectors();
    let v = vectors
        .iter()
        .find(|v| v.scenario == "gpu_zero")
        .expect("gpu_zero vector");

    let d = check_gpu_budget(&conn, 0);
    assert_eq!(d.allowed, v.expected_allowed, "gpu_zero: allowed mismatch");
    assert_eq!(d.reason, v.expected_reason, "gpu_zero: reason mismatch");
}

#[test]
fn parity_gpu_fits() {
    let (_tmp, conn) = build_fixture_db();
    let vectors = load_vectors();
    let v = vectors
        .iter()
        .find(|v| v.scenario == "gpu_fits")
        .expect("gpu_fits vector");

    let d = check_gpu_budget(&conn, 8192);
    assert_eq!(d.allowed, v.expected_allowed, "gpu_fits: allowed mismatch");
    assert_eq!(d.reason, v.expected_reason, "gpu_fits: reason mismatch");
}

#[test]
fn parity_gpu_exceeds() {
    let (_tmp, conn) = build_fixture_db();
    let vectors = load_vectors();
    let v = vectors
        .iter()
        .find(|v| v.scenario == "gpu_exceeds")
        .expect("gpu_exceeds vector");

    let d = check_gpu_budget(&conn, 100_000);
    assert_eq!(
        d.allowed, v.expected_allowed,
        "gpu_exceeds: allowed mismatch"
    );
    assert_eq!(d.reason, v.expected_reason, "gpu_exceeds: reason mismatch");
}

// ── fail-closed tests ─────────────────────────────────────────────────────────

#[test]
fn fail_closed_port_on_bad_db() {
    // A connection with no schema — get_process_by_port returns an error.
    // Must deny, never allow.
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let conn = Connection::open(tmp.path()).expect("open");
    // No tables created — query will fail.
    let d = check_port(&conn, 4242);
    assert!(
        !d.allowed,
        "fail-closed: must deny when registry unreadable, got allow"
    );
    assert!(
        d.reason.starts_with("registry unreadable:"),
        "expected 'registry unreadable:' prefix, got: {:?}",
        d.reason
    );
}

#[test]
fn fail_closed_gpu_on_bad_db() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let conn = Connection::open(tmp.path()).expect("open");
    let d = check_gpu_budget(&conn, 8192);
    assert!(
        !d.allowed,
        "fail-closed: must deny when registry unreadable, got allow"
    );
    assert!(
        d.reason.starts_with("registry unreadable:"),
        "expected 'registry unreadable:' prefix, got: {:?}",
        d.reason
    );
}

#[test]
fn gpu_negative_is_no_claim() {
    let (_tmp, conn) = build_fixture_db();
    let d = check_gpu_budget(&conn, -1);
    assert!(d.allowed);
    assert_eq!(d.reason, "no GPU claim");
}

// ── unit: available_mb math ───────────────────────────────────────────────────

#[test]
fn available_mb_math() {
    // Verify the fixture constant arithmetic matches what Python computes.
    assert_eq!(
        FIXTURE_AVAILABLE_MB,
        FIXTURE_TOTAL_MB - FIXTURE_RESERVE_MB - FIXTURE_ALLOCATED_MB
    );
    assert_eq!(FIXTURE_AVAILABLE_MB, 73_728);
}
