//! Write path for the hash-chained audit ledger (PS-D — privilege surface).
//!
//! Ported from Python `fleet_watch.events.{get_last_hash, log_event}` and
//! `fleet_watch.referee.claim_port`. This is the kernel's first WRITE surface:
//! it appends hash-linked rows to the `events` table and is the foundation the
//! claim / reconciler / preempt patchsets build on.
//!
//! DELIBERATELY NOT PORTED (own audited patch, see PATCHSET_PLAN PS-D audit):
//!   * `preempt_port` — embeds `os.kill(SIGTERM)` (kill authority) + process
//!     displacement; cannot be parity-tested without killing a real process,
//!     and on a multi-writer registry a bug could kill the wrong session.
//!   * `claim_repo` / `preflight_register` repo-path — call the mutating
//!     `check_repo_with_session` reconciler (its own patchset).
//!
//! Fail-closed (Invariant #5): an unknown event type is rejected; a failed
//! audit write turns a claim into a DENY ("can't testify → don't grant").
//! The timestamp is INJECTED by the caller (deterministic kernel) rather than
//! read from an internal `datetime.now`.

use crate::events::{compute_event_hash, GENESIS_HASH};
use crate::registry;
use crate::Decision;
use rusqlite::Connection;

/// Valid event types — mirrors `fleet_watch.events.EVENT_TYPES`. Fail-closed:
/// an unknown type is rejected rather than written to the audit log.
pub const EVENT_TYPES: &[&str] = &[
    "REGISTER",
    "HEARTBEAT",
    "RELEASE",
    "STALE",
    "PREEMPT",
    "RESTART",
    "KILL",
    "THERMAL",
    "MEMORY_PRESSURE",
    "CONFLICT",
    "CLEAN",
    "CLAIM",
    "SESSION_START",
    "SESSION_HEARTBEAT",
    "SESSION_CLOSE",
    "REAP",
    "REAP_SESSION",
    "FUSE_TRIPPED",
    "GPU_BUDGET_DENY",
    "GPU_MEMORY_PRESSURE",
    "GPU_WORKING_SET_DENY",
    "RUNAWAY_DETECTED",
    "RUNAWAY_KILL",
    "RUNAWAY_KILL_FAILED",
    "ORPHAN_RUNNERS_DETECTED",
    "FLEET_PKILL_EXECUTED",
    "MEMORY_PRESSURE_RISING",
];

/// True when `event_type` is a known Fleet Watch event type.
pub fn is_valid_event_type(event_type: &str) -> bool {
    EVENT_TYPES.contains(&event_type)
}

/// Errors from the ledger write path.
#[derive(Debug)]
pub enum LedgerError {
    UnknownEventType(String),
    Db(rusqlite::Error),
}

impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LedgerError::UnknownEventType(t) => write!(f, "unknown event type: {t}"),
            LedgerError::Db(e) => write!(f, "registry write failed: {e}"),
        }
    }
}

impl std::error::Error for LedgerError {}

impl From<rusqlite::Error> for LedgerError {
    fn from(e: rusqlite::Error) -> Self {
        LedgerError::Db(e)
    }
}

/// Return the hash of the most recent event, or `GENESIS_HASH` if the log is
/// empty. Port of `fleet_watch.events.get_last_hash`.
pub fn get_last_hash(conn: &Connection) -> rusqlite::Result<String> {
    let mut stmt = conn.prepare("SELECT hash FROM events ORDER BY id DESC LIMIT 1")?;
    let mut rows = stmt.query([])?;
    match rows.next()? {
        Some(row) => row.get(0),
        None => Ok(GENESIS_HASH.to_owned()),
    }
}

/// Append one event to the hash-chained audit log. Port of
/// `fleet_watch.events.log_event`.
///
/// `detail` is the already-serialized compact-JSON detail string (matching
/// Python `json.dumps(..., separators=(",", ":"))`); the kernel hashes the
/// string and never re-serializes, so there is no serializer-format drift.
/// Returns the new row id. Fail-closed: an unknown `event_type` is rejected
/// before any write.
pub fn log_event(
    conn: &Connection,
    timestamp: &str,
    event_type: &str,
    pid: Option<i64>,
    workstream: Option<&str>,
    detail: &str,
) -> Result<i64, LedgerError> {
    if !is_valid_event_type(event_type) {
        return Err(LedgerError::UnknownEventType(event_type.to_owned()));
    }
    let prev_hash = get_last_hash(conn)?;
    let event_hash = compute_event_hash(&prev_hash, timestamp, event_type, detail);
    conn.execute(
        "INSERT INTO events (timestamp, event_type, pid, workstream, detail, prev_hash, hash) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![timestamp, event_type, pid, workstream, detail, prev_hash, event_hash],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Standalone port claim: check availability, then record a `CLAIM` (granted)
/// or `CONFLICT` (blocked) event. Port of `fleet_watch.referee.claim_port`
/// (which performs no registration — it records the attempt only).
///
/// Fail-closed: if the audit write fails, the claim is DENIED — the kernel will
/// not grant a claim it cannot testify to.
pub fn claim_port(conn: &Connection, timestamp: &str, port: i64) -> Decision {
    let decision = crate::checks::check_port(conn, port);
    let detail = if decision.allowed {
        format!(r#"{{"resource":"port","port":{port}}}"#)
    } else {
        // Mirror Python: CONFLICT detail carries the holder pid (or null).
        let holder_pid = registry::get_process_by_port(conn, port)
            .ok()
            .flatten()
            .map(|h| h.pid);
        match holder_pid {
            Some(pid) => {
                format!(r#"{{"resource":"port","port":{port},"holder_pid":{pid}}}"#)
            }
            None => format!(r#"{{"resource":"port","port":{port},"holder_pid":null}}"#),
        }
    };
    let event_type = if decision.allowed {
        "CLAIM"
    } else {
        "CONFLICT"
    };
    match log_event(conn, timestamp, event_type, None, None, &detail) {
        Ok(_) => decision,
        Err(e) => Decision::deny(format!("claim audit-log write failed: {e}")),
    }
}
