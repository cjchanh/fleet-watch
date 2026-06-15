//! Kill-authority surface — port preemption (PS-D-preempt).
//!
//! SECURITY-CRITICAL: this is the ONLY module that can terminate a process.
//! `preempt_port` is generic over a [`Signaller`] so the syscall path is fully
//! testable WITHOUT ever sending a real signal — production wires
//! [`RealSignaller`] (libc); every test uses a recording mock.
//!
//! Ported from `fleet_watch.referee.preempt_port`. Fail-closed: holder
//! unreadable or priority not exceeded → DENY (no kill); PREEMPT audit-write
//! failure → DENY (no kill — testimony before action); release failure → DENY
//! (the port was not safely reclaimed).
//!
//! The timestamp is injected (deterministic); the grace wait is encapsulated in
//! the `Signaller` so tests run instantly.

use crate::ledger;
use crate::registry;
use crate::Decision;
use rusqlite::Connection;

/// Process-control syscalls preemption needs. Production uses [`RealSignaller`]
/// (libc); tests inject a mock that records calls and never touches a real
/// process. Security boundary: only `RealSignaller` issues real signals.
pub trait Signaller {
    /// Send SIGTERM to `pid`. Returns `true` if delivered, `false` if the
    /// process was already gone (mirrors Python catching `ProcessLookupError`).
    fn terminate(&self, pid: i64) -> bool;
    /// `true` if `pid` is alive (a `kill(pid, 0)` probe succeeds).
    fn is_alive(&self, pid: i64) -> bool;
    /// Block until `pid` exits or `grace_seconds` elapses (poll + sleep).
    fn wait_for_exit(&self, pid: i64, grace_seconds: u64);
}

/// Production signaller backed by libc — the ONLY implementation that issues
/// real process-control syscalls. Never constructed by tests.
pub struct RealSignaller;

impl Signaller for RealSignaller {
    fn terminate(&self, pid: i64) -> bool {
        // SIGTERM; rc != 0 (e.g. ESRCH "no such process") → already gone.
        let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
        rc == 0
    }
    fn is_alive(&self, pid: i64) -> bool {
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }
    fn wait_for_exit(&self, pid: i64, grace_seconds: u64) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(grace_seconds);
        while std::time::Instant::now() < deadline {
            if !self.is_alive(pid) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
}

/// Preempt a port from a lower-priority holder. Port of
/// `fleet_watch.referee.preempt_port`.
///
/// Order (faithful to Python, with fail-closed guards): read holder → priority
/// gate → log PREEMPT (testimony first) → `signaller.terminate` (SIGTERM) →
/// `wait_for_exit` (grace) → `release_process` (free the port). The holder pid
/// is captured once at entry — no TOCTOU re-query before the kill/release.
pub fn preempt_port<S: Signaller>(
    conn: &mut Connection,
    timestamp: &str,
    port: i64,
    new_priority: i64,
    reason: &str,
    grace_seconds: u64,
    signaller: &S,
) -> Decision {
    let holder = match registry::get_process_by_port(conn, port) {
        Ok(Some(h)) => h,
        Ok(None) => return Decision::allow("port already free"),
        Err(e) => return Decision::deny(format!("registry unreadable: {e}")),
    };
    if new_priority <= holder.priority {
        return Decision::deny(format!(
            "cannot preempt: new priority {new_priority} <= holder priority {}",
            holder.priority
        ));
    }
    // Testimony before action: record intent before any irreversible kill.
    let detail = serde_json::json!({
        "port": port,
        "holder_pid": holder.pid,
        "holder_priority": holder.priority,
        "new_priority": new_priority,
        "reason": reason,
        "grace_seconds": grace_seconds,
    })
    .to_string();
    if let Err(e) = ledger::log_event(
        conn,
        timestamp,
        "PREEMPT",
        Some(holder.pid),
        Some(&holder.workstream),
        &detail,
    ) {
        return Decision::deny(format!("preempt audit-log write failed: {e}"));
    }
    let holder_pid = holder.pid; // captured — no re-query before kill/release.
    signaller.terminate(holder_pid);
    signaller.wait_for_exit(holder_pid, grace_seconds);
    if let Err(e) = ledger::release_process(conn, timestamp, holder_pid) {
        // The PREEMPT event (logged above) already testifies the kill. The
        // holder was terminated but its registry entry could not be released —
        // `release_process` is transactional, so the registry rolled back clean
        // (consistent but stale). Deny fail-closed and say so accurately: the
        // port is NOT reclaimed; the stale dead holder is reaped later by the
        // reconciler's dead-PID path. The Decision itself testifies the
        // half-state rather than reading as a generic failure.
        return Decision::deny(format!(
            "preempt: PID {holder_pid} terminated but release failed ({e}); \
             port not reclaimed — reconciler will reap"
        ));
    }
    Decision::allow(format!(
        "preempted PID {holder_pid} ({}) for: {reason}",
        holder.name
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{verify_chain, Event};
    use std::cell::RefCell;
    use std::fs;

    /// Recording mock — never issues a real signal. `wait_for_exit` is a no-op
    /// so tests run instantly.
    struct MockSignaller {
        terminated: RefCell<Vec<i64>>,
    }
    impl MockSignaller {
        fn new() -> Self {
            MockSignaller {
                terminated: RefCell::new(Vec::new()),
            }
        }
    }
    impl Signaller for MockSignaller {
        fn terminate(&self, pid: i64) -> bool {
            self.terminated.borrow_mut().push(pid);
            true
        }
        fn is_alive(&self, _pid: i64) -> bool {
            false
        }
        fn wait_for_exit(&self, _pid: i64, _grace: u64) {}
    }

    const SCHEMA: &str = "
        CREATE TABLE processes (pid INTEGER PRIMARY KEY, session_id TEXT NOT NULL,
            workstream TEXT NOT NULL, name TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 3,
            port INTEGER, gpu_mb INTEGER DEFAULT 0, repo_dir TEXT, model TEXT,
            restart_policy TEXT NOT NULL DEFAULT 'ALERT_ONLY', start_cmd TEXT,
            start_time TEXT NOT NULL, last_heartbeat TEXT NOT NULL,
            expected_duration_min INTEGER, UNIQUE(port), UNIQUE(repo_dir));
        CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
            event_type TEXT NOT NULL, pid INTEGER, workstream TEXT, detail TEXT,
            prev_hash TEXT, hash TEXT NOT NULL);
        CREATE TABLE gpu_budget (id INTEGER PRIMARY KEY CHECK (id=1),
            total_mb INTEGER NOT NULL DEFAULT 131072, reserve_mb INTEGER NOT NULL DEFAULT 16384,
            allocated_mb INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE session_leases (session_id TEXT PRIMARY KEY, owner_pid INTEGER,
            started_at TEXT NOT NULL, last_heartbeat_at TEXT NOT NULL, shutdown_at TEXT,
            status TEXT NOT NULL DEFAULT 'ACTIVE');
        CREATE TABLE external_resources (provider TEXT NOT NULL, external_id TEXT NOT NULL,
            session_id TEXT NOT NULL, start_time TEXT NOT NULL, last_seen TEXT NOT NULL,
            PRIMARY KEY(provider, external_id));
    ";

    fn build_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn
    }

    fn insert_holder(conn: &Connection, pid: i64, port: i64, priority: i64, gpu_mb: i64, ts: &str) {
        conn.execute(
            "INSERT INTO processes (pid,session_id,workstream,name,priority,port,gpu_mb,start_time,last_heartbeat) \
             VALUES (?1,'s1','ws','demo',?2,?3,?4,?5,?5)",
            rusqlite::params![pid, priority, port, gpu_mb, ts],
        )
        .unwrap();
    }

    #[test]
    fn preempt_free_port_allows_without_kill() {
        let mut conn = build_db();
        let sig = MockSignaller::new();
        let d = preempt_port(
            &mut conn,
            "2026-01-01T00:00:00+00:00",
            7777,
            5,
            "x",
            30,
            &sig,
        );
        assert!(d.allowed);
        assert_eq!(d.reason, "port already free");
        assert!(
            sig.terminated.borrow().is_empty(),
            "must not kill a free port"
        );
    }

    #[test]
    fn preempt_equal_priority_denies_without_kill() {
        let ts = "2026-01-01T00:00:00+00:00";
        let mut conn = build_db();
        insert_holder(&conn, 999, 4242, 5, 0, ts);
        let sig = MockSignaller::new();
        let d = preempt_port(&mut conn, ts, 4242, 5, "x", 30, &sig); // equal priority
        assert!(!d.allowed);
        assert!(d.reason.contains("cannot preempt"));
        assert!(
            sig.terminated.borrow().is_empty(),
            "must not kill when priority is not exceeded"
        );
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM processes WHERE pid=999", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n, 1, "holder must remain when preempt denied");
    }

    #[test]
    fn preempt_higher_priority_matches_python() {
        let v: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/preempt_vectors.json"
            ))
            .unwrap(),
        )
        .unwrap();
        let ts = v["fixed_ts"].as_str().unwrap();
        let mut conn = build_db();
        conn.execute(
            "INSERT INTO gpu_budget (id,total_mb,reserve_mb,allocated_mb) VALUES (1,131072,16384,4096)",
            [],
        )
        .unwrap();
        insert_holder(&conn, 999, 4242, 3, 2048, ts);
        let sig = MockSignaller::new();
        let d = preempt_port(&mut conn, ts, 4242, 5, "upgrade", 30, &sig);

        assert_eq!(d.allowed, v["preempt"]["allowed"].as_bool().unwrap());
        assert_eq!(d.reason, v["preempt"]["reason"].as_str().unwrap());
        assert_eq!(
            *sig.terminated.borrow(),
            vec![999_i64],
            "SIGTERM to holder pid"
        );

        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM processes WHERE pid=999", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n, v["proc_remaining"].as_i64().unwrap(), "process released");
        let gpu: i64 = conn
            .query_row("SELECT allocated_mb FROM gpu_budget WHERE id=1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            gpu,
            v["gpu_allocated_after"].as_i64().unwrap(),
            "gpu budget decremented"
        );

        let (etype, detail, prev, hash): (String, String, String, String) = conn
            .query_row(
                "SELECT event_type,detail,prev_hash,hash FROM events ORDER BY id LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        let ev0 = &v["events"][0];
        assert_eq!(etype, ev0["event_type"].as_str().unwrap());
        assert_eq!(
            detail,
            ev0["detail"].as_str().unwrap(),
            "PREEMPT detail must match Python byte-for-byte"
        );
        assert_eq!(prev, ev0["prev_hash"].as_str().unwrap());
        assert_eq!(
            hash,
            ev0["hash"].as_str().unwrap(),
            "PREEMPT hash must match Python"
        );

        let events = vec![Event {
            timestamp: ts.to_string(),
            event_type: etype,
            detail,
            prev_hash: prev,
            hash,
        }];
        assert_eq!(verify_chain(&events), (true, 1));
    }
}
