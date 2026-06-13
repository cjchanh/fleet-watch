//! Read-only guard checks — `check_port` and `check_gpu_budget`.
//!
//! Faithful port of `fleet_watch.referee.check_port` (line 88) and
//! `fleet_watch.referee.check_gpu_budget` (line 236).
//!
//! Fail-closed contract: any `rusqlite::Error` (DB unreadable, row missing,
//! type mismatch) produces `Decision::deny("registry unreadable: <err>")`.
//! No `unwrap`, no `panic`, no allow-on-error path.

use rusqlite::Connection;

use crate::registry;
use crate::Decision;

// ── check_port ───────────────────────────────────────────────────────────────

/// Return whether `port` is currently free.
///
/// Faithful port of `fleet_watch.referee.check_port`:
/// ```python
/// holder = registry.get_process_by_port(conn, port)
/// if holder is None:
///     return Decision(allowed=True, reason="port available")
/// return Decision(
///     allowed=False,
///     reason=f"port {port} claimed by PID {holder['pid']} ({holder['name']})",
///     holder=holder,
/// )
/// ```
///
/// Error path: any registry failure → `deny("registry unreadable: <err>")`.
pub fn check_port(conn: &Connection, port: i64) -> Decision {
    match registry::get_process_by_port(conn, port) {
        Err(e) => Decision::deny(format!("registry unreadable: {e}")),
        Ok(None) => Decision::allow("port available"),
        Ok(Some(h)) => Decision::deny(format!(
            "port {port} claimed by PID {pid} ({name})",
            pid = h.pid,
            name = h.name,
        )),
    }
}

// ── check_gpu_budget ─────────────────────────────────────────────────────────

/// Return whether `gpu_mb` fits within the current GPU ledger.
///
/// Faithful port of `fleet_watch.referee.check_gpu_budget`:
/// ```python
/// if gpu_mb <= 0:
///     return Decision(allowed=True, reason="no GPU claim")
/// budget = registry.get_gpu_budget(conn)
/// if gpu_mb <= budget["available_mb"]:
///     return Decision(allowed=True,
///         reason=f"{gpu_mb}MB fits in {budget['available_mb']}MB available")
/// return Decision(allowed=False, reason=(
///     f"GPU budget exceeded: requesting {gpu_mb}MB but only "
///     f"{budget['available_mb']}MB available "
///     f"({budget['allocated_mb']}MB allocated of "
///     f"{budget['total_mb'] - budget['reserve_mb']}MB allocatable)"
/// ))
/// ```
///
/// Error path: any registry failure → `deny("registry unreadable: <err>")`.
pub fn check_gpu_budget(conn: &Connection, gpu_mb: i64) -> Decision {
    if gpu_mb <= 0 {
        return Decision::allow("no GPU claim");
    }
    let budget = match registry::get_gpu_budget(conn) {
        Ok(b) => b,
        Err(e) => return Decision::deny(format!("registry unreadable: {e}")),
    };
    if gpu_mb <= budget.available_mb {
        return Decision::allow(format!(
            "{gpu_mb}MB fits in {avail}MB available",
            avail = budget.available_mb,
        ));
    }
    Decision::deny(format!(
        "GPU budget exceeded: requesting {gpu_mb}MB but only \
         {avail}MB available \
         ({alloc}MB allocated of {allocatable}MB allocatable)",
        avail = budget.available_mb,
        alloc = budget.allocated_mb,
        allocatable = budget.total_mb - budget.reserve_mb,
    ))
}
