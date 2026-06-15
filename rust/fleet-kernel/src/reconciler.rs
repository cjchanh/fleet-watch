//! Single-writer repo reconciler — `check_repo` + `check_repo_with_session`.
//!
//! Faithful Rust port of `fleet_watch.referee.check_repo_with_session` (lines
//! 100–233) and `_session_holder_from_lease` (line 28).  This is the
//! single-writer enforcement core: it is stateful (may GC stale leases and
//! auto-release dead-PID process rows), so it requires `&mut Connection`.
//!
//! Ported invariants:
//!   * DEAD-PID detection uses `sig.is_alive(pid)` in place of Python's
//!     `os.kill(pid, 0)` so the kernel is always testable without real signals.
//!   * Stale-lease GC condition: `(owner_dead OR owner_missing) AND age > 180s`
//!     — matches `DEFAULT_STALE_SECONDS` in `registry.py`.
//!   * On GC: `close_session_lease` + `log_event("CLEAN", ...)` with the
//!     exact compact-JSON detail Python builds; the closed lease is appended
//!     to `stale_holders`.
//!   * Fail-closed: any registry error → DENY ("registry unreadable: …").
//!     Uncertain state (alive/dead unknown) → treat as HELD, never release.
//!
//! The timestamp `ts` is injected (deterministic kernel, never reads the wall
//! clock directly), matching the injection pattern used in `ledger.rs` and
//! `preempt.rs`.

use crate::ledger;
use crate::preempt::Signaller;
use crate::registry;
use crate::Decision;
use chrono::DateTime;
use rusqlite::Connection;
use serde_json::{json, Value};

/// Stale-lease threshold in seconds. Mirrors `registry.DEFAULT_STALE_SECONDS`.
const DEFAULT_STALE_SECONDS: i64 = 180;

// ── _session_holder_from_lease (line 28 Python) ──────────────────────────────

/// Build the holder dict for a session lease. Faithful port of Python
/// `_session_holder_from_lease`:
///
/// ```python
/// {
///     "pid": lease.get("owner_pid"),
///     "name": f"session {lease['session_id']}",
///     "workstream": "session",
///     "priority": 3,
///     "port": None,
///     "repo_dir": lease.get("repo_dir"),
///     "gpu_mb": 0,
///     "session_id": lease["session_id"],
///     "repo_lock_mode": lease.get("repo_lock_mode", "cooperative"),
///     "write_scopes": lease.get("write_scopes", []),
/// }
/// ```
fn session_holder_from_lease(lease: &registry::SessionLeaseRow) -> Value {
    let pid_val: Value = match lease.owner_pid {
        Some(p) => Value::Number(p.into()),
        None => Value::Null,
    };
    let repo_val: Value = match &lease.repo_dir {
        Some(r) => Value::String(r.clone()),
        None => Value::Null,
    };
    let scopes_val: Value = Value::Array(
        lease
            .write_scopes
            .iter()
            .map(|s| Value::String(s.clone()))
            .collect(),
    );
    json!({
        "pid": pid_val,
        "name": format!("session {}", lease.session_id),
        "workstream": "session",
        "priority": 3,
        "port": null,
        "repo_dir": repo_val,
        "gpu_mb": 0,
        "session_id": lease.session_id,
        "repo_lock_mode": lease.repo_lock_mode,
        "write_scopes": scopes_val,
    })
}

// ── heartbeat age helper ──────────────────────────────────────────────────────

/// Return seconds elapsed from `iso_ts` to `now_ts` (both RFC 3339). `None` on a
/// malformed timestamp — the caller then treats the lease as HELD (fail-closed,
/// matching Python `_age_seconds` returning `None`).
///
/// Uses `chrono` for robust RFC-3339 parsing rather than hand-rolled date math:
/// the stale-GC threshold this feeds is single-writer-load-bearing, so it must
/// handle every offset / fractional-second form correctly, not just the
/// `timespec="seconds"` form Fleet Watch currently emits.
fn age_seconds(iso_ts: &str, now_ts: &str) -> Option<i64> {
    let ts = DateTime::parse_from_rfc3339(iso_ts).ok()?;
    let now = DateTime::parse_from_rfc3339(now_ts).ok()?;
    Some(now.timestamp() - ts.timestamp())
}

// ── check_repo ────────────────────────────────────────────────────────────────

/// Return whether `repo_dir` is available. Calls `check_repo_with_session`
/// with `current_session_id = None` and no write scopes, matching the Python:
/// ```python
/// def check_repo(conn, repo_dir):
///     return check_repo_with_session(conn, repo_dir, current_session_id=None)
/// ```
pub fn check_repo<S: Signaller>(
    conn: &mut Connection,
    ts: &str,
    repo_dir: &str,
    sig: &S,
) -> Decision {
    check_repo_with_session(conn, ts, repo_dir, None, &[], false, sig)
}

// ── check_repo_with_session ───────────────────────────────────────────────────

/// Single-writer repo check. Faithful port of Python
/// `fleet_watch.referee.check_repo_with_session` (lines 105–233).
///
/// `write_scopes` are already-resolved absolute paths (call
/// `normalize_write_scopes` before passing here, matching Python's
/// `normalize_write_scopes(resolved_repo_dir, write_scopes)` call at line 114).
/// Pass `&[]` for no scopes.
///
/// Control flow (faithful to Python):
/// 1. Resolve `repo_dir` to an absolute path.
/// 2. Normalize `write_scopes` against the resolved repo dir.
/// 3. Look up the process holder (`get_process_by_repo`).
///
///    a. If found: check liveness (`sig.is_alive`).
///       - Dead → auto-release + CLEAN event → ALLOW "stale lock cleared".
///       - Same-session → ALLOW "owned by current session".
///       - Alive foreign → DENY.
///
///    b. If no process holder: check external resources, then session leases.
///       - External resource from a different session → DENY.
///       - Session leases: GC dead+stale, collect advisories, check overlaps/exclusive.
pub fn check_repo_with_session<S: Signaller>(
    conn: &mut Connection,
    ts: &str,
    repo_dir: &str,
    current_session_id: Option<&str>,
    write_scopes: &[String],
    exclusive: bool,
    sig: &S,
) -> Decision {
    // Python uses `if current_session_id` truthiness throughout — an empty
    // string means "no session". Normalise Some("") -> None up front so every
    // ownership / same-session bypass below matches Python exactly.
    let current_session_id = current_session_id.filter(|s| !s.is_empty());

    // Step 1: resolve repo_dir (mirrors Python `str(Path(repo_dir).resolve())`).
    // Use std::fs::canonicalize if it exists; otherwise lexical resolve.
    let resolved_repo = crate::resolve_repo(repo_dir);

    // Step 2: normalize write scopes (mirrors `normalize_write_scopes(resolved, ws)`).
    let requested_scopes = crate::normalize_write_scopes(Some(&resolved_repo), write_scopes);

    // Step 3: check process holder.
    let holder = match registry::get_process_by_repo(conn, &resolved_repo) {
        Err(e) => return Decision::deny(format!("registry unreadable: {e}")),
        Ok(h) => h,
    };

    if let Some(holder) = holder {
        // Python: try os.kill(pid, 0)
        if !sig.is_alive(holder.pid) {
            // Dead PID — auto-release + CLEAN event.
            if let Err(e) = ledger::release_process(conn, ts, holder.pid) {
                return Decision::deny(format!("registry unreadable: {e}"));
            }
            // Python logs detail {"reason":"dead_pid","repo_dir":repo_dir} using
            // the ORIGINAL repo_dir (not resolved). Same `json!` + compact
            // serialization pattern as the session-lease CLEAN path below.
            let detail = serde_json::to_string(&json!({
                "reason": "dead_pid",
                "repo_dir": repo_dir,
            }))
            .unwrap_or_else(|_| "{}".to_owned());
            // log_event error is non-fatal per Python (Python doesn't check it).
            let _ = ledger::log_event(
                conn,
                ts,
                "CLEAN",
                Some(holder.pid),
                Some(&holder.workstream),
                &detail,
            );
            return Decision::allow("repo available (stale lock cleared)");
        }
        // Process is alive. Python: PermissionError (EPERM) → treat as alive.
        // Check same-session bypass.
        if let Some(sid) = current_session_id {
            if holder.session_id == sid {
                return Decision::allow("repo available (owned by current session)");
            }
        }
        // Alive process not owned by the current session (or no session context
        // was supplied) — the repo is held.
        return Decision::deny(format!(
            "repo {resolved_repo} locked by PID {pid} ({name})",
            pid = holder.pid,
            name = holder.name,
        ));
    }

    // No process holder — check external resources.
    let external_holders = match registry::get_external_resources_by_repo(conn, &resolved_repo) {
        Err(e) => return Decision::deny(format!("registry unreadable: {e}")),
        Ok(v) => v,
    };

    if !external_holders.is_empty() {
        for external in &external_holders {
            if let Some(sid) = current_session_id {
                if external.session_id == sid {
                    continue;
                }
            }
            return Decision::deny(format!(
                "repo {resolved_repo} locked by external {} resource {} ({})",
                external.provider, external.external_id, external.name,
            ));
        }
        // All external holders belong to current session.
        return Decision::allow("repo available (owned by current session)");
    }

    // No process holder, no external holder — check session leases.
    let session_leases = match registry::get_active_session_leases_by_repo(conn, &resolved_repo) {
        Err(e) => return Decision::deny(format!("registry unreadable: {e}")),
        Ok(v) => v,
    };

    let mut owned_by_current_session = false;
    let mut advisory_holders: Vec<Value> = Vec::new();
    let mut stale_holders: Vec<Value> = Vec::new();

    for lease in &session_leases {
        // Check if this is the current session's own lease.
        if let Some(sid) = current_session_id {
            if lease.session_id == sid {
                owned_by_current_session = true;
                continue;
            }
        }

        let owner_pid = lease.owner_pid;
        let heartbeat_age = age_seconds(&lease.last_heartbeat_at, ts);
        let owner_dead = owner_pid.is_some_and(|p| !sig.is_alive(p));
        let owner_missing = owner_pid.is_none();

        // GC condition: (dead OR missing) AND age > DEFAULT_STALE_SECONDS
        let should_gc = (owner_missing || owner_dead)
            && heartbeat_age.is_some_and(|a| a > DEFAULT_STALE_SECONDS);

        if should_gc {
            // Close the lease.
            if let Err(e) = ledger::close_session_lease(conn, ts, &lease.session_id) {
                // Fail-closed: if we can't close the lease, treat it as held.
                return Decision::deny(format!("registry unreadable: {e}"));
            }
            // Log CLEAN event with the same detail dict Python builds.
            let reason_str = if owner_dead {
                "dead_session_owner"
            } else {
                "ownerless_stale_session"
            };
            let detail = serde_json::to_string(&json!({
                "reason": reason_str,
                "repo_dir": resolved_repo,
                "session_id": lease.session_id,
            }))
            .unwrap_or_else(|_| "{}".to_owned());
            let _ = ledger::log_event(conn, ts, "CLEAN", owner_pid, Some("session"), &detail);
            stale_holders.push(session_holder_from_lease(lease));
            continue;
        }

        // Live (or indeterminate) lease — evaluate it.
        let lease_holder = session_holder_from_lease(lease);
        let lease_mode = &lease.repo_lock_mode;
        let held_scopes: Vec<String> = lease.write_scopes.clone();
        let overlaps = crate::overlap_paths(&requested_scopes, &held_scopes);

        // Python: `if exclusive or lease_mode == "exclusive":`
        if exclusive || lease_mode == "exclusive" {
            let reason = if lease_mode == "exclusive" {
                format!(
                    "repo {resolved_repo} locked by exclusive session {}",
                    lease.session_id
                )
            } else {
                format!(
                    "exclusive repo lock blocked by active session {}",
                    lease.session_id
                )
            };
            let mut d = Decision::deny(reason);
            d.holder = Some(lease_holder.clone());
            d.holders = vec![lease_holder];
            d.overlap_paths = overlaps;
            d.stale_holders = stale_holders;
            return d;
        }

        // Python: `if requested_scopes and held_scopes and overlaps:`
        if !requested_scopes.is_empty() && !held_scopes.is_empty() && !overlaps.is_empty() {
            let mut d = Decision::deny(format!(
                "repo {resolved_repo} write scope overlaps active session {}",
                lease.session_id
            ));
            d.holder = Some(lease_holder.clone());
            d.holders = vec![lease_holder];
            d.overlap_paths = overlaps;
            d.stale_holders = stale_holders;
            return d;
        }

        advisory_holders.push(lease_holder);
    }

    // Post-loop decisions.
    if owned_by_current_session {
        let mut d = Decision::allow("repo available (owned by current session)");
        d.stale_holders = stale_holders;
        d.safe_mode = Some("same-session".to_owned());
        return d;
    }

    if !advisory_holders.is_empty() {
        let (reason, safe_mode) = if !requested_scopes.is_empty() {
            (
                "repo available; cooperative sessions have no overlapping write scopes",
                "cooperative-write",
            )
        } else {
            (
                "repo available; cooperative sessions present",
                "declare --write-scope before editing",
            )
        };
        let mut d = Decision::allow(reason);
        d.holders = advisory_holders;
        d.stale_holders = stale_holders;
        d.safe_mode = Some(safe_mode.to_owned());
        return d;
    }

    let reason = if stale_holders.is_empty() {
        "repo available"
    } else {
        "repo available (stale session lease cleared)"
    };
    let mut d = Decision::allow(reason);
    d.stale_holders = stale_holders;
    d
}

#[cfg(test)]
mod tests {
    // Unit tests for age_seconds (pure function, no DB).
    use super::age_seconds;

    #[test]
    fn age_seconds_same_ts_is_zero() {
        let ts = "2026-06-13T12:00:00+00:00";
        assert_eq!(age_seconds(ts, ts), Some(0));
    }

    #[test]
    fn age_seconds_300s_difference() {
        let older = "2026-06-13T11:55:00+00:00";
        let newer = "2026-06-13T12:00:00+00:00";
        assert_eq!(age_seconds(older, newer), Some(300));
    }

    #[test]
    fn age_seconds_60s_difference() {
        let older = "2026-06-13T11:59:00+00:00";
        let newer = "2026-06-13T12:00:00+00:00";
        assert_eq!(age_seconds(older, newer), Some(60));
    }

    #[test]
    fn age_seconds_malformed_returns_none() {
        assert_eq!(age_seconds("not-a-date", "2026-06-13T12:00:00+00:00"), None);
    }
}
