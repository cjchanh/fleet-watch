//! Read-only registry layer — mirrors the relevant getters from
//! `fleet_watch.registry` (Python).  This module never writes to the
//! database; it is the safe read surface for `checks.rs` and `reconciler.rs`.
//!
//! SQL and math are ported faithfully from:
//!   - `registry.py` `get_process_by_port`              (line 719)
//!   - `registry.py` `get_gpu_budget`                   (line 736)
//!   - `registry.py` `get_process_by_repo`              (line 727)
//!   - `registry.py` `get_external_resources_by_repo`   (line 1200)
//!   - `registry.py` `get_active_session_leases_by_repo`(line 860)
//!   - `registry.py` `SCHEMA` CREATE TABLE statements

use rusqlite::{Connection, OpenFlags, OptionalExtension, Result};

// ── DB open ──────────────────────────────────────────────────────────────────

/// Open the Fleet Watch registry database in read-only mode.
///
/// Matches `sqlite3.connect(str(path), timeout=10)` with the SQLITE_OPEN_READONLY
/// flag.  Fails cleanly if the file does not exist.
pub fn open_readonly(path: &std::path::Path) -> Result<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
}

// ── ProcessRow ───────────────────────────────────────────────────────────────

/// Minimal row from the `processes` table — the fields `check_port` and
/// `reconciler` need.
///
/// Column order matches the SCHEMA:
/// pid, session_id, workstream, name, priority, port, gpu_mb, repo_dir,
/// model, restart_policy, start_cmd, start_time, last_heartbeat,
/// expected_duration_min
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessRow {
    pub pid: i64,
    pub session_id: String,
    pub workstream: String,
    pub name: String,
    pub priority: i64,
    pub port: Option<i64>,
    pub repo_dir: Option<String>,
}

/// Return the process that has claimed `port`, or `None` if the port is free.
///
/// Faithful port of `registry.get_process_by_port`:
/// ```sql
/// SELECT * FROM processes WHERE port = ?
/// ```
/// and then `_row_to_dict` extracts the fields.
pub fn get_process_by_port(conn: &Connection, port: i64) -> Result<Option<ProcessRow>> {
    let mut stmt = conn.prepare(
        "SELECT pid, session_id, workstream, name, priority, port, repo_dir \
         FROM processes WHERE port = ?",
    )?;
    let mut rows = stmt.query([port])?;
    match rows.next()? {
        None => Ok(None),
        Some(row) => Ok(Some(ProcessRow {
            pid: row.get(0)?,
            session_id: row.get(1)?,
            workstream: row.get(2)?,
            name: row.get(3)?,
            priority: row.get(4)?,
            port: row.get(5)?,
            repo_dir: row.get(6)?,
        })),
    }
}

/// Return the process that has locked `repo_dir`, or `None` if the repo is free.
///
/// Faithful port of `registry.get_process_by_repo`:
/// ```python
/// resolved = str(Path(repo_dir).resolve())
/// row = conn.execute("SELECT * FROM processes WHERE repo_dir = ?", (resolved,))
/// ```
/// The caller is responsible for passing the already-resolved path.
pub fn get_process_by_repo(conn: &Connection, resolved_repo: &str) -> Result<Option<ProcessRow>> {
    conn.query_row(
        "SELECT pid, session_id, workstream, name, priority, port, repo_dir \
         FROM processes WHERE repo_dir = ?",
        [resolved_repo],
        |row| {
            Ok(ProcessRow {
                pid: row.get(0)?,
                session_id: row.get(1)?,
                workstream: row.get(2)?,
                name: row.get(3)?,
                priority: row.get(4)?,
                port: row.get(5)?,
                repo_dir: row.get(6)?,
            })
        },
    )
    .optional()
}

// ── SessionLeaseRow ──────────────────────────────────────────────────────────

/// One row from `session_leases` — the fields `reconciler` needs.
///
/// Mirrors the Python `_session_lease_row_to_dict` output, with `write_scopes`
/// already decoded from compact JSON (`_decode_write_scopes`).
#[derive(Debug, Clone, PartialEq)]
pub struct SessionLeaseRow {
    pub session_id: String,
    pub owner_pid: Option<i64>,
    pub repo_dir: Option<String>,
    pub repo_lock_mode: String,
    pub write_scopes: Vec<String>,
    pub last_heartbeat_at: String,
}

/// Decode the compact-JSON write_scopes column to a `Vec<String>`.
///
/// Faithful port of `registry._decode_write_scopes`:
/// - NULL or empty string → `[]`
/// - Invalid JSON or non-array → `[]`
/// - Filters out non-string elements
fn decode_write_scopes(raw: Option<&str>) -> Vec<String> {
    let raw = match raw {
        Some(r) if !r.is_empty() => r,
        _ => return Vec::new(),
    };
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(serde_json::Value::Array(arr)) => arr
            .into_iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    }
}

/// Return all ACTIVE session leases for `repo_dir` (already resolved), ordered
/// by last_heartbeat_at DESC.
///
/// Faithful port of `registry.get_active_session_leases_by_repo`:
/// ```sql
/// SELECT ... FROM session_leases
/// WHERE repo_dir = ? AND status = 'ACTIVE' AND shutdown_at IS NULL
/// ORDER BY last_heartbeat_at DESC
/// ```
/// `_session_lease_row_to_dict` normalises `repo_lock_mode` to
/// `"cooperative"` when NULL and decodes `write_scopes`.
pub fn get_active_session_leases_by_repo(
    conn: &Connection,
    resolved_repo: &str,
) -> Result<Vec<SessionLeaseRow>> {
    let mut stmt = conn.prepare(
        "SELECT session_id, owner_pid, repo_dir, repo_lock_mode, write_scopes, last_heartbeat_at \
         FROM session_leases \
         WHERE repo_dir = ? AND status = 'ACTIVE' AND shutdown_at IS NULL \
         ORDER BY last_heartbeat_at DESC",
    )?;
    let mut rows = stmt.query([resolved_repo])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let raw_scopes: Option<String> = row.get(4)?;
        let repo_lock_mode: Option<String> = row.get(3)?;
        out.push(SessionLeaseRow {
            session_id: row.get(0)?,
            owner_pid: row.get(1)?,
            repo_dir: row.get(2)?,
            repo_lock_mode: repo_lock_mode.unwrap_or_else(|| "cooperative".to_owned()),
            write_scopes: decode_write_scopes(raw_scopes.as_deref()),
            last_heartbeat_at: row.get(5)?,
        });
    }
    Ok(out)
}

// ── ExternalResourceRow ──────────────────────────────────────────────────────

/// Fields from `external_resources` needed by the reconciler.
///
/// The reconciler only needs to check `session_id` and build a deny reason
/// from `provider`, `external_id`, and `name`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalResourceRow {
    pub session_id: String,
    pub provider: String,
    pub external_id: String,
    pub name: String,
}

/// Return all external resources associated with `repo_dir` (resolved), ordered
/// by priority DESC, start_time ASC.
///
/// Faithful port of `registry.get_external_resources_by_repo`:
/// ```sql
/// SELECT ... FROM external_resources WHERE repo_dir = ? ORDER BY priority DESC, start_time ASC
/// ```
pub fn get_external_resources_by_repo(
    conn: &Connection,
    resolved_repo: &str,
) -> Result<Vec<ExternalResourceRow>> {
    let mut stmt = conn.prepare(
        "SELECT provider, external_id, session_id, name \
         FROM external_resources \
         WHERE repo_dir = ? \
         ORDER BY priority DESC, start_time ASC",
    )?;
    let mut rows = stmt.query([resolved_repo])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(ExternalResourceRow {
            provider: row.get(0)?,
            external_id: row.get(1)?,
            session_id: row.get(2)?,
            name: row.get(3)?,
        });
    }
    Ok(out)
}

// ── GpuBudget ────────────────────────────────────────────────────────────────

/// Current GPU budget state.
///
/// `available_mb` is computed exactly as `registry.get_gpu_budget`:
/// ```python
/// "available_mb": total - reserve - allocated
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct GpuBudget {
    pub total_mb: i64,
    pub reserve_mb: i64,
    pub allocated_mb: i64,
    pub available_mb: i64,
}

/// Return the current GPU budget row from the singleton.
///
/// Faithful port of `registry.get_gpu_budget`:
/// ```sql
/// SELECT total_mb, reserve_mb, allocated_mb FROM gpu_budget WHERE id = 1
/// ```
/// with `available_mb = total - reserve - allocated`.
pub fn get_gpu_budget(conn: &Connection) -> Result<GpuBudget> {
    let mut stmt =
        conn.prepare("SELECT total_mb, reserve_mb, allocated_mb FROM gpu_budget WHERE id = 1")?;
    let mut rows = stmt.query([])?;
    let row = rows
        .next()?
        .ok_or_else(|| rusqlite::Error::QueryReturnedNoRows)?;
    let total: i64 = row.get(0)?;
    let reserve: i64 = row.get(1)?;
    let allocated: i64 = row.get(2)?;
    Ok(GpuBudget {
        total_mb: total,
        reserve_mb: reserve,
        allocated_mb: allocated,
        available_mb: total - reserve - allocated,
    })
}
