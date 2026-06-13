//! Read-only registry layer — mirrors the relevant getters from
//! `fleet_watch.registry` (Python).  This module never writes to the
//! database; it is the safe read surface for `checks.rs`.
//!
//! SQL and math are ported faithfully from:
//!   - `registry.py` `get_process_by_port`   (line 719)
//!   - `registry.py` `get_gpu_budget`         (line 736)
//!   - `registry.py` `SCHEMA` CREATE TABLE statements

use rusqlite::{Connection, OpenFlags, Result};

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

/// Minimal row from the `processes` table — the fields `check_port` needs.
///
/// Column order matches the SCHEMA:
/// pid, session_id, workstream, name, priority, port, gpu_mb, repo_dir,
/// model, restart_policy, start_cmd, start_time, last_heartbeat,
/// expected_duration_min
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessRow {
    pub pid: i64,
    pub workstream: String,
    pub name: String,
    pub priority: i64,
    pub port: Option<i64>,
}

/// Return the process that has claimed `port`, or `None` if the port is free.
///
/// Faithful port of `registry.get_process_by_port`:
/// ```sql
/// SELECT * FROM processes WHERE port = ?
/// ```
/// and then `_row_to_dict` extracts `pid` + `name`.
pub fn get_process_by_port(conn: &Connection, port: i64) -> Result<Option<ProcessRow>> {
    let mut stmt = conn.prepare(
        "SELECT pid, session_id, workstream, name, priority, port FROM processes WHERE port = ?",
    )?;
    let mut rows = stmt.query([port])?;
    match rows.next()? {
        None => Ok(None),
        Some(row) => Ok(Some(ProcessRow {
            pid: row.get(0)?,
            workstream: row.get(2)?,
            name: row.get(3)?,
            priority: row.get(4)?,
            port: row.get(5)?,
        })),
    }
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
