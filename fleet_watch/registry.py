"""SQLite registry for Fleet Watch process tracking."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FLEET_DIR = Path.home() / ".fleet-watch"
DB_PATH = FLEET_DIR / "registry.db"
DEFAULT_GPU_TOTAL_MB = 131072
DEFAULT_GPU_RESERVE_MB = 16384

SCHEMA = """
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

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    pid         INTEGER,
    workstream  TEXT,
    detail      TEXT,
    prev_hash   TEXT,
    hash        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gpu_budget (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    total_mb        INTEGER NOT NULL DEFAULT 131072,
    reserve_mb      INTEGER NOT NULL DEFAULT 16384,
    allocated_mb    INTEGER NOT NULL DEFAULT 0
);
"""

RESTART_POLICIES = frozenset({
    "RESTART_ALWAYS",
    "RESTART_ON_FAILURE",
    "RESTART_NEVER",
    "ALERT_ONLY",
})


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dir() -> Path:
    FLEET_DIR.mkdir(parents=True, exist_ok=True)
    return FLEET_DIR


def _configured_budget_defaults() -> tuple[int, int]:
    config_path = FLEET_DIR / "config.json"
    total_mb = DEFAULT_GPU_TOTAL_MB
    reserve_mb = DEFAULT_GPU_RESERVE_MB

    if not config_path.exists():
        return total_mb, reserve_mb

    try:
        config = json.loads(config_path.read_text())
    except json.JSONDecodeError:
        return total_mb, reserve_mb

    try:
        total_mb = int(config.get("gpu_total_mb", total_mb))
        reserve_mb = int(config.get("gpu_reserve_mb", reserve_mb))
    except (TypeError, ValueError):
        return DEFAULT_GPU_TOTAL_MB, DEFAULT_GPU_RESERVE_MB

    if total_mb <= 0 or reserve_mb < 0 or reserve_mb >= total_mb:
        return DEFAULT_GPU_TOTAL_MB, DEFAULT_GPU_RESERVE_MB

    return total_mb, reserve_mb


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    ensure_dir()
    total_mb, reserve_mb = _configured_budget_defaults()
    conn = sqlite3.connect(str(path), timeout=10)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    # Ensure gpu_budget singleton exists
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, ?, ?, 0)",
        (total_mb, reserve_mb),
    )
    conn.execute(
        "UPDATE gpu_budget SET total_mb = ?, reserve_mb = ? WHERE id = 1",
        (total_mb, reserve_mb),
    )
    conn.commit()
    return conn


def register_process(
    conn: sqlite3.Connection,
    pid: int,
    name: str,
    workstream: str,
    session_id: str | None = None,
    port: int | None = None,
    gpu_mb: int = 0,
    repo_dir: str | None = None,
    model: str | None = None,
    priority: int = 3,
    restart_policy: str = "ALERT_ONLY",
    start_cmd: str | None = None,
    expected_duration_min: int | None = None,
) -> None:
    if restart_policy not in RESTART_POLICIES:
        raise ValueError(f"Invalid restart policy: {restart_policy}")
    if not 1 <= priority <= 5:
        raise ValueError(f"Priority must be 1-5, got {priority}")

    now = _now_iso()
    sid = session_id or f"cli-{pid}"

    # Resolve repo_dir to absolute path
    resolved_repo = str(Path(repo_dir).resolve()) if repo_dir else None

    conn.execute(
        """INSERT INTO processes
           (pid, session_id, workstream, name, priority, port, gpu_mb, repo_dir,
            model, restart_policy, start_cmd, start_time, last_heartbeat,
            expected_duration_min)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (pid, sid, workstream, name, priority, port, gpu_mb, resolved_repo,
         model, restart_policy, start_cmd, now, now, expected_duration_min),
    )
    # Update GPU budget
    if gpu_mb > 0:
        conn.execute(
            "UPDATE gpu_budget SET allocated_mb = allocated_mb + ? WHERE id = 1",
            (gpu_mb,),
        )
    conn.commit()


def release_process(conn: sqlite3.Connection, pid: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT pid, name, workstream, gpu_mb FROM processes WHERE pid = ?", (pid,)
    ).fetchone()
    if not row:
        return None

    gpu_mb = row[3] or 0
    conn.execute("DELETE FROM processes WHERE pid = ?", (pid,))
    if gpu_mb > 0:
        conn.execute(
            "UPDATE gpu_budget SET allocated_mb = MAX(0, allocated_mb - ?) WHERE id = 1",
            (gpu_mb,),
        )
    conn.commit()
    return {"pid": row[0], "name": row[1], "workstream": row[2], "gpu_mb": gpu_mb}


def release_port(conn: sqlite3.Connection, port: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT pid, name, workstream, gpu_mb FROM processes WHERE port = ?", (port,)
    ).fetchone()
    if not row:
        return None
    return release_process(conn, row[0])


def heartbeat(conn: sqlite3.Connection, pid: int) -> bool:
    now = _now_iso()
    cursor = conn.execute(
        "UPDATE processes SET last_heartbeat = ? WHERE pid = ?", (now, pid)
    )
    conn.commit()
    return cursor.rowcount > 0


def get_process(conn: sqlite3.Connection, pid: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM processes WHERE pid = ?", (pid,)).fetchone()
    if not row:
        return None
    return _row_to_dict(row, conn)


def get_all_processes(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM processes ORDER BY priority DESC, start_time ASC").fetchall()
    return [_row_to_dict(r, conn) for r in rows]


def get_process_by_port(conn: sqlite3.Connection, port: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM processes WHERE port = ?", (port,)).fetchone()
    if not row:
        return None
    return _row_to_dict(row, conn)


def get_process_by_repo(conn: sqlite3.Connection, repo_dir: str) -> dict[str, Any] | None:
    resolved = str(Path(repo_dir).resolve())
    row = conn.execute("SELECT * FROM processes WHERE repo_dir = ?", (resolved,)).fetchone()
    if not row:
        return None
    return _row_to_dict(row, conn)


def get_gpu_budget(conn: sqlite3.Connection) -> dict[str, int]:
    row = conn.execute("SELECT total_mb, reserve_mb, allocated_mb FROM gpu_budget WHERE id = 1").fetchone()
    total, reserve, allocated = row
    return {
        "total_mb": total,
        "reserve_mb": reserve,
        "allocated_mb": allocated,
        "available_mb": total - reserve - allocated,
    }


def get_stale_processes(conn: sqlite3.Connection, stale_seconds: int = 180) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM processes").fetchall()
    now = datetime.now(timezone.utc)
    stale = []
    for r in rows:
        proc = _row_to_dict(r, conn)
        last_hb = datetime.fromisoformat(proc["last_heartbeat"])
        if last_hb.tzinfo is None:
            last_hb = last_hb.replace(tzinfo=timezone.utc)
        age = (now - last_hb).total_seconds()
        if age > stale_seconds:
            proc["stale_seconds"] = int(age)
            stale.append(proc)
    return stale


def clean_dead_pids(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Remove entries for PIDs that no longer exist."""
    rows = conn.execute("SELECT pid, name, workstream, gpu_mb FROM processes").fetchall()
    cleaned = []
    for pid, name, ws, gpu_mb in rows:
        try:
            os.kill(pid, 0)  # Check if process exists
        except ProcessLookupError:
            release_process(conn, pid)
            cleaned.append({"pid": pid, "name": name, "workstream": ws})
        except PermissionError:
            pass  # Process exists but we can't signal it — leave it
    return cleaned


def get_claimed_ports(conn: sqlite3.Connection) -> dict[int, int]:
    """Return {port: pid} for all claimed ports."""
    rows = conn.execute("SELECT port, pid FROM processes WHERE port IS NOT NULL").fetchall()
    return {port: pid for port, pid in rows}


def get_locked_repos(conn: sqlite3.Connection) -> dict[str, int]:
    """Return {repo_dir: pid} for all locked repos."""
    rows = conn.execute("SELECT repo_dir, pid FROM processes WHERE repo_dir IS NOT NULL").fetchall()
    return {repo: pid for repo, pid in rows}


def _row_to_dict(row: tuple, conn: sqlite3.Connection) -> dict[str, Any]:
    cols = [
        "pid", "session_id", "workstream", "name", "priority",
        "port", "gpu_mb", "repo_dir", "model", "restart_policy",
        "start_cmd", "start_time", "last_heartbeat", "expected_duration_min",
    ]
    return dict(zip(cols, row))
