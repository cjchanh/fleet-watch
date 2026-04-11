"""SQLite registry for Fleet Watch process tracking."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FLEET_DIR = Path.home() / ".fleet-watch"
DB_PATH = FLEET_DIR / "registry.db"
DEFAULT_GPU_TOTAL_MB = 131072
DEFAULT_GPU_RESERVE_MB = 16384
DEFAULT_STALE_SECONDS = 180

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

CREATE TABLE IF NOT EXISTS session_leases (
    session_id          TEXT PRIMARY KEY,
    owner_pid           INTEGER,
    owner_ppid          INTEGER,
    owner_pgid          INTEGER,
    owner_tty           TEXT,
    repo_dir            TEXT,
    started_at          TEXT NOT NULL,
    last_heartbeat_at   TEXT NOT NULL,
    shutdown_at         TEXT,
    status              TEXT NOT NULL DEFAULT 'ACTIVE'
);

CREATE TABLE IF NOT EXISTS external_resources (
    provider        TEXT NOT NULL,
    resource_type   TEXT NOT NULL,
    external_id     TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    workstream      TEXT NOT NULL,
    name            TEXT NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 3,
    gpu_mb          INTEGER DEFAULT 0,
    repo_dir        TEXT,
    model           TEXT,
    status          TEXT NOT NULL DEFAULT 'ACTIVE',
    started_by      TEXT,
    owner_tool      TEXT,
    endpoint        TEXT,
    cleanup_cmd     TEXT,
    safe_to_delete  INTEGER NOT NULL DEFAULT 0,
    metadata        TEXT,
    start_time      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    PRIMARY KEY(provider, external_id)
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

SESSION_LEASE_STATUSES = frozenset({
    "ACTIVE",
    "CLOSED",
})

PROCESS_STATES = frozenset({
    "live",
    "disconnected",
    "stale_candidate",
    "orphan_confirmed",
    "exited",
})


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dir() -> Path:
    FLEET_DIR.mkdir(parents=True, exist_ok=True)
    return FLEET_DIR


def _resolve_repo_dir(repo_dir: str | None) -> str | None:
    return str(Path(repo_dir).resolve()) if repo_dir else None


def _age_seconds(iso_ts: str | None) -> int | None:
    if not iso_ts:
        return None
    ts = datetime.fromisoformat(iso_ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return int((datetime.now(timezone.utc) - ts).total_seconds())


def _pid_exists(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _inspect_process(pid: int | None) -> dict[str, Any] | None:
    if pid is None or pid <= 0:
        return None
    if not _pid_exists(pid):
        return None

    try:
        result = subprocess.run(
            ["ps", "-o", "ppid=", "-o", "pgid=", "-o", "tty=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired) as exc:
        return {
            "pid": pid,
            "alive": True,
            "inspectable": False,
            "ppid": None,
            "pgid": None,
            "tty": None,
            "error": str(exc),
        }

    line = result.stdout.strip()
    if result.returncode != 0 or not line:
        return {
            "pid": pid,
            "alive": True,
            "inspectable": False,
            "ppid": None,
            "pgid": None,
            "tty": None,
            "error": result.stderr.strip() or "ps inspection failed",
        }

    parts = line.split(None, 2)
    if len(parts) < 2:
        return {
            "pid": pid,
            "alive": True,
            "inspectable": False,
            "ppid": None,
            "pgid": None,
            "tty": None,
            "error": f"unexpected ps output: {line}",
        }

    tty = parts[2] if len(parts) >= 3 else "?"
    try:
        return {
            "pid": pid,
            "alive": True,
            "inspectable": True,
            "ppid": int(parts[0]),
            "pgid": int(parts[1]),
            "tty": tty,
        }
    except ValueError:
        return {
            "pid": pid,
            "alive": True,
            "inspectable": False,
            "ppid": None,
            "pgid": None,
            "tty": tty,
            "error": f"unexpected ps output: {line}",
        }


def _is_parent_chain_detached(pid: int) -> bool | None:
    info = _inspect_process(pid)
    if info is None:
        return True
    if not info.get("inspectable"):
        return None

    seen: set[int] = {pid}
    current = info
    while True:
        parent_pid = current["ppid"]
        if parent_pid in (0, 1):
            return True
        if parent_pid in seen:
            return None
        if not _pid_exists(parent_pid):
            return True

        parent_info = _inspect_process(parent_pid)
        if parent_info is None:
            return True
        if not parent_info.get("inspectable"):
            return None

        parent_tty = (parent_info.get("tty") or "").strip()
        if parent_tty and parent_tty not in {"?", "??"}:
            return False

        seen.add(parent_pid)
        current = parent_info


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


def upsert_session_lease(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    owner_pid: int | None = None,
    repo_dir: str | None = None,
    status: str = "ACTIVE",
) -> None:
    if status not in SESSION_LEASE_STATUSES:
        raise ValueError(f"Invalid session lease status: {status}")

    now = _now_iso()
    resolved_repo = _resolve_repo_dir(repo_dir)
    inspect = _inspect_process(owner_pid)
    owner_ppid = inspect.get("ppid") if inspect else None
    owner_pgid = inspect.get("pgid") if inspect else None
    owner_tty = inspect.get("tty") if inspect else None

    conn.execute(
        """
        INSERT INTO session_leases (
            session_id, owner_pid, owner_ppid, owner_pgid, owner_tty, repo_dir,
            started_at, last_heartbeat_at, shutdown_at, status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
        ON CONFLICT(session_id) DO UPDATE SET
            owner_pid = COALESCE(excluded.owner_pid, session_leases.owner_pid),
            owner_ppid = COALESCE(excluded.owner_ppid, session_leases.owner_ppid),
            owner_pgid = COALESCE(excluded.owner_pgid, session_leases.owner_pgid),
            owner_tty = COALESCE(excluded.owner_tty, session_leases.owner_tty),
            repo_dir = COALESCE(excluded.repo_dir, session_leases.repo_dir),
            last_heartbeat_at = excluded.last_heartbeat_at,
            shutdown_at = NULL,
            status = excluded.status
        """,
        (
            session_id,
            owner_pid,
            owner_ppid,
            owner_pgid,
            owner_tty,
            resolved_repo,
            now,
            now,
            status,
        ),
    )
    conn.commit()


def heartbeat_session_lease(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    owner_pid: int | None = None,
    repo_dir: str | None = None,
) -> bool:
    lease = get_session_lease(conn, session_id)
    if lease is None:
        if owner_pid is None:
            return False
        upsert_session_lease(conn, session_id, owner_pid=owner_pid, repo_dir=repo_dir)
        return True

    now = _now_iso()
    pid_to_use = owner_pid if owner_pid is not None else lease.get("owner_pid")
    inspect = _inspect_process(pid_to_use)
    owner_ppid = inspect.get("ppid") if inspect else lease.get("owner_ppid")
    owner_pgid = inspect.get("pgid") if inspect else lease.get("owner_pgid")
    owner_tty = inspect.get("tty") if inspect else lease.get("owner_tty")
    resolved_repo = _resolve_repo_dir(repo_dir) or lease.get("repo_dir")
    cursor = conn.execute(
        """
        UPDATE session_leases
        SET owner_pid = COALESCE(?, owner_pid),
            owner_ppid = ?,
            owner_pgid = ?,
            owner_tty = ?,
            repo_dir = COALESCE(?, repo_dir),
            last_heartbeat_at = ?,
            shutdown_at = NULL,
            status = 'ACTIVE'
        WHERE session_id = ?
        """,
        (
            pid_to_use,
            owner_ppid,
            owner_pgid,
            owner_tty,
            resolved_repo,
            now,
            session_id,
        ),
    )
    conn.commit()
    return cursor.rowcount > 0


def close_session_lease(conn: sqlite3.Connection, session_id: str) -> bool:
    now = _now_iso()
    cursor = conn.execute(
        """
        UPDATE session_leases
        SET shutdown_at = ?,
            last_heartbeat_at = ?,
            status = 'CLOSED'
        WHERE session_id = ?
        """,
        (now, now, session_id),
    )
    conn.commit()
    return cursor.rowcount > 0


def get_session_lease(conn: sqlite3.Connection, session_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT session_id, owner_pid, owner_ppid, owner_pgid, owner_tty, repo_dir,
               started_at, last_heartbeat_at, shutdown_at, status
        FROM session_leases
        WHERE session_id = ?
        """,
        (session_id,),
    ).fetchone()
    if not row:
        return None
    return _session_lease_row_to_dict(row)


def list_session_leases(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT session_id, owner_pid, owner_ppid, owner_pgid, owner_tty, repo_dir,
               started_at, last_heartbeat_at, shutdown_at, status
        FROM session_leases
        ORDER BY started_at ASC
        """
    ).fetchall()
    return [_session_lease_row_to_dict(row) for row in rows]


def list_active_session_leases(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT session_id, owner_pid, owner_ppid, owner_pgid, owner_tty, repo_dir,
               started_at, last_heartbeat_at, shutdown_at, status
        FROM session_leases
        WHERE status = 'ACTIVE' AND shutdown_at IS NULL
        ORDER BY last_heartbeat_at DESC
        """
    ).fetchall()
    return [_session_lease_row_to_dict(row) for row in rows]


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
    manage_session_lease: bool = True,
) -> None:
    if restart_policy not in RESTART_POLICIES:
        raise ValueError(f"Invalid restart policy: {restart_policy}")
    if not 1 <= priority <= 5:
        raise ValueError(f"Priority must be 1-5, got {priority}")

    now = _now_iso()
    sid = session_id or f"cli-{pid}"

    # Resolve repo_dir to absolute path
    resolved_repo = _resolve_repo_dir(repo_dir)

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
    if manage_session_lease and get_session_lease(conn, sid) is None:
        inspect = _inspect_process(pid) or {}
        conn.execute(
            """
            INSERT INTO session_leases (
                session_id, owner_pid, owner_ppid, owner_pgid, owner_tty, repo_dir,
                started_at, last_heartbeat_at, shutdown_at, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 'ACTIVE')
            """,
            (
                sid,
                pid,
                inspect.get("ppid"),
                inspect.get("pgid"),
                inspect.get("tty"),
                resolved_repo,
                now,
                now,
            ),
        )
    conn.commit()


def release_process(conn: sqlite3.Connection, pid: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT pid, session_id, name, workstream, gpu_mb FROM processes WHERE pid = ?", (pid,)
    ).fetchone()
    if not row:
        return None

    gpu_mb = row[4] or 0
    conn.execute("DELETE FROM processes WHERE pid = ?", (pid,))
    if gpu_mb > 0:
        conn.execute(
            "UPDATE gpu_budget SET allocated_mb = MAX(0, allocated_mb - ?) WHERE id = 1",
            (gpu_mb,),
        )
    session_id = row[1]
    if session_id == f"cli-{pid}":
        remaining = conn.execute(
            "SELECT COUNT(*) FROM processes WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]
        external_remaining = conn.execute(
            "SELECT COUNT(*) FROM external_resources WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]
        if remaining == 0 and external_remaining == 0:
            now = _now_iso()
            conn.execute(
                """
                UPDATE session_leases
                SET shutdown_at = COALESCE(shutdown_at, ?),
                    last_heartbeat_at = ?,
                    status = 'CLOSED'
                WHERE session_id = ?
                """,
                (now, now, session_id),
            )
    conn.commit()
    return {"pid": row[0], "name": row[2], "workstream": row[3], "gpu_mb": gpu_mb}


def release_port(conn: sqlite3.Connection, port: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT pid, name, workstream, gpu_mb FROM processes WHERE port = ?", (port,)
    ).fetchone()
    if not row:
        return None
    return release_process(conn, row[0])


def heartbeat(conn: sqlite3.Connection, pid: int) -> bool:
    now = _now_iso()
    row = conn.execute(
        "SELECT session_id, repo_dir FROM processes WHERE pid = ?",
        (pid,),
    ).fetchone()
    if row is None:
        return False
    cursor = conn.execute(
        "UPDATE processes SET last_heartbeat = ? WHERE pid = ?", (now, pid)
    )
    lease = get_session_lease(conn, row[0])
    if lease is not None and lease.get("owner_pid") == pid:
        inspect = _inspect_process(pid)
        conn.execute(
            """
            UPDATE session_leases
            SET owner_ppid = ?,
                owner_pgid = ?,
                owner_tty = ?,
                repo_dir = COALESCE(?, repo_dir),
                last_heartbeat_at = ?,
                shutdown_at = NULL,
                status = 'ACTIVE'
            WHERE session_id = ?
            """,
            (
                (inspect or {}).get("ppid"),
                (inspect or {}).get("pgid"),
                (inspect or {}).get("tty"),
                row[1],
                now,
                row[0],
            ),
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


def get_stale_processes(
    conn: sqlite3.Connection,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> list[dict[str, Any]]:
    return [
        proc
        for proc in get_process_classifications(conn, stale_seconds=stale_seconds)
        if (proc.get("heartbeat_age_seconds") or 0) > stale_seconds
    ]


def get_reapable_processes(
    conn: sqlite3.Connection,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> list[dict[str, Any]]:
    return [
        proc
        for proc in get_process_classifications(conn, stale_seconds=stale_seconds)
        if proc["classification"] == "orphan_confirmed"
    ]


def clean_dead_pids(
    conn: sqlite3.Connection,
    exclude_pids: set[int] | None = None,
) -> list[dict[str, Any]]:
    """Remove entries for PIDs that no longer exist.

    *exclude_pids*, when provided, are skipped — they were just confirmed
    alive by the discovery scan and should not be reaped.
    """
    rows = conn.execute("SELECT pid, name, workstream, gpu_mb FROM processes").fetchall()
    skip = exclude_pids or set()
    cleaned = []
    for pid, name, ws, gpu_mb in rows:
        if pid in skip:
            continue
        if not _pid_exists(pid):
            release_process(conn, pid)
            cleaned.append({"pid": pid, "name": name, "workstream": ws})
    return cleaned


def get_claimed_ports(conn: sqlite3.Connection) -> dict[int, int]:
    """Return {port: pid} for all claimed ports."""
    rows = conn.execute("SELECT port, pid FROM processes WHERE port IS NOT NULL").fetchall()
    return {port: pid for port, pid in rows}


def get_locked_repos(conn: sqlite3.Connection) -> dict[str, int]:
    """Return {repo_dir: pid} for all locked repos."""
    rows = conn.execute("SELECT repo_dir, pid FROM processes WHERE repo_dir IS NOT NULL").fetchall()
    return {repo: pid for repo, pid in rows}


def _session_lease_blocks_repo(
    lease: dict[str, Any],
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> bool:
    if lease.get("status") != "ACTIVE" or lease.get("shutdown_at") is not None:
        return False

    owner_pid = lease.get("owner_pid")
    if owner_pid is not None and _pid_exists(owner_pid):
        return True

    heartbeat_age = _age_seconds(lease.get("last_heartbeat_at"))
    return heartbeat_age is not None and heartbeat_age <= stale_seconds


def get_active_session_leases_by_repo(
    conn: sqlite3.Connection,
    repo_dir: str,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> list[dict[str, Any]]:
    resolved = str(Path(repo_dir).resolve())
    rows = conn.execute(
        """
        SELECT session_id, owner_pid, owner_ppid, owner_pgid, owner_tty, repo_dir,
               started_at, last_heartbeat_at, shutdown_at, status
        FROM session_leases
        WHERE repo_dir = ?
        ORDER BY last_heartbeat_at DESC
        """,
        (resolved,),
    ).fetchall()
    leases = [_session_lease_row_to_dict(row) for row in rows]
    return [
        lease
        for lease in leases
        if _session_lease_blocks_repo(lease, stale_seconds=stale_seconds)
    ]


def get_effective_locked_repos(
    conn: sqlite3.Connection,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> dict[str, int | None]:
    locks = get_locked_repos(conn)
    for lease in list_session_leases(conn):
        repo_dir = lease.get("repo_dir")
        if not repo_dir or repo_dir in locks:
            continue
        if _session_lease_blocks_repo(lease, stale_seconds=stale_seconds):
            locks[repo_dir] = lease.get("owner_pid")
    return locks


def get_process_classifications(
    conn: sqlite3.Connection,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for proc in get_all_processes(conn):
        process_alive = _pid_exists(proc["pid"])
        heartbeat_age = _age_seconds(proc.get("last_heartbeat"))
        stale = heartbeat_age is not None and heartbeat_age > stale_seconds
        lease = get_session_lease(conn, proc["session_id"])
        lease_present = lease is not None
        lease_active = bool(lease and lease["status"] == "ACTIVE" and lease["shutdown_at"] is None)
        owner_pid = lease.get("owner_pid") if lease else None
        owner_alive = _pid_exists(owner_pid) if owner_pid else None
        process_info = _inspect_process(proc["pid"]) if process_alive else None
        parent_chain_detached = (
            _is_parent_chain_detached(proc["pid"])
            if process_alive else True
        )
        evidence: list[str] = []

        if not process_alive:
            classification = "exited"
            evidence.append("registered PID is no longer running")
        elif lease_active and owner_alive and not stale:
            classification = "live"
            evidence.append(f"active session lease owner PID {owner_pid} is alive")
        elif lease_present and not lease_active and stale and parent_chain_detached is True:
            classification = "orphan_confirmed"
            evidence.append("process heartbeat expired")
            evidence.append("session lease is closed or owner is gone")
            evidence.append("parent chain is detached")
        elif stale:
            if lease_present:
                classification = "stale_candidate"
                evidence.append("process heartbeat expired")
                evidence.append(f"session lease status={lease['status']}")
                if owner_pid:
                    evidence.append(
                        "session owner alive"
                        if owner_alive else "session owner missing"
                    )
                if parent_chain_detached is None:
                    evidence.append("parent chain inspection unavailable")
                elif parent_chain_detached:
                    evidence.append("parent chain detached")
                else:
                    evidence.append("parent chain still attached")
            else:
                classification = "disconnected"
                evidence.append("process heartbeat expired")
                evidence.append("session lease missing")
                if parent_chain_detached is None:
                    evidence.append("parent chain inspection unavailable")
                elif parent_chain_detached:
                    evidence.append("parent chain detached")
                else:
                    evidence.append("parent chain still attached")
        else:
            classification = "disconnected"
            if not lease_present:
                evidence.append("session lease missing")
            elif not lease_active:
                evidence.append(f"session lease closed ({lease['status']})")
            elif owner_pid and not owner_alive:
                evidence.append(f"session owner PID {owner_pid} is not running")
            else:
                evidence.append("ownership evidence incomplete")

        item = dict(proc)
        item.update({
            "classification": classification,
            "heartbeat_age_seconds": heartbeat_age,
            "stale_seconds": heartbeat_age if stale else 0,
            "process_alive": process_alive,
            "session_lease_present": lease_present,
            "session_lease_status": lease["status"] if lease else "MISSING",
            "session_lease_owner_pid": owner_pid,
            "session_lease_owner_alive": owner_alive,
            "session_lease_last_heartbeat_age_seconds": (
                _age_seconds(lease.get("last_heartbeat_at")) if lease else None
            ),
            "session_lease_shutdown_at": lease.get("shutdown_at") if lease else None,
            "parent_pid": process_info.get("ppid") if process_info else None,
            "process_group_id": process_info.get("pgid") if process_info else None,
            "tty": process_info.get("tty") if process_info else None,
            "parent_chain_detached": parent_chain_detached,
            "safe_to_reap": classification == "orphan_confirmed",
            "evidence": evidence,
        })
        results.append(item)
    return results


def register_external_resource(
    conn: sqlite3.Connection,
    *,
    provider: str,
    resource_type: str,
    external_id: str,
    session_id: str | None = None,
    workstream: str,
    name: str,
    priority: int = 3,
    gpu_mb: int = 0,
    repo_dir: str | None = None,
    model: str | None = None,
    status: str = "ACTIVE",
    started_by: str | None = None,
    owner_tool: str | None = None,
    endpoint: str | None = None,
    cleanup_cmd: str | None = None,
    safe_to_delete: bool = False,
    metadata: dict[str, Any] | None = None,
) -> None:
    if not provider:
        raise ValueError("provider is required")
    if not resource_type:
        raise ValueError("resource_type is required")
    if not external_id:
        raise ValueError("external_id is required")
    if not 1 <= priority <= 5:
        raise ValueError(f"Priority must be 1-5, got {priority}")

    now = _now_iso()
    sid = session_id or f"{provider}-{external_id}"
    resolved_repo = _resolve_repo_dir(repo_dir)
    metadata_json = json.dumps(metadata or {}, separators=(",", ":"))

    conn.execute(
        """INSERT OR REPLACE INTO external_resources
           (provider, resource_type, external_id, session_id, workstream, name,
            priority, gpu_mb, repo_dir, model, status, started_by, owner_tool,
            endpoint, cleanup_cmd, safe_to_delete, metadata, start_time, last_seen)
           VALUES (
             ?, ?, ?, ?, ?, ?,
             ?, ?, ?, ?, ?, ?, ?,
             ?, ?, ?, ?,
             COALESCE(
               (SELECT start_time FROM external_resources WHERE provider = ? AND external_id = ?),
               ?
             ),
             ?
           )""",
        (
            provider,
            resource_type,
            external_id,
            sid,
            workstream,
            name,
            priority,
            gpu_mb,
            resolved_repo,
            model,
            status,
            started_by,
            owner_tool,
            endpoint,
            cleanup_cmd,
            1 if safe_to_delete else 0,
            metadata_json,
            provider,
            external_id,
            now,
            now,
        ),
    )
    lease = get_session_lease(conn, sid)
    if lease is None:
        conn.execute(
            """
            INSERT INTO session_leases (
                session_id, owner_pid, owner_ppid, owner_pgid, owner_tty, repo_dir,
                started_at, last_heartbeat_at, shutdown_at, status
            )
            VALUES (?, NULL, NULL, NULL, NULL, ?, ?, ?, NULL, 'ACTIVE')
            """,
            (sid, resolved_repo, now, now),
        )
        conn.commit()
        return

    conn.execute(
        """
        UPDATE session_leases
        SET repo_dir = COALESCE(?, repo_dir),
            last_heartbeat_at = ?,
            shutdown_at = NULL,
            status = 'ACTIVE'
        WHERE session_id = ?
        """,
        (resolved_repo, now, sid),
    )
    conn.commit()


def heartbeat_external_resource(
    conn: sqlite3.Connection,
    *,
    provider: str,
    external_id: str,
    status: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> bool:
    now = _now_iso()
    fields = ["last_seen = ?"]
    params: list[Any] = [now]
    if status is not None:
        fields.append("status = ?")
        params.append(status)
    if metadata is not None:
        fields.append("metadata = ?")
        params.append(json.dumps(metadata, separators=(",", ":")))
    params.extend([provider, external_id])
    cursor = conn.execute(
        f"UPDATE external_resources SET {', '.join(fields)} WHERE provider = ? AND external_id = ?",
        params,
    )
    conn.commit()
    return cursor.rowcount > 0


def release_external_resource(
    conn: sqlite3.Connection,
    *,
    provider: str,
    external_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """SELECT provider, resource_type, external_id, session_id, workstream, name,
                  priority, gpu_mb, repo_dir, model, status, started_by, owner_tool,
                  endpoint, cleanup_cmd, safe_to_delete, metadata, start_time, last_seen
           FROM external_resources
           WHERE provider = ? AND external_id = ?""",
        (provider, external_id),
    ).fetchone()
    if not row:
        return None
    conn.execute(
        "DELETE FROM external_resources WHERE provider = ? AND external_id = ?",
        (provider, external_id),
    )
    session_id = row[3]
    if session_id == f"{provider}-{external_id}":
        remaining = conn.execute(
            "SELECT COUNT(*) FROM external_resources WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]
        proc_remaining = conn.execute(
            "SELECT COUNT(*) FROM processes WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]
        if remaining == 0 and proc_remaining == 0:
            now = _now_iso()
            conn.execute(
                """
                UPDATE session_leases
                SET shutdown_at = COALESCE(shutdown_at, ?),
                    last_heartbeat_at = ?,
                    status = 'CLOSED'
                WHERE session_id = ?
                """,
                (now, now, session_id),
            )
    conn.commit()
    return _external_row_to_dict(row)


def get_external_resource(
    conn: sqlite3.Connection,
    *,
    provider: str,
    external_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """SELECT provider, resource_type, external_id, session_id, workstream, name,
                  priority, gpu_mb, repo_dir, model, status, started_by, owner_tool,
                  endpoint, cleanup_cmd, safe_to_delete, metadata, start_time, last_seen
           FROM external_resources
           WHERE provider = ? AND external_id = ?""",
        (provider, external_id),
    ).fetchone()
    if not row:
        return None
    return _external_row_to_dict(row)


def get_all_external_resources(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT provider, resource_type, external_id, session_id, workstream, name,
                  priority, gpu_mb, repo_dir, model, status, started_by, owner_tool,
                  endpoint, cleanup_cmd, safe_to_delete, metadata, start_time, last_seen
           FROM external_resources
           ORDER BY priority DESC, start_time ASC"""
    ).fetchall()
    return [_external_row_to_dict(r) for r in rows]


def get_external_resources_by_repo(
    conn: sqlite3.Connection,
    repo_dir: str,
) -> list[dict[str, Any]]:
    resolved = str(Path(repo_dir).resolve())
    rows = conn.execute(
        """SELECT provider, resource_type, external_id, session_id, workstream, name,
                  priority, gpu_mb, repo_dir, model, status, started_by, owner_tool,
                  endpoint, cleanup_cmd, safe_to_delete, metadata, start_time, last_seen
           FROM external_resources
           WHERE repo_dir = ?
           ORDER BY priority DESC, start_time ASC""",
        (resolved,),
    ).fetchall()
    return [_external_row_to_dict(r) for r in rows]


def replace_provider_resources(
    conn: sqlite3.Connection,
    *,
    provider: str,
    resources: list[dict[str, Any]],
) -> None:
    existing = {
        item["external_id"]: item
        for item in get_all_external_resources(conn)
        if item["provider"] == provider
    }
    seen_ids = {item["external_id"] for item in resources}
    for resource in resources:
        prior = existing.get(resource["external_id"])
        register_external_resource(
            conn,
            provider=provider,
            resource_type=resource["resource_type"],
            external_id=resource["external_id"],
            session_id=(prior["session_id"] if prior else None),
            workstream=(prior["workstream"] if prior else resource.get("workstream", provider)),
            name=(prior["name"] if prior else resource["name"]),
            priority=(prior["priority"] if prior else resource.get("priority", 3)),
            gpu_mb=resource.get("gpu_mb", prior["gpu_mb"] if prior else 0),
            repo_dir=(prior["repo_dir"] if prior else resource.get("repo_dir")),
            model=(prior["model"] if prior else resource.get("model")),
            status=resource.get("status", "ACTIVE"),
            started_by=(prior["started_by"] if prior else resource.get("started_by")),
            owner_tool=(prior["owner_tool"] if prior else resource.get("owner_tool")),
            endpoint=(prior["endpoint"] if prior else resource.get("endpoint")),
            cleanup_cmd=(prior["cleanup_cmd"] if prior else resource.get("cleanup_cmd")),
            safe_to_delete=(prior["safe_to_delete"] if prior else resource.get("safe_to_delete", False)),
            metadata={
                **(prior["metadata"] if prior else {}),
                **resource.get("metadata", {}),
            },
        )
    for external_id in existing:
        if external_id not in seen_ids:
            release_external_resource(conn, provider=provider, external_id=external_id)


def _row_to_dict(row: tuple, conn: sqlite3.Connection) -> dict[str, Any]:
    cols = [
        "pid", "session_id", "workstream", "name", "priority",
        "port", "gpu_mb", "repo_dir", "model", "restart_policy",
        "start_cmd", "start_time", "last_heartbeat", "expected_duration_min",
    ]
    return dict(zip(cols, row))


def _session_lease_row_to_dict(row: tuple) -> dict[str, Any]:
    cols = [
        "session_id",
        "owner_pid",
        "owner_ppid",
        "owner_pgid",
        "owner_tty",
        "repo_dir",
        "started_at",
        "last_heartbeat_at",
        "shutdown_at",
        "status",
    ]
    return dict(zip(cols, row))


def _external_row_to_dict(row: tuple) -> dict[str, Any]:
    cols = [
        "provider",
        "resource_type",
        "external_id",
        "session_id",
        "workstream",
        "name",
        "priority",
        "gpu_mb",
        "repo_dir",
        "model",
        "status",
        "started_by",
        "owner_tool",
        "endpoint",
        "cleanup_cmd",
        "safe_to_delete",
        "metadata",
        "start_time",
        "last_seen",
    ]
    data = dict(zip(cols, row))
    data["safe_to_delete"] = bool(data["safe_to_delete"])
    data["metadata"] = json.loads(data["metadata"]) if data["metadata"] else {}
    return data
