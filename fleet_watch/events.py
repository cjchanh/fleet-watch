"""Hash-chained event log for Fleet Watch."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

GENESIS_HASH = "genesis"

EVENT_TYPES = frozenset({
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
})


def _compute_hash(prev_hash: str, timestamp: str, event_type: str, detail: str) -> str:
    payload = f"{prev_hash}|{timestamp}|{event_type}|{detail}"
    return hashlib.sha256(payload.encode()).hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_last_hash(conn: sqlite3.Connection) -> str:
    """Return the hash of the most recent event, or the genesis hash."""
    row = conn.execute(
        "SELECT hash FROM events ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else GENESIS_HASH


def log_event(
    conn: sqlite3.Connection,
    event_type: str,
    pid: int | None = None,
    workstream: str | None = None,
    detail: dict[str, Any] | None = None,
) -> int:
    """Append one event to the hash-chained audit log."""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"Unknown event type: {event_type}")

    ts = _now_iso()
    detail_str = json.dumps(detail or {}, separators=(",", ":"))
    prev_hash = get_last_hash(conn)
    event_hash = _compute_hash(prev_hash, ts, event_type, detail_str)

    cursor = conn.execute(
        """INSERT INTO events (timestamp, event_type, pid, workstream, detail, prev_hash, hash)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (ts, event_type, pid, workstream, detail_str, prev_hash, event_hash),
    )
    conn.commit()
    return cursor.lastrowid  # type: ignore[return-value]


def get_events(
    conn: sqlite3.Connection,
    hours: int = 24,
    event_type: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return recent events filtered by age and optional event type."""
    query = "SELECT id, timestamp, event_type, pid, workstream, detail, hash FROM events"
    conditions: list[str] = []
    params: list[Any] = []

    if hours > 0:
        cutoff = datetime.now(timezone.utc).isoformat(timespec="seconds")
        conditions.append(f"timestamp >= datetime(?, '-{hours} hours')")
        params.append(cutoff)

    if event_type:
        conditions.append("event_type = ?")
        params.append(event_type)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    return [
        {
            "id": r[0],
            "timestamp": r[1],
            "event_type": r[2],
            "pid": r[3],
            "workstream": r[4],
            "detail": json.loads(r[5]) if r[5] else {},
            "hash": r[6],
        }
        for r in rows
    ]


def verify_chain(conn: sqlite3.Connection) -> tuple[bool, int]:
    """Verify the hash chain integrity. Returns (valid, checked_count)."""
    rows = conn.execute(
        "SELECT id, timestamp, event_type, detail, prev_hash, hash FROM events ORDER BY id ASC"
    ).fetchall()

    if not rows:
        return True, 0

    expected_prev = GENESIS_HASH
    for row in rows:
        _, ts, etype, detail_str, prev_hash, stored_hash = row
        if prev_hash != expected_prev:
            return False, 0
        computed = _compute_hash(prev_hash, ts, etype, detail_str or "{}")
        if computed != stored_hash:
            return False, 0
        expected_prev = stored_hash

    return True, len(rows)
