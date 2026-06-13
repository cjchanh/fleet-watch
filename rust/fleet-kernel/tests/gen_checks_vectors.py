#!/usr/bin/env python3
"""
Generate checks_vectors.json for the parity test.

Creates a temp sqlite, applies the fleet-watch SCHEMA, inserts fixture rows,
runs referee.check_port + referee.check_gpu_budget, emits
{scenario, expected_allowed, expected_reason} for each call.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path

# ── inline the relevant SCHEMA fragments (avoids sys.path games) ─────────────
PROCESSES_DDL = """
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
"""

GPU_BUDGET_DDL = """
CREATE TABLE IF NOT EXISTS gpu_budget (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    total_mb        INTEGER NOT NULL DEFAULT 131072,
    reserve_mb      INTEGER NOT NULL DEFAULT 16384,
    allocated_mb    INTEGER NOT NULL DEFAULT 0
);
"""

# Fixture values
FIXTURE_TOTAL_MB = 131072      # 128 GiB
FIXTURE_RESERVE_MB = 16384     # 16 GiB
FIXTURE_ALLOCATED_MB = 40960   # 40 GiB already allocated
# available = 131072 - 16384 - 40960 = 73728
FIXTURE_AVAILABLE_MB = FIXTURE_TOTAL_MB - FIXTURE_RESERVE_MB - FIXTURE_ALLOCATED_MB

FIXTURE_PID = 999
FIXTURE_NAME = "demo"
FIXTURE_PORT = 4242


def setup_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.executescript(PROCESSES_DDL + GPU_BUDGET_DDL)

    # Insert the fixture process
    conn.execute(
        """INSERT INTO processes
           (pid, session_id, workstream, name, priority, port, gpu_mb,
            repo_dir, model, restart_policy, start_cmd, start_time, last_heartbeat)
           VALUES (?, 'sess-1', 'test', ?, 3, ?, 0, NULL, NULL, 'ALERT_ONLY',
                   NULL, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')""",
        (FIXTURE_PID, FIXTURE_NAME, FIXTURE_PORT),
    )

    # Insert the gpu_budget singleton
    conn.execute(
        "INSERT INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) VALUES (1, ?, ?, ?)",
        (FIXTURE_TOTAL_MB, FIXTURE_RESERVE_MB, FIXTURE_ALLOCATED_MB),
    )
    conn.commit()
    return conn


# ── inline referee logic exactly as Python referee.py ─────────────────────────
def get_process_by_port(conn: sqlite3.Connection, port: int):
    """Mirrors registry.get_process_by_port — returns dict or None."""
    row = conn.execute("SELECT * FROM processes WHERE port = ?", (port,)).fetchone()
    if not row:
        return None
    # _row_to_dict maps the column names
    cols = [
        "pid", "session_id", "workstream", "name", "priority",
        "port", "gpu_mb", "repo_dir", "model", "restart_policy",
        "start_cmd", "start_time", "last_heartbeat", "expected_duration_min",
    ]
    return dict(zip(cols, row))


def get_gpu_budget(conn: sqlite3.Connection) -> dict:
    """Mirrors registry.get_gpu_budget exactly."""
    row = conn.execute(
        "SELECT total_mb, reserve_mb, allocated_mb FROM gpu_budget WHERE id = 1"
    ).fetchone()
    total, reserve, allocated = row
    return {
        "total_mb": total,
        "reserve_mb": reserve,
        "allocated_mb": allocated,
        "available_mb": total - reserve - allocated,
    }


def check_port(conn: sqlite3.Connection, port: int) -> dict:
    """Mirrors referee.check_port exactly."""
    holder = get_process_by_port(conn, port)
    if holder is None:
        return {"allowed": True, "reason": "port available"}
    return {
        "allowed": False,
        "reason": f"port {port} claimed by PID {holder['pid']} ({holder['name']})",
        "holder": holder,
    }


def check_gpu_budget(conn: sqlite3.Connection, gpu_mb: int) -> dict:
    """Mirrors referee.check_gpu_budget exactly."""
    if gpu_mb <= 0:
        return {"allowed": True, "reason": "no GPU claim"}
    budget = get_gpu_budget(conn)
    if gpu_mb <= budget["available_mb"]:
        return {
            "allowed": True,
            "reason": f"{gpu_mb}MB fits in {budget['available_mb']}MB available",
        }
    return {
        "allowed": False,
        "reason": (
            f"GPU budget exceeded: requesting {gpu_mb}MB but only "
            f"{budget['available_mb']}MB available "
            f"({budget['allocated_mb']}MB allocated of "
            f"{budget['total_mb'] - budget['reserve_mb']}MB allocatable)"
        ),
    }


def main() -> None:
    out_path = Path(__file__).parent / "checks_vectors.json"

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tf:
        conn = setup_db(tf.name)

        vectors = []

        # ── check_port ─────────────────────────────────────────────────────
        # Port 4242 is claimed
        result = check_port(conn, FIXTURE_PORT)
        vectors.append({
            "scenario": "port_claimed",
            "check": "check_port",
            "args": {"port": FIXTURE_PORT},
            "expected_allowed": result["allowed"],
            "expected_reason": result["reason"],
        })

        # Port 5000 is free
        result = check_port(conn, 5000)
        vectors.append({
            "scenario": "port_free",
            "check": "check_port",
            "args": {"port": 5000},
            "expected_allowed": result["allowed"],
            "expected_reason": result["reason"],
        })

        # ── check_gpu_budget ───────────────────────────────────────────────
        # gpu_mb = 0 → "no GPU claim"
        result = check_gpu_budget(conn, 0)
        vectors.append({
            "scenario": "gpu_zero",
            "check": "check_gpu_budget",
            "args": {"gpu_mb": 0},
            "expected_allowed": result["allowed"],
            "expected_reason": result["reason"],
        })

        # gpu_mb that fits (e.g. 8192 MB, well under available_mb=73728)
        fits_mb = 8192
        result = check_gpu_budget(conn, fits_mb)
        vectors.append({
            "scenario": "gpu_fits",
            "check": "check_gpu_budget",
            "args": {"gpu_mb": fits_mb},
            "expected_allowed": result["allowed"],
            "expected_reason": result["reason"],
        })

        # gpu_mb that exceeds (e.g. 100000 MB, over available_mb=73728)
        exceeds_mb = 100_000
        result = check_gpu_budget(conn, exceeds_mb)
        vectors.append({
            "scenario": "gpu_exceeds",
            "check": "check_gpu_budget",
            "args": {"gpu_mb": exceeds_mb},
            "expected_allowed": result["allowed"],
            "expected_reason": result["reason"],
        })

        conn.close()

    out_path.write_text(json.dumps(vectors, indent=2) + "\n")
    print(f"Wrote {len(vectors)} vectors to {out_path}")
    for v in vectors:
        print(f"  {v['scenario']:20s}  allowed={v['expected_allowed']}  reason={v['expected_reason']!r}")


if __name__ == "__main__":
    main()
