#!/usr/bin/env python3
"""
Generate reconciler_vectors.json — parity oracle for the single-writer reconciler.

Uses a REPO path that resolves to itself (no /tmp symlink on macOS).

Monkeypatches:
  - registry._pid_exists  -> scripted alive/dead map
  - registry._age_seconds -> relative to FIXED_DT (deterministic)
  - registry._now_iso     -> FIXED_TS
  - events._now_iso       -> FIXED_TS
"""
from __future__ import annotations

import json
import sys
import unittest.mock
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

WTREE = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(WTREE))

from fleet_watch import events as fw_events, registry as fw_registry, referee as fw_referee

FIXED_TS = "2026-06-13T12:00:00+00:00"
FIXED_DT = datetime.fromisoformat(FIXED_TS)

OLD_TS    = (FIXED_DT - timedelta(seconds=300)).isoformat(timespec="seconds")
RECENT_TS = (FIXED_DT - timedelta(seconds=60)).isoformat(timespec="seconds")

# Must resolve to itself (no /tmp symlink on macOS).
REPO = "/Users/cj/tmp/fleet-watch-test-repo"
# Verify at import time.
assert str(Path(REPO).expanduser()) == REPO, f"REPO must be absolute and non-symlinked: {REPO}"
# The referee resolves repo_dir with Path.resolve(); since the dir doesn't exist,
# resolve() on a non-existent path just returns the path itself on Python 3.10+.
RESOLVED_REPO = str(Path(REPO).resolve())

# Scope paths used in tests — must also resolve consistently.
SCOPE_SRC   = f"{REPO}/src"
FOREIGN_SCOPE = f"{REPO}/subdir-a"


def build_db():
    import sqlite3
    conn = sqlite3.connect(":memory:", timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(fw_registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id,total_mb,reserve_mb,allocated_mb) "
        "VALUES (1,131072,16384,0)"
    )
    conn.commit()
    return conn


def dump_events(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT id,timestamp,event_type,pid,workstream,detail,prev_hash,hash "
        "FROM events ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def dump_leases(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT session_id,owner_pid,repo_dir,repo_lock_mode,write_scopes,"
        "started_at,last_heartbeat_at,shutdown_at,status "
        "FROM session_leases ORDER BY session_id"
    ).fetchall()
    result = []
    for r in rows:
        scopes = fw_registry._decode_write_scopes(r["write_scopes"])
        result.append({
            "session_id": r["session_id"],
            "owner_pid": r["owner_pid"],
            "repo_dir": r["repo_dir"],
            "repo_lock_mode": r["repo_lock_mode"] or "cooperative",
            "write_scopes": scopes,
            "started_at": r["started_at"],
            "last_heartbeat_at": r["last_heartbeat_at"],
            "shutdown_at": r["shutdown_at"],
            "status": r["status"],
        })
    return result


def dump_processes(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT pid,session_id,workstream,name,priority,port,gpu_mb,repo_dir "
        "FROM processes ORDER BY pid"
    ).fetchall()
    return [dict(r) for r in rows]


def decision_to_dict(d: fw_referee.Decision) -> dict[str, Any]:
    return {
        "allowed": d.allowed,
        "reason": d.reason,
        "holder": d.holder,
        "holders": d.holders,
        "overlap_paths": d.overlap_paths,
        "stale_holders": d.stale_holders,
        "safe_mode": d.safe_mode,
    }


def make_scenarios() -> list[dict[str, Any]]:
    def setup_empty(conn):
        pass

    def setup_current_session_owns(conn):
        conn.execute(
            "INSERT INTO session_leases (session_id,owner_pid,repo_dir,repo_lock_mode,"
            "write_scopes,started_at,last_heartbeat_at,shutdown_at,status) "
            "VALUES ('sess-current',101,?,'cooperative',NULL,?,?,NULL,'ACTIVE')",
            (RESOLVED_REPO, FIXED_TS, FIXED_TS),
        )
        conn.commit()

    def setup_foreign_cooperative_no_overlap(conn):
        conn.execute(
            "INSERT INTO session_leases (session_id,owner_pid,repo_dir,repo_lock_mode,"
            "write_scopes,started_at,last_heartbeat_at,shutdown_at,status) "
            "VALUES ('sess-foreign',201,?,'cooperative',?,?,?,NULL,'ACTIVE')",
            (RESOLVED_REPO, json.dumps([FOREIGN_SCOPE]), FIXED_TS, FIXED_TS),
        )
        conn.commit()

    def setup_foreign_cooperative_overlap(conn):
        conn.execute(
            "INSERT INTO session_leases (session_id,owner_pid,repo_dir,repo_lock_mode,"
            "write_scopes,started_at,last_heartbeat_at,shutdown_at,status) "
            "VALUES ('sess-foreign',201,?,'cooperative',?,?,?,NULL,'ACTIVE')",
            (RESOLVED_REPO, json.dumps([SCOPE_SRC]), FIXED_TS, FIXED_TS),
        )
        conn.commit()

    def setup_foreign_exclusive(conn):
        conn.execute(
            "INSERT INTO session_leases (session_id,owner_pid,repo_dir,repo_lock_mode,"
            "write_scopes,started_at,last_heartbeat_at,shutdown_at,status) "
            "VALUES ('sess-exclusive',201,?,'exclusive',NULL,?,?,NULL,'ACTIVE')",
            (RESOLVED_REPO, FIXED_TS, FIXED_TS),
        )
        conn.commit()

    def setup_dead_stale(conn):
        conn.execute(
            "INSERT INTO session_leases (session_id,owner_pid,repo_dir,repo_lock_mode,"
            "write_scopes,started_at,last_heartbeat_at,shutdown_at,status) "
            "VALUES ('sess-dead-stale',201,?,'cooperative',NULL,?,?,NULL,'ACTIVE')",
            (RESOLVED_REPO, OLD_TS, OLD_TS),
        )
        conn.commit()

    def setup_dead_recent(conn):
        conn.execute(
            "INSERT INTO session_leases (session_id,owner_pid,repo_dir,repo_lock_mode,"
            "write_scopes,started_at,last_heartbeat_at,shutdown_at,status) "
            "VALUES ('sess-dead-recent',201,?,'cooperative',NULL,?,?,NULL,'ACTIVE')",
            (RESOLVED_REPO, RECENT_TS, RECENT_TS),
        )
        conn.commit()

    def setup_process_dead_pid(conn):
        conn.execute(
            "INSERT INTO processes (pid,session_id,workstream,name,priority,gpu_mb,"
            "repo_dir,start_time,last_heartbeat) "
            "VALUES (301,'sess-proc','ws','proc-holder',3,0,?,?,?)",
            (RESOLVED_REPO, FIXED_TS, FIXED_TS),
        )
        conn.commit()

    return [
        {
            "id": "a_free_repo",
            "desc": "repo completely free",
            "setup": setup_empty,
            "call": {"repo_dir": REPO, "current_session_id": None},
            "alive_map": {},
        },
        {
            "id": "b_owned_by_current_session",
            "desc": "current session owns this repo -> same-session bypass",
            "setup": setup_current_session_owns,
            "call": {"repo_dir": REPO, "current_session_id": "sess-current"},
            "alive_map": {101: True},
        },
        {
            "id": "c_foreign_live_cooperative_no_overlap",
            "desc": "foreign live cooperative, no write scopes on request -> advisory allow",
            "setup": setup_foreign_cooperative_no_overlap,
            "call": {"repo_dir": REPO, "current_session_id": "sess-mine", "write_scopes": None},
            "alive_map": {201: True},
        },
        {
            "id": "d_foreign_live_cooperative_overlap",
            "desc": "foreign live cooperative, write-scope overlaps -> deny",
            "setup": setup_foreign_cooperative_overlap,
            "call": {
                "repo_dir": REPO,
                "current_session_id": "sess-mine",
                "write_scopes": [SCOPE_SRC],
            },
            "alive_map": {201: True},
        },
        {
            "id": "e_foreign_exclusive",
            "desc": "foreign exclusive session -> deny",
            "setup": setup_foreign_exclusive,
            "call": {"repo_dir": REPO, "current_session_id": "sess-mine"},
            "alive_map": {201: True},
        },
        {
            "id": "f_dead_stale_lease_gc",
            "desc": "foreign dead+stale (>180s) -> GC'd, allow",
            "setup": setup_dead_stale,
            "call": {"repo_dir": REPO, "current_session_id": "sess-mine"},
            "alive_map": {201: False},
        },
        {
            "id": "g_dead_recent_lease_held",
            "desc": "foreign dead but recent (<180s) -> still held, deny (advisory)",
            "setup": setup_dead_recent,
            "call": {"repo_dir": REPO, "current_session_id": "sess-mine"},
            "alive_map": {201: False},
        },
        {
            "id": "h_process_holder_dead_pid",
            "desc": "processes row holds repo, PID dead -> auto-release, allow",
            "setup": setup_process_dead_pid,
            "call": {"repo_dir": REPO, "current_session_id": None},
            "alive_map": {301: False},
        },
    ]


def run_all() -> list[dict[str, Any]]:
    vectors: list[dict[str, Any]] = []

    for s in make_scenarios():
        conn = build_db()
        s["setup"](conn)

        alive_map: dict[int, bool] = s["alive_map"]

        def _pid_exists(pid, _m=alive_map):
            if pid is None or pid <= 0:
                return False
            return _m.get(pid, False)

        def _age_seconds(iso_ts, _dt=FIXED_DT):
            if not iso_ts:
                return None
            ts = datetime.fromisoformat(iso_ts)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return int((_dt - ts).total_seconds())

        def _now_iso():
            return FIXED_TS

        with (
            unittest.mock.patch.object(fw_registry, "_pid_exists", side_effect=_pid_exists),
            unittest.mock.patch.object(fw_registry, "_age_seconds", side_effect=_age_seconds),
            unittest.mock.patch.object(fw_registry, "_now_iso", side_effect=_now_iso),
            unittest.mock.patch.object(fw_events, "_now_iso", side_effect=_now_iso),
        ):
            decision = fw_referee.check_repo_with_session(conn, **s["call"])

        vectors.append({
            "scenario": s["id"],
            "description": s["desc"],
            "call": s["call"],
            "alive_map": {str(k): v for k, v in alive_map.items()},
            "decision": decision_to_dict(decision),
            "post_events": dump_events(conn),
            "post_leases": dump_leases(conn),
            "post_processes": dump_processes(conn),
        })

    return vectors


def main() -> None:
    out_path = Path(__file__).parent / "reconciler_vectors.json"
    vectors = run_all()
    out_path.write_text(json.dumps(vectors, indent=2) + "\n")
    print(f"Wrote {len(vectors)} vectors to {out_path}")
    print(f"RESOLVED_REPO={RESOLVED_REPO}")
    for v in vectors:
        d = v["decision"]
        n_ev = len(v["post_events"])
        print(f"  {v['scenario']:<42s}  allowed={d['allowed']}  events={n_ev}  reason={d['reason']!r}")


if __name__ == "__main__":
    main()
