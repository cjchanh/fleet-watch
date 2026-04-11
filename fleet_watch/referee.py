"""Referee — claim logic, budget enforcement, preemption for Fleet Watch."""

from __future__ import annotations

import os
import signal
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fleet_watch import events, registry


@dataclass
class Decision:
    allowed: bool
    reason: str
    holder: dict[str, Any] | None = None


def _session_holder_from_lease(lease: dict[str, Any]) -> dict[str, Any]:
    return {
        "pid": lease.get("owner_pid"),
        "name": f"session {lease['session_id']}",
        "workstream": "session",
        "priority": 3,
        "port": None,
        "repo_dir": lease.get("repo_dir"),
        "gpu_mb": 0,
        "session_id": lease["session_id"],
    }


def check_port(conn: sqlite3.Connection, port: int) -> Decision:
    holder = registry.get_process_by_port(conn, port)
    if holder is None:
        return Decision(allowed=True, reason="port available")
    return Decision(
        allowed=False,
        reason=f"port {port} claimed by PID {holder['pid']} ({holder['name']})",
        holder=holder,
    )


def check_repo(conn: sqlite3.Connection, repo_dir: str) -> Decision:
    return check_repo_with_session(conn, repo_dir, current_session_id=None)


def check_repo_with_session(
    conn: sqlite3.Connection,
    repo_dir: str,
    current_session_id: str | None,
) -> Decision:
    resolved_repo_dir = str(Path(repo_dir).resolve())
    holder = registry.get_process_by_repo(conn, resolved_repo_dir)
    if holder is None:
        external_holders = registry.get_external_resources_by_repo(conn, repo_dir)
        if not external_holders:
            session_leases = registry.get_active_session_leases_by_repo(conn, repo_dir)
            owned_by_current_session = False
            for lease in session_leases:
                if current_session_id and lease["session_id"] == current_session_id:
                    owned_by_current_session = True
                    continue

                owner_pid = lease.get("owner_pid")
                if owner_pid is not None and not registry._pid_exists(owner_pid):
                    heartbeat_age = registry._age_seconds(lease.get("last_heartbeat_at"))
                    if heartbeat_age is not None and heartbeat_age > registry.DEFAULT_STALE_SECONDS:
                        registry.close_session_lease(conn, lease["session_id"])
                        events.log_event(
                            conn,
                            "CLEAN",
                            pid=owner_pid,
                            workstream="session",
                            detail={"reason": "dead_session_owner", "repo_dir": resolved_repo_dir, "session_id": lease["session_id"]},
                        )
                        continue

                return Decision(
                    allowed=False,
                    reason=f"repo {resolved_repo_dir} locked by active session {lease['session_id']}",
                    holder=_session_holder_from_lease(lease),
                )
            if owned_by_current_session:
                return Decision(allowed=True, reason="repo available (owned by current session)")
            return Decision(allowed=True, reason="repo available")
        for external in external_holders:
            if current_session_id and external["session_id"] == current_session_id:
                continue
            return Decision(
                allowed=False,
                reason=(
                    f"repo {resolved_repo_dir} locked by external "
                    f"{external['provider']} resource {external['external_id']} ({external['name']})"
                ),
                holder=external,
            )
        return Decision(allowed=True, reason="repo available (owned by current session)")
    # Check if holder PID is still alive
    try:
        os.kill(holder["pid"], 0)
    except ProcessLookupError:
        # Holder is dead — auto-release
        registry.release_process(conn, holder["pid"])
        events.log_event(conn, "CLEAN", pid=holder["pid"], workstream=holder["workstream"],
                         detail={"reason": "dead_pid", "repo_dir": repo_dir})
        return Decision(allowed=True, reason="repo available (stale lock cleared)")
    except PermissionError:
        pass  # Process exists

    # Same-session bypass for local processes
    if current_session_id and holder.get("session_id") == current_session_id:
        return Decision(allowed=True, reason="repo available (owned by current session)")

    return Decision(
        allowed=False,
        reason=f"repo {resolved_repo_dir} locked by PID {holder['pid']} ({holder['name']})",
        holder=holder,
    )


def check_gpu_budget(conn: sqlite3.Connection, gpu_mb: int) -> Decision:
    if gpu_mb <= 0:
        return Decision(allowed=True, reason="no GPU claim")
    budget = registry.get_gpu_budget(conn)
    if gpu_mb <= budget["available_mb"]:
        return Decision(allowed=True, reason=f"{gpu_mb}MB fits in {budget['available_mb']}MB available")
    return Decision(
        allowed=False,
        reason=(
            f"GPU budget exceeded: requesting {gpu_mb}MB but only "
            f"{budget['available_mb']}MB available "
            f"({budget['allocated_mb']}MB allocated of "
            f"{budget['total_mb'] - budget['reserve_mb']}MB allocatable)"
        ),
    )


def summarize_holder(holder: dict[str, Any] | None) -> dict[str, Any] | None:
    if holder is None:
        return None
    return {
        "pid": holder.get("pid"),
        "name": holder["name"],
        "workstream": holder["workstream"],
        "priority": holder["priority"],
        "port": holder.get("port"),
        "repo_dir": holder["repo_dir"],
        "gpu_mb": holder["gpu_mb"],
    }


def suggest_ports(
    conn: sqlite3.Connection,
    preferred_ports: list[int],
    requested_port: int | None = None,
    limit: int = 5,
) -> list[int]:
    occupied = set(registry.get_claimed_ports(conn).keys())
    suggestions: list[int] = []

    for port in preferred_ports:
        if port == requested_port or port in occupied or port in suggestions:
            continue
        suggestions.append(port)
        if len(suggestions) >= limit:
            return suggestions

    if requested_port is None:
        start = 8000
    else:
        start = max(1024, requested_port - 5)

    for port in range(start, start + 200):
        if port == requested_port or port in occupied or port in suggestions:
            continue
        suggestions.append(port)
        if len(suggestions) >= limit:
            break

    return suggestions


def preflight_register(
    conn: sqlite3.Connection,
    port: int | None = None,
    gpu_mb: int = 0,
    repo_dir: str | None = None,
    current_session_id: str | None = None,
) -> list[Decision]:
    """Run all checks before registration. Returns list of failed decisions (empty = all clear)."""
    failures: list[Decision] = []

    if port is not None:
        d = check_port(conn, port)
        if not d.allowed:
            failures.append(d)

    if gpu_mb > 0:
        d = check_gpu_budget(conn, gpu_mb)
        if not d.allowed:
            failures.append(d)

    if repo_dir is not None:
        d = check_repo_with_session(conn, repo_dir, current_session_id=current_session_id)
        if not d.allowed:
            failures.append(d)

    return failures


def claim_port(conn: sqlite3.Connection, port: int) -> Decision:
    """Standalone port claim check (no registration)."""
    decision = check_port(conn, port)
    if decision.allowed:
        events.log_event(conn, "CLAIM", detail={"resource": "port", "port": port})
    else:
        events.log_event(conn, "CONFLICT", detail={"resource": "port", "port": port,
                                                     "holder_pid": decision.holder["pid"] if decision.holder else None})
    return decision


def claim_repo(conn: sqlite3.Connection, repo_dir: str) -> Decision:
    """Standalone repo claim check (no registration)."""
    decision = check_repo(conn, repo_dir)
    if decision.allowed:
        events.log_event(conn, "CLAIM", detail={"resource": "repo", "repo_dir": repo_dir})
    else:
        events.log_event(conn, "CONFLICT", detail={"resource": "repo", "repo_dir": repo_dir,
                                                     "holder_pid": decision.holder["pid"] if decision.holder else None})
    return decision


def preempt_port(
    conn: sqlite3.Connection,
    port: int,
    new_priority: int,
    reason: str,
    grace_seconds: int = 30,
) -> Decision:
    """Preempt a port from a lower-priority holder."""
    holder = registry.get_process_by_port(conn, port)
    if holder is None:
        return Decision(allowed=True, reason="port already free")

    if new_priority <= holder["priority"]:
        return Decision(
            allowed=False,
            reason=f"cannot preempt: new priority {new_priority} <= holder priority {holder['priority']}",
            holder=holder,
        )

    # Log the preemption
    events.log_event(
        conn, "PREEMPT",
        pid=holder["pid"],
        workstream=holder["workstream"],
        detail={
            "port": port,
            "holder_pid": holder["pid"],
            "holder_priority": holder["priority"],
            "new_priority": new_priority,
            "reason": reason,
            "grace_seconds": grace_seconds,
        },
    )

    # Send SIGTERM to the holder
    try:
        os.kill(holder["pid"], signal.SIGTERM)
    except ProcessLookupError:
        pass  # Already dead

    # Wait for grace period
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        try:
            os.kill(holder["pid"], 0)
            time.sleep(1)
        except ProcessLookupError:
            break  # Process exited

    # Force-release claims
    registry.release_process(conn, holder["pid"])

    return Decision(
        allowed=True,
        reason=f"preempted PID {holder['pid']} ({holder['name']}) for: {reason}",
        holder=holder,
    )
