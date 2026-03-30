"""State reporter — generates STATE_REPORT.md and state.json."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fleet_watch import discover, events, referee, registry


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _seconds_ago(iso_ts: str) -> int:
    ts = datetime.fromisoformat(iso_ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return int((datetime.now(timezone.utc) - ts).total_seconds())


def build_state(conn: sqlite3.Connection) -> dict[str, Any]:
    """Build the full state dict used by both reporters."""
    processes = registry.get_all_processes(conn)
    budget = registry.get_gpu_budget(conn)
    ports = registry.get_claimed_ports(conn)
    repos = registry.get_locked_repos(conn)
    stale = registry.get_stale_processes(conn)
    recent = events.get_events(conn, hours=1, limit=20)
    config = discover.load_config()
    preferred = discover.preferred_ports(config)
    safe_ports = referee.suggest_ports(conn, preferred_ports=preferred)

    # Count conflicts prevented in last 24h
    conflicts_24h = events.get_events(conn, hours=24, event_type="CONFLICT")

    return {
        "agent_interface": "fleet guard --json",
        "generated_utc": _now_iso(),
        "processes": processes,
        "process_count": len(processes),
        "gpu_budget": budget,
        "ports_claimed": ports,
        "preferred_ports": preferred,
        "safe_ports": safe_ports,
        "repos_locked": repos,
        "stale_processes": stale,
        "recent_events": recent,
        "conflicts_prevented_24h": len(conflicts_24h),
    }


def generate_markdown(state: dict[str, Any]) -> str:
    """Generate STATE_REPORT.md content."""
    lines: list[str] = []
    lines.append("# Fleet Watch State Report")
    lines.append(f"Generated: {state['generated_utc']}")
    lines.append("")

    # Active processes
    procs = state["processes"]
    lines.append(f"## Active Processes ({len(procs)})")
    if procs:
        lines.append("")
        lines.append("| PID | Name | Workstream | Port | GPU | Priority | Heartbeat |")
        lines.append("|-----|------|------------|------|-----|----------|-----------|")
        for p in procs:
            port = str(p["port"]) if p["port"] else "-"
            gpu = f"{p['gpu_mb']}MB" if p["gpu_mb"] else "0MB"
            age = _seconds_ago(p["last_heartbeat"])
            lines.append(f"| {p['pid']} | {p['name']} | {p['workstream']} | {port} | {gpu} | {p['priority']} | {age}s ago |")
    else:
        lines.append("")
        lines.append("No active processes.")
    lines.append("")

    # Resource budget
    budget = state["gpu_budget"]
    pct = int(budget["allocated_mb"] / max(budget["total_mb"] - budget["reserve_mb"], 1) * 100)
    lines.append("## Resource Budget")
    lines.append(f"- GPU: {budget['allocated_mb']} / {budget['total_mb'] - budget['reserve_mb']} MB allocated ({pct}%)")

    ports = state["ports_claimed"]
    if ports:
        lines.append(f"- Ports claimed: {', '.join(str(p) for p in sorted(ports.keys()))}")
    else:
        lines.append("- Ports claimed: none")

    safe_ports = state.get("safe_ports", [])
    if safe_ports:
        lines.append(f"- Suggested open ports: {', '.join(str(p) for p in safe_ports)}")

    repos = state["repos_locked"]
    if repos:
        for repo, pid in repos.items():
            lines.append(f"- Repo locked: {repo} (PID {pid})")
    else:
        lines.append("- Repos locked: none")
    lines.append("")

    # Recent events
    recent = state["recent_events"]
    lines.append(f"## Recent Events (last 1 hour, {len(recent)} entries)")
    if recent:
        for e in recent[:10]:
            pid_str = f" PID {e['pid']}" if e["pid"] else ""
            ws_str = f" ({e['workstream']})" if e["workstream"] else ""
            lines.append(f"- {e['timestamp']} {e['event_type']}{pid_str}{ws_str}")
    else:
        lines.append("No recent events.")
    lines.append("")

    # Conflicts
    lines.append("## Conflicts Prevented")
    lines.append(f"- Last 24 hours: {state['conflicts_prevented_24h']}")
    lines.append("")

    # Stale
    stale = state["stale_processes"]
    lines.append(f"## Stale Processes ({len(stale)})")
    if stale:
        for s in stale:
            lines.append(f"- PID {s['pid']} ({s['name']}) — heartbeat {s['stale_seconds']}s ago")
    else:
        lines.append("None.")
    lines.append("")

    return "\n".join(lines)


def generate_json(state: dict[str, Any]) -> str:
    """Generate state.json content."""
    return json.dumps(state, indent=2, default=str)


def write_report(conn: sqlite3.Connection, output_dir: Path | None = None) -> tuple[Path, Path]:
    """Write both STATE_REPORT.md and state.json. Returns (md_path, json_path)."""
    out = output_dir or registry.FLEET_DIR
    out.mkdir(parents=True, exist_ok=True)

    state = build_state(conn)

    md_path = out / "STATE_REPORT.md"
    md_path.write_text(generate_markdown(state))

    json_path = out / "state.json"
    json_path.write_text(generate_json(state))

    return md_path, json_path
