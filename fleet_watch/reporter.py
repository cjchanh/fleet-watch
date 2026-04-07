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
    external_resources = registry.get_all_external_resources(conn)
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
        "external_resources": external_resources,
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


def _human_duration(seconds: int) -> str:
    """Convert seconds to a human-readable duration."""
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
    return f"{seconds // 86400}d {(seconds % 86400) // 3600}h"


def generate_markdown(state: dict[str, Any]) -> str:
    """Generate STATE_REPORT.md — a rolling operational document.

    This file is regenerated every 60 seconds by launchd.
    Any session (human or agent) can read it to understand what's running,
    who owns it, and what's safe to touch.
    """
    lines: list[str] = []
    procs = state["processes"]
    external = state.get("external_resources", [])
    budget = state["gpu_budget"]
    usable = max(budget["total_mb"] - budget["reserve_mb"], 1)
    pct = int(budget["allocated_mb"] / usable * 100)

    lines.append("# Fleet Watch — What Is Running")
    lines.append(f"Generated: {state['generated_utc']}  ")
    lines.append(f"GPU: **{budget['allocated_mb']:,} / {usable:,} MB** ({pct}%)  ")
    lines.append(f"Local processes: {len(procs)} | Remote resources: {len(external)} | "
                 f"Conflicts prevented (24h): {state['conflicts_prevented_24h']}")
    lines.append("")

    # --- Workstream map ---
    ws_procs: dict[str, list[dict[str, Any]]] = {}
    for p in procs:
        ws_procs.setdefault(p["workstream"], []).append(p)
    ws_ext: dict[str, list[dict[str, Any]]] = {}
    for e in external:
        ws_ext.setdefault(e["workstream"], []).append(e)
    all_ws = sorted(set(list(ws_procs.keys()) + list(ws_ext.keys())))

    lines.append("## Workstreams")
    lines.append("")
    if not all_ws:
        lines.append("No active workstreams.")
    for ws in all_ws:
        ws_p = ws_procs.get(ws, [])
        ws_e = ws_ext.get(ws, [])
        ws_gpu = sum(p["gpu_mb"] for p in ws_p)
        ws_ports = [str(p["port"]) for p in ws_p if p.get("port")]
        lines.append(f"### {ws}")
        lines.append(f"Processes: {len(ws_p)} | Remote: {len(ws_e)} | GPU: {ws_gpu:,} MB | "
                     f"Ports: {', '.join(ws_ports) if ws_ports else 'none'}")
        lines.append("")
        if ws_p:
            lines.append("| PID | Name | Port | GPU | Session | Uptime |")
            lines.append("|-----|------|------|-----|---------|--------|")
            for p in ws_p:
                port = str(p["port"]) if p.get("port") else "-"
                gpu = f"{p['gpu_mb']:,}" if p["gpu_mb"] else "0"
                session = p.get("session_id") or "-"
                uptime = _human_duration(_seconds_ago(p["start_time"])) if p.get("start_time") else "-"
                lines.append(f"| {p['pid']} | {p['name']} | {port} | {gpu} | {session} | {uptime} |")
            lines.append("")
        if ws_e:
            for e in ws_e:
                meta = e.get("metadata", {})
                gpu_type = meta.get("gpuType", "?")
                template = meta.get("template", "?")
                owner = e.get("owner_tool") or "-"
                session = e.get("session_id") or "unclaimed"
                repo = e.get("repo_dir") or "none"
                endpoint = e.get("endpoint") or "-"
                age = _human_duration(_seconds_ago(e["last_seen"]))
                lines.append(f"**{e['provider']}:{e['external_id']}** — {e['status']}  ")
                lines.append(f"GPU: {gpu_type} | Template: {template} | "
                             f"Owner: {owner} | Session: {session}  ")
                lines.append(f"Repo: {repo} | Endpoint: {endpoint} | Last seen: {age} ago")
                lines.append("")
        lines.append("")

    # --- Ownership map ---
    sessions: dict[str, list[str]] = {}
    for p in procs:
        sid = p.get("session_id")
        if sid:
            sessions.setdefault(sid, []).append(f"PID {p['pid']} ({p['name']})")
    for e in external:
        sid = e.get("session_id")
        if sid:
            sessions.setdefault(sid, []).append(f"{e['provider']}:{e['external_id']} ({e['name']})")

    if sessions:
        lines.append("## Ownership")
        lines.append("")
        for sid, items in sorted(sessions.items()):
            lines.append(f"**{sid}**: {', '.join(items)}")
        lines.append("")

    # --- What's safe to stop ---
    stoppable: list[str] = []
    for e in external:
        if e.get("safe_to_delete"):
            cmd = e.get("cleanup_cmd") or f"fleet thunder release --uuid {e['external_id']}"
            stoppable.append(f"- {e['provider']}:{e['external_id']} ({e['name']}) — `{cmd}`")
    stale = state["stale_processes"]
    for s in stale:
        stoppable.append(f"- PID {s['pid']} ({s['name']}) — stale {_human_duration(s['stale_seconds'])}, "
                         f"likely dead. `fleet release --pid {s['pid']}`")

    lines.append("## Safe to Stop")
    lines.append("")
    if stoppable:
        for item in stoppable:
            lines.append(item)
    else:
        lines.append("Nothing flagged as safe to stop. All resources are active or owned.")
    lines.append("")

    # --- Resource budget ---
    lines.append("## Resource Budget")
    lines.append(f"- GPU total: {budget['total_mb']:,} MB (reserve {budget['reserve_mb']:,} MB)")
    lines.append(f"- GPU allocated: {budget['allocated_mb']:,} MB")
    lines.append(f"- GPU available: {budget['available_mb']:,} MB")
    lines.append("")

    ports = state["ports_claimed"]
    if ports:
        lines.append(f"- Ports claimed: {', '.join(str(p) for p in sorted(ports.keys()))}")
    else:
        lines.append("- Ports claimed: none")

    safe_ports = state.get("safe_ports", [])
    if safe_ports:
        lines.append(f"- Open ports: {', '.join(str(p) for p in safe_ports)}")

    repos = state["repos_locked"]
    ext_repos = [(e["repo_dir"], f"{e['provider']}:{e['external_id']}") for e in external if e.get("repo_dir")]
    if repos or ext_repos:
        lines.append("- Repos locked:")
        for repo, pid in repos.items():
            name = next((p["name"] for p in procs if p["pid"] == pid), f"PID {pid}")
            lines.append(f"  - {repo} — {name}")
        for repo, holder in ext_repos:
            lines.append(f"  - {repo} — {holder}")
    else:
        lines.append("- Repos locked: none")
    lines.append("")

    # --- Recent events ---
    recent = state["recent_events"]
    lines.append(f"## Recent Events ({len(recent)})")
    if recent:
        for e in recent[:10]:
            pid_str = f" PID {e['pid']}" if e["pid"] else ""
            ws_str = f" [{e['workstream']}]" if e["workstream"] else ""
            detail = e.get("detail", {})
            reason = ""
            if isinstance(detail, dict) and detail.get("reason"):
                reason = f" — {detail['reason']}"
            lines.append(f"- {e['timestamp']} **{e['event_type']}**{pid_str}{ws_str}{reason}")
    else:
        lines.append("No recent events.")
    lines.append("")

    # --- Agent interface ---
    lines.append("## How to Use")
    lines.append("")
    lines.append("```")
    lines.append("fleet guard --json --port N --repo PATH --gpu MB --session-id ID")
    lines.append("fleet thunder sync        # refresh Thunder instances")
    lines.append("fleet thunder claim       # claim ownership of an instance")
    lines.append("fleet thunder release     # release an instance")
    lines.append("fleet status              # human-readable overview")
    lines.append("```")
    lines.append("")

    return "\n".join(lines)


def generate_json(state: dict[str, Any]) -> str:
    """Generate state.json content."""
    return json.dumps(state, indent=2, default=str)


# Maximum changelog entries before decay (24h at 60s intervals)
CHANGELOG_MAX_LINES = 1440


def _diff_state(prev: dict[str, Any], curr: dict[str, Any]) -> dict[str, Any]:
    """Compute what changed between two state snapshots."""
    prev_pids = {p["pid"] for p in prev.get("processes", [])}
    curr_pids = {p["pid"] for p in curr.get("processes", [])}
    prev_ext = {(e["provider"], e["external_id"]) for e in prev.get("external_resources", [])}
    curr_ext = {(e["provider"], e["external_id"]) for e in curr.get("external_resources", [])}

    added_pids = curr_pids - prev_pids
    removed_pids = prev_pids - curr_pids
    added_ext = curr_ext - prev_ext
    removed_ext = prev_ext - curr_ext

    # Detect status changes on external resources
    prev_ext_status = {
        (e["provider"], e["external_id"]): e.get("status")
        for e in prev.get("external_resources", [])
    }
    status_changes = []
    for e in curr.get("external_resources", []):
        key = (e["provider"], e["external_id"])
        if key in prev_ext_status and prev_ext_status[key] != e.get("status"):
            status_changes.append({
                "provider": e["provider"],
                "external_id": e["external_id"],
                "old_status": prev_ext_status[key],
                "new_status": e.get("status"),
            })

    # Detect GPU budget changes
    prev_gpu = prev.get("gpu_budget", {}).get("allocated_mb", 0)
    curr_gpu = curr.get("gpu_budget", {}).get("allocated_mb", 0)

    delta: dict[str, Any] = {}
    if added_pids:
        delta["processes_added"] = [
            {"pid": p["pid"], "name": p["name"], "workstream": p["workstream"],
             "port": p.get("port"), "gpu_mb": p.get("gpu_mb", 0)}
            for p in curr["processes"] if p["pid"] in added_pids
        ]
    if removed_pids:
        delta["processes_removed"] = [
            {"pid": p["pid"], "name": p["name"], "workstream": p["workstream"]}
            for p in prev["processes"] if p["pid"] in removed_pids
        ]
    if added_ext:
        delta["external_added"] = [
            {"provider": e["provider"], "external_id": e["external_id"],
             "name": e["name"], "status": e.get("status")}
            for e in curr["external_resources"]
            if (e["provider"], e["external_id"]) in added_ext
        ]
    if removed_ext:
        delta["external_removed"] = [
            {"provider": e["provider"], "external_id": e["external_id"],
             "name": e["name"]}
            for e in prev["external_resources"]
            if (e["provider"], e["external_id"]) in removed_ext
        ]
    if status_changes:
        delta["status_changes"] = status_changes
    if prev_gpu != curr_gpu:
        delta["gpu_allocated_mb"] = {"old": prev_gpu, "new": curr_gpu}

    return delta


def _append_changelog(log_path: Path, entry: dict[str, Any]) -> None:
    """Append a changelog entry and decay old entries if needed."""
    line = json.dumps(entry, separators=(",", ":"), default=str) + "\n"
    with log_path.open("a") as f:
        f.write(line)

    # Decay: if file is too large, keep the newest half
    try:
        all_lines = log_path.read_text().splitlines()
        if len(all_lines) > CHANGELOG_MAX_LINES:
            keep = all_lines[len(all_lines) - CHANGELOG_MAX_LINES // 2:]
            log_path.write_text("\n".join(keep) + "\n")
    except Exception:
        pass  # Decay failure never blocks reporting


def write_report(conn: sqlite3.Connection, output_dir: Path | None = None) -> tuple[Path, Path]:
    """Write STATE_REPORT.md, state.json, and append to state_changelog.jsonl."""
    out = output_dir or registry.FLEET_DIR
    out.mkdir(parents=True, exist_ok=True)

    state = build_state(conn)

    # Diff against previous state for changelog
    json_path = out / "state.json"
    prev_state: dict[str, Any] = {}
    try:
        prev_state = json.loads(json_path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    delta = _diff_state(prev_state, state)
    if delta:
        log_path = out / "state_changelog.jsonl"
        entry = {"timestamp": state["generated_utc"], "delta": delta}
        _append_changelog(log_path, entry)

    md_path = out / "STATE_REPORT.md"
    md_path.write_text(generate_markdown(state))

    json_path.write_text(generate_json(state))

    return md_path, json_path
