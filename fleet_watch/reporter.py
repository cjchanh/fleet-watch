"""State reporter — generates STATE_REPORT.md and state.json."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fleet_watch import discover, events, referee, registry, syshealth


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _seconds_ago(iso_ts: str) -> int:
    ts = datetime.fromisoformat(iso_ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return int((datetime.now(timezone.utc) - ts).total_seconds())


def build_guard_state(conn: sqlite3.Connection) -> dict[str, Any]:
    """Build the minimal state needed for guard decisions. Fast path — no subprocess calls."""
    processes = registry.get_all_processes(conn)
    external_resources = registry.get_all_external_resources(conn)
    budget = registry.get_gpu_budget(conn)
    ports = registry.get_claimed_ports(conn)
    repos = registry.get_effective_locked_repos(conn)
    config = discover.load_config()
    preferred = discover.preferred_ports(config)
    safe_ports = referee.suggest_ports(conn, preferred_ports=preferred)

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
    }


def build_state(conn: sqlite3.Connection) -> dict[str, Any]:
    """Build the full observability state. Includes system health (subprocess calls)."""
    state = build_guard_state(conn)

    classifications = registry.get_process_classifications(conn)
    stale = [
        proc for proc in classifications
        if proc["classification"] in {"stale_candidate", "orphan_confirmed"}
    ]
    recent = events.get_events(conn, hours=1, limit=20)
    conflicts_24h = events.get_events(conn, hours=24, event_type="CONFLICT")

    config = discover.load_config()
    health_config = syshealth.load_health_config(config)
    memory = syshealth.get_memory_state()
    sessions = syshealth.get_session_processes(
        patterns=health_config["session_patterns"],
    )
    idle = syshealth.get_idle_processes(
        patterns=health_config["idle_patterns"],
        threshold_cpu=health_config["idle_cpu_threshold"],
    )
    gpu_monitor = discover.load_gpu_monitor_state()

    state.update({
        "session_leases": registry.list_session_leases(conn),
        "process_classifications": classifications,
        "stale_processes": stale,
        "recent_events": recent,
        "conflicts_prevented_24h": len(conflicts_24h),
        "system_memory": memory.to_dict(),
        "sessions": [
            {
                "pid": s.pid,
                "name": s.name,
                "kind": s.kind,
                "rss_mb": s.rss_mb,
                "cpu_pct": s.cpu_pct,
                "started": s.started,
                "tty": s.tty,
                "ppid": s.ppid,
                "pgid": s.pgid,
                "group_leader_pid": s.group_leader_pid,
                "member_pids": s.member_pids,
                "member_count": s.member_count,
                "parent_chain_detached": s.parent_chain_detached,
                "classification": s.classification,
                "attention": s.attention,
                "evidence": s.evidence,
            }
            for s in sessions
        ],
        "idle_processes": idle,
        "gpu_memory_monitor": gpu_monitor,
    })
    return state


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
    classifications = state.get("process_classifications", [])
    leases = {
        lease["session_id"]: lease
        for lease in state.get("session_leases", [])
    }
    classification_by_pid = {
        item["pid"]: item
        for item in classifications
    }

    if classifications:
        counts: dict[str, int] = {}
        for item in classifications:
            counts[item["classification"]] = counts.get(item["classification"], 0) + 1
        summary = ", ".join(
            f"{count} {name}"
            for name, count in sorted(counts.items())
        )
        lines.append("## Reconciliation")
        lines.append("")
        lines.append(summary)
        lines.append("")

    session_ids = sorted({
        *(lease["session_id"] for lease in leases.values()),
        *(p["session_id"] for p in procs if p.get("session_id")),
        *(e["session_id"] for e in external if e.get("session_id")),
    })
    if session_ids:
        lines.append("## Ownership")
        lines.append("")
        for sid in session_ids:
            lease = leases.get(sid)
            if lease:
                owner_pid = lease["owner_pid"] if lease["owner_pid"] is not None else "-"
                owner_tty = lease["owner_tty"] or "-"
                repo = lease["repo_dir"] or "-"
                lines.append(
                    f"**{sid}** — {lease['status']} | owner PID {owner_pid} | tty {owner_tty} | repo {repo}"
                )
            else:
                lines.append(f"**{sid}** — UNLEASED")

            for p in [proc for proc in procs if proc.get("session_id") == sid]:
                classification = classification_by_pid.get(p["pid"], {})
                lines.append(
                    f"- PID {p['pid']} ({p['name']}) — {classification.get('classification', 'unknown')}"
                )
            for e in [item for item in external if item.get("session_id") == sid]:
                lines.append(
                    f"- {e['provider']}:{e['external_id']} ({e['name']}) — {e['status']}"
                )
        lines.append("")

    # --- System health ---
    mem = state.get("system_memory", {})
    if mem:
        pressure = mem.get("pressure_pct", -1)
        indicator = syshealth.pressure_label(pressure)
        if not mem.get("available", True) or pressure < 0:
            lines.append("## System Memory — UNAVAILABLE")
            lines.append("Memory telemetry not supported on this platform.")
        else:
            lines.append(f"## System Memory — {indicator} ({pressure}% pressure)")
            lines.append(f"- Total: {mem['total_mb']:,} MB")
            lines.append(f"- Active: {mem['active_mb']:,} MB | Wired: {mem['wired_mb']:,} MB | "
                         f"Compressed: {mem['compressed_mb']:,} MB")
            lines.append(f"- Free: {mem['free_mb']:,} MB | Inactive: {mem['inactive_mb']:,} MB | "
                         f"Available: {mem['available_mb']:,} MB")
            lines.append(f"- Pageouts: {mem.get('pageouts', 0):,} | Swapins: {mem.get('swapins', 0):,}")
        lines.append("")

    gpu_monitor = state.get("gpu_memory_monitor") or {}
    if gpu_monitor:
        lines.append("## GPU Memory Watch")
        lines.append("")
        pageout_rate = gpu_monitor.get("pageout_rate")
        if pageout_rate:
            lines.append(
                f"- Pageouts/sec: {pageout_rate['pageouts_per_sec']} | "
                f"Swapins/sec: {pageout_rate['swapins_per_sec']}"
            )
        footprints = gpu_monitor.get("gpu_process_footprints", [])
        if footprints:
            total_fp = sum(item.get("resident_mb", 0) for item in footprints)
            lines.append(
                f"- GPU workload footprint: {total_fp:,} MB across {len(footprints)} process(es)"
            )
        alerts = gpu_monitor.get("alerts", [])
        if alerts:
            lines.append("- Alerts:")
            for alert in alerts[:5]:
                if alert["type"] == "pageout_thrashing":
                    rate = alert["pageout_rate"]["pageouts_per_sec"]
                    lines.append(f"  - pageout thrashing detected at {rate} pageouts/sec")
                elif alert["type"] == "process_footprint_overcommit":
                    proc = alert["process"]
                    lines.append(
                        f"  - PID {proc['pid']} ({proc['name']}) at {proc['resident_mb']} MB "
                        f"with {alert['available_mb']} MB available"
                    )
        else:
            lines.append("- Alerts: none")
        lines.append("")

    # --- Sessions ---
    sess_list = state.get("sessions", [])
    if sess_list:
        total_rss = sum(s["rss_mb"] for s in sess_list)
        by_kind: dict[str, int] = {}
        for s in sess_list:
            by_kind[s["kind"]] = by_kind.get(s["kind"], 0) + 1
        kind_summary = ", ".join(f"{count} {kind}" for kind, count in sorted(by_kind.items()))
        lines.append(f"## Sessions ({kind_summary} — {total_rss:,} MB total)")
        lines.append("")
        attention = [s for s in sess_list if s.get("attention")]
        if attention:
            lines.append(f"Attention required: {len(attention)} detached hot session(s)")
            lines.append("")
        lines.append("| PID | Type | State | RSS | CPU | TTY | N | Started |")
        lines.append("|-----|------|-------|-----|-----|-----|---|---------|")
        for s in sorted(sess_list, key=lambda x: x["rss_mb"], reverse=True):
            lines.append(f"| {s['pid']} | {s['kind']} | {s.get('classification', 'attached')} | "
                         f"{s['rss_mb']:,} MB | {s['cpu_pct']:.1f}% | {s['tty']} | "
                         f"{s.get('member_count', 1)} | {s['started']} |")
        lines.append("")

        if attention:
            lines.append("## Attention Required Sessions")
            lines.append("")
            for s in sorted(attention, key=lambda x: x["cpu_pct"], reverse=True):
                evidence = "; ".join(s.get("evidence", [])[:3])
                lines.append(
                    f"- PID {s['pid']} ({s['kind']}) — {s['classification']} — "
                    f"CPU {s['cpu_pct']:.1f}% — {s['rss_mb']:,} MB"
                    + (f" — {evidence}" if evidence else "")
                )
            lines.append("")

    # --- Idle processes ---
    idle_list = state.get("idle_processes", [])
    if idle_list:
        total_idle_rss = sum(p["rss_mb"] for p in idle_list)
        lines.append(f"## Idle Processes ({len(idle_list)} — {total_idle_rss:,} MB reclaimable)")
        lines.append("")
        for p in idle_list:
            cmd_short = p["command"].split("/")[-1][:60] if "/" in p["command"] else p["command"][:60]
            lines.append(f"- PID {p['pid']} — {p['rss_mb']:,} MB — CPU {p['cpu_pct']:.1f}% — `{cmd_short}`")
        lines.append("")

    # --- What's safe to stop ---
    stoppable: list[str] = []
    for e in external:
        if e.get("safe_to_delete"):
            cmd = e.get("cleanup_cmd") or f"fleet thunder release --uuid {e['external_id']}"
            stoppable.append(f"- {e['provider']}:{e['external_id']} ({e['name']}) — `{cmd}`")
    for s in classifications:
        if s["classification"] != "orphan_confirmed":
            continue
        evidence = "; ".join(s.get("evidence", [])[:3])
        stoppable.append(
            f"- PID {s['pid']} ({s['name']}) — orphan confirmed; {evidence}. "
            f"`fleet reap --confirm`"
        )

    lines.append("## Safe to Stop")
    lines.append("")
    if stoppable:
        for item in stoppable:
            lines.append(item)
    else:
        lines.append("Nothing flagged as safe to stop. All resources are active or owned.")
    lines.append("")

    stale_candidates = [
        item for item in classifications
        if item["classification"] == "stale_candidate"
    ]
    if stale_candidates:
        lines.append("## Stale Candidates")
        lines.append("")
        for item in stale_candidates:
            evidence = "; ".join(item.get("evidence", [])[:3])
            lines.append(
                f"- PID {item['pid']} ({item['name']}) — heartbeat stale {_human_duration(item['stale_seconds'])}; {evidence}"
            )
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
            if pid is not None:
                name = next((p["name"] for p in procs if p["pid"] == pid), f"PID {pid}")
            else:
                name = "session lease"
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
