from __future__ import annotations

import importlib
import json
import sys
from typing import Any

import click

from fleet_watch import counters, discover as discover_mod
from fleet_watch import events, registry, reporter, syshealth
from fleet_watch.cli_support import (
    STATUS_DISCOVERY_TIMEOUT_SECONDS,
    _build_reconcile_payload,
    _get_conn,
    _notify_attention,
    _run_bounded,
    _run_bounded_result,
)
from fleet_watch.discovery import ollama_runners, orphan_detector


def _auto_reclaim_flip() -> dict[str, Any]:
    """The operator's kill-path flip as the daemon would resolve it, READ-ONLY.

    Nothing here arms, disarms or writes anything — this surface exists so an
    operator can confirm the auto-reclaim kill path is off without opening a
    config file. The state is the daemon's own resolution
    (``reap.resolve_auto_reclaim``, re-read fresh every tick), so this field and
    the daemon's next tick can never disagree, and ``source`` says which of the
    two it came from: the operator's ``auto_reclaim`` config key, or the shipped
    default. An unreadable or invalid configuration reports ``off`` exactly as
    the daemon would — uncertainty is never displayed as permission.

    ``configured`` is kept separate from ``source`` so an operator who typed a
    value the daemon rejected (``auto_reclaim: on-ish``) can see that they DID
    set a key, rather than reading "default" as "no key was set".
    """
    reap_mod = importlib.import_module("fleet_watch.commands.reap")
    key = reap_mod.AUTO_RECLAIM_CONFIG_KEY
    try:
        state = reap_mod.resolve_auto_reclaim()
    except Exception:  # noqa: BLE001 — an unreadable flip reports off, never a guess
        state = reap_mod.AUTO_RECLAIM_OFF
    configured = False
    source = "default"
    try:
        config = discover_mod.load_config()
        if isinstance(config, dict):
            configured = key in config
            raw = config.get(key)
            accepted = (
                isinstance(raw, str)
                and raw.strip().lower() in reap_mod.AUTO_RECLAIM_STATES
            )
            if accepted:
                source = f"config:{key}"
    except Exception:  # noqa: BLE001 — an unreadable config is the default, not a guess
        configured = False
    return {
        "state": state,
        "source": source,
        "config_key": key,
        "configured": configured,
        "armed": state != reap_mod.AUTO_RECLAIM_OFF,
    }


def _auto_reclaim_flip_line(flip: dict[str, Any]) -> str:
    """One human line for the flip, honest about WHY it resolved the way it did."""
    if flip["source"] == "default":
        why = (
            f"default — the configured {flip['config_key']} value is not one of "
            "off/dry_run/on"
            if flip["configured"]
            else f"default — no {flip['config_key']} key in the config file"
        )
    else:
        why = f"from config key {flip['config_key']}"
    return f"Auto-reclaim flip: {flip['state']} ({why})"


@click.command()
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def status(as_json: bool):
    """Show current fleet state including discovered ollama runners and orphan detection."""
    conn = _get_conn()
    # Auto-clean dead PIDs
    cleaned = registry.clean_dead_pids(conn)
    for c in cleaned:
        events.log_event(conn, "CLEAN", pid=c["pid"], workstream=c["workstream"],
                         detail={"reason": "dead_pid", "name": c["name"]})

    # H1: discover ollama runners. The bounded helper returns an explicit
    # reason; an exception is not reported as a clean empty scan.
    discovery_timeout = float(STATUS_DISCOVERY_TIMEOUT_SECONDS)
    runner_reports, runner_reason = _run_bounded_result(
        ollama_runners.discover_ollama_runners,
        timeout_seconds=discovery_timeout,
        default=[],
    )
    ollama_scan_timed_out = runner_reason in {"timeout", "in_flight"}
    runner_entries = ollama_runners.runner_entries_for_status(runner_reports)
    actual_gpu = ollama_runners.total_actual_gpu_mb(runner_reports)

    # H3: detect orphan runners. Same bound, with a distinct error state for
    # timeout, already-in-flight work, and an exception.
    orphan_result, orphan_reason = _run_bounded_result(
        orphan_detector.detect_orphans,
        timeout_seconds=discovery_timeout,
        default=orphan_detector.OrphanDetectionResult(),
    )
    orphan_probe_timed_out = orphan_reason in {"timeout", "in_flight"}
    if orphan_reason in {"timeout", "in_flight", "error"}:
        orphan_result.error = f"probe_{orphan_reason}"
    if orphan_result.orphans_detected:
        events.log_event(
            conn,
            "ORPHAN_RUNNERS_DETECTED",
            workstream="inference",
            detail=orphan_result.to_dict(),
        )

    # Increment discovery counters
    gate_counters = counters.load_counters()
    gate_counters.increment("ollama_runner_discovery")
    gate_counters.increment("orphan_detector")
    counters.save_counters(gate_counters)

    if as_json:
        # Pass the already-bounded results through so build_state() does not
        # trigger a second, unbounded discovery scan (see build_state()
        # docstring) -- this was the actual 8s `fleet status` hang: the
        # bounded call above returned on time, then build_state() re-ran the
        # same unbounded probe and blocked on it.
        state = reporter.build_state(
            conn,
            runner_reports=runner_reports,
            orphan_result=orphan_result,
            bounded=True,
        )
        degraded_payload = {
            # Legacy booleans remain additive-compatible for existing panel and
            # hook consumers; the reason fields carry the honest distinction.
            "ollama_runner_scan_timed_out": ollama_scan_timed_out,
            "orphan_probe_timed_out": orphan_probe_timed_out,
        }
        if runner_reason is not None or orphan_reason is not None:
            degraded_payload.update(
                {
                    "ollama_runner_scan": {
                        "degraded": runner_reason is not None,
                        "reason": runner_reason,
                    },
                    "orphan_probe": {
                        "degraded": orphan_reason is not None,
                        "reason": orphan_reason,
                    },
                }
            )
        state["discovery_degraded"] = degraded_payload
        # Read-only: the resolved kill-path flip and where it came from, so an
        # operator can confirm it is off without opening the config by hand.
        state["auto_reclaim"] = _auto_reclaim_flip()
        click.echo(json.dumps(state, indent=2, default=str))
    else:
        procs = registry.get_all_processes(conn)
        budget = registry.get_gpu_budget(conn)

        if not procs and not runner_entries:
            click.echo("No active processes.")
        else:
            total_count = len(procs) + len(runner_entries)
            click.echo(f"Active processes ({total_count}):")
            click.echo(f"{'PID':>7}  {'Name':<24} {'Workstream':<18} {'Port':<6} {'GPU':>8} {'Pri':>3}")
            click.echo("-" * 78)
            for p in procs:
                port = str(p["port"]) if p["port"] else "-"
                gpu = f"{p['gpu_mb']}MB" if p["gpu_mb"] else "0MB"
                click.echo(f"{p['pid']:>7}  {p['name']:<24} {p['workstream']:<18} {port:<6} {gpu:>8} {p['priority']:>3}")
            # H1: synthetic runner entries
            for entry in runner_entries:
                port = str(entry["port"]) if entry.get("port") else "-"
                gpu = f"{entry['gpu_mb']}MB"
                click.echo(
                    f"{entry['pid']:>7}  {entry['name']:<24} {entry['workstream']:<18} "
                    f"{port:<6} {gpu:>8} {entry['priority']:>3}  [runner]"
                )

        if actual_gpu > 0:
            click.echo(f"\nActual Ollama GPU: {actual_gpu:,} MB ({len(runner_reports)} serve instance(s))")

        external = registry.get_all_external_resources(conn)
        if external:
            click.echo("")
            click.echo(f"External resources ({len(external)}):")
            click.echo(f"{'Provider':<10} {'ID':<12} {'Status':<10} {'Repo':<30} {'Name'}")
            click.echo("-" * 96)
            for item in external:
                repo = item["repo_dir"] or "-"
                repo_display = repo if len(repo) <= 30 else "..." + repo[-27:]
                click.echo(
                    f"{item['provider']:<10} {item['external_id']:<12} {item['status']:<10} "
                    f"{repo_display:<30} {item['name']}"
                )

        alloc = budget["allocated_mb"]
        total = budget["total_mb"] - budget["reserve_mb"]
        click.echo(f"\nGPU: {alloc}/{total} MB allocated ({int(alloc/max(total,1)*100)}%)")

        ports = registry.get_claimed_ports(conn)
        if ports:
            click.echo(f"Ports: {', '.join(str(p) for p in sorted(ports.keys()))}")

        # H3: surface orphan detection
        if orphan_result.orphans_detected:
            click.echo(f"\nORPHAN_RUNNERS_DETECTED: {len(orphan_result.orphan_pids)} orphan(s)")
            click.echo(f"  Orphan PIDs: {' '.join(str(p) for p in orphan_result.orphan_pids)}")
            click.echo(f"  Estimated recovered: {orphan_result.estimated_recovered_mb:,} MB")
            click.echo(f"  Suggested: {orphan_result.suggested_kill_command}")

        if runner_reason is not None or orphan_reason is not None:
            degraded = []
            if runner_reason is not None:
                degraded.append(f"ollama runner scan ({runner_reason})")
            if orphan_reason is not None:
                degraded.append(f"orphan probe ({orphan_reason})")
            click.echo(
                f"\nDEGRADED/UNKNOWN: {', '.join(degraded)}; "
                "no clean-empty claim was made."
            )

        # The kill path's state is a fact an operator must be able to read here,
        # not something they have to go find a config file for.
        click.echo(f"\n{_auto_reclaim_flip_line(_auto_reclaim_flip())}")

    conn.close()


@click.command()
def report():
    """Generate STATE_REPORT.md and state.json."""
    conn = _get_conn()
    md_path, json_path = reporter.write_report(conn)
    click.echo(f"Written: {md_path}")
    click.echo(f"Written: {json_path}")
    conn.close()


@click.command()
@click.option("--type", "event_type", default=None, help="Filter by event type")
@click.option("--hours", type=int, default=24, help="Hours to look back")
def history(event_type: str | None, hours: int):
    """Show recent events."""
    conn = _get_conn()
    evts = events.get_events(conn, hours=hours, event_type=event_type, limit=50)
    if not evts:
        click.echo("No events found.")
    else:
        for e in evts:
            pid_str = f" PID {e['pid']}" if e["pid"] else ""
            ws_str = f" ({e['workstream']})" if e["workstream"] else ""
            detail = e.get("detail", {})
            detail_str = f" {json.dumps(detail)}" if detail else ""
            click.echo(f"{e['timestamp']} {e['event_type']}{pid_str}{ws_str}{detail_str}")
    conn.close()


@click.command()
def stale():
    """List heartbeat-stale processes and dead-owner session leases.

    Path C (HONESTY): cross-checks PID liveness on ACTIVE session leases so a
    provably-dead owner (PID gone or recycled) is surfaced as dead instead of
    silently reported as ACTIVE.
    """
    conn = _get_conn()
    stale_procs = registry.get_stale_processes(conn)
    dead_owner_leases = [
        lease
        for lease in registry.list_active_session_leases(conn)
        if lease.get("owner_pid") is not None
        and not registry._lease_owner_alive(lease)
    ]
    if not stale_procs and not dead_owner_leases:
        click.echo("No stale processes.")
    else:
        for s in stale_procs:
            evidence = "; ".join(s.get("evidence", [])[:3])
            click.echo(
                f"PID {s['pid']} ({s['name']}) — {s['classification']} — "
                f"heartbeat {s['stale_seconds']}s ago"
                + (f" — {evidence}" if evidence else "")
            )
        for lease in dead_owner_leases:
            age = registry._age_seconds(lease.get("last_heartbeat_at"))
            age_str = f"{age}s ago" if age is not None else "unknown"
            click.echo(
                f"SESSION {lease['session_id']} "
                f"(PID {lease.get('owner_pid')}) — dead_session_owner — "
                f"owner not alive; heartbeat {age_str}; "
                f"lock {lease.get('repo_lock_mode', 'cooperative')}"
            )
    conn.close()


@click.command()
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def reconcile(as_json: bool):
    """Inspect process ownership state without mutating registry rows."""
    conn = _get_conn()
    payload = _build_reconcile_payload(conn)
    conn.close()

    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        return

    summary = payload["summary"]
    if not payload["processes"]:
        click.echo("No registered processes.")
        return

    summary_text = ", ".join(
        f"{count} {name}"
        for name, count in sorted(summary.items())
    )
    click.echo(f"Reconciliation: {summary_text}")
    for item in payload["processes"]:
        evidence = "; ".join(item.get("evidence", [])[:2])
        click.echo(
            f"PID {item['pid']} ({item['name']}) — {item['classification']}"
            + (f" — {evidence}" if evidence else "")
        )


@click.command("decisions")
@click.option("--hours", type=int, default=24, help="Hours of decision receipts to include")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON")
def decisions(hours: int, as_json: bool):
    """Expose bounded process-policy receipts and their freshness."""
    conn = _get_conn()
    rows = events.get_events(
        conn, hours=hours, event_type="PROCESS_DECISION", limit=100
    )
    receipts = [row.get("detail", {}).get("receipt", {}) for row in rows]
    generated = registry._now_iso()
    latest_ts = rows[0].get("timestamp") if rows else None
    if latest_ts is None:
        freshness = {"status": "unknown", "latest_event": None}
    else:
        age = registry._age_seconds(latest_ts)
        freshness = {
            "status": "fresh" if age is not None and age <= 300 else "stale",
            "latest_event": latest_ts,
            "age_seconds": age,
        }
    payload = {
        "schema_version": "fleet-watch/decision-feed/v1",
        "generated_utc": generated,
        "freshness": freshness,
        "receipts": receipts,
    }
    conn.close()
    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        return
    if not receipts:
        click.echo("No process decision receipts.")
        return
    click.echo(f"Decision receipts: {len(receipts)} ({freshness['status']})")
    for receipt in receipts[:20]:
        click.echo(
            f"PID {receipt.get('observed_identity', {}).get('pid', '?')} — "
            f"{receipt.get('permitted_action', '?')} — {receipt.get('outcome', '?')} — "
            f"{receipt.get('reason', '?')}"
        )


@click.command()
@click.option("--json", "as_json", is_flag=True, help="Machine-readable JSON")
def health(as_json: bool):
    """Show system health: RAM pressure, sessions, idle processes."""
    config = discover_mod.load_config()
    health_config = syshealth.load_health_config(config)

    mem = syshealth.get_memory_state()
    gpu_monitor = discover_mod.load_gpu_monitor_state()
    sessions = syshealth.get_session_processes(
        patterns=health_config["session_patterns"],
    )
    idle = syshealth.get_idle_processes(
        patterns=health_config["idle_patterns"],
        threshold_cpu=health_config["idle_cpu_threshold"],
    )

    if as_json:
        click.echo(json.dumps({
            "memory": mem.to_dict(),
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
            "idle": idle,
            "gpu_memory_monitor": gpu_monitor,
        }, indent=2))
        return

    pressure = mem.pressure_pct
    indicator = syshealth.pressure_label(pressure, health_config["pressure_thresholds"])
    if not mem.is_available:
        click.echo("Memory: UNAVAILABLE (telemetry not supported on this platform)")
    else:
        click.echo(f"Memory: {indicator} ({pressure}% pressure)")
        click.echo(f"  Total: {mem.total_mb:,} MB | Active: {mem.active_mb:,} MB | "
                   f"Compressed: {mem.compressed_mb:,} MB | Free: {mem.free_mb:,} MB")
    click.echo("")

    if sessions:
        flagged = [s for s in sessions if s.attention]
        by_kind: dict[str, list] = {}
        for s in sessions:
            by_kind.setdefault(s.kind, []).append(s)
        kind_summary = ", ".join(f"{len(v)} {k}" for k, v in sorted(by_kind.items()))
        total_rss = sum(s.rss_mb for s in sessions)
        click.echo(f"Sessions: {kind_summary} ({total_rss:,} MB)")
        if flagged:
            click.echo(f"Attention: {len(flagged)} detached hot session(s)")
            for s in sorted(flagged, key=lambda x: x.cpu_pct, reverse=True):
                evidence = "; ".join(s.evidence[:2])
                click.echo(
                    f"  PID {s.pid:>7}  {s.kind:<12} {s.cpu_pct:>5.1f}%  "
                    f"{s.rss_mb:>6} MB  {evidence}"
                )
            click.echo("")
        click.echo(
            f"{'PID':>7}  {'Type':<12} {'State':<13} {'RSS':>8}  "
            f"{'CPU':>6}  {'TTY':<6} {'N':>2}  Started"
        )
        click.echo("-" * 86)
        for s in sorted(
            sessions,
            key=lambda x: (0 if x.attention else 1, -x.cpu_pct, -x.rss_mb),
        ):
            click.echo(
                f"{s.pid:>7}  {s.kind:<12} {s.classification:<13} "
                f"{s.rss_mb:>6} MB  {s.cpu_pct:>5.1f}%  {s.tty:<6} "
                f"{s.member_count:>2}  {s.started}"
            )
    else:
        click.echo("Sessions: none detected")
    click.echo("")

    if idle:
        total_idle = sum(p["rss_mb"] for p in idle)
        click.echo(f"Idle processes: {len(idle)} ({total_idle:,} MB reclaimable)")
        for p in idle:
            cmd_short = p["command"].split("/")[-1][:50] if "/" in p["command"] else p["command"][:50]
            click.echo(f"  PID {p['pid']:>7}  {p['rss_mb']:>6} MB  CPU {p['cpu_pct']:>5.1f}%  {cmd_short}")
    else:
        click.echo("Idle processes: none detected")

    if gpu_monitor:
        click.echo("")
        alerts = gpu_monitor.get("alerts", [])
        footprints = gpu_monitor.get("gpu_process_footprints", [])
        click.echo(
            f"GPU memory watch: {len(footprints)} workload(s), {len(alerts)} alert(s)"
        )
        for alert in alerts[:5]:
            if alert["type"] == "pageout_thrashing":
                click.echo(
                    f"  Pageout thrashing: {alert['pageout_rate']['pageouts_per_sec']} pageouts/sec"
                )
            elif alert["type"] == "process_footprint_overcommit":
                proc = alert["process"]
                click.echo(
                    f"  PID {proc['pid']:>7}  {proc['resident_mb']:>6} MB  "
                    f"{proc['name']} exceeds {alert['available_mb']} MB available"
                )

    flagged = [s for s in sessions if s.attention]
    if flagged:
        _notify_attention(flagged)


@click.command()
@click.option("--lines", "max_lines", type=int, default=20, help="Number of entries to show")
@click.option("--json", "as_json", is_flag=True, help="Raw JSONL output")
def changelog(max_lines: int, as_json: bool):
    """Show rolling state changelog (what changed and when)."""
    log_path = registry.FLEET_DIR / "state_changelog.jsonl"
    if not log_path.exists():
        click.echo("No changelog yet. Run `fleet discover` to start recording.")
        return

    all_lines = log_path.read_text().strip().splitlines()
    tail = all_lines[-max_lines:]

    if as_json:
        for line in tail:
            click.echo(line)
        return

    for line in tail:
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = entry.get("timestamp", "?")
        delta = entry.get("delta", {})
        parts: list[str] = []
        for p in delta.get("processes_added", []):
            parts.append(f"+{p['name']} (PID {p['pid']}, {p.get('gpu_mb', 0)}MB)")
        for p in delta.get("processes_removed", []):
            parts.append(f"-{p['name']} (PID {p['pid']})")
        for e in delta.get("external_added", []):
            parts.append(f"+{e['provider']}:{e['external_id']} ({e['name']})")
        for e in delta.get("external_removed", []):
            parts.append(f"-{e['provider']}:{e['external_id']} ({e['name']})")
        for s in delta.get("status_changes", []):
            parts.append(f"{s['provider']}:{s['external_id']} {s['old_status']}→{s['new_status']}")
        gpu = delta.get("gpu_allocated_mb")
        if gpu:
            parts.append(f"GPU {gpu['old']}→{gpu['new']}MB")
        if parts:
            click.echo(f"{ts}  {' | '.join(parts)}")
