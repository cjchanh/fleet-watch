from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import click

from fleet_watch import autonomous as autonomous_mod
from fleet_watch import boot_map as boot_map_mod
from fleet_watch import census as census_mod
from fleet_watch import claude_lease_twin
from fleet_watch import counters, discover as discover_mod
from fleet_watch import events, gpu_estimator, referee, registry, reporter, runaway, syshealth
from fleet_watch.cli_support import (
    DEFAULT_REPORT_BUDGET_S,
    REPORT_ATTEMPT_MARKER,
    REPORT_BUDGET_ENV,
    REPORT_MIN_INTERVAL_ENV,
    STATUS_DISCOVERY_TIMEOUT_SECONDS,
    _ack,
    _build_guard_payload,
    _build_reconcile_payload,
    _census_registry_rows,
    _cooperative_alternative,
    _default_owner_pid,
    _documents_root,
    _executable_supports_census,
    _extract_json_document,
    _float_env,
    _get_conn,
    _holder_conflict_text,
    _holder_text,
    _is_documents_path,
    _is_fleet_owned,
    _load_tnr_instances,
    _mark_report_attempt,
    _mcp_reap_candidates,
    _mcp_surface_lines,
    _notify_attention,
    _notify_conflict,
    _publish_report_after_ack,
    _reject_negative_gpu,
    _render_census,
    _render_guard,
    _render_launchd_plist,
    _report_budget_seconds,
    _report_is_fresh,
    _report_min_interval_seconds,
    _repo_unblock_command,
    _resolved_session_id,
    _run_bounded,
    _run_runaway_tick,
    _terminate_orphan,
)
from fleet_watch.discovery import mcp_orphan_detector, ollama_runners, orphan_detector
from fleet_watch.guards import memory_pressure

@click.command()
@click.option("--pid", type=int, required=True, help="Process ID")
@click.option("--name", required=True, help="Human-readable name")
@click.option("--workstream", required=True, help="Workstream name (e.g. inference, training)")
@click.option("--session-id", default=None, help="Session identifier")
@click.option("--port", type=int, default=None, help="Port to claim")
@click.option("--gpu", "gpu_mb", type=int, default=0, help="GPU memory claim in MB")
@click.option("--repo", "repo_dir", default=None, help="Repo directory to lock")
@click.option("--model", default=None, help="Model name if applicable")
@click.option("--priority", type=click.IntRange(1, 5), default=3, help="Priority 1-5")
@click.option("--restart-policy", type=click.Choice(sorted(registry.RESTART_POLICIES)), default="ALERT_ONLY", help="Restart policy for the process")
@click.option("--start-cmd", default=None, help="Command to restart the process")
@click.option("--expected-duration", type=int, default=None, help="Expected duration in minutes")
def register(pid: int, name: str, workstream: str, session_id: str | None,
             port: int | None, gpu_mb: int, repo_dir: str | None, model: str | None,
             priority: int, restart_policy: str, start_cmd: str | None,
             expected_duration: int | None):
    """Register a process with Fleet Watch."""
    conn = _get_conn()

    # Preflight checks
    failures = referee.preflight_register(
        conn,
        port=port,
        gpu_mb=gpu_mb,
        repo_dir=repo_dir,
        current_session_id=session_id,
        # A process registering a port it ALREADY holds is the documented
        # explicit path ("Use `fleet register` when explicit claims are more
        # reliable than discovery"), and it was refused with "port held by the
        # OS" — held by the very PID being registered. The referee verifies
        # this PID against the socket table, so a fabricated --pid cannot use
        # it to switch the OS check off.
        owner_pid=pid,
    )
    if failures:
        for f in failures:
            click.echo(f"DENY: {f.reason}", err=True)
        conn.close()
        sys.exit(1)

    try:
        registry.register_process(
            conn, pid=pid, name=name, workstream=workstream, session_id=session_id,
            port=port, gpu_mb=gpu_mb, repo_dir=repo_dir, model=model,
            priority=priority, restart_policy=restart_policy, start_cmd=start_cmd,
            expected_duration_min=expected_duration,
        )
    except Exception as e:  # noqa: BLE001 — register must not traceback; type rides in ERROR
        click.echo(f"ERROR: {type(e).__name__}: {e}", err=True)
        conn.close()
        sys.exit(1)

    events.log_event(conn, "REGISTER", pid=pid, workstream=workstream,
                     detail={"name": name, "port": port, "gpu_mb": gpu_mb,
                             "repo_dir": repo_dir, "priority": priority})
    click.echo(f"Registered PID {pid} ({name})")
    conn.close()


@click.command()
@click.option("--pid", type=int, default=None, help="Release all claims for a PID")
@click.option("--port", type=int, default=None, help="Release a specific port")
def release(pid: int | None, port: int | None):
    """Release claims for a process."""
    if pid is None and port is None:
        click.echo("Specify --pid or --port", err=True)
        sys.exit(2)

    conn = _get_conn()

    if pid is not None:
        result = registry.release_process(conn, pid)
        if result:
            events.log_event(conn, "RELEASE", pid=pid, workstream=result["workstream"],
                             detail={"name": result["name"]})
            click.echo(f"Released PID {pid} ({result['name']})")
        else:
            click.echo(f"PID {pid} not found", err=True)
            conn.close()
            sys.exit(2)

    if port is not None:
        result = registry.release_port(conn, port)
        if result:
            events.log_event(conn, "RELEASE", pid=result["pid"], workstream=result["workstream"],
                             detail={"port": port, "name": result["name"]})
            click.echo(f"Released port {port} (was PID {result['pid']})")
        else:
            click.echo(f"Port {port} not claimed", err=True)
            conn.close()
            sys.exit(2)

    conn.close()


@click.command("share-repo")
@click.argument("repo_dir")
def share_repo(repo_dir: str):
    """Close active editorial session leases for a Documents path."""
    resolved_repo = Path(repo_dir).expanduser().resolve()
    if not _is_documents_path(resolved_repo):
        click.echo(
            f"DENY: share-repo is limited to {_documents_root()} paths",
            err=True,
        )
        sys.exit(2)

    conn = _get_conn()
    leases = registry.get_active_session_leases_by_repo(conn, str(resolved_repo))
    if not leases:
        click.echo(f"No active session lease found for {resolved_repo}", err=True)
        conn.close()
        sys.exit(1)

    released: list[dict[str, Any]] = []
    for lease in leases:
        if registry.close_session_lease(conn, lease["session_id"]):
            events.log_event(
                conn,
                "SESSION_CLOSE",
                pid=lease.get("owner_pid"),
                workstream="session",
                detail={
                    "session_id": lease["session_id"],
                    "repo_dir": str(resolved_repo),
                    "source": "share-repo",
                },
            )
            released.append(lease)

    if not released:
        click.echo(f"No active session lease found for {resolved_repo}", err=True)
        conn.close()
        sys.exit(1)

    conn.close()
    for lease in released:
        _ack(f"Released session lease {lease['session_id']} for {resolved_repo}")
    _publish_report_after_ack("share-repo release")


@click.command()
@click.option("--pid", type=int, required=True, help="PID of the process to heartbeat")
def heartbeat(pid: int):
    """Update heartbeat for a process."""
    conn = _get_conn()
    if registry.heartbeat(conn, pid):
        events.log_event(conn, "HEARTBEAT", pid=pid)
        click.echo(f"Heartbeat updated for PID {pid}")
    else:
        click.echo(f"PID {pid} not found", err=True)
        conn.close()
        sys.exit(2)
    conn.close()


@click.command()
def clean():
    """Remove entries for dead PIDs."""
    conn = _get_conn()
    cleaned = registry.clean_dead_pids(conn)
    if not cleaned:
        click.echo("No dead PIDs found.")
    else:
        for c in cleaned:
            events.log_event(conn, "CLEAN", pid=c["pid"], workstream=c["workstream"],
                             detail={"reason": "dead_pid", "name": c["name"]})
            click.echo(f"Cleaned PID {c['pid']} ({c['name']})")
    conn.close()


@click.command()
@click.option(
    "--auto-kill",
    is_flag=True,
    default=False,
    help="Explicitly allow Fleet-owned runaway processes to be signaled",
)
def discover(auto_kill: bool):
    """Auto-discover running processes and sync registry + state.json."""
    config = discover_mod.load_config()
    conn = _get_conn()
    result = discover_mod.sync(conn, config=config)
    reporter.write_report(conn)

    for a in result["added"]:
        click.echo(f"+ PID {a['pid']} ({a['name']})")
    for c in result["cleaned"]:
        click.echo(f"- PID {c['pid']} ({c['name']}) [dead]")
    skipped_list = result.get("skipped", [])
    for skipped in skipped_list:
        click.echo(f"! PID {skipped['pid']} ({skipped['name']}) skipped: {skipped['reason']}")
    thunder_count = result.get("thunder_synced", 0)
    if thunder_count:
        click.echo(f"Thunder: {thunder_count} instance(s) synced")
    leases_cleaned = result.get("session_leases_cleaned", 0)
    if leases_cleaned:
        click.echo(f"Cleaned {leases_cleaned} stale session lease(s)")
    if not result["added"] and not result["cleaned"] and not skipped_list and not thunder_count and not leases_cleaned:
        click.echo("No changes. Registry is current.")
    # Alert on conflicts via macOS notification
    if skipped_list:
        _notify_conflict(skipped_list)
    # Alert on detached hot sessions
    health_config = syshealth.load_health_config(config)
    flagged = [
        s for s in syshealth.get_session_processes(
            patterns=health_config["session_patterns"],
        )
        if s.attention
    ]
    if flagged:
        _notify_attention(flagged)

    # Runaway detection: persistent tracker across discover invocations
    tracker_path = registry.FLEET_DIR / "runaway_tracker.json"
    tracker = runaway.DaemonRunawayTracker.load(tracker_path)
    _run_runaway_tick(conn, tracker, tracker_path=tracker_path,
                      auto_kill=auto_kill)

    # NS-17 B3: read-only MCP-orphan surfacing (never kills here). Fail-soft —
    # a detector error must not break discover. The opt-in kill path
    # (fleet reap --include-mcp) is the governed remainder, spec 2616440.
    try:
        for _line in _mcp_surface_lines(mcp_orphan_detector.detect()):
            click.echo(_line)
    except Exception as exc:  # noqa: BLE001 — surfacing must never break discover
        click.echo(f"! MCP orphan scan skipped: {type(exc).__name__}", err=True)

    conn.close()


@click.command()
@click.option("--interval", type=int, default=60, help="Seconds between scans")
@click.option(
    "--auto-kill",
    is_flag=True,
    default=False,
    help="Explicitly allow Fleet-owned runaway processes to be signaled",
)
@click.option("--autonomous", is_flag=True, help="Run bounded autopilot reconciler instead of passive discovery")
@click.option("--once", is_flag=True, help="Run one autonomous reconciliation cycle and exit")
@click.option("--repo", "repo_dir", default=None, help="Repo root for autonomous reconciliation")
@click.option("--policy", "policy_path", default=None, help="Autonomous reconciler policy path")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable autonomous result")
def watch(interval: int, auto_kill: bool, autonomous: bool, once: bool,
          repo_dir: str | None, policy_path: str | None, as_json: bool):
    """Run continuous discovery loop (foreground daemon)."""
    import signal
    import time

    if autonomous:
        repo = Path(repo_dir).expanduser() if repo_dir else Path.cwd()
        policy = Path(policy_path).expanduser() if policy_path else None
        result = autonomous_mod.run_once(repo=repo, policy_path=policy)
        if as_json or once:
            click.echo(json.dumps(result, indent=2, sort_keys=True))
        else:
            click.echo(f"{result['verdict']}: {result.get('reason') or result.get('action')}")
        sys.exit(0 if result.get("allowed", False) else 1)

    click.echo(f"Fleet Watch running. Scanning every {interval}s. Ctrl-C to stop.")

    running = True
    tracker = runaway.DaemonRunawayTracker()

    def _stop(signum, frame):
        nonlocal running
        running = False
        click.echo("\nStopping.")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    while running:
        try:
            conn = _get_conn()
            result = discover_mod.sync(conn)
            reporter.write_report(conn)

            for a in result["added"]:
                click.echo(f"+ PID {a['pid']} ({a['name']})")
            for c in result["cleaned"]:
                click.echo(f"- PID {c['pid']} ({c['name']}) [dead]")
            for skipped in result.get("skipped", []):
                click.echo(
                    f"! PID {skipped['pid']} ({skipped['name']}) skipped: {skipped['reason']}"
                )

            # CPU/runtime heuristics are advisory unless this invocation opts in.
            _run_runaway_tick(conn, tracker, auto_kill=auto_kill)

            conn.close()
        except Exception as e:  # noqa: BLE001 — watch loop must not traceback
            click.echo(f"Error: {type(e).__name__}: {e}", err=True)

        # Interruptible sleep
        for _ in range(interval):
            if not running:
                break
            time.sleep(1)

    click.echo("Fleet Watch stopped.")
