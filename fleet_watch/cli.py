"""Click CLI for Fleet Watch."""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import click

from fleet_watch import discover as discover_mod
from fleet_watch import events, gpu_estimator, referee, registry, reporter, runaway, syshealth


def _get_conn():
    return registry.connect()


def _holder_text(holder: dict[str, Any] | None) -> str:
    if holder is None:
        return "none"
    if holder.get("pid") is None:
        return holder["name"]
    return f"PID {holder['pid']} ({holder['name']})"


def _holder_conflict_text(holder: dict[str, Any] | None) -> str:
    if holder is None:
        return "unknown holder"
    if holder.get("pid") is not None:
        return f"PID {holder['pid']} ({holder['name']})"
    provider = holder.get("provider")
    external_id = holder.get("external_id")
    if provider and external_id:
        return f"{provider}:{external_id} ({holder['name']})"
    return holder["name"]


def _resolved_session_id(session_id: str | None) -> str | None:
    if session_id:
        return session_id
    fleet_sid = os.environ.get("FLEET_SESSION_ID")
    if fleet_sid:
        return fleet_sid
    term_sid = os.environ.get("TERM_SESSION_ID")
    if term_sid:
        return f"term-{term_sid}"
    return None


def _notify_conflict(skipped: list[dict[str, Any]]) -> None:
    """Send macOS notification for resource conflicts found during discovery."""
    count = len(skipped)
    names = ", ".join(s["name"] for s in skipped[:3])
    title = "Fleet Watch: Resource Conflict"
    body = f"{count} conflict(s): {names}"
    try:
        subprocess.run(
            [
                "osascript", "-e",
                f'display notification "{body}" with title "{title}"',
            ],
            capture_output=True,
            timeout=3,
        )
    except Exception:
        pass  # Notification failure never blocks discovery


def _notify_attention(sessions: list[syshealth.SessionProcess]) -> None:
    """Send macOS notification when detached hot sessions require attention."""
    if not sessions:
        return
    total_cpu = sum(s.cpu_pct for s in sessions)
    title = "Fleet Watch: Attention Required"
    body = f"{len(sessions)} detached hot session(s) — {total_cpu:.0f}% total CPU"
    try:
        subprocess.run(
            [
                "osascript", "-e",
                f'display notification "{body}" with title "{title}"',
            ],
            capture_output=True,
            timeout=3,
        )
    except Exception:
        pass


_CODEX_ORPHAN_RE = re.compile(r"codex/codex\b")


def _is_fleet_owned(conn: sqlite3.Connection, proc: runaway.RunawayProcess) -> bool:
    """Check if a process is owned by Fleet Watch.

    Auto-kill requires real ownership evidence, not regex classification.
    Two paths qualify:
    1. Registered in Fleet Watch registry (explicit registration via discover/register)
    2. Codex binary orphan — launched by our bootstrap but never registered
       (narrow exception: only the Codex native binary path, not broad patterns)
    """
    if registry.get_process(conn, proc.pid) is not None:
        return True
    if _CODEX_ORPHAN_RE.search(proc.command):
        return True
    return False


def _run_runaway_tick(
    conn: sqlite3.Connection,
    tracker: runaway.DaemonRunawayTracker,
    tracker_path: Path | None = None,
    auto_kill: bool = True,
) -> list[runaway.RunawayProcess]:
    """Run one runaway tracker tick, log events, kill Fleet-owned runaways if auto_kill.

    Auto-kill requires real ownership evidence: registry entry or Codex orphan match.
    Unowned processes (ML training, ffmpeg, external vllm) get an EXTERNAL warning only.
    """
    try:
        newly_flagged = tracker.tick()
    except Exception:
        return []
    for proc in newly_flagged:
        fleet_owned = _is_fleet_owned(conn, proc)
        events.log_event(
            conn,
            "RUNAWAY_DETECTED",
            pid=proc.pid,
            workstream="runaway",
            detail={
                "cpu_pct": proc.cpu_pct,
                "runtime_seconds": proc.runtime_seconds,
                "command": proc.command[:200],
                "consecutive_ticks": runaway.DAEMON_CONSECUTIVE_TICKS,
                "fleet_owned": fleet_owned,
            },
        )
        if auto_kill and fleet_owned:
            success = runaway.kill_runaway(proc.pid)
            event_type = "RUNAWAY_KILL" if success else "RUNAWAY_KILL_FAILED"
            events.log_event(
                conn,
                event_type,
                pid=proc.pid,
                workstream="runaway",
                detail={
                    "cpu_pct": proc.cpu_pct,
                    "command": proc.command[:200],
                },
            )
            status = "killed" if success else "KILL FAILED"
            click.echo(
                f"RUNAWAY: PID {proc.pid} ({proc.name}) — "
                f"CPU {proc.cpu_pct:.1f}% for {runaway.DAEMON_CONSECUTIVE_TICKS} ticks — {status}"
            )
        else:
            label = "WARNING" if fleet_owned else "EXTERNAL"
            click.echo(
                f"{label}: runaway PID {proc.pid} ({proc.name}) — "
                f"CPU {proc.cpu_pct:.1f}% for {runaway.DAEMON_CONSECUTIVE_TICKS} consecutive ticks"
            )
    if tracker_path is not None:
        tracker.save(tracker_path)
    return newly_flagged


def _extract_json_document(raw: str) -> Any:
    raw = raw.strip()
    if not raw:
        raise ValueError("empty JSON payload")
    match = re.search(r"([\[{])", raw)
    if not match:
        raise ValueError("no JSON document found")
    return json.loads(raw[match.start():])


def _load_tnr_instances() -> list[dict[str, Any]]:
    result = subprocess.run(
        ["tnr", "status", "--json"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise click.ClickException(result.stderr.strip() or result.stdout.strip() or "tnr status failed")
    payload = discover_mod.parse_tnr_instances_output(result.stdout, result.stderr)
    if payload is None:
        raise click.ClickException("unexpected tnr status payload")
    return payload


def _build_guard_payload(
    conn,
    port: int | None = None,
    repo_dir: str | None = None,
    gpu_mb: int | None = None,
    framework: str | None = None,
    model_hint: str | None = None,
    current_session_id: str | None = None,
    runaway_tracker: runaway.DaemonRunawayTracker | None = None,
) -> dict[str, Any]:
    state = reporter.build_guard_state(conn)
    budget = state["gpu_budget"]
    payload: dict[str, Any] = {
        "allowed": True,
        "request": {
            "port": port,
            "repo_dir": str(Path(repo_dir).resolve()) if repo_dir else None,
            "gpu_mb": gpu_mb,
            "framework": framework,
            "model": model_hint,
        },
        "checks": {},
        "state": {
            "process_count": state["process_count"],
            "occupied_ports": sorted(state["ports_claimed"].keys()),
            "safe_ports": state.get("safe_ports", []),
            "locked_repos": sorted(state["repos_locked"].keys()),
            "gpu_budget": budget,
            "external_resources": state.get("external_resources", []),
        },
    }

    # Advisory: include active runaway warnings if tracker is available
    if runaway_tracker is not None:
        warnings = runaway_tracker.get_active_warnings()
        if warnings:
            payload["runaways"] = warnings

    if port is not None:
        decision = referee.check_port(conn, port)
        payload["checks"]["port"] = {
            "allowed": decision.allowed,
            "reason": decision.reason,
            "holder": referee.summarize_holder(decision.holder),
            "suggested_ports": referee.suggest_ports(
                conn,
                preferred_ports=state.get("preferred_ports", []),
                requested_port=port,
            ),
        }
        payload["allowed"] = payload["allowed"] and decision.allowed

    if repo_dir is not None:
        decision = referee.check_repo_with_session(
            conn,
            repo_dir,
            current_session_id=current_session_id,
        )
        payload["checks"]["repo"] = {
            "allowed": decision.allowed,
            "reason": decision.reason,
            "holder": referee.summarize_holder(decision.holder),
        }
        payload["allowed"] = payload["allowed"] and decision.allowed

    if gpu_mb is not None:
        decision = referee.check_gpu_budget(conn, gpu_mb)
        gpu_check: dict[str, Any] = {
            "allowed": decision.allowed,
            "reason": decision.reason,
            "requested_mb": gpu_mb,
            "available_mb": max(0, budget["available_mb"]),
            "suggested_max_mb": max(0, budget["available_mb"]),
        }

        # Working set estimation — catches memory overcommit
        mem = syshealth.get_memory_state()
        physical_ram = mem.total_mb if mem.is_available else 0
        config = discover_mod.load_config()
        reserve = gpu_estimator.resolve_effective_reserve_mb(
            physical_ram,
            config.get("gpu_reserve_mb", registry.DEFAULT_GPU_RESERVE_MB),
        )

        estimate = gpu_estimator.estimate_working_set(
            framework=framework,
            command=model_hint,
            physical_ram_mb=physical_ram,
            reserve_mb=reserve,
            config_overrides=config.get("gpu_estimator"),
            allow_model_fallback=False,
        )
        if estimate.source != "insufficient_input":
            gpu_check["working_set"] = estimate.to_dict()

        if estimate.grounded and not estimate.fits:
            gpu_check["allowed"] = False
            gpu_check["reason"] = "working_set_exceeds_physical_ram"
            gpu_check["detail"] = (
                f"working set {estimate.total_mb}MB exceeds "
                f"physical RAM ({physical_ram}MB) minus "
                f"reserve ({estimate.available_after_reserve_mb}MB available)"
            )

        payload["checks"]["gpu"] = gpu_check
        payload["allowed"] = payload["allowed"] and gpu_check["allowed"]

    return payload


def _render_guard(payload: dict[str, Any]) -> list[str]:
    lines = ["ALLOW" if payload["allowed"] else "DENY"]
    checks = payload["checks"]

    if "port" in checks:
        port = payload["request"]["port"]
        port_check = checks["port"]
        if port_check["allowed"]:
            lines.append(f"Port {port}: available")
        else:
            lines.append(f"Port {port}: taken by {_holder_text(port_check['holder'])}")
            suggested = port_check.get("suggested_ports", [])
            if suggested:
                lines.append(f"Suggested ports: {', '.join(str(p) for p in suggested)}")

    if "repo" in checks:
        repo_dir = payload["request"]["repo_dir"]
        repo_check = checks["repo"]
        if repo_check["allowed"]:
            lines.append(f"Repo {repo_dir}: available")
        else:
            lines.append(f"Repo {repo_dir}: locked by {_holder_text(repo_check['holder'])}")

    if "gpu" in checks:
        gpu_check = checks["gpu"]
        requested_mb = gpu_check["requested_mb"]
        ws = gpu_check.get("working_set")
        if gpu_check["allowed"]:
            lines.append(
                f"GPU {requested_mb}MB: available "
                f"({gpu_check['available_mb']}MB free)"
            )
            if ws:
                lines.append(
                    f"  Working set: {ws['total_mb']}MB "
                    f"(weights {ws['weights_mb']} + kv {ws['kv_cache_mb']} "
                    f"+ act {ws['activations_mb']}) × {ws['overhead_multiplier']}x"
                )
                lines.append(
                    f"  Physical RAM available after reserve: {ws['available_after_reserve_mb']}MB"
                )
        else:
            lines.append(f"GPU {requested_mb}MB: {gpu_check.get('detail', gpu_check['reason'])}")
            if ws:
                lines.append(
                    f"  Breakdown: weights {ws['weights_mb']}MB + "
                    f"kv_cache {ws['kv_cache_mb']}MB + "
                    f"activations {ws['activations_mb']}MB "
                    f"× {ws['overhead_multiplier']}x ({ws['framework']})"
                )
                lines.append(
                    f"  Physical RAM available after reserve: {ws['available_after_reserve_mb']}MB"
                )
                if not ws.get("grounded", True):
                    lines.append("  Note: advisory only; provide explicit framework/model for enforcement")
                if ws.get("suggestion"):
                    lines.append(f"  Suggestion: {ws['suggestion']}")

    state = payload["state"]
    lines.append(
        f"GPU budget available: {max(0, state['gpu_budget']['available_mb'])}MB "
        f"({state['gpu_budget']['allocated_mb']}MB allocated)"
    )
    if state["safe_ports"]:
        lines.append(
            "Open ports: " + ", ".join(str(port) for port in state["safe_ports"])
        )
    if state["locked_repos"]:
        lines.append("Locked repos: " + ", ".join(state["locked_repos"]))

    return lines


def _default_owner_pid() -> int:
    parent = os.getppid()
    return parent if parent > 1 else os.getpid()


def _build_reconcile_payload(conn) -> dict[str, Any]:
    processes = registry.get_process_classifications(conn)
    summary: dict[str, int] = {}
    for item in processes:
        summary[item["classification"]] = summary.get(item["classification"], 0) + 1
    return {
        "generated_utc": registry._now_iso(),
        "summary": summary,
        "processes": processes,
    }


def _terminate_orphan(pid: int, grace_seconds: float = 1.5) -> bool:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if not registry._pid_exists(pid):
            return True
        time.sleep(0.1)

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if not registry._pid_exists(pid):
            return True
        time.sleep(0.1)
    return not registry._pid_exists(pid)


def _render_launchd_plist(executable: str, interval: int) -> str:
    log_path = Path.home() / "Library/Logs/fleet-watch.log"
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" "
        "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n"
        "<plist version=\"1.0\">\n"
        "<dict>\n"
        "    <key>Label</key>\n"
        "    <string>io.fleet-watch</string>\n"
        "    <key>ProgramArguments</key>\n"
        "    <array>\n"
        f"        <string>{executable}</string>\n"
        "        <string>discover</string>\n"
        "    </array>\n"
        "    <key>StartInterval</key>\n"
        f"    <integer>{interval}</integer>\n"
        "    <key>RunAtLoad</key>\n"
        "    <true/>\n"
        "    <key>Nice</key>\n"
        "    <integer>19</integer>\n"
        "    <key>ProcessType</key>\n"
        "    <string>Background</string>\n"
        "    <key>StandardOutPath</key>\n"
        f"    <string>{log_path}</string>\n"
        "    <key>StandardErrorPath</key>\n"
        f"    <string>{log_path}</string>\n"
        "</dict>\n"
        "</plist>\n"
    )


@click.group()
@click.version_option(package_name="fleet-watch")
def cli():
    """Fleet Watch — Process governance for AI workloads."""
    pass


@cli.command()
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def status(as_json: bool):
    """Show current fleet state."""
    conn = _get_conn()
    # Auto-clean dead PIDs
    cleaned = registry.clean_dead_pids(conn)
    for c in cleaned:
        events.log_event(conn, "CLEAN", pid=c["pid"], workstream=c["workstream"],
                         detail={"reason": "dead_pid", "name": c["name"]})

    if as_json:
        state = reporter.build_state(conn)
        click.echo(json.dumps(state, indent=2, default=str))
    else:
        procs = registry.get_all_processes(conn)
        budget = registry.get_gpu_budget(conn)

        if not procs:
            click.echo("No active processes.")
        else:
            click.echo(f"Active processes ({len(procs)}):")
            click.echo(f"{'PID':>7}  {'Name':<20} {'Workstream':<18} {'Port':<6} {'GPU':>8} {'Pri':>3}")
            click.echo("-" * 72)
            for p in procs:
                port = str(p["port"]) if p["port"] else "-"
                gpu = f"{p['gpu_mb']}MB" if p["gpu_mb"] else "0MB"
                click.echo(f"{p['pid']:>7}  {p['name']:<20} {p['workstream']:<18} {port:<6} {gpu:>8} {p['priority']:>3}")

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

    conn.close()


@cli.command()
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
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        conn.close()
        sys.exit(1)

    events.log_event(conn, "REGISTER", pid=pid, workstream=workstream,
                     detail={"name": name, "port": port, "gpu_mb": gpu_mb,
                             "repo_dir": repo_dir, "priority": priority})
    click.echo(f"Registered PID {pid} ({name})")
    conn.close()


@cli.command()
@click.option("--port", type=int, default=None, help="Port to check")
@click.option("--repo", "repo_dir", default=None, help="Repo directory to check")
@click.option("--gpu", "gpu_mb", type=int, default=None, help="GPU MB to check")
@click.option("--session-id", default=None, help="Current session ID for owned-resource bypass")
def check(port: int | None, repo_dir: str | None, gpu_mb: int | None, session_id: str | None):
    """Check if a resource is available. Exit 0=available, 1=taken."""
    if port is None and repo_dir is None and gpu_mb is None:
        click.echo("Specify --port, --repo, or --gpu", err=True)
        sys.exit(2)

    current_session_id = _resolved_session_id(session_id)
    conn = _get_conn()
    failed = False

    if port is not None:
        decision = referee.check_port(conn, port)
        if decision.allowed:
            click.echo(f"Port {port}: available")
        else:
            click.echo(f"Port {port}: TAKEN by PID {decision.holder['pid']} ({decision.holder['name']})", err=True)
            failed = True

    if repo_dir is not None:
        decision = referee.check_repo_with_session(
            conn,
            repo_dir,
            current_session_id=current_session_id,
        )
        if decision.allowed:
            click.echo(f"Repo {repo_dir}: available")
        else:
            click.echo(f"Repo {repo_dir}: LOCKED by {_holder_conflict_text(decision.holder)}", err=True)
            failed = True

    if gpu_mb is not None:
        decision = referee.check_gpu_budget(conn, gpu_mb)
        if decision.allowed:
            click.echo(f"GPU {gpu_mb}MB: available")
        else:
            click.echo(f"GPU {gpu_mb}MB: {decision.reason}", err=True)
            failed = True

    conn.close()
    sys.exit(1 if failed else 0)


@cli.command()
@click.option("--port", type=int, default=None, help="Port to guard")
@click.option("--repo", "repo_dir", default=None, help="Repo directory to guard")
@click.option("--gpu", "gpu_mb", type=int, default=None, help="GPU MB to guard")
@click.option("--framework", default=None, help="Inference framework (candle, mlx, ollama, vllm)")
@click.option("--model", "model_hint", default=None, help="Model name/path for working set estimation")
@click.option("--session-id", default=None, help="Current session ID for owned-resource bypass")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def guard(
    port: int | None,
    repo_dir: str | None,
    gpu_mb: int | None,
    framework: str | None,
    model_hint: str | None,
    session_id: str | None,
    as_json: bool,
):
    """Canonical pre-flight interface for agents and operators."""
    current_session_id = _resolved_session_id(session_id)
    conn = _get_conn()
    for cleaned in registry.clean_dead_pids(conn):
        events.log_event(
            conn,
            "CLEAN",
            pid=cleaned["pid"],
            workstream=cleaned["workstream"],
            detail={"reason": "dead_pid", "name": cleaned["name"]},
        )

    payload = _build_guard_payload(
        conn,
        port=port,
        repo_dir=repo_dir,
        gpu_mb=gpu_mb,
        framework=framework,
        model_hint=model_hint,
        current_session_id=current_session_id,
    )

    gpu_check = payload["checks"].get("gpu")
    if gpu_check and not gpu_check.get("allowed", True):
        event_type = (
            "GPU_WORKING_SET_DENY"
            if gpu_check.get("reason") == "working_set_exceeds_physical_ram"
            else "GPU_BUDGET_DENY"
        )
        events.log_event(
            conn,
            event_type,
            workstream="guard",
            detail={
                "requested_mb": gpu_check.get("requested_mb"),
                "reason": gpu_check.get("reason"),
                "detail": gpu_check.get("detail"),
                "framework": framework,
                "model": model_hint,
                "working_set": gpu_check.get("working_set"),
            },
        )
    conn.close()

    # Advisory: scan for active runaway processes (never crash the guard)
    try:
        runaways = runaway.scan_runaways()
    except Exception:
        runaways = []
    if runaways:
        payload["runaways"] = [r.to_dict() for r in runaways]

    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
    else:
        for line in _render_guard(payload):
            click.echo(line)

    sys.exit(0 if payload["allowed"] else 1)


# Keep 'claim' as alias for backward compat
@cli.command(hidden=True)
@click.option("--port", type=int, default=None, help="Port to check")
@click.option("--repo", "repo_dir", default=None, help="Repo directory to check")
@click.option("--gpu", "gpu_mb", type=int, default=None, help="GPU MB to check")
@click.pass_context
def claim(ctx, port, repo_dir, gpu_mb):
    """Alias for 'check' (deprecated)."""
    ctx.invoke(check, port=port, repo_dir=repo_dir, gpu_mb=gpu_mb, session_id=None)


@cli.command()
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


@cli.command()
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


@cli.group()
def session():
    """Manage explicit session leases for long-lived agent ownership."""
    pass


@session.command("start")
@click.option("--session-id", required=True, help="Session identifier")
@click.option("--owner-pid", type=int, default=None, help="Owning shell/launcher PID")
@click.option("--repo", "repo_dir", default=None, help="Repo directory associated with the session")
def session_start(session_id: str, owner_pid: int | None, repo_dir: str | None):
    """Open or refresh a session lease."""
    conn = _get_conn()
    failures = referee.preflight_register(
        conn,
        repo_dir=repo_dir,
        current_session_id=session_id,
    )
    if failures:
        for failure in failures:
            click.echo(f"DENY: {failure.reason}", err=True)
        conn.close()
        sys.exit(1)

    resolved_owner_pid = owner_pid or _default_owner_pid()
    registry.upsert_session_lease(
        conn,
        session_id,
        owner_pid=resolved_owner_pid,
        repo_dir=repo_dir,
    )
    events.log_event(
        conn,
        "SESSION_START",
        pid=resolved_owner_pid,
        detail={"session_id": session_id, "repo_dir": repo_dir},
    )
    reporter.write_report(conn)
    click.echo(f"Session {session_id} active (owner PID {resolved_owner_pid})")
    conn.close()


@session.command("heartbeat")
@click.option("--session-id", required=True, help="Session identifier")
@click.option("--owner-pid", type=int, default=None, help="Owning shell/launcher PID")
@click.option("--repo", "repo_dir", default=None, help="Repo directory associated with the session")
def session_heartbeat(session_id: str, owner_pid: int | None, repo_dir: str | None):
    """Refresh a session lease heartbeat."""
    conn = _get_conn()
    resolved_owner_pid = owner_pid or _default_owner_pid()
    ok = registry.heartbeat_session_lease(
        conn,
        session_id,
        owner_pid=resolved_owner_pid,
        repo_dir=repo_dir,
    )
    if not ok:
        click.echo(f"Session {session_id} not found", err=True)
        conn.close()
        sys.exit(2)
    events.log_event(
        conn,
        "SESSION_HEARTBEAT",
        pid=resolved_owner_pid,
        detail={"session_id": session_id, "repo_dir": repo_dir},
    )
    reporter.write_report(conn)
    click.echo(f"Session heartbeat updated for {session_id}")
    conn.close()


@session.command("ensure")
@click.option("--session-id", required=True, help="Session identifier")
@click.option("--owner-pid", type=int, default=None, help="Owning shell/launcher PID")
@click.option("--repo", "repo_dir", default=None, help="Repo directory associated with the session")
@click.option("--retries", type=int, default=3, help="Max retry attempts on transient failure")
@click.option("--retry-delay", type=float, default=2.0, help="Seconds between retries")
def session_ensure(
    session_id: str,
    owner_pid: int | None,
    repo_dir: str | None,
    retries: int,
    retry_delay: float,
):
    """Open or refresh a session lease with automatic retry on transient failure.

    Fail-open: on final failure, exits 0 with a stderr warning so the
    calling process (e.g. Codex bootstrap) is never blocked.
    """
    resolved_owner_pid = owner_pid or _default_owner_pid()
    last_err: Exception | None = None

    for attempt in range(1, retries + 1):
        conn = None
        try:
            conn = _get_conn()
            failures = referee.preflight_register(
                conn,
                repo_dir=repo_dir,
                current_session_id=session_id,
            )
            if failures:
                for failure in failures:
                    click.echo(f"DENY: {failure.reason}", err=True)
                conn.close()
                sys.exit(1)

            registry.upsert_session_lease(
                conn,
                session_id,
                owner_pid=resolved_owner_pid,
                repo_dir=repo_dir,
            )
            events.log_event(
                conn,
                "SESSION_START",
                pid=resolved_owner_pid,
                detail={"session_id": session_id, "repo_dir": repo_dir, "source": "ensure"},
            )
            reporter.write_report(conn)
            conn.close()
            click.echo(f"Session {session_id} active (owner PID {resolved_owner_pid})")
            return
        except (sqlite3.OperationalError, sqlite3.DatabaseError, OSError) as exc:
            last_err = exc
            if conn is not None:
                try:
                    conn.close()
                except (sqlite3.Error, OSError):
                    pass
            if attempt < retries:
                click.echo(
                    f"fleet session ensure: attempt {attempt}/{retries} failed ({exc}), retrying...",
                    err=True,
                )
                time.sleep(retry_delay)

    click.echo(
        f"fleet session ensure: all {retries} attempts failed ({last_err}). "
        f"Session {session_id} is UNTRACKED.",
        err=True,
    )


@session.command("close")
@click.option("--session-id", required=True, help="Session identifier")
def session_close(session_id: str):
    """Close a session lease without touching attached processes."""
    conn = _get_conn()
    ok = registry.close_session_lease(conn, session_id)
    if not ok:
        click.echo(f"Session {session_id} not found", err=True)
        conn.close()
        sys.exit(2)
    events.log_event(
        conn,
        "SESSION_CLOSE",
        detail={"session_id": session_id},
    )
    reporter.write_report(conn)
    click.echo(f"Session {session_id} closed")
    conn.close()


@cli.command()
@click.option("--port", type=int, required=True, help="Port to preempt")
@click.option("--priority", type=click.IntRange(1, 5), required=True, help="Priority of the requesting workload")
@click.option("--reason", required=True, help="Audit reason for the preemption")
@click.option("--grace", type=int, default=30, help="Grace period in seconds")
def preempt(port: int, priority: int, reason: str, grace: int):
    """Preempt a resource from a lower-priority holder."""
    conn = _get_conn()
    decision = referee.preempt_port(conn, port, priority, reason, grace_seconds=grace)
    if decision.allowed:
        click.echo(f"Preempted: {decision.reason}")
    else:
        click.echo(f"DENY: {decision.reason}", err=True)
        conn.close()
        sys.exit(1)
    conn.close()


@cli.command()
def report():
    """Generate STATE_REPORT.md and state.json."""
    conn = _get_conn()
    md_path, json_path = reporter.write_report(conn)
    click.echo(f"Written: {md_path}")
    click.echo(f"Written: {json_path}")
    conn.close()


@cli.command()
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


@cli.command()
def stale():
    """List heartbeat-stale processes with evidence-based classification."""
    conn = _get_conn()
    stale_procs = registry.get_stale_processes(conn)
    if not stale_procs:
        click.echo("No stale processes.")
    else:
        for s in stale_procs:
            evidence = "; ".join(s.get("evidence", [])[:3])
            click.echo(
                f"PID {s['pid']} ({s['name']}) — {s['classification']} — "
                f"heartbeat {s['stale_seconds']}s ago"
                + (f" — {evidence}" if evidence else "")
            )
    conn.close()


@cli.command()
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


@cli.command()
@click.option("--confirm", is_flag=True, help="Kill and release orphan-confirmed processes")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def reap(confirm: bool, as_json: bool):
    """Kill only orphan-confirmed processes. Dry-run by default."""
    conn = _get_conn()
    candidates = registry.get_reapable_processes(conn)
    released: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    if confirm:
        for item in candidates:
            terminated = _terminate_orphan(item["pid"])
            if not terminated and registry._pid_exists(item["pid"]):
                failed.append({
                    "pid": item["pid"],
                    "name": item["name"],
                    "reason": "failed to terminate orphan-confirmed PID",
                })
                continue

            released_item = registry.release_process(conn, item["pid"])
            if released_item is None:
                failed.append({
                    "pid": item["pid"],
                    "name": item["name"],
                    "reason": "process disappeared before registry release",
                })
                continue
            released.append(released_item)
            events.log_event(
                conn,
                "REAP",
                pid=item["pid"],
                workstream=item["workstream"],
                detail={
                    "reason": "orphan_confirmed",
                    "session_id": item["session_id"],
                },
            )
        reporter.write_report(conn)

    payload = {
        "confirmed": confirm,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "released": released,
        "failed": failed,
    }
    conn.close()

    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        sys.exit(1 if failed else 0)

    if not candidates:
        click.echo("No orphan-confirmed processes.")
        return

    if not confirm:
        click.echo("Dry run. Orphan-confirmed processes:")
        for item in candidates:
            evidence = "; ".join(item.get("evidence", [])[:3])
            click.echo(
                f"PID {item['pid']} ({item['name']}) — {evidence}"
            )
        click.echo("Run `fleet reap --confirm` to terminate and release these rows.")
        return

    for item in released:
        click.echo(f"Reaped PID {item['pid']} ({item['name']})")
    for item in failed:
        click.echo(f"FAIL: PID {item['pid']} ({item['name']}) — {item['reason']}", err=True)
    sys.exit(1 if failed else 0)


@cli.command("reap-sessions")
@click.option("--confirm", is_flag=True, help="Kill detached hot sessions (dry-run by default)")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def reap_sessions(confirm: bool, as_json: bool):
    """Kill detached_hot syshealth sessions. Dry-run by default."""
    config = discover_mod.load_config()
    health_config = syshealth.load_health_config(config)
    sessions = syshealth.get_session_processes(
        patterns=health_config["session_patterns"],
    )
    candidates = [s for s in sessions if s.attention and s.classification == "detached_hot"]

    killed: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    if confirm:
        conn = _get_conn()
        try:
            for sess in candidates:
                member_results: list[dict[str, Any]] = []
                all_ok = True
                for pid in sess.member_pids:
                    ok = _terminate_orphan(pid)
                    member_results.append({"pid": pid, "terminated": ok})
                    if not ok and registry._pid_exists(pid):
                        all_ok = False

                entry = {
                    "pid": sess.pid,
                    "kind": sess.kind,
                    "name": sess.name,
                    "member_pids": sess.member_pids,
                    "cpu_pct": sess.cpu_pct,
                    "rss_mb": sess.rss_mb,
                    "members": member_results,
                }
                if all_ok:
                    killed.append(entry)
                else:
                    entry["reason"] = "one or more member PIDs could not be terminated"
                    failed.append(entry)

                events.log_event(
                    conn,
                    "REAP_SESSION",
                    pid=sess.pid,
                    workstream="session",
                    detail={
                        "kind": sess.kind,
                        "member_pids": sess.member_pids,
                        "cpu_pct": sess.cpu_pct,
                        "classification": sess.classification,
                        "success": all_ok,
                    },
                )
            reporter.write_report(conn)
        finally:
            conn.close()

    payload = {
        "confirmed": confirm,
        "candidate_count": len(candidates),
        "candidates": [
            {
                "pid": s.pid,
                "kind": s.kind,
                "name": s.name,
                "member_pids": s.member_pids,
                "cpu_pct": s.cpu_pct,
                "rss_mb": s.rss_mb,
                "classification": s.classification,
                "evidence": s.evidence,
            }
            for s in candidates
        ],
        "killed": killed,
        "failed": failed,
    }

    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        sys.exit(1 if failed else 0)
        return

    if not candidates:
        click.echo("No detached hot sessions.")
        return

    if not confirm:
        click.echo("Dry run. Detached hot sessions:")
        for s in candidates:
            evidence = "; ".join(s.evidence[:3])
            click.echo(
                f"  PID {s.pid} ({s.kind}) — {s.cpu_pct:.1f}% CPU — "
                f"{s.member_count} member(s) — {evidence}"
            )
        click.echo("Run `fleet reap-sessions --confirm` to terminate these sessions.")
        return

    for entry in killed:
        click.echo(f"Killed session PID {entry['pid']} ({entry['kind']}) — {len(entry['member_pids'])} member(s)")
    for entry in failed:
        click.echo(f"FAIL: session PID {entry['pid']} ({entry['kind']}) — {entry['reason']}", err=True)
    sys.exit(1 if failed else 0)


@cli.command()
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


@cli.command()
@click.pass_context
def context(ctx):
    """Backward-compatible alias for `fleet guard --json`."""
    ctx.invoke(
        guard,
        port=None,
        repo_dir=None,
        gpu_mb=None,
        framework=None,
        model_hint=None,
        session_id=None,
        as_json=True,
    )


@cli.command()
@click.option("--no-auto-kill", is_flag=True, default=False,
              help="Log runaway processes without killing them")
def discover(no_auto_kill: bool):
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
                      auto_kill=not no_auto_kill)
    conn.close()


@cli.command()
@click.option("--interval", type=int, default=60, help="Seconds between scans")
@click.option("--no-auto-kill", is_flag=True, default=False,
              help="Log runaway processes without killing them")
def watch(interval: int, no_auto_kill: bool):
    """Run continuous discovery loop (foreground daemon)."""
    import signal
    import time

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

            # Runaway detection: kill by default, --no-auto-kill for warning only
            _run_runaway_tick(conn, tracker, auto_kill=not no_auto_kill)

            conn.close()
        except Exception as e:
            click.echo(f"Error: {e}", err=True)

        # Interruptible sleep
        for _ in range(interval):
            if not running:
                break
            time.sleep(1)

    click.echo("Fleet Watch stopped.")


@cli.command("install-launchd")
@click.option("--interval", type=int, default=60, help="Seconds between scans")
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path),
    default=Path.home() / "Library/LaunchAgents/io.fleet-watch.plist",
    help="Where to write the plist",
)
@click.option("--load/--no-load", default=True, help="Load the agent after writing")
def install_launchd(interval: int, output_path: Path, load: bool):
    """Write a launchd plist with the real fleet executable path."""
    executable = shutil.which("fleet")
    if executable is None:
        click.echo("fleet executable not found in PATH", err=True)
        sys.exit(1)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(_render_launchd_plist(executable, interval))
    click.echo(f"Written: {output_path}")

    if not load:
        return

    subprocess.run(
        ["launchctl", "unload", str(output_path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    result = subprocess.run(
        ["launchctl", "load", str(output_path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode != 0:
        click.echo(result.stderr.strip() or result.stdout.strip(), err=True)
        sys.exit(result.returncode)

    click.echo("Loaded: io.fleet-watch")


@cli.command()
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


@cli.command()
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


@cli.group()
def thunder():
    """Thunder instance coordination."""
    pass


@thunder.command("sync")
def thunder_sync():
    """Sync Thunder instances from `tnr status --json` into Fleet Watch."""
    conn = _get_conn()
    instances = _load_tnr_instances()
    mapped: list[dict[str, Any]] = []
    for item in instances:
        external_id = str(item.get("uuid") or item.get("name") or item.get("id"))
        mapped.append(
            {
                "resource_type": "instance",
                "external_id": external_id,
                "name": f"Thunder {external_id}",
                "status": str(item.get("status") or "UNKNOWN"),
                "metadata": item,
                "cleanup_cmd": f"tnr delete {item.get('id')} --yes",
                "safe_to_delete": False,
                "endpoint": None,
                "gpu_mb": 0,
            }
        )
    registry.replace_provider_resources(conn, provider="thunder", resources=mapped)
    reporter.write_report(conn)
    click.echo(f"Synced {len(mapped)} Thunder instance(s)")
    conn.close()


@thunder.command("claim")
@click.option("--uuid", "external_id", required=True, help="Thunder instance UUID")
@click.option("--session-id", required=True, help="Owning session identifier")
@click.option("--repo", "repo_dir", default=None, help="Repo directory associated with the instance")
@click.option("--workstream", default="thunder", help="Owning workstream")
@click.option("--name", default=None, help="Human-readable resource name")
@click.option("--priority", type=click.IntRange(1, 5), default=3, help="Priority 1-5 for arbitration")
@click.option("--started-by", default=None, help="Human or tool that started the instance")
@click.option("--owner-tool", default=None, help="Owning tool (e.g. codex, claude)")
@click.option("--model", default=None, help="Model ID or family")
@click.option("--endpoint", default=None, help="Primary model endpoint")
@click.option("--status", default="RUNNING", help="Resource status")
@click.option("--cleanup-cmd", default=None, help="Cleanup command to remove the instance")
@click.option("--safe-to-delete/--unsafe-to-delete", default=False, help="Mark whether the instance is safe to delete automatically")
def thunder_claim(
    external_id: str,
    session_id: str,
    repo_dir: str | None,
    workstream: str,
    name: str | None,
    priority: int,
    started_by: str | None,
    owner_tool: str | None,
    model: str | None,
    endpoint: str | None,
    status: str,
    cleanup_cmd: str | None,
    safe_to_delete: bool,
):
    """Claim ownership metadata for a Thunder instance."""
    conn = _get_conn()
    failures = referee.preflight_register(
        conn,
        repo_dir=repo_dir,
        current_session_id=session_id,
    )
    if failures:
        for failure in failures:
            click.echo(f"DENY: {failure.reason}", err=True)
        conn.close()
        sys.exit(1)

    prior = registry.get_external_resource(conn, provider="thunder", external_id=external_id)
    metadata = prior["metadata"] if prior else {}
    resolved_name = name or (prior["name"] if prior else f"Thunder {external_id}")
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id=external_id,
        session_id=session_id,
        workstream=workstream,
        name=resolved_name,
        priority=priority,
        gpu_mb=0,
        repo_dir=repo_dir or (prior["repo_dir"] if prior else None),
        model=model or (prior["model"] if prior else None),
        status=status or (prior["status"] if prior else "RUNNING"),
        started_by=started_by or (prior["started_by"] if prior else None),
        owner_tool=owner_tool or (prior["owner_tool"] if prior else None),
        endpoint=endpoint or (prior["endpoint"] if prior else None),
        cleanup_cmd=cleanup_cmd or (prior["cleanup_cmd"] if prior else None),
        safe_to_delete=safe_to_delete if safe_to_delete else (prior["safe_to_delete"] if prior else False),
        metadata=metadata,
    )
    events.log_event(
        conn,
        "REGISTER",
        workstream=workstream,
        detail={"provider": "thunder", "external_id": external_id, "repo_dir": repo_dir, "session_id": session_id},
    )
    reporter.write_report(conn)
    click.echo(f"Claimed thunder:{external_id}")
    conn.close()


@thunder.command("heartbeat")
@click.option("--uuid", "external_id", required=True, help="Thunder instance UUID")
@click.option("--status", default=None, help="Updated status")
def thunder_heartbeat(external_id: str, status: str | None):
    """Refresh last_seen for a Thunder resource."""
    conn = _get_conn()
    ok = registry.heartbeat_external_resource(
        conn,
        provider="thunder",
        external_id=external_id,
        status=status,
    )
    if not ok:
        click.echo(f"Thunder resource {external_id} not found", err=True)
        conn.close()
        sys.exit(2)
    events.log_event(
        conn,
        "HEARTBEAT",
        workstream="thunder",
        detail={"provider": "thunder", "external_id": external_id, "status": status},
    )
    reporter.write_report(conn)
    click.echo(f"Heartbeat updated for thunder:{external_id}")
    conn.close()


@thunder.command("release")
@click.option("--uuid", "external_id", required=True, help="Thunder instance UUID")
def thunder_release(external_id: str):
    """Release a Thunder resource from Fleet Watch."""
    conn = _get_conn()
    result = registry.release_external_resource(conn, provider="thunder", external_id=external_id)
    if not result:
        click.echo(f"Thunder resource {external_id} not found", err=True)
        conn.close()
        sys.exit(2)
    events.log_event(
        conn,
        "RELEASE",
        workstream=result["workstream"],
        detail={"provider": "thunder", "external_id": external_id, "name": result["name"]},
    )
    reporter.write_report(conn)
    click.echo(f"Released thunder:{external_id}")
    conn.close()


@cli.command("runaway")
@click.option("--kill", "do_kill", is_flag=True, help="SIGKILL flagged processes (default: dry-run)")
@click.option("--cpu-threshold", type=float, default=runaway.DEFAULT_CPU_THRESHOLD,
              help="CPU percentage threshold (default 90)")
@click.option("--sustained-seconds", type=int, default=runaway.DEFAULT_SUSTAINED_SECONDS,
              help="Minimum runtime in seconds (default 60)")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def runaway_scan(do_kill: bool, cpu_threshold: float, sustained_seconds: int, as_json: bool):
    """Detect and optionally kill runaway high-CPU processes."""
    flagged = runaway.scan_runaways(
        cpu_threshold=cpu_threshold,
        sustained_seconds=sustained_seconds,
    )

    killed: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    if do_kill and flagged:
        conn = _get_conn()
        try:
            for proc in flagged:
                success = runaway.kill_runaway(proc.pid)
                entry = proc.to_dict()
                if success:
                    killed.append(entry)
                    events.log_event(
                        conn,
                        "RUNAWAY_KILL",
                        pid=proc.pid,
                        workstream="runaway",
                        detail={
                            "cpu_pct": proc.cpu_pct,
                            "runtime_seconds": proc.runtime_seconds,
                            "command": proc.command[:200],
                        },
                    )
                else:
                    entry["reason"] = "kill failed"
                    failed.append(entry)
        finally:
            conn.close()

    payload = {
        "confirmed": do_kill,
        "cpu_threshold": cpu_threshold,
        "sustained_seconds": sustained_seconds,
        "flagged_count": len(flagged),
        "flagged": [p.to_dict() for p in flagged],
        "killed": killed,
        "failed": failed,
    }

    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        sys.exit(1 if failed else 0)
        return

    if not flagged:
        click.echo("No runaway processes detected.")
        return

    if not do_kill:
        click.echo(f"Dry run. {len(flagged)} runaway process(es) detected (>{cpu_threshold}% CPU, >{sustained_seconds}s):")
        for proc in flagged:
            click.echo(
                f"  PID {proc.pid:>7}  {proc.name:<20} CPU {proc.cpu_pct:>5.1f}%  "
                f"runtime {proc.runtime_seconds}s  {proc.command[:60]}"
            )
        click.echo("Run `fleet runaway --kill` to terminate these processes.")
        return

    for entry in killed:
        click.echo(f"Killed PID {entry['pid']} ({entry['name']}) — CPU {entry['cpu_pct']}%")
    for entry in failed:
        click.echo(f"FAIL: PID {entry['pid']} ({entry['name']}) — {entry.get('reason', 'unknown')}", err=True)
    sys.exit(1 if failed else 0)
def main():
    """Run the Fleet Watch CLI entrypoint."""
    cli()


if __name__ == "__main__":
    main()
