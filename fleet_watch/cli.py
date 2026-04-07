"""Click CLI for Fleet Watch."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import click

from fleet_watch import discover as discover_mod
from fleet_watch import events, referee, registry, reporter


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
    )
    if result.returncode != 0:
        raise click.ClickException(result.stderr.strip() or result.stdout.strip() or "tnr status failed")
    payload = _extract_json_document(result.stdout)
    if not isinstance(payload, list):
        raise click.ClickException("unexpected tnr status payload")
    return payload


def _build_guard_payload(
    conn,
    port: int | None = None,
    repo_dir: str | None = None,
    gpu_mb: int | None = None,
    current_session_id: str | None = None,
) -> dict[str, Any]:
    state = reporter.build_state(conn)
    budget = state["gpu_budget"]
    payload: dict[str, Any] = {
        "allowed": True,
        "request": {
            "port": port,
            "repo_dir": str(Path(repo_dir).resolve()) if repo_dir else None,
            "gpu_mb": gpu_mb,
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
        payload["checks"]["gpu"] = {
            "allowed": decision.allowed,
            "reason": decision.reason,
            "requested_mb": gpu_mb,
            "available_mb": max(0, budget["available_mb"]),
            "suggested_max_mb": max(0, budget["available_mb"]),
        }
        payload["allowed"] = payload["allowed"] and decision.allowed

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
        if gpu_check["allowed"]:
            lines.append(
                f"GPU {requested_mb}MB: available "
                f"({gpu_check['available_mb']}MB free)"
            )
        else:
            lines.append(f"GPU {requested_mb}MB: {gpu_check['reason']}")

    state = payload["state"]
    lines.append(
        f"GPU available: {max(0, state['gpu_budget']['available_mb'])}MB "
        f"({state['gpu_budget']['allocated_mb']}MB allocated)"
    )
    if state["safe_ports"]:
        lines.append(
            "Open ports: " + ", ".join(str(port) for port in state["safe_ports"])
        )
    if state["locked_repos"]:
        lines.append("Locked repos: " + ", ".join(state["locked_repos"]))

    return lines


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
@click.option("--restart-policy", type=click.Choice(sorted(registry.RESTART_POLICIES)), default="ALERT_ONLY")
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
        decision = referee.check_repo_with_session(conn, repo_dir, current_session_id=session_id)
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
@click.option("--session-id", default=None, help="Current session ID for owned-resource bypass")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def guard(
    port: int | None,
    repo_dir: str | None,
    gpu_mb: int | None,
    session_id: str | None,
    as_json: bool,
):
    """Canonical pre-flight interface for agents and operators."""
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
        current_session_id=session_id,
    )
    conn.close()

    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
    else:
        for line in _render_guard(payload):
            click.echo(line)

    sys.exit(0 if payload["allowed"] else 1)


# Keep 'claim' as alias for backward compat
@cli.command(hidden=True)
@click.option("--port", type=int, default=None)
@click.option("--repo", "repo_dir", default=None)
@click.option("--gpu", "gpu_mb", type=int, default=None)
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
@click.option("--pid", type=int, required=True)
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


@cli.command()
@click.option("--port", type=int, required=True)
@click.option("--priority", type=click.IntRange(1, 5), required=True)
@click.option("--reason", required=True)
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
    """List processes with stale heartbeats (>180s)."""
    conn = _get_conn()
    stale_procs = registry.get_stale_processes(conn)
    if not stale_procs:
        click.echo("No stale processes.")
    else:
        for s in stale_procs:
            click.echo(f"PID {s['pid']} ({s['name']}) — heartbeat {s['stale_seconds']}s ago")
    conn.close()


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
    ctx.invoke(guard, port=None, repo_dir=None, gpu_mb=None, session_id=None, as_json=True)


@cli.command()
def discover():
    """Auto-discover running processes and sync registry + state.json."""
    conn = _get_conn()
    result = discover_mod.sync(conn)
    reporter.write_report(conn)
    conn.close()
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
    if not result["added"] and not result["cleaned"] and not skipped_list and not thunder_count:
        click.echo("No changes. Registry is current.")
    # Alert on conflicts via macOS notification
    if skipped_list:
        _notify_conflict(skipped_list)


@cli.command()
@click.option("--interval", type=int, default=60, help="Seconds between scans")
def watch(interval: int):
    """Run continuous discovery loop (foreground daemon)."""
    import signal
    import time

    click.echo(f"Fleet Watch running. Scanning every {interval}s. Ctrl-C to stop.")

    running = True

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
            conn.close()

            for a in result["added"]:
                click.echo(f"+ PID {a['pid']} ({a['name']})")
            for c in result["cleaned"]:
                click.echo(f"- PID {c['pid']} ({c['name']}) [dead]")
            for skipped in result.get("skipped", []):
                click.echo(
                    f"! PID {skipped['pid']} ({skipped['name']}) skipped: {skipped['reason']}"
                )
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
    )
    result = subprocess.run(
        ["launchctl", "load", str(output_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        click.echo(result.stderr.strip() or result.stdout.strip(), err=True)
        sys.exit(result.returncode)

    click.echo("Loaded: io.fleet-watch")


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
@click.option("--priority", type=click.IntRange(1, 5), default=3)
@click.option("--started-by", default=None, help="Human or tool that started the instance")
@click.option("--owner-tool", default=None, help="Owning tool (e.g. codex, claude)")
@click.option("--model", default=None, help="Model ID or family")
@click.option("--endpoint", default=None, help="Primary model endpoint")
@click.option("--status", default="RUNNING", help="Resource status")
@click.option("--cleanup-cmd", default=None, help="Cleanup command to remove the instance")
@click.option("--safe-to-delete/--unsafe-to-delete", default=False)
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


def main():
    cli()


if __name__ == "__main__":
    main()
