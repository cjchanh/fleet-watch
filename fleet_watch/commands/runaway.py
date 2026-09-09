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

@click.command("runaway")
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
