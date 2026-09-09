from __future__ import annotations

import json
import os
import shutil
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

@click.command("install-launchd")
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
