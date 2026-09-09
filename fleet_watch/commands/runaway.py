from __future__ import annotations

import json
import sys
from typing import Any

import click

from fleet_watch import events, runaway
from fleet_watch.cli_support import (
    _get_conn,
)

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
