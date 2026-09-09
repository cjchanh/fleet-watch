from __future__ import annotations

import sys

import click

from fleet_watch import counters
from fleet_watch import events, referee
from fleet_watch.cli_support import (
    _get_conn,
)

@click.command()
@click.argument("pattern")
@click.option("--confirm", is_flag=True, help="Execute kill (dry-run by default)")
@click.option("--cascade", is_flag=True, help="Also kill child processes (depth 2)")
def pkill(pattern: str, confirm: bool, cascade: bool):
    """Kill processes matching PATTERN. Dry-run by default.

    Requires explicit --confirm to execute. Use --cascade to also kill
    child processes up to depth 2. Emits FLEET_PKILL_EXECUTED event.

    Never invokable from other Fleet Watch code paths — operator-typed only.
    """
    from fleet_watch import pkill as pkill_mod

    result = pkill_mod.execute_pkill(pattern, cascade=cascade, confirm=confirm)

    gate_counters = counters.load_counters()
    gate_counters.increment("pkill_cascade")
    counters.save_counters(gate_counters)

    if result.errors and "no processes matching pattern" in result.errors[0]:
        click.echo(f"No processes matching '{pattern}'", err=True)
        sys.exit(1)

    if not confirm:
        click.echo(f"Dry run. {len(result.targets)} process(es) would be killed:")
        for target in result.targets:
            children_note = (
                f" (+{len(target.children)} children)" if target.children else ""
            )
            click.echo(
                f"  PID {target.pid:>7}  {target.rss_mb:>6} MB  {target.name}"
                + children_note
            )
            if cascade and target.children:
                for child in target.children:
                    click.echo(
                        f"    child PID {child.pid:>7}  {child.rss_mb:>6} MB  {child.name}"
                    )
        if cascade:
            total_freed = sum(
                t.rss_mb + sum(c.rss_mb for c in t.children)
                for t in result.targets
            )
        else:
            total_freed = sum(t.rss_mb for t in result.targets)
        click.echo(f"Potential memory freed: {total_freed:,} MB")
        click.echo("Run `fleet pkill --confirm [--cascade] <pattern>` to execute.")
        return

    click.echo(f"Killed {len(result.pids_killed)} process(es)")
    if result.children_killed:
        click.echo(f"Killed {len(result.children_killed)} child process(es)")
    click.echo(f"Memory freed: {result.total_rss_freed_mb:,} MB")

    if result.errors:
        for err in result.errors:
            click.echo(f"ERROR: {err}", err=True)

    if result.killed_any:
        conn = _get_conn()
        events.log_event(
            conn,
            "FLEET_PKILL_EXECUTED",
            workstream="operator",
            detail={
                "pattern": pattern,
                "cascade": cascade,
                "pids_killed": result.pids_killed,
                "children_killed": result.children_killed,
                "operator_authorized": True,
                "total_rss_freed_mb": result.total_rss_freed_mb,
            },
        )
        conn.close()

    sys.exit(0 if not result.errors else 1)


@click.command()
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
