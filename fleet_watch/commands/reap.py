from __future__ import annotations

import json
import sys
from typing import Any

import click

from fleet_watch import discover as discover_mod
from fleet_watch import events, registry, reporter, syshealth
from fleet_watch.cli_support import (
    _get_conn,
    _mcp_reap_candidates,
    _terminate_orphan,
)

@click.command()
@click.option("--confirm", is_flag=True, help="Kill and release orphan-confirmed processes")
@click.option("--include-mcp", is_flag=True, default=False,
              help="Also reap dead-session MCP servers (opt-in; kill requires --confirm)")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def reap(confirm: bool, include_mcp: bool, as_json: bool):
    """Kill only orphan-confirmed processes. Dry-run by default.

    With --include-mcp, also reaps dead-session MCP servers (NS-17 B3). The
    live-session-never-reaped invariant is guaranteed by the detector.
    """
    conn = _get_conn()
    candidates = registry.get_reapable_processes(conn)
    if include_mcp:
        candidates = candidates + _mcp_reap_candidates()
    released: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    if confirm:
        for item in candidates:
            if item.get("source") == "mcp":
                # MCP orphan: terminate by PID; not a registry row, so no release.
                terminated = _terminate_orphan(item["pid"])
                if not terminated and registry._pid_exists(item["pid"]):
                    failed.append({
                        "pid": item["pid"], "name": item["name"],
                        "reason": "failed to terminate MCP orphan PID",
                    })
                    continue
                released.append({"pid": item["pid"], "name": item["name"], "source": "mcp"})
                events.log_event(
                    conn, "REAP", pid=item["pid"],
                    workstream=item.get("workstream", "mcp"),
                    detail={"reason": "mcp_orphan_confirmed",
                            "session_id": item.get("session_id")},
                )
                continue
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


@click.command("reap-sessions")
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
