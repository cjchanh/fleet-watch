from __future__ import annotations

import sys
from typing import Any

import click

from fleet_watch import events, referee, registry, reporter
from fleet_watch.cli_support import (
    _get_conn,
    _load_tnr_instances,
)

@click.group()
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
