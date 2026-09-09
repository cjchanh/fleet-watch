from __future__ import annotations

import json
import sys

import click

from fleet_watch import events, referee, registry, runaway
from fleet_watch.cli_support import (
    _build_guard_payload,
    _get_conn,
    _holder_conflict_text,
    _reject_negative_gpu,
    _render_guard,
    _resolved_session_id,
)

@click.command()
@click.option("--port", type=int, default=None, help="Port to check")
@click.option("--repo", "repo_dir", default=None, help="Repo directory to check")
@click.option("--write-scope", "write_scopes", multiple=True, help="Repo-relative or absolute path this command may edit")
@click.option("--exclusive-repo-lock", is_flag=True, help="Require exclusive repo ownership")
@click.option("--gpu", "gpu_mb", type=int, default=None, callback=_reject_negative_gpu, help="GPU MB to check")
@click.option("--session-id", default=None, help="Current session ID for owned-resource bypass")
def check(
    port: int | None,
    repo_dir: str | None,
    write_scopes: tuple[str, ...],
    exclusive_repo_lock: bool,
    gpu_mb: int | None,
    session_id: str | None,
):
    """Check if a resource is available. Exit 0=available, 1=taken."""
    if port is None and repo_dir is None and gpu_mb is None:
        click.echo("Specify --port, --repo, or --gpu", err=True)
        sys.exit(2)
    if write_scopes and repo_dir is None:
        click.echo("--write-scope requires --repo", err=True)
        sys.exit(2)

    current_session_id = _resolved_session_id(session_id)
    conn = _get_conn()
    failed = False

    if port is not None:
        decision = referee.check_port(conn, port)
        if decision.allowed:
            click.echo(f"Port {port}: available")
        else:
            # holder is None when the OS holds the port but nothing in the
            # registry claims it — which is the case this whole check exists
            # to catch, so it must not be the one that crashes.
            if decision.holder is not None:
                click.echo(
                    f"Port {port}: TAKEN by PID {decision.holder['pid']} ({decision.holder['name']})",
                    err=True,
                )
            else:
                click.echo(f"Port {port}: TAKEN — {decision.reason}", err=True)
            failed = True

    if repo_dir is not None:
        decision = referee.check_repo_with_session(
            conn,
            repo_dir,
            current_session_id=current_session_id,
            write_scopes=write_scopes,
            exclusive=exclusive_repo_lock,
        )
        if decision.allowed:
            click.echo(f"Repo {repo_dir}: {decision.reason}")
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


@click.command()
@click.option("--port", type=int, default=None, help="Port to guard")
@click.option("--repo", "repo_dir", default=None, help="Repo directory to guard")
@click.option("--write-scope", "write_scopes", multiple=True, help="Repo-relative or absolute path this command may edit")
@click.option("--exclusive-repo-lock", is_flag=True, help="Require exclusive repo ownership")
@click.option("--gpu", "gpu_mb", type=int, default=None, callback=_reject_negative_gpu, help="GPU MB to guard")
@click.option("--framework", default=None, help="Inference framework (candle, mlx, ollama, vllm)")
@click.option("--model", "model_hint", default=None, help="Model name/path for working set estimation")
@click.option("--session-id", default=None, help="Current session ID for owned-resource bypass")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def guard(
    port: int | None,
    repo_dir: str | None,
    write_scopes: tuple[str, ...],
    exclusive_repo_lock: bool,
    gpu_mb: int | None,
    framework: str | None,
    model_hint: str | None,
    session_id: str | None,
    as_json: bool,
):
    """Canonical pre-flight interface for agents and operators."""
    if write_scopes and repo_dir is None:
        raise click.UsageError("--write-scope requires --repo")
    # INV #1 (fail-closed): the guard decision path touches the DB (connect,
    # clean, build payload). Any error here — DB locked, unreachable, malformed —
    # must return {allowed: false}, never propagate an unhandled exception to the
    # caller (CLAUDE.md Local Invariant #1 + Local Fail-Closed Rule).
    try:
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
        stale_session_leases_cleaned = registry.clean_stale_session_leases(conn)
        for cleaned in stale_session_leases_cleaned:
            events.log_event(
                conn,
                "CLEAN",
                pid=cleaned["owner_pid"],
                workstream="session",
                detail={**cleaned, "source": "guard"},
            )

        payload = _build_guard_payload(
            conn,
            port=port,
            repo_dir=repo_dir,
            write_scopes=write_scopes,
            exclusive_repo_lock=exclusive_repo_lock,
            gpu_mb=gpu_mb,
            framework=framework,
            model_hint=model_hint,
            current_session_id=current_session_id,
            stale_session_leases_cleaned=stale_session_leases_cleaned,
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
    except Exception as exc:  # noqa: BLE001 — fail-closed: deny on any guard-path error
        reason = f"guard_error_fail_closed: {type(exc).__name__}: {exc}"
        if as_json:
            click.echo(json.dumps({"allowed": False, "reason": reason}, default=str))
        else:
            click.echo(f"DENY (guard fail-closed): {reason}", err=True)
        sys.exit(1)

    # Advisory: scan for active runaway processes (never crash the guard)
    try:
        runaways = runaway.scan_runaways()
    except Exception:  # noqa: BLE001 — runaway scan is advisory; never flips allow/deny
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
@click.command(hidden=True)
@click.option("--port", type=int, default=None, help="Port to check")
@click.option("--repo", "repo_dir", default=None, help="Repo directory to check")
@click.option("--gpu", "gpu_mb", type=int, default=None, callback=_reject_negative_gpu, help="GPU MB to check")
@click.pass_context
def claim(ctx, port, repo_dir, gpu_mb):
    """Alias for 'check' (deprecated)."""
    ctx.invoke(
        check,
        port=port,
        repo_dir=repo_dir,
        write_scopes=(),
        exclusive_repo_lock=False,
        gpu_mb=gpu_mb,
        session_id=None,
    )


@click.command()
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
