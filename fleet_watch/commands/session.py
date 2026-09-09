from __future__ import annotations

import json
import os
import sqlite3
import sys
import time

import click

from fleet_watch import claude_lease_twin
from fleet_watch import events, referee, registry
from fleet_watch.cli_support import (
    _ack,
    _default_owner_pid,
    _get_conn,
    _publish_report_after_ack,
)

@click.group()
def session():
    """Manage explicit session leases for long-lived agent ownership."""
    pass


@session.command("start")
@click.option("--session-id", required=True, help="Session identifier")
@click.option("--owner-pid", type=int, default=None, help="Owning shell/launcher PID")
@click.option("--repo", "repo_dir", default=None, help="Repo directory associated with the session")
@click.option("--write-scope", "write_scopes", multiple=True, help="Repo-relative or absolute path this session may edit")
@click.option("--exclusive-repo-lock", is_flag=True, help="Make this session an exclusive repo holder")
def session_start(
    session_id: str,
    owner_pid: int | None,
    repo_dir: str | None,
    write_scopes: tuple[str, ...],
    exclusive_repo_lock: bool,
):
    """Open or refresh a session lease.

    ACK CONTRACT (read this before treating a failure as a denial). The lease is
    durable the moment ``upsert_session_lease`` commits; the acknowledgement on
    stdout is emitted immediately after, BEFORE the observability report is
    refreshed (see ``_publish_report_after_ack``). A caller that nonetheless
    times out waiting for this process MUST NOT conclude the claim failed — a
    timeout is evidence about the courier, not about the registry. Read the
    registry back instead::

        fleet session list --json

    and treat the claim as SUCCEEDED iff ``session_leases`` contains an entry
    with this ``--session-id`` where ``status == "ACTIVE"``, ``shutdown_at`` is
    null, ``repo_dir`` equals the resolved ``--repo``, ``repo_lock_mode``
    matches the mode requested, ``write_scopes`` matches the requested scopes
    resolved to absolute paths, ``owner_alive`` is not False, and
    ``last_heartbeat_at`` is a few seconds old (proving THIS invocation landed
    rather than a lease from an earlier turn). Anything short of a full match
    stays a failure. ``fleet status --json`` carries the same rows but also runs
    the unbounded discovery fan-out that causes these timeouts, so
    ``session list --json`` is the read-back this contract names. A non-zero
    exit that printed ``DENY:`` is a real refusal and is never read back.

    IDEMPOTENCY on re-invocation with the same ``--session-id``: the row is
    keyed by session id and written ``ON CONFLICT DO UPDATE``, so a retry
    refreshes exactly one lease — no duplicate, no ownership transfer,
    ``started_at`` preserved, ``shutdown_at`` cleared, heartbeat refreshed,
    scopes/mode replaced with the ones passed (identical on a retry), and
    preflight re-run so a retry still cannot take a scope a peer claimed in the
    interim. The one value a retry DOES change is ``fencing_epoch``, which
    ``registry.upsert_session_lease`` advances on every grant. Its only reader
    is ``registry.fencing_token_valid`` — advisory, with no production call
    site — so a retry is safe today; a future epoch consumer must re-read the
    epoch after a retry instead of caching one from before it.
    """
    if write_scopes and repo_dir is None:
        raise click.UsageError("--write-scope requires --repo")
    resolved_owner_pid = owner_pid or _default_owner_pid()
    owner_metadata = registry.collect_owner_metadata(resolved_owner_pid)
    conn = _get_conn()
    failures = referee.preflight_register(
        conn,
        repo_dir=repo_dir,
        current_session_id=session_id,
        write_scopes=write_scopes,
        exclusive_repo_lock=exclusive_repo_lock,
    )
    if failures:
        for failure in failures:
            click.echo(f"DENY: {failure.reason}", err=True)
        conn.close()
        sys.exit(1)

    if exclusive_repo_lock:
        grant = registry.grant_exclusive_lease(
            conn,
            session_id,
            owner_metadata=owner_metadata,
            repo_dir=repo_dir,
            write_scopes=write_scopes,
        )
        if not grant["allowed"]:
            click.echo(f"DENY: {grant['reason']}", err=True)
            conn.close()
            sys.exit(1)
    else:
        registry.upsert_session_lease(
            conn,
            session_id,
            owner_pid=resolved_owner_pid,
            repo_dir=repo_dir,
            repo_lock_mode="cooperative",
            write_scopes=write_scopes,
            owner_metadata=owner_metadata,
        )
    events.log_event(
        conn,
        "SESSION_START",
        pid=resolved_owner_pid,
        detail={
            "session_id": session_id,
            "repo_dir": repo_dir,
            "repo_lock_mode": "exclusive" if exclusive_repo_lock else "cooperative",
            "write_scopes": list(write_scopes),
        },
    )
    # The claim is durable here. Acknowledge it, release the handle, and only
    # then refresh the report — under a budget, because that refresh is
    # observability and this command's answer is authorization.
    _ack(f"Session {session_id} active (owner PID {resolved_owner_pid})")
    conn.close()
    _publish_report_after_ack("session start")


@session.command("heartbeat")
@click.option("--session-id", required=True, help="Session identifier")
@click.option("--owner-pid", type=int, default=None, help="Owning shell/launcher PID")
@click.option("--repo", "repo_dir", default=None, help="Repo directory associated with the session")
@click.option("--write-scope", "write_scopes", multiple=True, help="Repo-relative or absolute path this session may edit")
@click.option("--exclusive-repo-lock", is_flag=True, help="Make this session an exclusive repo holder")
def session_heartbeat(
    session_id: str,
    owner_pid: int | None,
    repo_dir: str | None,
    write_scopes: tuple[str, ...],
    exclusive_repo_lock: bool,
):
    """Refresh a session lease heartbeat."""
    if write_scopes and repo_dir is None:
        raise click.UsageError("--write-scope requires --repo")
    conn = _get_conn()
    resolved_owner_pid = owner_pid or _default_owner_pid()
    if exclusive_repo_lock:
        owner_metadata = registry.collect_owner_metadata(resolved_owner_pid)
        grant = registry.grant_exclusive_lease(
            conn,
            session_id,
            owner_metadata=owner_metadata,
            repo_dir=repo_dir,
            write_scopes=write_scopes if write_scopes else None,
        )
        if not grant["allowed"]:
            click.echo(f"DENY: {grant['reason']}", err=True)
            conn.close()
            sys.exit(1)
        ok = True
    else:
        ok = registry.heartbeat_session_lease(
            conn,
            session_id,
            owner_pid=resolved_owner_pid,
            repo_dir=repo_dir,
            repo_lock_mode=None,
            write_scopes=write_scopes if write_scopes else None,
        )
    if not ok:
        click.echo(f"Session {session_id} not found", err=True)
        conn.close()
        sys.exit(2)
    events.log_event(
        conn,
        "SESSION_HEARTBEAT",
        pid=resolved_owner_pid,
        detail={
            "session_id": session_id,
            "repo_dir": repo_dir,
            "repo_lock_mode": "exclusive" if exclusive_repo_lock else None,
            "write_scopes": list(write_scopes),
        },
    )
    # Same ordering rule as `session start`: heartbeats run every turn from
    # several sessions at once, so this is the call site that GENERATES the
    # `ps`-fan-out contention that made the claim path slow.
    _ack(f"Session heartbeat updated for {session_id}")
    conn.close()
    _publish_report_after_ack("session heartbeat")


@session.command("ensure")
@click.option("--session-id", required=True, help="Session identifier")
@click.option("--owner-pid", type=int, default=None, help="Owning shell/launcher PID")
@click.option("--repo", "repo_dir", default=None, help="Repo directory associated with the session")
@click.option("--write-scope", "write_scopes", multiple=True, help="Repo-relative or absolute path this session may edit")
@click.option("--exclusive-repo-lock", is_flag=True, help="Make this session an exclusive repo holder")
@click.option("--retries", type=int, default=3, help="Max retry attempts on transient failure")
@click.option("--retry-delay", type=float, default=2.0, help="Seconds between retries")
def session_ensure(
    session_id: str,
    owner_pid: int | None,
    repo_dir: str | None,
    write_scopes: tuple[str, ...],
    exclusive_repo_lock: bool,
    retries: int,
    retry_delay: float,
):
    """Open or refresh a session lease with automatic retry on transient failure.

    Fail-open: on final failure, exits 0 with a stderr warning so the
    calling process (e.g. Codex bootstrap) is never blocked.
    """
    if write_scopes and repo_dir is None:
        raise click.UsageError("--write-scope requires --repo")
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
                write_scopes=write_scopes,
                exclusive_repo_lock=exclusive_repo_lock,
            )
            if failures:
                for failure in failures:
                    click.echo(f"DENY: {failure.reason}", err=True)
                conn.close()
                sys.exit(1)

            if exclusive_repo_lock:
                owner_metadata = registry.collect_owner_metadata(resolved_owner_pid)
                grant = registry.grant_exclusive_lease(
                    conn,
                    session_id,
                    owner_metadata=owner_metadata,
                    repo_dir=repo_dir,
                    write_scopes=write_scopes if write_scopes else None,
                )
                if not grant["allowed"]:
                    if str(grant.get("reason", "")).startswith("registry locked"):
                        raise sqlite3.OperationalError(grant["reason"])
                    click.echo(f"DENY: {grant['reason']}", err=True)
                    conn.close()
                    sys.exit(1)
            else:
                registry.upsert_session_lease(
                    conn,
                    session_id,
                    owner_pid=resolved_owner_pid,
                    repo_dir=repo_dir,
                    repo_lock_mode=None,
                    write_scopes=write_scopes if write_scopes else None,
                )
            events.log_event(
                conn,
                "SESSION_START",
                pid=resolved_owner_pid,
                detail={
                    "session_id": session_id,
                    "repo_dir": repo_dir,
                    "source": "ensure",
                    "repo_lock_mode": "exclusive" if exclusive_repo_lock else "cooperative",
                    "write_scopes": list(write_scopes),
                },
            )
            # Ack before the report, same rule as `session start`. It also
            # removes a real retry hazard: the report used to run inside this
            # try, so an OSError from writing STATE_REPORT.md re-entered the
            # retry loop and re-granted a lease that had already committed.
            # `_publish_report_after_ack` never raises, so it cannot.
            _ack(f"Session {session_id} active (owner PID {resolved_owner_pid})")
            conn.close()
            _publish_report_after_ack("session ensure")
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
    """Close a session lease without touching attached processes.

    Authorized to the lease's own lineage only — the owner PID, a descendant of
    it, or an ancestor of it (the shell that spawned the session) — plus anyone
    at all once the owner is provably dead. A session id is a public locator, so
    holding it is not authority. There is no --force: an override on a
    revocation path is a fail-open path.

    Two requester identities are tried because ``fleet`` is itself a child of
    the shell that invoked it: ``os.getppid()`` is the true requester and is the
    only one that can prove the ANCESTOR relation, while ``os.getpid()`` covers
    the descendant walk from this process.
    """
    conn = _get_conn()
    lease = registry.get_session_lease(conn, session_id)
    if lease is None:
        click.echo(f"Session {session_id} not found", err=True)
        conn.close()
        sys.exit(2)

    allowed, reason = registry.authorize_session_close(conn, session_id, os.getppid())
    if not allowed:
        allowed, self_reason = registry.authorize_session_close(
            conn, session_id, os.getpid()
        )
        if allowed:
            reason = self_reason
    if not allowed:
        click.echo(f"DENY: {reason}", err=True)
        conn.close()
        sys.exit(3)

    ok = registry.close_session_lease(conn, session_id)
    if not ok:
        click.echo(f"Session {session_id} not found", err=True)
        conn.close()
        sys.exit(2)

    twin = claude_lease_twin.clear_twin_lease(
        session_id,
        lease.get("repo_dir"),
        lease.get("owner_pid"),
    )
    events.log_event(
        conn,
        "SESSION_CLOSE",
        detail={
            "session_id": session_id,
            "requester_pid": os.getppid(),
            "authorization": reason,
            "twin_lease": twin,
        },
    )
    # Release is durable here; same ordering rule as the claim paths. A close
    # that appears to fail is as damaging as a claim that appears to fail —
    # the caller retries a revocation it already completed.
    _ack(f"Session {session_id} closed")
    if twin["cleared"]:
        click.echo(f"Cleared Claude twin lease {twin['path']}")
    conn.close()
    _publish_report_after_ack("session close")


@session.command("list")
@click.option("--all", "show_all", is_flag=True, help="Include closed leases (default: active only)")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def session_list(show_all: bool, as_json: bool):
    """List session leases (active only by default).

    Read-only: enumerates the explicit session leases other commands open via
    ``session start``/``ensure``. This is the missing read counterpart to
    start/heartbeat/close — leases could be opened and closed but never listed.
    """
    conn = _get_conn()
    try:
        leases = (
            registry.list_session_leases(conn)
            if show_all
            else registry.list_active_session_leases(conn)
        )
        # Path C (HONESTY): cross-check PID liveness so a dead-owner lease is
        # never reported as a plain ACTIVE one. ``owner_alive`` is create-time
        # aware (defeats PID reuse); ``None`` means the lease has no owner PID.
        for lease in leases:
            lease["owner_alive"] = (
                registry._lease_owner_alive(lease)
                if lease.get("owner_pid") is not None
                else None
            )
    finally:
        conn.close()

    if as_json:
        click.echo(json.dumps({"session_leases": leases, "count": len(leases)}, indent=2, default=str))
        return

    if not leases:
        click.echo("No session leases." if show_all else "No active session leases.")
        return

    click.echo(
        f"{'SESSION_ID':<40} {'PID':>7} {'OWNER':<6} {'STATUS':<8} "
        f"{'REPO':<28} LAST_HEARTBEAT"
    )
    for lease in leases:
        owner_alive = lease.get("owner_alive")
        owner_str = (
            "-" if owner_alive is None else ("alive" if owner_alive else "DEAD")
        )
        click.echo(
            f"{str(lease.get('session_id', '?')):<40} "
            f"{str(lease.get('owner_pid') or '-'):>7} "
            f"{owner_str:<6} "
            f"{str(lease.get('status', '?')):<8} "
            f"{str(lease.get('repo_dir') or '-'):<28} "
            f"{lease.get('last_heartbeat_at') or '-'}"
        )


@session.command("check")
@click.option("--repo", "repo_dir", required=True, help="Repo you are about to write")
@click.option("--me", "my_session_id", default=None, help="Your own session id (excluded from conflicts)")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def session_check(repo_dir: str, my_session_id: str | None, as_json: bool):
    """Single-writer preflight: is another LIVE session already on this repo?

    Read-only. A lease counts only if its owner PID is alive AND its heartbeat
    is fresh; a null ``repo_dir`` is resolved from the owner PID's cwd, so
    conflicts surface even for sessions that never bound their repo.

    Exit: 0 ALLOW (safe to write) · 3 CONFLICT (another live session holds it) ·
    4 UNKNOWN (liveness unresolvable; fail-closed).
    """
    from fleet_watch import session_coupling as sc

    leases = sc.load_active_leases()
    verdict = sc.single_writer_check(
        repo_dir, my_session_id, leases, age_of=sc.default_age_of
    )
    if as_json:
        click.echo(json.dumps(
            {"decision": verdict.decision, "repo": verdict.repo,
             "reason": verdict.reason, "conflicts": verdict.conflicts},
            indent=2, default=str))
    else:
        click.echo(f"{verdict.decision}: {verdict.reason}")
        for c in verdict.conflicts:
            click.echo(
                f"  conflict: session {c.get('session_id')} "
                f"PID {c.get('owner_pid')} ({c.get('repo_lock_mode')}) "
                f"hb {c.get('last_heartbeat_at')}")
    sys.exit(verdict.exit_code)
