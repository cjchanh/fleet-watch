from __future__ import annotations

import json
import sqlite3
import sys
from typing import Any

import click

from fleet_watch import discover as discover_mod
from fleet_watch import events, process_policy, registry, reporter, syshealth
from fleet_watch.cli_support import (
    _candidate_from_process_row,
    _decide_orphan,
    _get_conn,
    _mcp_reap_candidates,
    _terminate_orphan,
)
from fleet_watch.discovery import mcp_orphan_detector

VERIFIED_MCP_MODE = "mcp-verified-reclaim"


def _mcp_identity_probes() -> process_policy.Probes:
    """Path 1 probes (kernel identity). Seam: tests inject fakes."""
    return process_policy.default_probes()


def _mcp_reclaim_probes(conn: sqlite3.Connection) -> process_policy.ReclaimProbes:
    """Path 2 probes (session registry + parent chain). Seam: tests inject fakes."""
    return process_policy.reclaim_probes_for_conn(conn)


def _mcp_signal_sender() -> Any:
    """Signal sender for the verified path. ``None`` = the real ``os.kill``.

    A seam so tests can prove exactly which signals were sent without ever
    signalling a real process.
    """
    return None


def _owner_claim(candidate: mcp_orphan_detector.MCPCandidate) -> process_policy.MCPOwnerClaim:
    """Translate a captured candidate into the owner claim a decision re-proves."""
    return process_policy.MCPOwnerClaim(
        session_pid=candidate.ppid,
        session_create_time=candidate.owner.session_create_time,
        session_id="",
        reparented=candidate.owner.reparented,
        alive_at_scan=candidate.owner.alive_at_scan,
        cmd=candidate.cmd,
        rss_mb=candidate.rss_mb,
        scan_create_time=candidate.create_time,
        source="mcp",
    )


def _log_reclaim_decision(
    conn: sqlite3.Connection, decision: process_policy.ProcessDecision, phase: str,
) -> dict[str, Any]:
    """Append one PROCESS_DECISION receipt to the hash-chained event log.

    Called BEFORE any signal is sent: a decision that cannot be written down is
    not acted on, so the audit trail can never lag the kill.
    """
    receipt = decision.to_dict()
    events.log_event(
        conn,
        "PROCESS_DECISION",
        pid=receipt.get("pid"),
        workstream="mcp_reclaim",
        detail={"phase": phase, "mode": VERIFIED_MCP_MODE, "receipt": receipt},
    )
    return receipt


def _mcp_verified_reclaim(*, do_kill: bool, do_kill_force: bool, as_json: bool) -> None:
    """`fleet reap --mcp --verify` — two-path verified reclaim of MCP orphans.

    Dry-run unless ``--kill``. Phase order is load-bearing: capture, decide,
    AUDIT, act, audit. Nothing is signalled before its decision receipt is in
    the chain, and a denied or dry-run plan never reaches a signal at all.
    """
    conn = _get_conn()
    payload: dict[str, Any] = {
        "mode": VERIFIED_MCP_MODE,
        "verify": True,
        "dry_run": not do_kill,
        "kill": do_kill,
        "kill_force": do_kill_force,
        "policy_version": process_policy.POLICY_VERSION,
        "schema_version": process_policy.PROCESS_DECISION_SCHEMA,
        "verifier_identity": process_policy.SECOND_PATH,
        "identity_path": process_policy.IDENTITY_PATH,
        "candidates": [],
        "decisions": [],
        "exited": [],
        "denied": [],
        "failed": [],
    }
    try:
        candidates, scan_error = mcp_orphan_detector.scan_candidates()
        if scan_error:
            # An unscannable host is UNKNOWN, never "nothing to clean".
            payload["scan_error"] = scan_error
            payload["candidate_count"] = 0
            _emit_verified(payload, as_json, [])
            sys.exit(1)

        probes = _mcp_identity_probes()
        reclaim = _mcp_reclaim_probes(conn)
        sender = _mcp_signal_sender()

        # Phase 1 — capture each candidate's identity snapshot.
        snapshots: dict[int, Any] = {}
        claims: dict[int, process_policy.MCPOwnerClaim] = {}
        for candidate in candidates:
            claim = _owner_claim(candidate)
            claims[candidate.pid] = claim
            snapshots[candidate.pid] = process_policy.capture_mcp_identity(
                candidate.pid, probes=probes, claim=claim,
            )
            payload["candidates"].append({
                **candidate.to_dict(),
                "snapshot_captured": snapshots[candidate.pid] is not None,
            })
        payload["candidate_count"] = len(candidates)

        # Phase 2 — decide (identity re-validated against the snapshot + the
        # independent second path). Pure: no signal is possible here.
        plans: dict[int, process_policy.ProcessDecision] = {}
        for candidate in candidates:
            pid = candidate.pid
            plan = process_policy.decide_mcp_reclaim(
                pid, snapshots[pid], claims[pid],
                probes=probes, reclaim=reclaim, kill=do_kill,
            )
            plans[pid] = plan
            payload["decisions"].append(plan.to_dict())

        # Phase 3 — audit every decision BEFORE any signal.
        for pid, plan in plans.items():
            _log_reclaim_decision(conn, plan, "plan")

        # Phase 4 — act on authorized plans only.
        for pid, plan in plans.items():
            final = plan
            if plan.authorized and plan.action == process_policy.RECLAIM_ACTION_SIGTERM:
                final = process_policy.execute_mcp_reclaim(
                    plan, snapshots[pid], claims[pid],
                    probes=probes, reclaim=reclaim, force=do_kill_force,
                    sender=sender,
                )
                _log_reclaim_decision(conn, final, "outcome")
            if final.outcome == process_policy.RECLAIM_OUTCOME_EXITED:
                payload["exited"].append(final.to_dict())
            elif final.outcome == process_policy.RECLAIM_OUTCOME_DENIED:
                payload["denied"].append(final.to_dict())
            elif final.outcome in {
                process_policy.RECLAIM_OUTCOME_SURVIVED,
                process_policy.RECLAIM_OUTCOME_FAILED,
            }:
                payload["failed"].append(final.to_dict())
    finally:
        conn.close()

    _emit_verified(payload, as_json, plans.values())
    if do_kill and (payload["failed"] or payload["denied"]):
        sys.exit(1)


def _emit_verified(
    payload: dict[str, Any],
    as_json: bool,
    plans: Any,
) -> None:
    """Render the verified reclaim report. JSON is the contract; text is a summary."""
    if as_json:
        click.echo(json.dumps(payload, indent=2, default=str))
        return
    error = payload.get("scan_error")
    if error:
        click.echo(f"MCP scan UNKNOWN: {error}", err=True)
        return
    click.echo(
        f"MCP verified reclaim — {payload['candidate_count']} dead-session candidate(s), "
        f"{len(payload['exited'])} exited, {len(payload['denied'])} denied, "
        f"{len(payload['failed'])} failed ({'DRY RUN' if payload['dry_run'] else 'KILL'})"
    )
    for plan in plans:
        verdict = plan.reason
        if plan.action != process_policy.RECLAIM_ACTION_NONE:
            verdict = f"{plan.action} → {plan.outcome} ({plan.reason})"
        click.echo(f"  PID {plan.pid}: {verdict}")
    if payload["dry_run"]:
        click.echo("Dry run: no signal sent. Add --kill to act on verified candidates.")


@click.command()
@click.option("--confirm", is_flag=True, help="Kill and release orphan-confirmed processes")
@click.option("--include-mcp", is_flag=True, default=False,
              help="Also reap dead-session MCP servers (opt-in; kill requires --confirm)")
@click.option("--mcp", "mcp_surface", is_flag=True, default=False,
              help="Use the verified MCP reclaim path (requires --verify)")
@click.option("--verify", is_flag=True, default=False,
              help="Run the two-path verified decision for every MCP candidate (dry-run)")
@click.option("--kill", "do_kill", is_flag=True, default=False,
              help="Act: SIGTERM verified MCP candidates (requires --mcp --verify)")
@click.option("--kill-force", "do_kill_force", is_flag=True, default=False,
              help="Allow SIGKILL after a bounded graceful attempt (requires --kill)")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def reap(confirm: bool, include_mcp: bool, as_json: bool, mcp_surface: bool,
         verify: bool, do_kill: bool, do_kill_force: bool):
    """Kill only orphan-confirmed processes. Dry-run by default.

    With --include-mcp, also reaps dead-session MCP servers (NS-17 B3). The
    live-session-never-reaped invariant is guaranteed by the detector.

    `fleet reap --mcp --verify` is the GOVERNED path for MCP dead-session
    orphans: every candidate must pass an identity re-check (pid + kernel
    create-time + executable + uid) AND an independent second path (session
    registry + parent chain) before a signal is even permitted. It reports
    receipts and sends nothing; `--kill` acts on verified candidates only, one
    bounded SIGTERM each, and `--kill-force` is required before any SIGKILL.
    """
    if verify or do_kill or do_kill_force or mcp_surface:
        if not mcp_surface:
            raise click.UsageError("--mcp --verify is the MCP verified path; pass --mcp")
        if not verify:
            raise click.UsageError(
                "--mcp requires --verify (the verified path never acts unverified)"
            )
        if not do_kill and do_kill_force:
            raise click.UsageError("--kill-force requires --kill")
        if (do_kill or do_kill_force) and confirm:
            raise click.UsageError(
                "--confirm is the legacy reap path; the verified path uses --kill"
            )
        _mcp_verified_reclaim(do_kill=do_kill, do_kill_force=do_kill_force, as_json=as_json)
        return
    conn = _get_conn()
    candidates = registry.get_reapable_processes(conn)
    if include_mcp:
        candidates = candidates + _mcp_reap_candidates()
    released: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    decision_receipts: list[dict[str, Any]] = []

    if confirm:
        for item in candidates:
            if item.get("source") == "mcp":
                # MCP candidates remain advisory unless a disposable registry
                # row and full stdio/session evidence are supplied.
                mcp_candidate = {
                    "classification": "orphan_confirmed",
                    "owner_dead": True,
                    "lease_active": False,
                    "parent_alive": False,
                    "session_alive": False,
                    "stdio_peer_alive": False,
                    "active_work": False,
                    "inspection_complete": False,
                }
                decision_receipts.append(
                    _decide_orphan(
                        item["pid"],
                        candidate=mcp_candidate,
                        operator_confirmed=True,
                    ).receipt()
                )
                terminated = _terminate_orphan(
                    item["pid"],
                    candidate={
                        "classification": "orphan_confirmed",
                        "owner_dead": True,
                        "lease_active": False,
                        "parent_alive": False,
                        "session_alive": False,
                        "stdio_peer_alive": False,
                        "active_work": False,
                        "inspection_complete": False,
                    },
                    operator_confirmed=True,
                )
                if not terminated:
                    failed.append({
                        "pid": item["pid"], "name": item["name"],
                        "reason": "MCP candidate lacks disposable identity/stdio proof",
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
            row = registry.get_process(conn, item["pid"])
            lease = registry.get_session_lease(conn, item["session_id"]) if row else None
            candidate = (
                _candidate_from_process_row({**row, "session_lease": lease})
                if row
                else {"classification": None, "inspection_complete": False}
            )
            registration = registry.get_disposable_registration(conn, item["pid"])
            decision_receipts.append(
                _decide_orphan(
                    item["pid"],
                    candidate=candidate,
                    disposable_registration=registration,
                    operator_confirmed=True,
                ).receipt()
            )
            terminated = _terminate_orphan(
                item["pid"],
                candidate=candidate,
                disposable_registration=registration,
                operator_confirmed=True,
            )
            if not terminated:
                failed.append({
                    "pid": item["pid"],
                    "name": item["name"],
                    "reason": "policy denied or termination unverified",
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
        "decision_receipts": decision_receipts,
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
                    ok = _terminate_orphan(
                        pid,
                        candidate={
                            "classification": "orphan_confirmed",
                            "owner_dead": True,
                            "lease_active": False,
                            "parent_alive": False,
                            "session_alive": False,
                            "stdio_peer_alive": False,
                            "active_work": False,
                            "inspection_complete": False,
                        },
                        operator_confirmed=True,
                    )
                    member_results.append({"pid": pid, "terminated": ok})
                    if not ok:
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
