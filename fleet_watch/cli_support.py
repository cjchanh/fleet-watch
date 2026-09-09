"""Shared helpers for the Fleet Watch Click CLI."""

from __future__ import annotations

import json
import os
import queue
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
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
from fleet_watch.discovery import mcp_orphan_detector, ollama_runners, orphan_detector
from fleet_watch.guards import memory_pressure

# fleet_watch.boot_coverage, fleet_watch.pkill, and fleet_watch.github_sitrep
# are imported lazily inside the functions that use them (below) — each has a
# single subcommand-scoped call site and none is referenced at decorator
# (import-time) evaluation, so every other subcommand (e.g. `session list`,
# `guard`) skips their import cost. fleet_watch.census is imported eagerly
# (as census_mod) instead: _render_census's signature references it at
# definition time, so it cannot stay function-local.
#
# ollama_runners/orphan_detector/memory_pressure and mcp_orphan_detector
# stay module-level: fleet_watch.discover (kept eager here since discover_mod
# is used by 9 call sites incl. discover()/watch()) already imports all four
# unconditionally as its own dependencies, so lazily re-importing them from
# _build_guard_payload()/status() measured zero net change to session-list
# import cost — moving them would add diff surface with no verified benefit.
# discover()/watch() themselves are deliberately left untouched to avoid
# overlapping the in-flight --auto-kill fail-closed fix on fleet_watch/cli.py
# (uncommitted on main at time of writing).


# `fleet status` calls two probes `fleet guard` never touches:
# ollama_runners.discover_ollama_runners() (pgrep/ps fan-out, one subprocess
# call per discovered ollama runner, each individually bounded but with no
# cap on the total across N runners) and orphan_detector.detect_orphans()
# (an HTTP GET against a local ollama /api/ps port plus a `ps aux` call).
# Every underlying call already carries its own timeout, but nothing bounded
# the SUM across all of them, so a slow/loaded box or several stacked runners
# could push `status` well past its 2s advisory budget (observed: an 8s
# timeout in CURRENT_MACHINE_STATE while `guard`, which skips both probes,
# stayed fast). STATUS_DISCOVERY_TIMEOUT_SECONDS caps each probe's WALL-CLOCK
# contribution to `status`; on timeout the probe degrades to its safe empty
# default rather than hanging the command.
STATUS_DISCOVERY_TIMEOUT_SECONDS = 3.0


def _run_bounded(fn, *, timeout_seconds: float, default: Any):
    """Run ``fn()`` in a daemon thread bounded by ``timeout_seconds``.

    Returns ``(result, timed_out)``. On timeout, returns ``default`` and
    ``timed_out=True`` without waiting for the underlying call to finish.
    The worker thread is daemonized specifically so a still-blocked
    subprocess/socket call inside ``fn`` can never hold the CLI process open
    past its own exit -- a non-daemon thread (e.g. via
    concurrent.futures.ThreadPoolExecutor, whose atexit hook joins pending
    workers) would still make the process hang even after this function
    returns a degraded result.
    """
    result_queue: "queue.Queue[tuple[str, Any]]" = queue.Queue(maxsize=1)

    def _worker() -> None:
        try:
            result_queue.put(("ok", fn()))
        except Exception as exc:  # noqa: BLE001 -- degrade, never propagate
            result_queue.put(("error", exc))

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    try:
        kind, payload = result_queue.get(timeout=timeout_seconds)
    except queue.Empty:
        return default, True
    if kind == "error":
        return default, False
    return payload, False


def _get_conn():
    return registry.connect()


def _reject_negative_gpu(
    ctx: click.Context, param: click.Parameter, value: int | None
) -> int | None:
    if value is not None and value < 0:
        raise click.BadParameter("must not be negative")
    return value


# ── Post-commit report refresh on the lease CLAIM paths ──────────────────────
#
# WHY THIS EXISTS. `session start` used to run `reporter.write_report(conn)`
# INLINE, after the lease row was already committed, and only then echo the
# acknowledgement. Measured on a temp registry with 30 leases (quiet machine):
# preflight 0.9ms + upsert-and-commit 11.9ms + event 0.2ms = 13.8ms of decision,
# then 897.9ms of report — 98.5% of the command runs after the claim is durable
# and cannot change it. `reporter.build_state` fans out to `ps`/socket probes
# (`get_session_processes` 445ms, `detect_orphans` 98ms, `get_idle_processes`
# 93ms, `discover_ollama_runners` 64ms) with per-item timeouts but no aggregate
# cap; its own docstring records an 8s `fleet status` from the same probes.
#
# The caller is a GATE. `~/.claude/hooks/single_writer_guard.py` runs this
# command with a 5s subprocess timeout and resolves any failure to HALT, so a
# slow report made the command report FAILURE for a claim that had SUCCEEDED —
# observed live 2026-09-03 03:1xZ (`single_writer_claim_failed: timed out after
# 5 seconds` while `fleet status --json` showed the lease ACTIVE, one scope,
# started 03:10:56). An observability rewrite that cannot affect the grant must
# not be able to falsify the grant's acknowledgement.
#
# So on the claim paths the order is: decide -> commit -> log -> ACK -> report,
# and the report runs on its own connection under a wall-clock budget. If the
# budget is exceeded the lease is already durable and already acknowledged; the
# published report keeps its PRIOR generation (every report file is written via
# tempfile + os.replace, so an abandoned refresh is never a torn file) and the
# caller is told on stderr. The DB work here is ~14ms and `registry.connect`
# already sets busy_timeout=5000 — lock contention was never the cause, so it
# is not what is being fixed.
REPORT_BUDGET_ENV = "FLEET_REPORT_BUDGET_S"
DEFAULT_REPORT_BUDGET_S = 2.0
# Coalescing window. Several sessions heartbeat every turn, so without this each
# one pays for a full rebuild of a report the previous one just published — N
# sessions produce N identical scans per turn. `fleet discover` republishes the
# same report every 60s under launchd regardless, so the freshness this trades
# away is bounded by the window, not unbounded.
REPORT_MIN_INTERVAL_ENV = "FLEET_REPORT_MIN_INTERVAL_S"
DEFAULT_REPORT_MIN_INTERVAL_S = 10.0
# Coalescing keys on the newest of {completed generation, attempted refresh}.
# ATTEMPTS have to count: on a host where the rebuild reliably exceeds the
# budget, a success-only key never fires, so every claim would pay the full
# budget forever and buy nothing. `fleet discover` (launchd, 60s) runs the same
# report UNBOUNDED and is the guaranteed publisher; the claim path is
# best-effort by design and must never be the thing standing between a gate and
# its answer.
REPORT_ATTEMPT_MARKER = ".report_refresh_attempt"


def _float_env(name: str, default: float, *, allow_zero: bool = False) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if value < 0 or (value == 0 and not allow_zero):
        return default
    return value


def _report_budget_seconds() -> float:
    """Wall-clock budget for the post-ack report refresh (env-overridable)."""
    return _float_env(REPORT_BUDGET_ENV, DEFAULT_REPORT_BUDGET_S)


def _report_min_interval_seconds() -> float:
    """Skip the refresh when the published report is younger than this.

    ``0`` disables coalescing (every claim refreshes).
    """
    return _float_env(
        REPORT_MIN_INTERVAL_ENV, DEFAULT_REPORT_MIN_INTERVAL_S, allow_zero=True
    )


def _report_is_fresh() -> bool:
    """True when the report was published OR attempted inside the window.

    ``state.json`` is flipped LAST by ``reporter.write_report`` via
    ``os.replace``, so a fresh mtime there proves a completed generation, never
    a partial one. The attempt marker covers the case that success alone cannot:
    a host slow enough that the rebuild never finishes inside the budget.
    """
    window = _report_min_interval_seconds()
    if window <= 0:
        return False
    newest: float | None = None
    for name in ("state.json", REPORT_ATTEMPT_MARKER):
        try:
            mtime = (registry.FLEET_DIR / name).stat().st_mtime
        except OSError:
            continue
        newest = mtime if newest is None else max(newest, mtime)
    if newest is None:
        return False
    return 0 <= time.time() - newest < window


def _mark_report_attempt() -> None:
    """Record that a refresh was started, so peers coalesce even if it fails."""
    try:
        registry.FLEET_DIR.mkdir(parents=True, exist_ok=True)
        (registry.FLEET_DIR / REPORT_ATTEMPT_MARKER).touch()
    except OSError:
        pass


def _publish_report_after_ack(context: str) -> bool:
    """Refresh the published report after the caller's write is already durable.

    Returns True when the refresh completed within its budget, or was skipped
    because a completed generation is already younger than the coalescing
    window. A False return is NOT a failure of the operation that called it: by
    contract this runs only after the registry write committed and the
    acknowledgement was emitted.

    The refresh opens its OWN read-only-in-practice connection, so the caller
    may close its handle first, and runs on a daemon thread so an overrun is
    abandoned at process exit rather than blocking it.
    """
    if _report_is_fresh():
        return True
    _mark_report_attempt()

    budget = _report_budget_seconds()
    outcome: dict[str, Any] = {}

    def _refresh() -> None:
        conn = None
        try:
            conn = registry.connect()
            reporter.write_report(conn)
            outcome["ok"] = True
        except Exception as exc:  # noqa: BLE001 - never fatal to a committed claim
            outcome["error"] = exc
        finally:
            if conn is not None:
                try:
                    conn.close()
                except (OSError, sqlite3.Error):
                    conn = None

    worker = threading.Thread(
        target=_refresh, name=f"fleet-report-{context}", daemon=True
    )
    worker.start()
    worker.join(budget)

    if worker.is_alive():
        click.echo(
            f"WARN: fleet report refresh exceeded {budget:g}s after {context}; "
            "the lease is committed and this command's result stands. "
            "~/.fleet-watch/state.json keeps its previous generation.",
            err=True,
        )
        return False
    error = outcome.get("error")
    if error is not None:
        click.echo(
            f"WARN: fleet report refresh failed after {context}: {error}; "
            "the lease is committed and this command's result stands.",
            err=True,
        )
        return False
    return True


def _ack(message: str) -> None:
    """Emit a claim acknowledgement and flush it before any slower work runs.

    stdout is block-buffered when the caller is a pipe (every hook invocation),
    so the flush is what makes "ack before report" true for a streaming reader
    and not merely true in source order.
    """
    click.echo(message)
    try:
        sys.stdout.flush()
    except (ValueError, OSError):
        pass


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


def _documents_root() -> Path:
    return (Path.home() / "Documents").resolve()


def _is_documents_path(path: Path) -> bool:
    try:
        path.resolve().relative_to(_documents_root())
    except ValueError:
        return False
    return True


def _cooperative_alternative(holder: dict[str, Any], requested_exclusive: bool) -> str:
    """The path forward that does NOT require the holder's lease to end.

    Which one is truthful depends on WHICH rule refused. An exclusive lease is
    not arbitrated by scope at all (referee.check_repo_with_session denies on
    the mode before it looks at overlap), so offering `--write-scope` there
    would be the same species of false advice this function exists to remove.
    """
    if holder.get("repo_lock_mode") == "exclusive":
        return "no --write-scope declaration bypasses an exclusive lease"
    if requested_exclusive:
        return (
            "or drop --exclusive-repo-lock and declare --write-scope <paths>, "
            "which proceeds when scopes do not overlap"
        )
    return (
        "or narrow --write-scope <paths> so it does not overlap the holder's "
        "scopes, and proceed"
    )


def _repo_unblock_command(
    conn: sqlite3.Connection | None,
    holder: dict[str, Any] | None,
    requested_exclusive: bool = False,
) -> str | None:
    """Remedy text for a repo denial — a COMMAND only when one actually works.

    Before 2026-09-03 this always emitted `fleet session close --session-id
    <holder>`. Since `authorize_session_close` landed, that command DENIES for
    a live foreign holder unless the requester is its owner, a descendant, or
    an ancestor — so the guard was routing every blocked agent to a refusal
    and calling it the unblock. The key name is unchanged (the JSON schema is
    a cross-repo contract, see CLAUDE.md Cross-Folder Dependencies); what
    changed is that its value is now true.
    """
    if holder is None:
        return None
    session_id = holder.get("session_id")
    if holder.get("workstream") == "session" and session_id:
        close_cmd = f"fleet session close --session-id {session_id}"
        if conn is None:
            return close_cmd
        authority = registry.describe_session_close_authority(conn, session_id)
        status = authority["status"]
        if status in {"reapable", "absent"}:
            # Reaping a dead owner is not a privilege — anyone may run it, and
            # invariant 3 says a dead owner must not hold a repo to the TTL.
            return close_cmd
        alternative = _cooperative_alternative(holder, requested_exclusive)
        if status == "ttl_only":
            return (
                f"session {session_id} has no owner pid: `{close_cmd}` fails "
                f"closed for every requester and the lease clears only on "
                f"heartbeat TTL expiry ({registry.DEFAULT_STALE_SECONDS}s) — "
                f"{alternative}"
            )
        if status == "uninspectable":
            return (
                f"session {session_id} owner pid {authority['owner_pid']} "
                f"cannot be inspected: `{close_cmd}` fails closed for every "
                f"requester until it can be — {alternative}"
            )
        return (
            f"session {session_id} is live (owner pid {authority['owner_pid']}): "
            f"you from any of your own terminals, or that pid, a descendant of "
            f"it, or the terminal that spawned it, may run `{close_cmd}` "
            f"(another agent may not) — {alternative}"
        )
    if holder.get("pid") is not None:
        return f"fleet release --pid {holder['pid']}"
    return None


def _resolved_session_id(session_id: str | None) -> str | None:
    if session_id:
        return session_id
    fleet_sid = os.environ.get("FLEET_SESSION_ID")
    if fleet_sid:
        return fleet_sid
    term_sid = os.environ.get("TERM_SESSION_ID")
    if term_sid:
        return f"term-{term_sid}"
    return None


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
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return


def _notify_attention(sessions: list[syshealth.SessionProcess]) -> None:
    """Send macOS notification when detached hot sessions require attention."""
    if not sessions:
        return
    total_cpu = sum(s.cpu_pct for s in sessions)
    title = "Fleet Watch: Attention Required"
    body = f"{len(sessions)} detached hot session(s) — {total_cpu:.0f}% total CPU"
    try:
        subprocess.run(
            [
                "osascript", "-e",
                f'display notification "{body}" with title "{title}"',
            ],
            capture_output=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return


_CODEX_ORPHAN_RE = re.compile(r"codex/codex\b")


def _is_fleet_owned(conn: sqlite3.Connection, proc: runaway.RunawayProcess) -> bool:
    """Check if a process is owned by Fleet Watch.

    Auto-kill requires real ownership evidence, not regex classification.
    Two paths qualify:
    1. Registered in Fleet Watch registry (explicit registration via discover/register)
    2. Codex binary orphan — launched by our bootstrap but never registered
       (narrow exception: only the Codex native binary path, not broad patterns)
    """
    if registry.get_process(conn, proc.pid) is not None:
        return True
    if _CODEX_ORPHAN_RE.search(proc.command):
        return True
    return False


def _run_runaway_tick(
    conn: sqlite3.Connection,
    tracker: runaway.DaemonRunawayTracker,
    tracker_path: Path | None = None,
    auto_kill: bool = True,
) -> list[runaway.RunawayProcess]:
    """Run one runaway tracker tick, log events, kill Fleet-owned runaways if auto_kill.

    Auto-kill requires real ownership evidence: registry entry or Codex orphan match.
    Unowned processes (ML training, ffmpeg, external vllm) get an EXTERNAL warning only.
    """
    try:
        newly_flagged = tracker.tick()
    except Exception:  # noqa: BLE001 — tick failure must not crash discover; no guard reads this
        return []
    for proc in newly_flagged:
        fleet_owned = _is_fleet_owned(conn, proc)
        events.log_event(
            conn,
            "RUNAWAY_DETECTED",
            pid=proc.pid,
            workstream="runaway",
            detail={
                "cpu_pct": proc.cpu_pct,
                "runtime_seconds": proc.runtime_seconds,
                "command": proc.command[:200],
                "consecutive_ticks": runaway.DAEMON_CONSECUTIVE_TICKS,
                "fleet_owned": fleet_owned,
            },
        )
        if auto_kill and fleet_owned:
            success = runaway.kill_runaway(proc.pid)
            event_type = "RUNAWAY_KILL" if success else "RUNAWAY_KILL_FAILED"
            events.log_event(
                conn,
                event_type,
                pid=proc.pid,
                workstream="runaway",
                detail={
                    "cpu_pct": proc.cpu_pct,
                    "command": proc.command[:200],
                },
            )
            status = "killed" if success else "KILL FAILED"
            click.echo(
                f"RUNAWAY: PID {proc.pid} ({proc.name}) — "
                f"CPU {proc.cpu_pct:.1f}% for {runaway.DAEMON_CONSECUTIVE_TICKS} ticks — {status}"
            )
        else:
            label = "WARNING" if fleet_owned else "EXTERNAL"
            click.echo(
                f"{label}: runaway PID {proc.pid} ({proc.name}) — "
                f"CPU {proc.cpu_pct:.1f}% for {runaway.DAEMON_CONSECUTIVE_TICKS} consecutive ticks"
            )
    if tracker_path is not None:
        tracker.save(tracker_path)
    return newly_flagged


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
        timeout=10,
    )
    if result.returncode != 0:
        raise click.ClickException(result.stderr.strip() or result.stdout.strip() or "tnr status failed")
    payload = discover_mod.parse_tnr_instances_output(result.stdout, result.stderr)
    if payload is None:
        raise click.ClickException("unexpected tnr status payload")
    return payload


def _build_guard_payload(
    conn,
    port: int | None = None,
    repo_dir: str | None = None,
    write_scopes: tuple[str, ...] | list[str] | None = None,
    exclusive_repo_lock: bool = False,
    gpu_mb: int | None = None,
    framework: str | None = None,
    model_hint: str | None = None,
    current_session_id: str | None = None,
    runaway_tracker: runaway.DaemonRunawayTracker | None = None,
    stale_session_leases_cleaned: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    state = reporter.build_guard_state(conn)
    budget = state["gpu_budget"]
    normalized_write_scopes = referee.normalize_write_scopes(repo_dir, write_scopes)

    # H1: discover ollama runners for guard decisions
    runner_reports = ollama_runners.discover_ollama_runners()
    runner_entries = ollama_runners.runner_entries_for_status(runner_reports)
    actual_gpu = ollama_runners.total_actual_gpu_mb(runner_reports)

    payload: dict[str, Any] = {
        "allowed": True,
        "request": {
            "port": port,
            "repo_dir": str(Path(repo_dir).resolve()) if repo_dir else None,
            "write_scopes": normalized_write_scopes,
            "exclusive_repo_lock": exclusive_repo_lock,
            "gpu_mb": gpu_mb,
            "framework": framework,
            "model": model_hint,
        },
        "checks": {},
        "state": {
            "process_count": state["process_count"],
            "occupied_ports": sorted(state["ports_claimed"].keys()),
            "safe_ports": state.get("safe_ports", []),
            "locked_repos": sorted(state["repos_locked"].keys()),
            "gpu_budget": budget,
            "system_memory": syshealth.get_memory_state().to_dict(),
            "swap": syshealth.get_swap_state().to_dict(),
            "external_resources": state.get("external_resources", []),
            "ollama_runners": [r.to_dict() for r in runner_reports],
            "ollama_runner_entries": runner_entries,
            "actual_ollama_gpu_mb": actual_gpu,
        },
    }
    registry_warnings = registry.registry_warnings_for()
    if registry_warnings:
        payload["registry_warnings"] = registry_warnings

    # Advisory: include active runaway warnings if tracker is available
    if runaway_tracker is not None:
        warnings = runaway_tracker.get_active_warnings()
        if warnings:
            payload["runaways"] = warnings

    # H2: swap-pressure budget gate (fires before port/repo/GPU checks)
    gate_counters = counters.load_counters()
    gate_counters.increment("memory_pressure_gate")
    counters.save_counters(gate_counters)

    swap_verdict = memory_pressure.check_swap_pressure()
    swap_decision = memory_pressure.guard_decision(
        swap_verdict,
        gpu_requested=(gpu_mb is not None and gpu_mb > 0),
        audit_cycles=gate_counters.memory_pressure_gate,
    )
    if swap_verdict.warning:
        events.log_event(
            conn,
            "MEMORY_PRESSURE_RISING",
            workstream="guard",
            detail=swap_verdict.to_dict(),
        )
    payload["checks"]["swap_pressure"] = swap_decision
    if not swap_decision["allowed"]:
        payload["allowed"] = False

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
        stale_holders = [
            {
                "pid": lease.get("owner_pid"),
                "name": f"session {lease['session_id']}",
                "workstream": "session",
                "priority": 3,
                "port": None,
                "repo_dir": lease.get("repo_dir"),
                "gpu_mb": 0,
                "session_id": lease["session_id"],
                "repo_lock_mode": lease.get("repo_lock_mode", "cooperative"),
                "write_scopes": lease.get("write_scopes", []),
            }
            for lease in (stale_session_leases_cleaned or [])
            if lease.get("repo_dir") == str(Path(repo_dir).resolve())
        ]
        decision = referee.check_repo_with_session(
            conn,
            repo_dir,
            current_session_id=current_session_id,
            write_scopes=normalized_write_scopes,
            exclusive=exclusive_repo_lock,
        )
        repo_check = {
            "allowed": decision.allowed,
            "reason": decision.reason,
            "holder": referee.summarize_holder(decision.holder),
            "holders": [referee.summarize_holder(holder) for holder in decision.holders],
            "overlap_paths": decision.overlap_paths,
            "stale_holders": [
                referee.summarize_holder(holder)
                for holder in [*stale_holders, *decision.stale_holders]
            ],
            "safe_mode": decision.safe_mode,
            "evidence": decision.evidence or {
                "source": "fleet_registry_and_git_lock_probe",
                "status": "allow" if decision.allowed else "deny",
                "detail": decision.reason,
            },
        }
        unblock_command = _repo_unblock_command(
            conn, decision.holder, requested_exclusive=exclusive_repo_lock
        )
        if not decision.allowed and unblock_command:
            repo_check["unblock_command"] = unblock_command
        payload["checks"]["repo"] = repo_check
        payload["allowed"] = payload["allowed"] and decision.allowed

    if gpu_mb is not None:
        mem = syshealth.get_memory_state()
        swap = syshealth.get_swap_state()
        # ``swap_pressure`` is the authoritative launch gate. Keep the legacy
        # check shape for callers, but do not let its computed pressure proxy
        # contradict the kernel pressure level and scaled available-RAM floor.
        pressure_allowed = bool(swap_decision["allowed"])
        pressure_blockers = (
            syshealth.launch_pressure_blockers(mem, swap)
            if not pressure_allowed
            else []
        )
        pressure_check = {
            "allowed": pressure_allowed,
            "reason": swap_decision.get(
                "reason", "memory pressure within launch limits"
            ),
            "blockers": pressure_blockers,
            "memory": mem.to_dict(),
            "swap": swap.to_dict(),
        }
        payload["checks"]["memory_pressure"] = pressure_check

        # Probe once and pass the SAME measurement to both the decision and the
        # reported numbers. ``available_mb``/``suggested_max_mb`` used to be
        # computed straight from the ledger, independent of the decision — so a
        # correct DENY still handed the operator a confident ledger figure as
        # though it were device headroom. The keys are unchanged (the
        # fleet_guard_hook reads them); only their provenance is now real.
        residency = referee.probe_gpu_residency()
        decision = referee.check_gpu_budget(conn, gpu_mb, residency=residency)
        effective_allocated_mb = max(budget["allocated_mb"], residency.resident_mb)
        effective_available_mb = (
            budget["total_mb"] - budget["reserve_mb"] - effective_allocated_mb
        )
        gpu_check: dict[str, Any] = {
            "allowed": decision.allowed,
            "reason": decision.reason,
            "requested_mb": gpu_mb,
            "available_mb": max(0, effective_available_mb),
            "suggested_max_mb": max(0, effective_available_mb),
            "provenance": {
                "ledger_allocated_mb": budget["allocated_mb"],
                "telemetry_resident_mb": residency.resident_mb,
                "telemetry_status": residency.status,
                "telemetry_sources": list(residency.sources),
                "unmeasured_runtimes": list(referee.GPU_UNMEASURED_RUNTIMES),
                "detail": residency.detail,
            },
        }
        if not residency.measured:
            # An unreadable device is not 'all of it is free'. Report no
            # headroom rather than a number the measurement does not support.
            gpu_check["available_mb"] = 0
            gpu_check["suggested_max_mb"] = 0

        # Working set estimation — catches memory overcommit
        physical_ram = mem.total_mb if mem.is_available else 0
        config = discover_mod.load_config()
        reserve = gpu_estimator.resolve_effective_reserve_mb(
            physical_ram,
            config.get("gpu_reserve_mb", registry.DEFAULT_GPU_RESERVE_MB),
        )

        estimate = gpu_estimator.estimate_working_set(
            framework=framework,
            command=model_hint,
            physical_ram_mb=physical_ram,
            reserve_mb=reserve,
            config_overrides=config.get("gpu_estimator"),
            allow_model_fallback=False,
        )
        if estimate.source != "insufficient_input":
            gpu_check["working_set"] = estimate.to_dict()

        if estimate.grounded and not estimate.fits:
            gpu_check["allowed"] = False
            gpu_check["reason"] = "working_set_exceeds_physical_ram"
            gpu_check["detail"] = (
                f"working set {estimate.total_mb}MB exceeds "
                f"physical RAM ({physical_ram}MB) minus "
                f"reserve ({estimate.available_after_reserve_mb}MB available)"
            )

        payload["checks"]["gpu"] = gpu_check
        payload["allowed"] = payload["allowed"] and gpu_check["allowed"]

    evidence_sources = {
        "swap_pressure": "kernel_memory_pressure",
        "port": "fleet_registry_and_socket_table",
        "repo": "fleet_registry_and_git_lock_probe",
        "memory_pressure": "system_memory_and_swap",
        "gpu": "fleet_ledger_and_device_telemetry",
    }
    for check_name, check in payload["checks"].items():
        check.setdefault(
            "evidence",
            {
                "source": evidence_sources.get(check_name, "fleet_guard"),
                "status": "allow" if check.get("allowed") else "deny",
                "detail": check.get("reason", "no reason reported"),
            },
        )

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
            lines.append(f"Repo {repo_dir}: {repo_check['reason']}")
            if repo_check.get("safe_mode"):
                lines.append(f"Safe mode: {repo_check['safe_mode']}")
            holders = repo_check.get("holders", [])
            if holders:
                lines.append(f"Active cooperative sessions: {len(holders)}")
        else:
            lines.append(f"Repo {repo_dir}: {repo_check['reason']} by {_holder_text(repo_check['holder'])}")
            overlaps = repo_check.get("overlap_paths", [])
            if overlaps:
                lines.append("Overlaps: " + ", ".join(overlaps))
            if repo_check.get("unblock_command"):
                lines.append(f"Unblock: {repo_check['unblock_command']}")

    if "gpu" in checks:
        gpu_check = checks["gpu"]
        requested_mb = gpu_check["requested_mb"]
        ws = gpu_check.get("working_set")
        if gpu_check["allowed"]:
            lines.append(
                f"GPU {requested_mb}MB: available "
                f"({gpu_check['available_mb']}MB free)"
            )
            if ws:
                lines.append(
                    f"  Working set: {ws['total_mb']}MB "
                    f"(weights {ws['weights_mb']} + kv {ws['kv_cache_mb']} "
                    f"+ act {ws['activations_mb']}) × {ws['overhead_multiplier']}x"
                )
                lines.append(
                    f"  Physical RAM available after reserve: {ws['available_after_reserve_mb']}MB"
                )
        else:
            lines.append(f"GPU {requested_mb}MB: {gpu_check.get('detail', gpu_check['reason'])}")
            if ws:
                lines.append(
                    f"  Breakdown: weights {ws['weights_mb']}MB + "
                    f"kv_cache {ws['kv_cache_mb']}MB + "
                    f"activations {ws['activations_mb']}MB "
                    f"× {ws['overhead_multiplier']}x ({ws['framework']})"
                )
                lines.append(
                    f"  Physical RAM available after reserve: {ws['available_after_reserve_mb']}MB"
                )
                if not ws.get("grounded", True):
                    lines.append("  Note: advisory only; provide explicit framework/model for enforcement")
                if ws.get("suggestion"):
                    lines.append(f"  Suggestion: {ws['suggestion']}")

    if "swap_pressure" in checks:
        swap_check = checks["swap_pressure"]
        verdict = swap_check.get("verdict", {})
        if not swap_check["allowed"]:
            lines.append(
                f"Swap pressure: {verdict.get('swap_used_pct', 0):.0f}% used — "
                f"{swap_check.get('reason', 'blocked')}"
            )
        elif swap_check.get("warning"):
            lines.append(swap_check["warning"])
        if swap_check.get("audit_mode"):
            note = swap_check.get("audit_note")
            if note:
                lines.append(f"  [audit] {note}")

    if "memory_pressure" in checks:
        pressure_check = checks["memory_pressure"]
        if not pressure_check["allowed"]:
            for blocker in pressure_check.get("blockers", []):
                code = blocker.get("code")
                if code == "SWAP_PRESSURE_HIGH":
                    lines.append(
                        "Memory pressure: swap "
                        f"{blocker['swap_used_pct']}% used; "
                        f"requires < {blocker['required_below_pct']}%"
                    )
                elif code == "SWAP_FREE_LOW":
                    lines.append(
                        "Memory pressure: swap free "
                        f"{blocker['swap_free_mb']}MB; "
                        f"requires >= {blocker['required_min_free_mb']}MB"
                    )
                elif code == "MEMORY_PRESSURE_HIGH":
                    lines.append(
                        "Memory pressure: "
                        f"{blocker['pressure_pct']}%; "
                        f"requires < {blocker['required_below_pct']}%"
                    )

    state = payload["state"]
    lines.append(
        f"GPU budget available: {max(0, state['gpu_budget']['available_mb'])}MB "
        f"({state['gpu_budget']['allocated_mb']}MB allocated)"
    )
    if state["safe_ports"]:
        lines.append(
            "Open ports: " + ", ".join(str(port) for port in state["safe_ports"])
        )
    if state["locked_repos"]:
        lines.append("Locked repos: " + ", ".join(state["locked_repos"]))

    return lines


def _default_owner_pid() -> int:
    parent = os.getppid()
    return parent if parent > 1 else os.getpid()


def _build_reconcile_payload(conn) -> dict[str, Any]:
    processes = registry.get_process_classifications(conn)
    summary: dict[str, int] = {}
    for item in processes:
        summary[item["classification"]] = summary.get(item["classification"], 0) + 1
    return {
        "generated_utc": registry._now_iso(),
        "summary": summary,
        "processes": processes,
    }


def _terminate_orphan(pid: int, grace_seconds: float = 1.5) -> bool:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if not registry._pid_exists(pid):
            return True
        time.sleep(0.1)

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if not registry._pid_exists(pid):
            return True
        time.sleep(0.1)
    return not registry._pid_exists(pid)


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


def _mcp_reap_candidates() -> list[dict[str, Any]]:
    """NS-17 B3: dead-session MCP servers as reap candidates (source='mcp').
    Opt-in via `fleet reap --include-mcp`. Reuses the audited detector, which
    guarantees a live-session server is NEVER an orphan. Fail-soft."""
    try:
        result = mcp_orphan_detector.detect()
    except Exception:  # noqa: BLE001 — a detector error must not break reap
        return []
    return [
        {
            "pid": pid, "name": "mcp-server", "workstream": "mcp",
            "session_id": None, "evidence": ["dead-session MCP server (parent dead)"],
            "source": "mcp",
        }
        for pid in result.orphan_pids
    ]


def _mcp_surface_lines(mcp: Any) -> list[str]:
    """NS-17 B3: format the read-only MCP-orphan surfacing for `fleet discover`.
    Pure (no I/O, no kill) so it is unit-testable. Returns echo lines."""
    if not getattr(mcp, "mcp_process_count", 0):
        return []
    if getattr(mcp, "orphans_detected", False):
        lines = [
            f"MCP: {len(mcp.orphan_pids)} dead-session orphan(s) of "
            f"{mcp.mcp_process_count} server(s), ~{mcp.estimated_recovered_mb}MB recoverable"
        ]
        if mcp.suggested_kill_command:
            lines.append(f"  suggested: {mcp.suggested_kill_command}")
        return lines
    return [f"MCP: {mcp.mcp_process_count} server(s) tracked, 0 orphans."]


def _census_registry_rows() -> list[dict[str, Any]]:
    """Read the Fleet Watch registry for the census. Never creates the DB."""
    try:
        if not registry.DB_PATH.exists():
            return []
        conn = _get_conn()
    except (sqlite3.Error, OSError):
        return []
    try:
        return registry.get_all_processes(conn)
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def _executable_supports_census(executable: str) -> bool:
    """Does the `fleet` on PATH actually have this subcommand?

    The installed entry point can be a pipx venv that is a different, older
    copy of this package than the source tree emitting the plist.
    """
    try:
        proc = subprocess.run(
            [executable, "census", "--help"],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0


def _render_census(
    payload: dict[str, Any], result: census_mod.CensusResult
) -> list[str]:
    totals = payload["totals"]
    machine = payload.get("machine", {})
    lines = [
        f"Fleet Census — {payload['host']} ({machine.get('os', 'unknown')}), "
        f"{payload['generated_at']}",
        f"  {totals['items']} items: {totals['keep']} keep, "
        f"{totals['investigate']} investigate, {totals['close']} close, "
        f"{totals['remove']} remove",
        "",
    ]

    ranked = payload.get("ranked_investigate") or []
    if ranked:
        lines.append(
            f"  Top investigate (ranked by RAM / CPU / failure cost, "
            f"{len(ranked)} of {totals.get('investigate', len(ranked))}):"
        )
        for entry in ranked:
            lines.append(f"      {census_mod.format_rank_line(entry)}")
        lines.append("")
    elif totals.get("investigate", 0) == 0:
        lines.append("  Top investigate: none.")
        lines.append("")

    for domain in payload["domains"]:
        domain_totals = domain["totals"]
        lines.append(f"  [{domain['domain']}] {domain_totals['items']} items")
        lines.append(
            f"      keep {domain_totals['keep']}  investigate "
            f"{domain_totals['investigate']}  close {domain_totals['close']}  "
            f"remove {domain_totals['remove']}"
        )
        for item in domain["items"]:
            if item["verdict"] == "keep":
                continue
            lines.append(
                f"      [{item['verdict'].upper():<11}] {item['label']} "
                f"({item['status']}) — {item['reason']}"
            )
    lines.append("")

    drift = payload["drift"]
    if drift["prior_receipt"] is None:
        lines.append(f"  Drift: no valid prior receipt ({drift['prior_status']}).")
    else:
        lines.append(
            f"  Drift vs {drift['prior_receipt']}: {len(drift['new_items'])} new, "
            f"{len(drift['disappeared'])} disappeared, "
            f"{len(drift['verdict_changes'])} verdict change(s)."
        )
        for change in drift["verdict_changes"]:
            lines.append(
                f"      {change['label']}: {change['from']} -> {change['to']}"
            )
        for entry in drift["new_items"]:
            lines.append(f"      NEW {entry['label']} ({entry['verdict']})")
        for entry in drift["disappeared"]:
            lines.append(f"      GONE {entry['label']} (was {entry['verdict']})")

    advisories = [
        (domain["domain"], item)
        for domain in payload["domains"]
        for item in domain["items"]
        if item.get("close_command")
    ]
    if advisories:
        lines.append("")
        lines.append(
            f"  Advisory close commands ({len(advisories)}) — Fleet Watch never runs "
            "these; they are for the operator:"
        )
        for _domain, item in advisories:
            lines.append(f"      {item['label']}: {item['close_command']}")

    failed_probes = [p for p in payload.get("probes", []) if not p["ok"]]
    if failed_probes:
        lines.append("")
        lines.append(f"  Probes that returned nothing ({len(failed_probes)}):")
        for probe in failed_probes:
            lines.append(f"      {probe['command']} — {probe['error']}")

    lines.append("")
    if result.dated_path is None:
        lines.append("  Receipt: not written (--no-receipt)")
    else:
        lines.append(f"  Receipt: {result.dated_path}")
        lines.append(f"  Latest:  {result.latest_path}")
    return lines

