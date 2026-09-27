"""`fleet reap --mcp --verify` — verified reclaim of MCP dead-session orphans.

THE PLANTED BADS (marked ``PLANTED BAD``): a live owning session, a recycled
target PID, a live session lease, unreadable ancestry, an unprovable owner, and a
re-parented server that grew a live parent. Each one MUST be rejected and MUST
receive no signal at all. If any of them is ever signalled, this batch is void.

Determinism: every kernel probe, clock, sleep, signal sender, lease table and
registry connection is injected. No test creates, observes, or signals a real
process. The only real subprocess anywhere is the fake ``ps`` stdout parsed in
:func:`test_scan_parses_bulk_ps_rows_with_kernel_create_time`, and that is a
string, not a process.

Two paths are under test, and both must agree before a signal is even permitted:
  * path 1 (identity): pid + kernel create-time + executable + uid, captured at
    discovery and re-observed at decision time;
  * path 2 (second path): session registry + parent chain, naming itself in the
    receipt's ``verifier_identity``.
"""

from __future__ import annotations

import importlib
import json
import signal as signal_mod
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from click.testing import CliRunner

from fleet_watch import cli, process_policy as pp
from fleet_watch.discovery import mcp_orphan_detector as M

reap_mod = importlib.import_module("fleet_watch.commands.reap")

TARGET = 4242
OWNER = 777
TARGET_CT = "CT-1"
OWNER_CT = "OCT-1"
START = 1_700_000_000.0


# ── deterministic fakes ──────────────────────────────────────────────────────

class FakeClock:
    def __init__(self, start: float = START) -> None:
        self.t = start
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


class FakeWorld:
    """One fake kernel + registry. Mutating it between calls is how the tests
    model the world changing under a decision (PID reuse, a new lease, a
    re-parented chain) — that is what proves each path re-probes instead of
    trusting the earlier verdict.
    """

    def __init__(self, *, start: float = START) -> None:
        self.clock = FakeClock(start)
        self.pids: dict[int, dict[str, Any]] = {}
        self.leases: list[dict[str, Any]] = []
        self.lease_alive: dict[str, bool] = {}
        self.lease_exc: BaseException | None = None
        self.lease_reads = 0
        self.caller_uid = 501
        self.self_pid = 1001
        self.parent_pid = 555
        self.raise_on: dict[str, BaseException] = {}
        self.signals: list[tuple[int, int]] = []
        self.observed: list[int] = []

    # -- world construction ---------------------------------------------------

    def add(
        self,
        pid: int,
        *,
        create_time: str,
        exe: str = "/usr/bin/python3",
        uid: int = 501,
        ppid: int = 0,
        exists: bool = True,
    ) -> "FakeWorld":
        self.pids[pid] = {
            "create_time": create_time, "exe": exe, "uid": uid,
            "ppid": ppid, "exists": exists,
        }
        return self

    def orphan(self, *, owner_pid: int = OWNER, owner_exists: bool = False) -> "FakeWorld":
        """A dead-session MCP server whose parent chain is already detached."""
        return self.add(TARGET, create_time=TARGET_CT, ppid=owner_pid).add(
            owner_pid, create_time=OWNER_CT, ppid=1, exists=owner_exists,
        )

    # -- injected probes ------------------------------------------------------

    def _guard(self, name: str) -> None:
        exc = self.raise_on.get(name)
        if exc is not None:
            raise exc

    def probes(self) -> pp.Probes:
        w = self

        def exists(pid: int) -> bool:
            w._guard("exists")
            return bool(w.pids.get(pid, {}).get("exists"))

        def create_time(pid: int) -> str | None:
            w._guard("create_time")
            row = w.pids.get(pid)
            if row is None or not row["exists"]:
                return None
            return row["create_time"]

        def exe(pid: int) -> str | None:
            w._guard("exe")
            row = w.pids.get(pid)
            return row["exe"] if row and row["exists"] else None

        def uid(pid: int) -> int | None:
            w._guard("uid")
            row = w.pids.get(pid)
            return row["uid"] if row and row["exists"] else None

        return pp.Probes(
            now=self.clock.now,
            sleep=self.clock.sleep,
            exists=exists,
            create_time=create_time,
            exe=exe,
            uid=uid,
            self_pid=lambda: self.self_pid,
            parent_pid=lambda: self.parent_pid,
            caller_uid=lambda: self.caller_uid,
        )

    def reclaim(self) -> pp.ReclaimProbes:
        w = self

        def ppid(pid: int) -> int | None:
            w._guard("ppid")
            row = w.pids.get(pid)
            if row is None or not row["exists"]:
                return None
            return row["ppid"]

        def active_leases() -> list[dict[str, Any]]:
            w.lease_reads += 1
            w._guard("active_leases")
            return list(w.leases)

        def lease_owner_alive(lease: dict[str, Any]) -> bool:
            w._guard("lease_owner_alive")
            return bool(w.lease_alive.get(str(lease.get("session_id"))))

        return pp.ReclaimProbes(
            ppid=ppid, exists=lambda pid: w.pids.get(pid, {}).get("exists", False),
            create_time=lambda pid: (w.pids.get(pid) or {}).get("create_time"),
            active_leases=active_leases, lease_owner_alive=lease_owner_alive,
        )

    def sender(self):
        w = self

        def send(pid: int, sig: int) -> None:
            w.signals.append((pid, sig))

        return send

    def claim(self, **over: Any) -> pp.MCPOwnerClaim:
        base = {
            "session_pid": OWNER,
            "session_create_time": OWNER_CT,
            "reparented": False,
            "alive_at_scan": False,
            "cmd": "python3 /cds/mcp_compile_server.py",
            "rss_mb": 70,
            "scan_create_time": TARGET_CT,
        }
        base.update(over)
        return pp.MCPOwnerClaim(**base)


def _never_signalled(world: FakeWorld) -> None:
    assert world.signals == [], f"PLANTED BAD was signalled: {world.signals}"


def _setup_dead_session() -> tuple[FakeWorld, pp.RecordedIdentity, pp.MCPOwnerClaim]:
    world = FakeWorld().orphan()
    claim = world.claim()
    recorded = pp.capture_mcp_identity(TARGET, probes=world.probes(), claim=claim)
    assert recorded is not None
    return world, recorded, claim


# ── candidate capture (the detector's evidence half) ─────────────────────────

def test_scan_captures_owner_identity_and_own_create_time():
    rows = [{"pid": TARGET, "ppid": OWNER, "rss_mb": 70,
             "cmd": "python3 /cds/mcp_compile_server.py", "create_time": TARGET_CT}]
    seen: list[int] = []

    def exists(pid: int) -> bool:
        seen.append(pid)
        return False

    def owner_ct(pid: int) -> str | None:
        return OWNER_CT if pid == OWNER else None

    candidates, error = M.scan_candidates(rows=rows, exists=exists, create_time=owner_ct)
    assert error is None
    assert len(candidates) == 1
    cand = candidates[0]
    assert cand.pid == TARGET and cand.ppid == OWNER
    assert cand.create_time == TARGET_CT
    assert cand.owner.session_create_time == OWNER_CT
    assert cand.owner.alive_at_scan is False
    assert cand.owner.reparented is False
    assert OWNER in seen  # the parent was probed, not assumed
    assert "mcp" in cand.to_dict()["source"]


def test_scan_never_candidates_a_live_session_server():
    rows = [{"pid": TARGET, "ppid": OWNER, "rss_mb": 70, "cmd": "mcp_compile_server.py",
             "create_time": TARGET_CT}]
    candidates, error = M.scan_candidates(
        rows=rows, exists=lambda pid: True, create_time=lambda pid: OWNER_CT)
    assert error is None
    assert candidates == []  # the load-bearing invariant, inherited not re-implemented


def test_scan_records_a_reparented_chain_without_an_owner_pid():
    rows = [{"pid": TARGET, "ppid": 1, "rss_mb": 70, "cmd": "mcp_sitrep_server.py",
             "create_time": TARGET_CT}]
    candidates, _error = M.scan_candidates(
        rows=rows, exists=lambda pid: True, create_time=lambda pid: OWNER_CT)
    owner = candidates[0].owner
    assert owner.reparented is True
    assert owner.session_pid == 1
    assert owner.session_create_time is None
    assert owner.alive_at_scan is None


def test_scan_marks_missing_create_time_as_unknown_not_a_default():
    rows = [{"pid": TARGET, "ppid": 1, "rss_mb": 70, "cmd": "mcp_sitrep_server.py"}]
    candidates, _error = M.scan_candidates(rows=rows, exists=lambda pid: False)
    assert candidates[0].create_time is None


def test_scan_proof_survives_an_unreadable_owner_probe():
    def boom(pid: int) -> str | None:
        raise OSError("ps unavailable")

    rows = [{"pid": TARGET, "ppid": OWNER, "cmd": "mcp_compile_server.py", "rss_mb": 1}]
    candidates, _error = M.scan_candidates(
        rows=rows, exists=lambda pid: False, create_time=boom)
    assert candidates[0].owner.session_create_time is None
    assert candidates[0].owner.alive_at_scan is None


def test_scan_error_yields_no_candidates_and_a_reason(monkeypatch):
    class _Result:
        returncode = 1
        stdout = ""

    monkeypatch.setattr(M.subprocess, "run", lambda *a, **k: _Result())
    candidates, error = M.scan_candidates()
    assert candidates == []
    assert error == "ps_nonzero_exit"  # an unscannable host is UNKNOWN, not clean


def test_scan_mcp_processes_parses_lstart_with_a_fixed_tz(monkeypatch):
    """The bulk scan must render lstart under the same fixed environment as
    registry._pid_create_time, or the two can never be compared for equality."""
    captured: dict[str, Any] = {}

    class _Result:
        returncode = 0
        stdout = (
            "  PID  PPID   RSS LSTART            COMMAND\n"
            f"{TARGET}  {OWNER} 71680 Sat Sep 26 16:14:03 2026 python3 /cds/mcp_compile_server.py\n"
            f"{TARGET + 1}  {OWNER} 20480 Sat Sep 26 16:14:04 2026 /usr/bin/less\n"
        )

    def fake_run(cmd, **kw):
        captured["cmd"] = cmd
        captured["env"] = kw.get("env") or {}
        return _Result()

    monkeypatch.setattr(M.subprocess, "run", fake_run)
    rows, error = M.scan_mcp_processes()
    assert error is None
    assert captured["env"].get("TZ") == "UTC"
    assert captured["env"].get("LC_ALL") == "C"
    assert [r["pid"] for r in rows] == [TARGET]  # `less` is not an MCP server
    assert rows[0]["create_time"] == "Sat Sep 26 16:14:03 2026"
    assert rows[0]["rss_mb"] == 70


def test_scan_mcp_processes_reports_a_failed_probe_as_unknown(monkeypatch):
    class _Result:
        returncode = 1
        stdout = ""

    monkeypatch.setattr(M.subprocess, "run", lambda *a, **k: _Result())
    assert M.scan_mcp_processes() == ([], "ps_nonzero_exit")

    def boom(*a, **k):
        raise subprocess.SubprocessError("ps died")

    monkeypatch.setattr(M.subprocess, "run", boom)
    rows, error = M.scan_mcp_processes()
    assert rows == [] and error == "ps_SubprocessError"


# ── second path: the owning session is provably gone ─────────────────────────

def test_owner_death_proven_by_absence():
    world = FakeWorld().orphan(owner_exists=False)
    ok, reason, ev = pp.verify_owner_dead(TARGET, world.claim(), world.reclaim())
    assert ok is True
    assert reason == "owner_pid_absent"
    assert ev["owner_death_proof"] == "owner_pid_absent"


def test_owner_death_proven_by_pid_recycling():
    world = FakeWorld().orphan(owner_exists=True)
    world.pids[OWNER]["create_time"] = "OCT-2"  # the OS recycled the integer
    ok, reason, ev = pp.verify_owner_dead(TARGET, world.claim(), world.reclaim())
    assert ok is True
    assert reason == "owner_pid_recycled"


def test_detached_chain_counts_as_owner_death_only_while_detached():
    world = FakeWorld().add(TARGET, create_time=TARGET_CT, ppid=1)
    ok, reason, _ev = pp.verify_owner_dead(
        TARGET, world.claim(session_pid=1, session_create_time=None, reparented=True),
        world.reclaim())
    assert ok is True and reason == "chain_detached"

    world.pids[TARGET]["ppid"] = OWNER  # a live parent appeared
    world.add(OWNER, create_time=OWNER_CT, exists=True)
    ok, reason, _ev = pp.verify_owner_dead(
        TARGET, world.claim(session_pid=1, session_create_time=None, reparented=True),
        world.reclaim())
    assert ok is False and reason == "parent_changed_since_scan"


def test_unreadable_ancestry_denies():
    world = FakeWorld().orphan()
    world.raise_on["ppid"] = PermissionError("denied")
    ok, reason, _ev = pp.verify_owner_dead(TARGET, world.claim(), world.reclaim())
    assert ok is False and reason == "ancestry_unreadable:PermissionError"


def test_missing_ppid_denies():
    world = FakeWorld().orphan()
    world.pids[TARGET]["ppid"] = None
    ok, reason, _ev = pp.verify_owner_dead(TARGET, world.claim(), world.reclaim())
    assert ok is False and reason == "ancestry_unreadable"


def test_unprovable_owner_create_time_denies():
    """Parent alive, create-time unreadable: identity is unprovable ⇒ DENY."""
    world = FakeWorld().orphan(owner_exists=True)
    world.pids[OWNER]["create_time"] = None
    ok, reason, _ev = pp.verify_owner_dead(TARGET, world.claim(), world.reclaim())
    assert ok is False and reason == "owner_identity_unprovable"


def test_owner_create_time_missing_at_scan_denies():
    world = FakeWorld().orphan(owner_exists=True)
    ok, reason, _ev = pp.verify_owner_dead(
        TARGET, world.claim(session_create_time=None), world.reclaim())
    assert ok is False and reason == "owner_create_time_missing_at_scan"


# ── second path: no live session lease references the process ────────────────

def test_no_lease_is_a_pass():
    world = FakeWorld().orphan()
    ok, reason, ev = pp.verify_no_live_lease(TARGET, world.claim(), world.reclaim())
    assert ok is True and reason == "no_live_session_lease"
    assert ev["watched_pids"] == sorted([TARGET, OWNER])


def test_stale_lease_whose_owner_is_gone_is_not_a_live_reference():
    world = FakeWorld().orphan()
    world.leases = [{"session_id": "sess-dead", "owner_pid": OWNER}]
    world.lease_alive["sess-dead"] = False
    ok, _reason, ev = pp.verify_no_live_lease(TARGET, world.claim(), world.reclaim())
    assert ok is True
    assert ev["stale_leases"] == ["sess-dead"]


def test_unreadable_lease_registry_denies():
    world = FakeWorld().orphan()
    world.raise_on["active_leases"] = sqlite_error()
    ok, reason, _ev = pp.verify_no_live_lease(TARGET, world.claim(), world.reclaim())
    assert ok is False and reason == "lease_registry_unreadable:OperationalError"


def test_unreadable_lease_owner_denies():
    world = FakeWorld().orphan()
    world.leases = [{"session_id": "sess-1", "owner_pid": OWNER}]
    world.raise_on["lease_owner_alive"] = RuntimeError("ps gone")
    ok, reason, ev = pp.verify_no_live_lease(TARGET, world.claim(), world.reclaim())
    assert ok is False and reason == "lease_owner_unreadable:RuntimeError"
    assert ev["unreadable_lease"] == "sess-1"


def sqlite_error() -> BaseException:
    return sqlite3.OperationalError("database is locked")


# ── the decision gate stack ──────────────────────────────────────────────────

def test_dry_run_verifies_but_sends_nothing():
    world, recorded, claim = _setup_dead_session()
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim())
    assert plan.authorized is True
    assert plan.action == pp.RECLAIM_ACTION_NONE
    assert plan.outcome == pp.RECLAIM_OUTCOME_REPORTED
    assert plan.reason == "dry_run_verified_candidate"
    assert plan.evidence["planned_action"] == pp.RECLAIM_ACTION_SIGTERM
    assert plan.signal is None
    _never_signalled(world)


def test_kill_request_authorizes_exactly_one_sigterm_plan():
    world, recorded, claim = _setup_dead_session()
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is True
    assert plan.action == pp.RECLAIM_ACTION_SIGTERM
    assert plan.outcome == pp.RECLAIM_OUTCOME_ALLOWED


def test_missing_identity_snapshot_denies():
    world, _recorded, claim = _setup_dead_session()
    plan = pp.decide_mcp_reclaim(
        TARGET, None, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is False
    assert plan.reason == "identity_snapshot_unavailable"
    _never_signalled(world)


def test_scan_create_time_must_agree_with_the_snapshot():
    world, recorded, claim = _setup_dead_session()
    agreed = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert agreed.authorized is True
    stale = pp.MCPOwnerClaim(**{**claim.to_dict(), "scan_create_time": "CT-OTHER"})
    denied = pp.decide_mcp_reclaim(
        TARGET, recorded, stale, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert denied.authorized is False
    assert denied.reason == "scan_create_time_mismatch"
    _never_signalled(world)


def test_unavailable_scan_create_time_denies():
    world, recorded, claim = _setup_dead_session()
    unknown = pp.MCPOwnerClaim(**{**claim.to_dict(), "scan_create_time": None})
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, unknown, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.reason == "scan_create_time_unavailable"
    _never_signalled(world)


# PLANTED BAD 1 — the owning session is ALIVE. Must be denied, never signalled.
def test_planted_bad_live_owning_session_is_denied_and_never_signalled():
    world = FakeWorld().orphan(owner_exists=True)
    claim = world.claim()
    recorded = pp.capture_mcp_identity(TARGET, probes=world.probes(), claim=claim)
    assert recorded is not None
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is False
    assert plan.reason == "owner_session_alive"
    assert plan.outcome == pp.RECLAIM_OUTCOME_DENIED
    assert plan.evidence["owner_proof"]["owner_exists_at_decision"] is True
    assert plan.evidence["lease_check"] == {"skipped": "owner_death_unproven"}
    _never_signalled(world)


# PLANTED BAD 2 — the target PID was recycled after the snapshot. Denied.
def test_planted_bad_target_pid_reuse_is_denied_and_never_signalled():
    world, recorded, claim = _setup_dead_session()
    world.pids[TARGET]["create_time"] = "CT-REUSED"  # PID handed to something else
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is False
    assert plan.reason == "pid_reuse"
    _never_signalled(world)


# PLANTED BAD 3 — a live session lease still references the target's session.
def test_planted_bad_live_session_lease_is_denied_and_never_signalled():
    world, recorded, claim = _setup_dead_session()
    world.leases = [{"session_id": "sess-live", "owner_pid": OWNER}]
    world.lease_alive["sess-live"] = True
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is False
    assert plan.reason.startswith("live_session_lease:")
    assert plan.evidence["lease_check"]["referencing_leases"] == ["sess-live"]
    _never_signalled(world)


# PLANTED BAD 4 — a live lease names the TARGET itself as its owner.
def test_planted_bad_lease_naming_the_target_is_denied():
    world, recorded, claim = _setup_dead_session()
    world.leases = [{"session_id": "sess-target", "owner_pid": TARGET}]
    world.lease_alive["sess-target"] = True
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is False
    assert plan.reason == "live_session_lease:sess-target"
    _never_signalled(world)


# PLANTED BAD 5 — the chain was re-attached under a different live parent.
def test_planted_bad_parent_changed_since_scan_is_denied():
    world, recorded, claim = _setup_dead_session()
    world.pids[TARGET]["ppid"] = 888
    world.add(888, create_time="PCT-1", exists=True)
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is False
    assert plan.reason == "parent_changed_since_scan"
    _never_signalled(world)


# PLANTED BAD 6 — a root-owned (system) target must stay protected even when the
# caller happens to share its uid, so the PROTECTION is what denies.
def test_planted_bad_root_owned_target_is_denied_as_system():
    world = FakeWorld().orphan()
    world.pids[TARGET]["uid"] = 0
    world.caller_uid = 0
    claim = world.claim()
    recorded = pp.capture_mcp_identity(TARGET, probes=world.probes(), claim=claim)
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is False
    assert plan.reason == "protected:system"
    assert "system" in plan.evidence["identity_decision"]["protections"]
    _never_signalled(world)


# PLANTED BAD 7 — an agent-named executable is a protection, not a cleanup target.
def test_planted_bad_agent_executable_is_denied():
    world = FakeWorld().orphan()
    world.pids[TARGET]["exe"] = "/usr/local/bin/claude"
    claim = world.claim()
    recorded = pp.capture_mcp_identity(TARGET, probes=world.probes(), claim=claim)
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is False
    assert "agent" in plan.reason
    _never_signalled(world)


def test_foreign_uid_is_denied_for_a_signal_action():
    world = FakeWorld().orphan()
    world.caller_uid = 502  # the caller is not the owner of the target
    claim = world.claim()
    recorded = pp.capture_mcp_identity(TARGET, probes=world.probes(), claim=claim)
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is False
    assert plan.reason == "uid_not_same"
    _never_signalled(world)


def test_executable_drift_between_snapshot_and_decision_is_denied():
    world, recorded, claim = _setup_dead_session()
    world.pids[TARGET]["exe"] = "/opt/other/python3"
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is False
    assert plan.reason == "exe_mismatch"
    _never_signalled(world)


def test_identity_probe_failure_denies_with_the_type_named():
    world, recorded, claim = _setup_dead_session()
    world.raise_on["create_time"] = PermissionError("no")
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is False
    assert plan.reason == "probe_permission"
    _never_signalled(world)


def test_a_vanished_target_denies_rather_than_reports_success():
    world = FakeWorld().orphan()
    claim = world.claim()
    recorded = pp.capture_mcp_identity(TARGET, probes=world.probes(), claim=claim)
    world.pids[TARGET]["exists"] = False
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    # A target that is gone leaves its ancestry unreadable; that is a DENY, and
    # it is never reported as a successful reap.
    assert plan.authorized is False
    assert plan.reason == "ancestry_unreadable"
    assert plan.outcome == pp.RECLAIM_OUTCOME_DENIED
    _never_signalled(world)


def test_every_candidate_proves_owner_death_before_the_lease_table_is_read():
    world, recorded, claim = _setup_dead_session()
    world.add(OWNER, create_time=OWNER_CT, exists=True)  # owner alive again
    pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert world.lease_reads == 0  # denied before the second path's other half ran


# ── execution: graceful first, force only behind its own flag ────────────────

def _kill_plan(world: FakeWorld):
    claim = world.claim()
    recorded = pp.capture_mcp_identity(TARGET, probes=world.probes(), claim=claim)
    assert recorded is not None
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    assert plan.authorized is True
    return plan, recorded, claim


def test_dry_run_plan_executes_to_nothing():
    world, recorded, claim = _setup_dead_session()
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim())
    result = pp.execute_mcp_reclaim(
        plan, recorded, claim, probes=world.probes(), reclaim=world.reclaim(),
        sender=world.sender())
    assert result is plan
    assert result.outcome == pp.RECLAIM_OUTCOME_REPORTED
    _never_signalled(world)


def test_denied_plan_executes_to_nothing():
    world = FakeWorld().orphan(owner_exists=True)  # PLANTED BAD, live owner
    claim = world.claim()
    recorded = pp.capture_mcp_identity(TARGET, probes=world.probes(), claim=claim)
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(), kill=True)
    result = pp.execute_mcp_reclaim(
        plan, recorded, claim, probes=world.probes(), reclaim=world.reclaim(),
        sender=world.sender())
    assert result.outcome == pp.RECLAIM_OUTCOME_DENIED
    _never_signalled(world)


def test_authorized_plan_sends_exactly_one_sigterm_and_reports_the_exit():
    world = FakeWorld().orphan()
    plan, recorded, claim = _kill_plan(world)
    send = world.sender()
    send_state = {"n": 0}

    def send_and_exit(pid: int, sig: int) -> None:
        send(pid, sig)
        send_state["n"] += 1
        world.pids[TARGET]["exists"] = False

    result = pp.execute_mcp_reclaim(
        plan, recorded, claim, probes=world.probes(), reclaim=world.reclaim(),
        sender=send_and_exit)
    assert world.signals == [(TARGET, int(signal_mod.SIGTERM))]
    assert result.outcome == pp.RECLAIM_OUTCOME_EXITED
    assert result.action == pp.RECLAIM_ACTION_SIGTERM
    assert result.signal == int(signal_mod.SIGTERM)
    assert result.reason == "graceful_exit"


def test_second_path_is_re_proved_at_the_moment_of_the_signal():
    world = FakeWorld().orphan()
    plan, recorded, claim = _kill_plan(world)
    world.leases = [{"session_id": "sess-new", "owner_pid": OWNER}]
    world.lease_alive["sess-new"] = True  # a lease appeared after the decision
    result = pp.execute_mcp_reclaim(
        plan, recorded, claim, probes=world.probes(), reclaim=world.reclaim(),
        sender=world.sender())
    assert result.authorized is False
    assert result.reason.startswith("second_path_recheck_denied:")
    assert result.evidence["second_path_recheck"]["lease_check"]["referencing_leases"] == [
        "sess-new",
    ]
    _never_signalled(world)


def test_survival_is_reported_and_never_escalates():
    world = FakeWorld().orphan()
    plan, recorded, claim = _kill_plan(world)
    result = pp.execute_mcp_reclaim(
        plan, recorded, claim, probes=world.probes(), reclaim=world.reclaim(),
        sender=world.sender(), grace_seconds=0.2)
    assert world.signals == [(TARGET, int(signal_mod.SIGTERM))]  # one, and only one
    assert result.outcome == pp.RECLAIM_OUTCOME_SURVIVED
    assert result.reason == "grace_timeout_force_not_requested"


def test_force_flag_adds_sigkill_after_a_fresh_identity_recheck():
    """The drift lands BETWEEN the graceful attempt and the escalation, so the
    SIGKILL's own fresh re-check is what has to catch it."""
    world = FakeWorld().orphan()
    plan, recorded, claim = _kill_plan(world)
    send = world.sender()

    def send_then_drift(pid: int, sig: int) -> None:
        send(pid, sig)
        world.pids[TARGET]["uid"] = 502  # identity drifted after SIGTERM

    result = pp.execute_mcp_reclaim(
        plan, recorded, claim, probes=world.probes(), reclaim=world.reclaim(),
        sender=send_then_drift, force=True, grace_seconds=0.2)
    assert world.signals == [(TARGET, int(signal_mod.SIGTERM))]  # no SIGKILL escaped
    assert result.outcome == pp.RECLAIM_OUTCOME_FAILED
    assert result.reason == "uid_mismatch"
    assert result.evidence["force_receipt"]["reason"] == "uid_mismatch"


def test_force_flag_sends_sigkill_only_after_grace_and_only_when_requested():
    world = FakeWorld().orphan()
    plan, recorded, claim = _kill_plan(world)
    calls: list[tuple[int, int]] = []

    def send_then_die(pid: int, sig: int) -> None:
        calls.append((pid, sig))
        if sig == int(signal_mod.SIGKILL):
            world.pids[TARGET]["exists"] = False

    result = pp.execute_mcp_reclaim(
        plan, recorded, claim, probes=world.probes(), reclaim=world.reclaim(),
        sender=send_then_die, force=True, grace_seconds=0.2)
    assert calls == [
        (TARGET, int(signal_mod.SIGTERM)),
        (TARGET, int(signal_mod.SIGKILL)),
    ]
    assert result.outcome == pp.RECLAIM_OUTCOME_EXITED
    assert result.action == pp.RECLAIM_ACTION_SIGKILL
    assert result.signal == int(signal_mod.SIGKILL)


def test_force_without_a_reason_sends_no_sigkill():
    world = FakeWorld().orphan()
    plan, recorded, claim = _kill_plan(world)
    result = pp.execute_mcp_reclaim(
        plan, recorded, claim, probes=world.probes(), reclaim=world.reclaim(),
        sender=world.sender(), force=True, force_reason="   ", grace_seconds=0.2)
    assert world.signals == [(TARGET, int(signal_mod.SIGTERM))]
    assert result.outcome == pp.RECLAIM_OUTCOME_SURVIVED


def test_graceful_path_uses_the_injected_clock_only():
    world = FakeWorld().orphan()
    plan, recorded, claim = _kill_plan(world)
    pp.execute_mcp_reclaim(
        plan, recorded, claim, probes=world.probes(), reclaim=world.reclaim(),
        sender=world.sender(), grace_seconds=0.3)
    assert world.clock.sleeps  # the bounded wait advanced the injected clock
    assert sum(world.clock.sleeps) == pytest.approx(0.3, abs=0.11)


# ── receipt contract ─────────────────────────────────────────────────────────

def test_receipt_schema_evidence_block_and_verifier_identity():
    world, recorded, claim = _setup_dead_session()
    plan = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim())
    receipt = plan.to_dict()
    assert receipt["schema_version"] == "fleet-watch/process-decision/v1"
    assert receipt["policy_version"] == pp.POLICY_VERSION
    assert receipt["verifier_identity"] == "session_registry+parent_chain"
    assert receipt["identity_path"] == "kernel_identity_probes"
    evidence = receipt["evidence"]
    assert evidence["owner_proof"]["owner_death_proof"] == "owner_pid_absent"
    assert evidence["lease_check"]["watched_pids"] == sorted([TARGET, OWNER])
    assert evidence["identity_decision"]["outcome"] == "allowed"
    assert evidence["create_time_cross_check"]["scan_create_time"] == TARGET_CT
    assert receipt["observed_identity"]["create_time"] == TARGET_CT
    json.dumps(receipt)  # the receipt is the wire format


def test_denied_receipt_is_also_a_full_receipt():
    world = FakeWorld().orphan(owner_exists=True)
    claim = world.claim()
    recorded = pp.capture_mcp_identity(TARGET, probes=world.probes(), claim=claim)
    receipt = pp.decide_mcp_reclaim(
        TARGET, recorded, claim, probes=world.probes(), reclaim=world.reclaim(),
        kill=True).to_dict()
    assert receipt["schema_version"] == "fleet-watch/process-decision/v1"
    assert receipt["authorized"] is False
    assert receipt["verifier_identity"] == "session_registry+parent_chain"
    assert receipt["evidence"]["owner_proof"]["owner_exists_at_decision"] is True
    assert receipt["evidence"]["lease_check"] == {"skipped": "owner_death_unproven"}


def test_owner_label_is_explicit_never_empty():
    assert pp.MCPOwnerClaim(session_pid=777).owner_label() == "mcp-owner-pid:777"
    assert pp.MCPOwnerClaim(session_pid=1, reparented=True).owner_label() == "mcp-owner:detached"
    assert pp.MCPOwnerClaim(session_id="sess-9").owner_label() == "sess-9"


# ── the CLI surface ──────────────────────────────────────────────────────────

class FakeConn:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _candidate(world: FakeWorld, pid: int = TARGET, owner_pid: int = OWNER) -> M.MCPCandidate:
    return M.MCPCandidate(
        pid=pid, ppid=owner_pid, rss_mb=70,
        cmd="python3 /cds/mcp_compile_server.py", create_time=world.pids[pid]["create_time"],
        owner=M.MCPOwnerSnapshot(
            session_pid=owner_pid,
            session_create_time=world.pids[owner_pid]["create_time"],
            reparented=owner_pid <= 1,
            alive_at_scan=False,
        ),
        evidence=(f"owning session pid {owner_pid} absent at scan time",),
    )


def _wire(monkeypatch, world: FakeWorld, candidates, *, events: list | None = None,
          conn: FakeConn | None = None) -> dict[str, Any]:
    """Inject every seam the verified path uses. The sender is a recorder, so a
    bug that signals a planted bad shows up as a recorded signal, never as a
    real kill."""
    state: dict[str, Any] = {"conn": conn or FakeConn(), "events": events if events is not None else []}
    monkeypatch.setattr(
        reap_mod.mcp_orphan_detector, "scan_candidates",
        lambda: (list(candidates), None))
    monkeypatch.setattr(reap_mod, "_mcp_identity_probes", world.probes)
    monkeypatch.setattr(reap_mod, "_mcp_reclaim_probes", lambda conn: world.reclaim())
    monkeypatch.setattr(reap_mod, "_mcp_signal_sender", world.sender)
    monkeypatch.setattr(reap_mod, "_get_conn", lambda: state["conn"])
    monkeypatch.setattr(
        reap_mod.events, "log_event",
        lambda *a, **k: state["events"].append((a[1], k.get("detail", {}))))
    return state


def _json(res) -> dict[str, Any]:
    return json.loads(res.output)


def test_cli_verify_dry_run_reports_receipts_and_sends_nothing(monkeypatch):
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])
    res = CliRunner().invoke(cli.reap, ["--mcp", "--verify", "--json"])
    assert res.exit_code == 0
    payload = _json(res)
    assert payload["mode"] == "mcp-verified-reclaim"
    assert payload["dry_run"] is True
    assert payload["verifier_identity"] == "session_registry+parent_chain"
    assert payload["candidate_count"] == 1
    decision = payload["decisions"][0]
    assert decision["schema_version"] == "fleet-watch/process-decision/v1"
    assert decision["outcome"] == "reported"
    assert payload["exited"] == [] and payload["denied"] == []
    _never_signalled(world)
    assert state["conn"].closed is True
    # the plan receipt is in the hash-chained event log
    assert [kind for kind, _ in state["events"]] == ["PROCESS_DECISION"]
    assert state["events"][0][1]["phase"] == "plan"


def test_cli_text_output_is_a_report_only_summary(monkeypatch):
    world = FakeWorld().orphan()
    _wire(monkeypatch, world, [_candidate(world)])
    res = CliRunner().invoke(cli.reap, ["--mcp", "--verify"])
    assert res.exit_code == 0
    assert "DRY RUN" in res.output
    assert "--kill" in res.output
    _never_signalled(world)


def test_cli_kill_sends_one_sigterm_and_reports_per_process_outcomes(monkeypatch):
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])

    def send_and_exit(pid: int, sig: int) -> None:
        world.signals.append((pid, sig))
        world.pids[pid]["exists"] = False

    monkeypatch.setattr(reap_mod, "_mcp_signal_sender", lambda: send_and_exit)
    res = CliRunner().invoke(cli.reap, ["--mcp", "--verify", "--kill", "--json"])
    assert res.exit_code == 0
    payload = _json(res)
    assert payload["exited"][0]["outcome"] == "exited"
    assert payload["exited"][0]["action"] == "sigterm"
    assert world.signals == [(TARGET, int(signal_mod.SIGTERM))]
    kinds = [kind for kind, _ in state["events"]]
    assert kinds == ["PROCESS_DECISION", "PROCESS_DECISION"]  # plan, then outcome
    assert [d["phase"] for _, d in state["events"]] == ["plan", "outcome"]


# PLANTED BAD through the CLI: the live owner must be denied, not killed.
def test_cli_planted_bad_live_session_is_denied_and_never_killed(monkeypatch):
    world = FakeWorld().orphan(owner_exists=True)
    _wire(monkeypatch, world, [_candidate(world)])
    res = CliRunner().invoke(cli.reap, ["--mcp", "--verify", "--kill", "--json"])
    payload = _json(res)
    assert payload["denied"][0]["reason"] == "owner_session_alive"
    assert payload["exited"] == []
    assert res.exit_code == 1  # asked to kill, could not → non-zero
    _never_signalled(world)


def test_cli_kill_force_escalates_only_behind_both_flags(monkeypatch):
    world = FakeWorld().orphan()
    _wire(monkeypatch, world, [_candidate(world)])

    def send(pid: int, sig: int) -> None:
        world.signals.append((pid, sig))
        if sig == int(signal_mod.SIGKILL):
            world.pids[pid]["exists"] = False

    monkeypatch.setattr(reap_mod, "_mcp_signal_sender", lambda: send)
    res = CliRunner().invoke(
        cli.reap, ["--mcp", "--verify", "--kill", "--kill-force", "--json"])
    assert res.exit_code == 0
    assert [sig for _pid, sig in world.signals] == [
        int(signal_mod.SIGTERM), int(signal_mod.SIGKILL),
    ]
    assert _json(res)["exited"][0]["action"] == "sigkill"


def test_cli_kill_without_verify_is_refused(monkeypatch):
    world = FakeWorld().orphan()
    _wire(monkeypatch, world, [_candidate(world)])
    res = CliRunner().invoke(cli.reap, ["--mcp", "--kill"])
    assert res.exit_code == 2
    assert "--verify" in res.output
    _never_signalled(world)


def test_cli_mcp_without_verify_is_refused(monkeypatch):
    world = FakeWorld().orphan()
    _wire(monkeypatch, world, [_candidate(world)])
    res = CliRunner().invoke(cli.reap, ["--mcp"])
    assert res.exit_code == 2
    assert "--verify" in res.output
    _never_signalled(world)


def test_cli_verify_without_mcp_is_refused(monkeypatch):
    world = FakeWorld().orphan()
    _wire(monkeypatch, world, [_candidate(world)])
    res = CliRunner().invoke(cli.reap, ["--verify"])
    assert res.exit_code == 2
    assert "--mcp" in res.output
    _never_signalled(world)


def test_cli_kill_force_without_kill_is_refused(monkeypatch):
    world = FakeWorld().orphan()
    _wire(monkeypatch, world, [_candidate(world)])
    res = CliRunner().invoke(cli.reap, ["--mcp", "--verify", "--kill-force"])
    assert res.exit_code == 2
    assert "--kill" in res.output
    _never_signalled(world)


def test_cli_legacy_confirm_is_refused_on_the_verified_path(monkeypatch):
    world = FakeWorld().orphan()
    _wire(monkeypatch, world, [_candidate(world)])
    res = CliRunner().invoke(cli.reap, ["--mcp", "--verify", "--kill", "--confirm"])
    assert res.exit_code == 2
    assert "--confirm" in res.output
    _never_signalled(world)


def test_cli_scan_error_is_unknown_not_a_clean_bill(monkeypatch):
    world = FakeWorld().orphan()
    _wire(monkeypatch, world, [])
    monkeypatch.setattr(
        reap_mod.mcp_orphan_detector, "scan_candidates", lambda: ([], "ps_nonzero_exit"))
    res = CliRunner().invoke(cli.reap, ["--mcp", "--verify", "--json"])
    assert res.exit_code == 1
    payload = _json(res)
    assert payload["scan_error"] == "ps_nonzero_exit"
    assert payload["candidate_count"] == 0
    _never_signalled(world)


def test_cli_closes_its_connection_on_the_scan_error_path(monkeypatch):
    world = FakeWorld().orphan()
    conn = FakeConn()
    _wire(monkeypatch, world, [], conn=conn)
    monkeypatch.setattr(
        reap_mod.mcp_orphan_detector, "scan_candidates", lambda: ([], "ps_nonzero_exit"))
    CliRunner().invoke(cli.reap, ["--mcp", "--verify", "--json"])
    assert conn.closed is True


def test_cli_writes_no_signal_when_the_audit_chain_refuses(monkeypatch):
    """No decision receipt in the hash chain ⇒ no kill. Fail-closed on audit."""
    world = FakeWorld().orphan()
    _wire(monkeypatch, world, [_candidate(world)])

    def refuse(*_a, **_k):
        raise sqlite_error()

    monkeypatch.setattr(reap_mod.events, "log_event", refuse)
    res = CliRunner().invoke(cli.reap, ["--mcp", "--verify", "--kill", "--json"])
    assert res.exit_code != 0
    assert isinstance(res.exception, sqlite3.OperationalError)
    _never_signalled(world)


def test_cli_legacy_include_mcp_path_is_untouched(monkeypatch):
    """The pre-existing advisory/legacy surface must keep its behaviour: --mcp
    is additive and leaves --include-mcp --confirm alone."""
    monkeypatch.setattr(cli.registry, "get_reapable_processes", lambda conn: [])
    calls: list[int] = []
    monkeypatch.setattr(reap_mod, "_terminate_orphan", lambda pid, **k: (calls.append(pid), True)[1])
    monkeypatch.setattr(
        reap_mod.mcp_orphan_detector, "detect",
        lambda: M.MCPOrphanResult(mcp_process_count=2, orphans_detected=True, orphan_pids=[7]))
    monkeypatch.setattr(reap_mod, "_get_conn", lambda: FakeConn())
    monkeypatch.setattr(reap_mod.events, "log_event", lambda *a, **k: None)
    monkeypatch.setattr(reap_mod.reporter, "write_report", lambda conn: None)
    res = CliRunner().invoke(cli.reap, ["--include-mcp", "--confirm"])
    assert res.exit_code == 0
    assert calls == [7]


# ── the batch tripwire ───────────────────────────────────────────────────────

def test_every_planted_bad_in_this_batch_is_rejected_by_the_real_command(monkeypatch):
    """BATCH VOID TRIPWIRE. Each planted bad is driven through the actual CLI
    with the most aggressive flags this command accepts (--kill --kill-force),
    and the whole batch must come back with ZERO signals. If any planted bad is
    ever signalled, this test fails and the batch is void.
    """
    def planted_bad_worlds() -> list[tuple[str, FakeWorld, M.MCPCandidate]]:
        out: list[tuple[str, FakeWorld, M.MCPCandidate]] = []

        live_owner = FakeWorld().orphan(owner_exists=True)
        out.append(("live_owning_session", live_owner, _candidate(live_owner)))

        # The candidate is captured first, then the kernel hands its PID to a
        # different process: the scan's create-time and the live one disagree.
        recycled = FakeWorld().orphan()
        recycled_candidate = _candidate(recycled)
        recycled.pids[TARGET]["create_time"] = "CT-REUSED"
        out.append(("target_pid_recycled", recycled, recycled_candidate))

        leased = FakeWorld().orphan()
        leased.leases = [{"session_id": "sess-live", "owner_pid": OWNER}]
        leased.lease_alive["sess-live"] = True
        out.append(("live_session_lease", leased, _candidate(leased)))

        leased_target = FakeWorld().orphan()
        leased_target.leases = [{"session_id": "sess-target", "owner_pid": TARGET}]
        leased_target.lease_alive["sess-target"] = True
        out.append(("lease_names_target", leased_target, _candidate(leased_target)))

        reparented = FakeWorld().orphan()
        reparented.pids[TARGET]["ppid"] = 888
        reparented.add(888, create_time="PCT-1", exists=True)
        out.append(("parent_changed_since_scan", reparented, _candidate(reparented)))

        root = FakeWorld().orphan()
        root.pids[TARGET]["uid"] = 0
        out.append(("root_owned_target", root, _candidate(root)))

        agent = FakeWorld().orphan()
        agent.pids[TARGET]["exe"] = "/usr/local/bin/claude"
        out.append(("agent_executable", agent, _candidate(agent)))

        unprovable = FakeWorld().orphan(owner_exists=True)
        unprovable.pids[OWNER]["create_time"] = None
        out.append(("owner_identity_unprovable", unprovable, _candidate(unprovable)))

        return out

    verdicts: dict[str, str] = {}
    for name, world, candidate in planted_bad_worlds():
        _wire(monkeypatch, world, [candidate])
        res = CliRunner().invoke(
            cli.reap, ["--mcp", "--verify", "--kill", "--kill-force", "--json"])
        payload = json.loads(res.output)
        assert payload["exited"] == [], f"{name} was exited: {payload['exited']}"
        assert payload["denied"], f"{name} produced no denial receipt: {payload}"
        verdicts[name] = payload["denied"][0]["reason"]
        assert res.exit_code == 1, f"{name} did not fail closed"

    assert verdicts == {
        "live_owning_session": "owner_session_alive",
        "target_pid_recycled": "scan_create_time_mismatch",
        "live_session_lease": "live_session_lease:sess-live",
        "lease_names_target": "live_session_lease:sess-target",
        "parent_changed_since_scan": "parent_changed_since_scan",
        "root_owned_target": "uid_not_same",
        "agent_executable": "protected:agent",
        "owner_identity_unprovable": "owner_identity_unprovable",
    }


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
