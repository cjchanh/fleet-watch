"""The daemon-side auto-reclaim flip — the operator's seam on the verified path.

WHAT IS UNDER TEST: ``auto_reclaim`` in the fleet-watch config file
(``~/.fleet-watch/config.json``), read FRESH on every daemon tick, with three
states and a default of ``off``:

  * ``off``       — the daemon behaves exactly as it did before the flip: it
                    never scans for reclaim candidates, writes no receipt, and
                    signals nothing.
  * ``dry_run``   — the daemon runs the verified path in its dry-run mode every
                    tick, receipts what it WOULD do (``would_act``), and sends
                    nothing at all.
  * ``on``        — the daemon runs the verified path in kill mode, and the
                    verified path's own denials still stand. The flip is a
                    request to evaluate, not a licence to signal: it can never
                    make a denied candidate actionable, and it can never
                    escalate to SIGKILL.

THE PLANTED BADS (marked ``PLANTED BAD``): the same eight the governed
``fleet reap --mcp --verify --kill --kill-force`` batch rejects — a live owning
session, a recycled target PID, a live session lease (referencing the owner and
naming the target), a re-parented server that grew a live parent, a root-owned
target, an agent executable, and an unprovable owner. Each one MUST be denied
and MUST receive no signal AT ALL, with the flip in its most aggressive state
(``on``). If any planted bad is ever signalled, this batch is void.

Determinism: every kernel probe, clock, lease table, registry connection, signal
sender and config read is injected. The one real process is a disposable child
this file spawns itself, it is signalled only through the trapped sender, and
every test reaps it in a fixture ``finally``. No test signals a process it did
not spawn, and no real MCP server is ever a candidate here.
"""

from __future__ import annotations

import importlib
import signal as signal_mod
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fleet_watch import cli_support, events, process_policy as pp
from fleet_watch.discovery import mcp_orphan_detector as M

reap_mod = importlib.import_module("fleet_watch.commands.reap")

TARGET = 4242
OWNER = 777
TARGET_CT = "CT-1"
OWNER_CT = "OCT-1"
START = 1_700_000_000.0

FLIP_OFF = "off"
FLIP_DRY_RUN = "dry_run"
FLIP_ON = "on"


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
    """One fake kernel + lease table. Mutating it between calls is how a test
    models the world changing under a decision."""

    def __init__(self, *, start: float = START) -> None:
        self.clock = FakeClock(start)
        self.pids: dict[int, dict[str, Any]] = {}
        self.leases: list[dict[str, Any]] = []
        self.lease_alive: dict[str, bool] = {}
        self.caller_uid = 501
        self.self_pid = 1001
        self.parent_pid = 555
        self.raise_on: dict[str, BaseException] = {}
        self.signals: list[tuple[int, int]] = []
        self.on_signal: Any = None  # optional hook: the trapped sender's real act
        # Optional per-PID liveness override, so a test can bind the fake
        # kernel's view of a PID to a REAL process (see the on-mode test).
        self.exists_override: dict[int, Any] = {}

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

    def _guard(self, name: str) -> None:
        exc = self.raise_on.get(name)
        if exc is not None:
            raise exc

    def probes(self) -> pp.Probes:
        w = self

        def exists(pid: int) -> bool:
            w._guard("exists")
            if pid in w.exists_override:
                return bool(w.exists_override[pid]())
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

    def sender(self) -> Any:
        """The trapped sender: records every signal. Nothing else can signal."""
        w = self

        def send(pid: int, sig: int) -> None:
            w.signals.append((pid, sig))
            if w.on_signal is not None:
                w.on_signal(pid, sig)

        return send


def _never_signalled(world: FakeWorld, why: str) -> None:
    assert world.signals == [], f"PLANTED BAD was signalled ({why}): {world.signals}"


class FakeConn:
    """The daemon's tick connection. The tick never closes it — the caller does."""

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


def _wire(monkeypatch, world: FakeWorld, candidates, *, conn: FakeConn | None = None,
          scan_error: str | None = None) -> dict[str, Any]:
    """Inject every seam the daemon tick reaches. The sender is a recorder, so a
    bug that signals a planted bad shows up as a recorded signal, never a kill."""
    state: dict[str, Any] = {
        "conn": conn or FakeConn(),
        "events": [],
        "timeline": [],
        "scans": 0,
    }
    tick = state["timeline"]

    def scan_candidates() -> tuple[list[Any], str | None]:
        state["scans"] += 1
        if scan_error is not None:
            return [], scan_error
        return list(candidates), None

    def log_event(_conn, kind, pid=None, workstream=None, detail=None) -> None:
        state["events"].append((kind, pid, workstream, detail or {}))
        phase = (detail or {}).get("phase")
        tick.append(f"receipt:{phase}:{pid}")

    monkeypatch.setattr(M, "scan_candidates", scan_candidates)
    monkeypatch.setattr(reap_mod, "_mcp_identity_probes", world.probes)
    monkeypatch.setattr(reap_mod, "_mcp_reclaim_probes", lambda conn: world.reclaim())
    monkeypatch.setattr(reap_mod, "_mcp_signal_sender", world.sender)
    # One module attribute, so the verified path's receipts AND the flip's own
    # receipts land in the same ordered timeline.
    monkeypatch.setattr(events, "log_event", log_event)
    return state


def _config(monkeypatch, config: dict[str, Any]) -> None:
    """Point the flip's only source (the config file) at an injected dict."""
    monkeypatch.setattr(reap_mod.discover_mod, "load_config", lambda: config)


def _tick(state: dict[str, Any]) -> dict[str, Any]:
    """One daemon tick, flip resolved the way the daemon resolves it."""
    return cli_support._run_auto_reclaim_tick(state["conn"])


def _auto_reclaim_receipts(state: dict[str, Any]) -> list[dict[str, Any]]:
    return [d for _k, _p, _w, d in state["events"] if d.get("phase") == "auto_reclaim"]


def _plan_receipts(state: dict[str, Any]) -> list[dict[str, Any]]:
    return [d for _k, _p, _w, d in state["events"] if d.get("phase") == "plan"]


@pytest.fixture
def disposable_child():
    """A real child process this file owns, reaped in the fixture's finally.

    The only real process in this file. It is signalled ONLY through the trapped
    sender below, and the test that uses it reaps it before it can leak.
    """
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        yield child
    finally:
        if child.poll() is None:  # no orphan shells, ever
            child.kill()
        child.wait(timeout=10)


# ── (a) the default is off: no scan, no receipt, no signal ────────────────────

def test_default_flip_is_off_even_with_a_verified_candidate_present(monkeypatch):
    """A config with no ``auto_reclaim`` key is the shipped default: the daemon
    does not even look for candidates, so nothing can be signalled."""
    _config(monkeypatch, {})
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])

    assert reap_mod.resolve_auto_reclaim() == FLIP_OFF
    result = _tick(state)

    assert result["flip"] == FLIP_OFF
    assert result == {"flip": "off", "considered": 0, "would_act": 0, "acted": 0, "denied": 0}
    assert state["scans"] == 0, "off must not even scan for reclaim candidates"
    assert state["events"] == []  # not one receipt: this is exactly yesterday's tick
    _never_signalled(world, "default off")


def test_off_leaves_the_existing_advisory_surface_untouched(monkeypatch):
    """Off means advisory-only, which is what `_mcp_surface_lines` already does."""
    _config(monkeypatch, {"auto_reclaim": FLIP_OFF})
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])

    result = _tick(state)

    assert result["flip"] == FLIP_OFF
    assert state["scans"] == 0 and state["events"] == []
    _never_signalled(world, "explicit off")
    # the advisory read-only surface still reports the same candidate
    assert cli_support._mcp_surface_lines(
        M.MCPOrphanResult(mcp_process_count=1, orphans_detected=True, orphan_pids=[TARGET])
    ) == [f"MCP: 1 dead-session orphan(s) of 1 server(s), ~0MB recoverable"]


# ── (b) dry_run: receipts what it would do, signals nothing ──────────────────

def test_dry_run_receipts_would_act_and_signals_nothing(monkeypatch):
    _config(monkeypatch, {"auto_reclaim": FLIP_DRY_RUN})
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])

    result = _tick(state)

    assert result["flip"] == FLIP_DRY_RUN
    assert result["would_act"] == 1
    assert result["acted"] == 0, "dry_run must never act"
    assert result["considered"] == 1
    # the verified path still receipted its own plan, then the flip receipted
    # would_act=True/acted=False for the same candidate
    assert len(_plan_receipts(state)) == 1
    flip_receipts = _auto_reclaim_receipts(state)
    assert len(flip_receipts) == 1
    assert flip_receipts[0]["flip"] == FLIP_DRY_RUN
    assert flip_receipts[0]["would_act"] is True
    assert flip_receipts[0]["acted"] is False
    assert flip_receipts[0]["receipt"]["outcome"] == pp.RECLAIM_OUTCOME_REPORTED
    _never_signalled(world, "dry_run")


def test_dry_run_receipts_would_act_false_for_a_denied_candidate(monkeypatch):
    _config(monkeypatch, {"auto_reclaim": FLIP_DRY_RUN})
    world = FakeWorld().orphan(owner_exists=True)  # PLANTED BAD: live owner
    state = _wire(monkeypatch, world, [_candidate(world)])

    result = _tick(state)

    flip_receipts = _auto_reclaim_receipts(state)
    assert result["would_act"] == 0 and result["denied"] == 1
    assert len(flip_receipts) == 1
    assert flip_receipts[0]["would_act"] is False
    assert flip_receipts[0]["acted"] is False
    _never_signalled(world, "dry_run with a live owner")


# ── (c) on: exactly one graceful SIGTERM, after the receipt, with the exit verified

def test_on_mode_signals_once_after_the_receipt_and_verifies_the_real_exit(
    monkeypatch, disposable_child,
):
    """The whole point of the seam, end to end.

    A disposable child this file spawned stands in for the orphan. The trapped
    sender records the signal and then delivers ONE real SIGTERM to that child —
    the only process any test here may signal. The exit the policy reports is
    that real process's real death, reaped by the fixture.
    """
    _config(monkeypatch, {"auto_reclaim": FLIP_ON})
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])
    timeline = state["timeline"]

    def on_signal(pid: int, sig: int) -> None:
        timeline.append(f"signal:{sig}")
        assert pid == TARGET
        assert sig == int(signal_mod.SIGTERM)
        disposable_child.terminate()
        disposable_child.wait(timeout=10)  # synchronous, so verification is honest

    world.on_signal = on_signal
    # bind the fake kernel's liveness for the target to the REAL child, so the
    # exit verification below is the policy observing a real death
    world.exists_override[TARGET] = lambda: disposable_child.poll() is None

    result = _tick(state)

    # exactly one signal, graceful, and never a SIGKILL
    assert world.signals == [(TARGET, int(signal_mod.SIGTERM))]
    assert [s for _p, s in world.signals].count(int(signal_mod.SIGTERM)) == 1
    assert int(signal_mod.SIGKILL) not in [s for _p, s in world.signals]

    # the receipt came first: plan receipt → signal → outcome receipt
    assert timeline == [
        f"receipt:plan:{TARGET}",
        f"signal:{int(signal_mod.SIGTERM)}",
        f"receipt:outcome:{TARGET}",
        f"receipt:auto_reclaim:{TARGET}",
    ]

    # exit verification: the real child is gone and its exit was verified
    assert disposable_child.poll() is not None
    assert disposable_child.returncode == -int(signal_mod.SIGTERM), (
        "the disposable child must really have died from the one SIGTERM"
    )
    flip_receipt = _auto_reclaim_receipts(state)[0]
    assert flip_receipt["flip"] == FLIP_ON
    assert flip_receipt["would_act"] is True
    assert flip_receipt["acted"] is True
    outcome = [d for _k, _p, _w, d in state["events"] if d.get("phase") == "outcome"][0]
    assert outcome["receipt"]["outcome"] == pp.RECLAIM_OUTCOME_EXITED
    assert outcome["receipt"]["reason"] == "graceful_exit"
    assert result["acted"] == 1 and result["would_act"] == 1
    # the tick never closes the caller's connection
    assert state["conn"].closed is False


def test_the_flip_cannot_escalate_to_sigkill(monkeypatch):
    """`on` authorises one graceful SIGTERM. A survivor is reported, never escalated."""
    _config(monkeypatch, {"auto_reclaim": FLIP_ON})
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])
    # the trapped sender records and does nothing: the target survives.

    result = _tick(state)

    assert world.signals == [(TARGET, int(signal_mod.SIGTERM))]
    outcome = [d for _k, _p, _w, d in state["events"] if d.get("phase") == "outcome"][0]
    assert outcome["receipt"]["outcome"] == pp.RECLAIM_OUTCOME_SURVIVED
    # the receipt names the reason it did not escalate: force was never requested
    assert outcome["receipt"]["reason"] == "grace_timeout_force_not_requested"
    assert result["acted"] == 1  # a signal was attempted, and honestly reported


def test_an_unwritable_audit_chain_denies_even_at_flip_on(monkeypatch):
    """Receipt-first, proven from the daemon side: no receipt, no signal."""
    _config(monkeypatch, {"auto_reclaim": FLIP_ON})
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])

    def refuse(*_a, **_k):
        raise RuntimeError("event chain unwritable")

    monkeypatch.setattr(events, "log_event", refuse)
    result = _tick(state)

    assert "error" in result
    assert result["acted"] == 0
    _never_signalled(world, "unwritable chain at flip on")


# ── (d) invalid values are off, never a guess ────────────────────────────────

@pytest.mark.parametrize("value", [
    "yes", "true", "kill", "on-ish", "dry-run", "dryrun", "off-ish", "  ",
    "", None, 1, 0, True, False, [], {}, ["on"], "ON; SIGKILL",
])
def test_an_invalid_flip_value_resolves_to_off(value, monkeypatch):
    _config(monkeypatch, {"auto_reclaim": value})
    assert reap_mod.resolve_auto_reclaim() == FLIP_OFF


@pytest.mark.parametrize("config", [
    {},                                   # no key at all
    {"auto_reclaim": None},
    {"auto_reclaim": 1},
    {"auto_reclaim": ["on"]},
    {"auto_reclaim": "on-ish"},
])
def test_an_untrusted_config_resolves_to_off(config, monkeypatch):
    _config(monkeypatch, config)
    assert reap_mod.resolve_auto_reclaim() == FLIP_OFF


def test_an_invalid_flip_value_on_the_console_signals_nothing(monkeypatch):
    _config(monkeypatch, {"auto_reclaim": "on-ish"})
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])

    result = _tick(state)

    assert result["flip"] == FLIP_OFF
    assert state["scans"] == 0 and state["events"] == []
    _never_signalled(world, "invalid flip value")


def test_an_unreadable_config_file_resolves_to_off(monkeypatch):
    """An unreadable flip source is OFF. Uncertainty is never read as permission."""
    def boom() -> dict[str, Any]:
        raise OSError("config unreadable")

    monkeypatch.setattr(reap_mod.discover_mod, "load_config", boom)
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])

    assert reap_mod.resolve_auto_reclaim() == FLIP_OFF
    result = _tick(state)

    assert result["flip"] == FLIP_OFF
    assert state["scans"] == 0
    _never_signalled(world, "unreadable config")


def test_no_environment_variable_can_arm_the_flip(monkeypatch):
    """The config file is the ONLY source — an ambient env var cannot arm a kill."""
    monkeypatch.setenv("FLEET_AUTO_RECLAIM", "on")
    monkeypatch.setenv("FLEET_REAP_AUTO_KILL", "1")
    _config(monkeypatch, {})
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])

    assert reap_mod.resolve_auto_reclaim() == FLIP_OFF
    result = _tick(state)

    assert result["flip"] == FLIP_OFF
    assert state["scans"] == 0
    _never_signalled(world, "env var attempted to arm the flip")


def test_the_flip_is_re_read_every_tick_so_off_takes_effect_immediately(monkeypatch):
    """No caching, no restart: the next tick reads the operator's current value."""
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])

    _config(monkeypatch, {"auto_reclaim": FLIP_ON})
    assert _tick(state)["flip"] == FLIP_ON
    _config(monkeypatch, {"auto_reclaim": FLIP_OFF})
    assert _tick(state)["flip"] == FLIP_OFF
    _config(monkeypatch, {"auto_reclaim": FLIP_DRY_RUN})
    assert _tick(state)["flip"] == FLIP_DRY_RUN


@pytest.mark.parametrize("spelling,expected", [
    ("on", FLIP_ON),
    ("ON", FLIP_ON),
    ("  on  ", FLIP_ON),
    ("dry_run", FLIP_DRY_RUN),
    ("DRY_RUN", FLIP_DRY_RUN),
    ("off", FLIP_OFF),
])
def test_the_three_states_are_the_only_accepted_values(spelling, expected, monkeypatch):
    _config(monkeypatch, {"auto_reclaim": spelling})
    assert reap_mod.resolve_auto_reclaim() == expected


# ── (e) a verified-path denial is never acted on, even with the flip on ──────

def test_a_verified_path_denial_is_never_acted_on_even_with_the_flip_on(monkeypatch):
    """PLANTED BAD: a live owning session. `on` is a request to evaluate, not a
    licence to signal — the verified path's denial stands and nothing is sent."""
    _config(monkeypatch, {"auto_reclaim": FLIP_ON})
    world = FakeWorld().orphan(owner_exists=True)
    state = _wire(monkeypatch, world, [_candidate(world)])

    result = _tick(state)

    assert result["flip"] == FLIP_ON
    assert result["denied"] == 1
    assert result["acted"] == 0 and result["would_act"] == 0
    denied = [d for _k, _p, _w, d in state["events"] if d.get("phase") == "plan"][0]
    assert denied["receipt"]["reason"] == "owner_session_alive"
    assert denied["receipt"]["authorized"] is False
    flip_receipt = _auto_reclaim_receipts(state)[0]
    assert flip_receipt["would_act"] is False and flip_receipt["acted"] is False
    _never_signalled(world, "live owner at flip on")


def test_a_scan_error_is_unknown_not_a_clean_bill_even_at_flip_on(monkeypatch):
    """An unscannable host is UNKNOWN, never "nothing to clean"."""
    _config(monkeypatch, {"auto_reclaim": FLIP_ON})
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [], scan_error="ps_nonzero_exit")

    result = _tick(state)

    assert result["scan_error"] == "ps_nonzero_exit"
    assert result["considered"] == 0 and result["acted"] == 0
    assert state["events"] == []
    _never_signalled(world, "unscannable host at flip on")


# ── the batch tripwire ───────────────────────────────────────────────────────

def test_every_planted_bad_in_this_batch_is_never_signalled_by_the_daemon(monkeypatch):
    """BATCH VOID TRIPWIRE.

    Each planted bad is driven through the real daemon tick with the flip in its
    most aggressive state (``on`` — the one that can signal at all), and the whole
    batch must come back with ZERO signals. If any planted bad is ever signalled,
    this test fails and the batch is void.
    """
    def planted_bad_worlds() -> list[tuple[str, FakeWorld, M.MCPCandidate]]:
        out: list[tuple[str, FakeWorld, M.MCPCandidate]] = []

        out.append(("live_owning_session",
                    FakeWorld().orphan(owner_exists=True),
                    _candidate(FakeWorld().orphan(owner_exists=True))))

        # captured first, then the kernel hands the PID to a different process
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

        unreadable = FakeWorld().orphan()
        unreadable.raise_on["ppid"] = PermissionError("denied")
        out.append(("unreadable_ancestry", unreadable, _candidate(unreadable)))

        return out

    _config(monkeypatch, {"auto_reclaim": FLIP_ON})
    verdicts: dict[str, str] = {}
    for name, world, candidate in planted_bad_worlds():
        state = _wire(monkeypatch, world, [candidate])
        result = _tick(state)

        _never_signalled(world, name)
        assert result["flip"] == FLIP_ON
        assert result["acted"] == 0, f"{name} was acted on: {result}"
        assert result["would_act"] == 0, f"{name} was actionable: {result}"
        assert result["denied"] == 1, f"{name} produced no denial receipt: {result}"
        denied = [d for _k, _p, _w, d in state["events"] if d.get("phase") == "plan"][0]
        assert denied["receipt"]["authorized"] is False
        verdicts[name] = denied["receipt"]["reason"]
        flip_receipt = _auto_reclaim_receipts(state)[0]
        assert flip_receipt["would_act"] is False and flip_receipt["acted"] is False

    assert verdicts == {
        "live_owning_session": "owner_session_alive",
        "target_pid_recycled": "scan_create_time_mismatch",
        "live_session_lease": "live_session_lease:sess-live",
        "lease_names_target": "live_session_lease:sess-target",
        "parent_changed_since_scan": "parent_changed_since_scan",
        "root_owned_target": "uid_not_same",
        "agent_executable": "protected:agent",
        "owner_identity_unprovable": "owner_identity_unprovable",
        "unreadable_ancestry": "ancestry_unreadable:PermissionError",
    }


def test_the_flip_only_ever_acts_through_the_one_governed_path(monkeypatch):
    """The tick owns no policy of its own: at flip on it is the verified pass that
    decides, and the only difference from the CLI is which flag was passed."""
    _config(monkeypatch, {"auto_reclaim": FLIP_ON})
    world = FakeWorld().orphan()
    state = _wire(monkeypatch, world, [_candidate(world)])

    seen: list[dict[str, Any]] = []
    real_pass = reap_mod.verified_reclaim_pass

    def spy(conn, **kwargs):
        seen.append(kwargs)
        return real_pass(conn, **kwargs)

    monkeypatch.setattr(reap_mod, "verified_reclaim_pass", spy)
    _tick(state)

    assert seen == [{"do_kill": True, "do_kill_force": False}]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
