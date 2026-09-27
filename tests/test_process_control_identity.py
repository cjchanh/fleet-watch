"""Fail-closed process identity/action contract tests (TDD RED phase first).

Written BEFORE ``fleet_watch/process_policy.py`` exists: the first run of this
file must fail at import, then pass once the contract is implemented.

Contract under test (repo AGENTS.md + slice brief):
  * identity requires PID + kernel create-time + executable + same UID;
  * observations must be fresh; missing / permission / stale / PID-reuse all
    DENY (never allow-by-default);
  * three action classes: ``advisory`` (report only), ``terminate`` (bounded
    graceful signal), ``force`` (explicit ForceGate);
  * protections: self, parent, system, WindowServer, active, agent, backup,
    ComfyUI, unknown;
  * terminate/force require session + owner evidence;
  * signal functions revalidate FRESH immediately before signalling (an
    earlier ALLOWED receipt is never trusted) and terminate never escalates;
  * every decision is a structured receipt with timestamps, reason, policy
    version, and outcome.

Determinism: every kernel probe, clock, sleep, and signal sender is injected.
NO test ever signals a real process — senders are recorders. The only real
process observed anywhere is a subprocess THIS file creates (positive
control), released by writing a newline to its stdin.
"""

from __future__ import annotations

import contextlib
import json
import signal as signal_mod
import subprocess
import sys

import pytest

from fleet_watch import process_policy as pp

PID = 4242
START_TIME = 1_700_000_000.0


# ── deterministic fakes ──────────────────────────────────────────────────────

class FakeClock:
    """Injected now()/sleep(): deterministic, never sleeps real time."""

    def __init__(self, start: float = START_TIME) -> None:
        self.t = start
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds

    def advance(self, seconds: float) -> None:
        self.t += seconds


class FakeSystem:
    """Mutable fake kernel; ``probes()`` builds callables reading current state.

    Mutating fields between calls models the world changing under us (PID
    reuse, a newly protected target) — this is how the revalidation tests
    prove the signal path re-probes instead of trusting the earlier decision.
    """

    def __init__(
        self,
        *,
        pid: int = PID,
        create_time: str = "CT-1",
        exe: str = "/usr/local/bin/fleet-worker",
        uid: int = 501,
        exists: bool = True,
        self_pid: int = 1001,
        parent_pid: int = 777,
        caller_uid: int = 501,
        start: float = START_TIME,
        probe_exc: dict[str, BaseException] | None = None,
    ) -> None:
        self.pid = pid
        self.create_time = create_time
        self.exe = exe
        self.uid = uid
        self.exists_flag = exists
        self.self_pid = self_pid
        self.parent_pid = parent_pid
        self.caller_uid = caller_uid
        self.clock = FakeClock(start)
        self.probe_exc = probe_exc or {}

    def probes(self) -> pp.Probes:
        sys_f = self

        def _maybe(name: str) -> None:
            exc = sys_f.probe_exc.get(name)
            if exc is not None:
                raise exc

        def exists(pid: int) -> bool:
            _maybe("exists")
            return sys_f.exists_flag

        def create_time(pid: int) -> str | None:
            _maybe("create_time")
            return sys_f.create_time if sys_f.exists_flag else None

        def exe(pid: int) -> str | None:
            _maybe("exe")
            return sys_f.exe if sys_f.exists_flag else None

        def uid(pid: int) -> int | None:
            _maybe("uid")
            return sys_f.uid if sys_f.exists_flag else None

        return pp.Probes(
            now=sys_f.clock.now,
            sleep=sys_f.clock.sleep,
            exists=exists,
            create_time=create_time,
            exe=exe,
            uid=uid,
            self_pid=lambda: sys_f.self_pid,
            parent_pid=lambda: sys_f.parent_pid,
            caller_uid=lambda: sys_f.caller_uid,
        )


class RecordingSender:
    """Signal sender that records instead of signalling. Never touches the OS."""

    def __init__(self, exc: BaseException | None = None, on_send=None) -> None:
        self.calls: list[tuple[int, int]] = []
        self.exc = exc
        self.on_send = on_send

    def __call__(self, pid: int, sig: int) -> None:
        self.calls.append((pid, sig))
        if self.on_send is not None:
            self.on_send(pid, sig)
        if self.exc is not None:
            raise self.exc


def _evidence(**kw) -> pp.OwnerEvidence:
    kw.setdefault("session_id", "sess-1")
    kw.setdefault("owner", "owner-1")
    kw.setdefault("status", "idle")
    kw.setdefault("kind", "user")
    return pp.OwnerEvidence(**kw)


def _recorded(sys_f: FakeSystem, evidence: pp.OwnerEvidence | None | object = ...):
    if evidence is ...:
        evidence = _evidence()
    return pp.RecordedIdentity(
        pid=sys_f.pid,
        create_time=sys_f.create_time,
        exe=sys_f.exe,
        uid=sys_f.uid,
        evidence=evidence,  # type: ignore[arg-type]
    )


def _observe(sys_f: FakeSystem) -> pp.Observation:
    return pp.observe(sys_f.pid, sys_f.probes())


def _clean() -> tuple[FakeSystem, pp.RecordedIdentity]:
    sys_f = FakeSystem()
    return sys_f, _recorded(sys_f)


def _open_gate() -> pp.ForceGate:
    return pp.ForceGate(allowed=True, reason="unit-test force gate")


# ── receipt shape ────────────────────────────────────────────────────────────

def test_advisory_decision_receipt_shape():
    sys_f, recorded = _clean()
    probes = sys_f.probes()
    obs = _observe(sys_f)

    receipt = pp.decide(obs, recorded, pp.ACTION_ADVISORY, probes)

    assert receipt.outcome == pp.OUTCOME_ALLOWED
    assert receipt.reason == "identity_proven"
    assert receipt.policy_version == pp.POLICY_VERSION
    assert receipt.action == pp.ACTION_ADVISORY
    assert receipt.pid == recorded.pid
    assert receipt.requested_at == START_TIME
    assert receipt.observed_at == obs.observed_at == START_TIME
    assert receipt.decided_at == START_TIME
    assert receipt.completed_at is None  # pure decision: nothing executed yet
    assert receipt.classification == pp.CLASS_USER
    assert receipt.protections == ()
    assert receipt.session_id == "sess-1"
    assert receipt.owner == "owner-1"
    assert receipt.signal is None


def test_receipt_to_dict_is_json_safe():
    sys_f, recorded = _clean()
    receipt = pp.decide(_observe(sys_f), recorded, pp.ACTION_TERMINATE, sys_f.probes())
    payload = json.loads(json.dumps(receipt.to_dict()))
    assert payload["policy_version"] == pp.POLICY_VERSION
    assert payload["outcome"] == pp.OUTCOME_ALLOWED
    assert payload["reason"] == "identity_proven"
    assert isinstance(payload["protections"], list)
    for key in (
        "action", "pid", "requested_at", "observed_at", "decided_at",
        "completed_at", "classification", "session_id", "owner", "signal",
    ):
        assert key in payload, key


# ── fail closed: missing / permission ────────────────────────────────────────

def test_decide_on_none_observation_fails_closed_for_every_action():
    _, recorded = _clean()
    probes = FakeSystem().probes()
    for action in (pp.ACTION_ADVISORY, pp.ACTION_TERMINATE, pp.ACTION_FORCE):
        receipt = pp.decide(None, recorded, action, probes)
        assert receipt.outcome == pp.OUTCOME_DENIED, (action, receipt)
        assert receipt.reason == "missing_observation"


def test_dead_process_fails_closed():
    sys_f = FakeSystem(exists=False)
    recorded = _recorded(sys_f)
    obs = _observe(sys_f)
    assert obs.error == "process_gone"
    receipt = pp.decide(obs, recorded, pp.ACTION_TERMINATE, sys_f.probes())
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "process_gone"


def test_missing_create_time_fails_closed():
    sys_f = FakeSystem(create_time="")
    recorded = _recorded(sys_f)
    obs = _observe(sys_f)
    assert obs.error == "missing_create_time"
    receipt = pp.decide(obs, recorded, pp.ACTION_ADVISORY, sys_f.probes())
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "missing_create_time"


def test_missing_exe_fails_closed():
    sys_f = FakeSystem(exe="")
    recorded = _recorded(sys_f)
    obs = _observe(sys_f)
    assert obs.error == "missing_exe"
    assert pp.decide(obs, recorded, pp.ACTION_ADVISORY, sys_f.probes()).reason == "missing_exe"


def test_missing_uid_fails_closed():
    sys_f = FakeSystem(uid=None)  # type: ignore[arg-type]
    recorded = _recorded(sys_f)
    obs = _observe(sys_f)
    assert obs.error == "missing_uid"
    assert pp.decide(obs, recorded, pp.ACTION_ADVISORY, sys_f.probes()).reason == "missing_uid"


def test_probe_permission_error_fails_closed():
    sys_f = FakeSystem(probe_exc={"exe": PermissionError("denied")})
    recorded = _recorded(sys_f)
    obs = _observe(sys_f)
    assert obs.error == "probe_permission"
    receipt = pp.decide(obs, recorded, pp.ACTION_TERMINATE, sys_f.probes())
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "probe_permission"


def test_probe_unexpected_exception_names_the_type_in_the_reason():
    sys_f = FakeSystem(probe_exc={"uid": RuntimeError("boom")})
    recorded = _recorded(sys_f)
    obs = _observe(sys_f)
    assert obs.error == "probe_error:RuntimeError"
    receipt = pp.decide(obs, recorded, pp.ACTION_TERMINATE, sys_f.probes())
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "probe_error:RuntimeError"


# ── fail closed: PID reuse / identity drift ──────────────────────────────────

def test_pid_reuse_denied_for_every_action():
    sys_f, recorded = _clean()
    recycled = pp.Observation(
        pid=recorded.pid,
        observed_at=START_TIME,
        create_time="CT-2-RECYCLED",
        exe=recorded.exe,
        uid=recorded.uid,
    )
    for action in (pp.ACTION_ADVISORY, pp.ACTION_TERMINATE, pp.ACTION_FORCE):
        receipt = pp.decide(recycled, recorded, action, sys_f.probes())
        assert receipt.outcome == pp.OUTCOME_DENIED, (action, receipt)
        assert receipt.reason == "pid_reuse"


def test_executable_change_denied():
    sys_f, recorded = _clean()
    drifted = pp.Observation(
        pid=recorded.pid,
        observed_at=START_TIME,
        create_time=recorded.create_time,
        exe="/usr/local/bin/something-else",
        uid=recorded.uid,
    )
    receipt = pp.decide(drifted, recorded, pp.ACTION_TERMINATE, sys_f.probes())
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "exe_mismatch"


def test_uid_change_denied():
    sys_f, recorded = _clean()
    drifted = pp.Observation(
        pid=recorded.pid,
        observed_at=START_TIME,
        create_time=recorded.create_time,
        exe=recorded.exe,
        uid=502,
    )
    receipt = pp.decide(drifted, recorded, pp.ACTION_TERMINATE, sys_f.probes())
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "uid_mismatch"


# ── fail closed: staleness ───────────────────────────────────────────────────

def test_stale_observation_denied_at_the_freshness_boundary():
    sys_f, recorded = _clean()
    probes = sys_f.probes()
    obs = _observe(sys_f)

    sys_f.clock.advance(pp.MAX_OBSERVATION_AGE_SECONDS)  # exactly at the edge
    allowed = pp.decide(obs, recorded, pp.ACTION_TERMINATE, probes)
    assert allowed.outcome == pp.OUTCOME_ALLOWED, allowed

    sys_f.clock.advance(0.5)  # now past the edge
    stale = pp.decide(obs, recorded, pp.ACTION_TERMINATE, probes)
    assert stale.outcome == pp.OUTCOME_DENIED
    assert stale.reason == "stale_observation"


def test_observation_from_the_future_denied():
    sys_f, recorded = _clean()
    future = pp.Observation(
        pid=recorded.pid,
        observed_at=START_TIME + 60.0,
        create_time=recorded.create_time,
        exe=recorded.exe,
        uid=recorded.uid,
    )
    receipt = pp.decide(future, recorded, pp.ACTION_TERMINATE, sys_f.probes())
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "stale_observation"


# ── same UID + owner/session evidence ────────────────────────────────────────

def test_foreign_uid_denied_for_signal_actions_but_not_advisory():
    sys_f = FakeSystem(caller_uid=502)  # we do not own uid 501
    recorded = _recorded(sys_f)
    obs = _observe(sys_f)

    advisory = pp.decide(obs, recorded, pp.ACTION_ADVISORY, sys_f.probes())
    assert advisory.outcome == pp.OUTCOME_ALLOWED  # report-only never signals

    terminate = pp.decide(obs, recorded, pp.ACTION_TERMINATE, sys_f.probes())
    assert terminate.outcome == pp.OUTCOME_DENIED
    assert terminate.reason == "uid_not_same"


@pytest.mark.parametrize(
    "evidence",
    [
        None,
        _evidence(session_id=""),
        _evidence(owner=""),
    ],
    ids=["no-evidence", "empty-session", "empty-owner"],
)
def test_terminate_without_session_owner_evidence_denied(evidence):
    sys_f = FakeSystem()
    recorded = _recorded(sys_f, evidence)
    obs = _observe(sys_f)
    receipt = pp.decide(obs, recorded, pp.ACTION_TERMINATE, sys_f.probes())
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "missing_owner_evidence"


# ── protection classes ───────────────────────────────────────────────────────

PROTECT_CASES = [
    ("self", dict(self_pid=PID), dict(status="idle", kind="user")),
    ("parent", dict(parent_pid=PID), dict(status="idle", kind="user")),
    ("system", dict(pid=64), dict(status="idle", kind="user")),
    (
        "windowserver",
        dict(exe="/System/Library/PrivateFrameworks/SkyLight.framework/"
                 "/Versions/A/Resources/WindowServer"),
        dict(status="idle", kind="user"),
    ),
    ("agent", dict(), dict(status="idle", kind="agent")),
    ("agent", dict(exe="/opt/homebrew/bin/codex"), dict(status="idle", kind="user")),
    ("backup", dict(exe="/opt/homebrew/bin/restic"), dict(status="idle", kind="user")),
    (
        "comfyui",
        dict(exe="/Users/cj/models/ComfyUI/.venv/bin/python3"),
        dict(status="idle", kind="user"),
    ),
    ("active", dict(), dict(status="active", kind="user")),
    ("unknown", dict(), dict(status="idle", kind="mystery")),
    ("unknown", dict(), dict(status="unrecognized", kind="user")),
]


@pytest.mark.parametrize(
    "tag,sys_over,evidence_over",
    PROTECT_CASES,
    ids=[f"{c[0]}-{i}" for i, c in enumerate(PROTECT_CASES)],
)
def test_protected_targets_deny_signals_and_stay_describable(tag, sys_over, evidence_over):
    sys_f = FakeSystem(**sys_over)
    recorded = _recorded(sys_f, _evidence(**evidence_over))
    probes = sys_f.probes()
    obs = _observe(sys_f)

    terminate = pp.decide(obs, recorded, pp.ACTION_TERMINATE, probes)
    assert terminate.outcome == pp.OUTCOME_DENIED, terminate
    assert terminate.reason == f"protected:{tag}", terminate

    force = pp.decide(obs, recorded, pp.ACTION_FORCE, probes, force_gate=_open_gate())
    assert force.outcome == pp.OUTCOME_DENIED, force
    assert force.reason == f"protected:{tag}", force

    advisory = pp.decide(obs, recorded, pp.ACTION_ADVISORY, probes)
    assert advisory.outcome == pp.OUTCOME_ALLOWED  # describable, but never signallable
    assert tag in advisory.protections, advisory


def test_idle_registered_user_process_terminate_allowed():
    """ALLOW control for the protection table: a proven, idle, same-UID,
    evidence-backed user process with no protection is terminatable."""
    sys_f, recorded = _clean()
    receipt = pp.decide(_observe(sys_f), recorded, pp.ACTION_TERMINATE, sys_f.probes())
    assert receipt.outcome == pp.OUTCOME_ALLOWED, receipt
    assert receipt.reason == "identity_proven"
    assert receipt.protections == ()


# ── action ladder + force gate ───────────────────────────────────────────────

def test_action_ladder_distinguishes_advisory_terminate_force():
    sys_f, recorded = _clean()
    probes = sys_f.probes()
    obs = _observe(sys_f)

    advisory = pp.decide(obs, recorded, pp.ACTION_ADVISORY, probes)
    terminate = pp.decide(obs, recorded, pp.ACTION_TERMINATE, probes)
    force_no_gate = pp.decide(obs, recorded, pp.ACTION_FORCE, probes)
    force_open = pp.decide(obs, recorded, pp.ACTION_FORCE, probes, force_gate=_open_gate())

    assert (advisory.action, terminate.action, force_open.action) == (
        pp.ACTION_ADVISORY, pp.ACTION_TERMINATE, pp.ACTION_FORCE,
    )
    assert advisory.outcome == pp.OUTCOME_ALLOWED
    assert terminate.outcome == pp.OUTCOME_ALLOWED
    assert force_no_gate.outcome == pp.OUTCOME_DENIED
    assert force_no_gate.reason == "force_not_permitted"  # default gate = closed
    assert force_open.outcome == pp.OUTCOME_ALLOWED


def test_force_gate_with_empty_reason_denied():
    sys_f, recorded = _clean()
    receipt = pp.decide(
        _observe(sys_f), recorded, pp.ACTION_FORCE, sys_f.probes(),
        force_gate=pp.ForceGate(allowed=True, reason="   "),
    )
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "force_not_permitted"


def test_unknown_action_fails_closed():
    sys_f, recorded = _clean()
    receipt = pp.decide(_observe(sys_f), recorded, "wipe", sys_f.probes())
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "unknown_action"


# ── signal path: fresh revalidation immediately before the signal ────────────

def test_terminate_revalidates_and_blocks_on_pid_reuse_in_the_window():
    """TOCTOU: identity proven at decision time, PID recycled before the
    signal → the fresh revalidation must DENY and NOTHING may be sent."""
    sys_f, recorded = _clean()
    sender = RecordingSender()
    earlier = pp.decide(_observe(sys_f), recorded, pp.ACTION_TERMINATE, sys_f.probes())
    assert earlier.outcome == pp.OUTCOME_ALLOWED

    sys_f.create_time = "CT-2-RECYCLED"  # world changes before the signal

    receipt = pp.terminate_gracefully(recorded, sys_f.probes(), sender=sender, grace_seconds=0.0)
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "pid_reuse"
    assert sender.calls == []
    assert receipt.signal is None
    assert receipt.completed_at is not None


def test_terminate_revalidates_and_blocks_on_new_protection_in_the_window():
    """TOCTOU: by signal time a protection applies to the target (here: the
    revalidation sees it as our parent) → DENY and NOTHING is sent.

    Note: a mutated *identity field* (exe) is caught even earlier, as
    exe_mismatch — see the next test — so the protection branch is exercised
    here via caller context, which is not part of the identity tuple.
    """
    sys_f, recorded = _clean()
    sender = RecordingSender()
    sys_f.parent_pid = PID  # a protection that appears only at revalidation time

    receipt = pp.terminate_gracefully(recorded, sys_f.probes(), sender=sender, grace_seconds=0.0)
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "protected:parent"
    assert sender.calls == []


def test_terminate_revalidates_and_blocks_on_executable_drift_in_the_window():
    """TOCTOU: the executable changes between decision and signal (same
    create-time = same process that exec'd elsewhere) → identity tuple broken
    → DENY exe_mismatch and NOTHING is sent."""
    sys_f, recorded = _clean()
    sender = RecordingSender()
    earlier = pp.decide(_observe(sys_f), recorded, pp.ACTION_TERMINATE, sys_f.probes())
    assert earlier.outcome == pp.OUTCOME_ALLOWED

    sys_f.exe = "/usr/local/bin/something-else"  # exec'd under the same PID

    receipt = pp.terminate_gracefully(recorded, sys_f.probes(), sender=sender, grace_seconds=0.0)
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "exe_mismatch"
    assert sender.calls == []


def test_terminate_revalidates_and_blocks_on_stale_world():
    """The decision must be made from an observation taken inside the call —
    an observation the caller captured long ago is stale at signal time."""
    sys_f, recorded = _clean()
    sender = RecordingSender()
    old_obs = _observe(sys_f)
    sys_f.clock.advance(pp.MAX_OBSERVATION_AGE_SECONDS + 1.0)

    # decide() with the stale observation denies...
    assert pp.decide(old_obs, recorded, pp.ACTION_TERMINATE, sys_f.probes()).reason == (
        "stale_observation"
    )
    # ...and the signal path never uses caller-held observations at all: it
    # re-observes with the (old) clock still far past freshness of any
    # previously captured observation, yet proceeds only on its own fresh one.
    receipt = pp.terminate_gracefully(recorded, sys_f.probes(), sender=sender, grace_seconds=0.0)
    assert receipt.outcome == pp.OUTCOME_SURVIVED  # fresh obs, signal attempted
    assert sender.calls == [(recorded.pid, signal_mod.SIGTERM)]


# ── signal path: bounded graceful signal, no escalation ──────────────────────

def test_terminate_sends_exactly_one_bounded_graceful_signal():
    sys_f, recorded = _clean()
    sender = RecordingSender()

    receipt = pp.terminate_gracefully(
        recorded, sys_f.probes(), sender=sender, grace_seconds=0.0
    )

    assert sender.calls == [(recorded.pid, signal_mod.SIGTERM)]  # no SIGKILL, ever
    assert receipt.outcome == pp.OUTCOME_SURVIVED
    assert receipt.reason == "grace_timeout"
    assert receipt.action == pp.ACTION_TERMINATE
    assert receipt.signal == signal_mod.SIGTERM
    assert receipt.completed_at is not None
    assert receipt.policy_version == pp.POLICY_VERSION


def test_terminate_reports_exit_within_grace():
    sys_f, recorded = _clean()
    sender = RecordingSender(on_send=lambda pid, sig: setattr(sys_f, "exists_flag", False))

    receipt = pp.terminate_gracefully(
        recorded, sys_f.probes(), sender=sender, grace_seconds=0.0
    )

    assert sender.calls == [(recorded.pid, signal_mod.SIGTERM)]
    assert receipt.outcome == pp.OUTCOME_EXITED
    assert receipt.reason == "graceful_exit"


def test_terminate_bounded_wait_uses_the_injected_clock_not_real_time():
    sys_f, recorded = _clean()
    sender = RecordingSender()  # process never dies

    receipt = pp.terminate_gracefully(
        recorded, sys_f.probes(), sender=sender, grace_seconds=2.0
    )

    assert receipt.outcome == pp.OUTCOME_SURVIVED
    # The wait was bounded by the injected clock — no real seconds elapsed,
    # and the recorded sleeps sum to (approximately) the grace window.
    assert sys_f.clock.sleeps, "bounded wait must poll via injected sleep"
    # FP tolerance: 20 × 0.1 accumulates ~2e-6 of representation error.
    assert abs(sum(sys_f.clock.sleeps) - 2.0) < 1e-3, sys_f.clock.sleeps
    assert sender.calls == [(recorded.pid, signal_mod.SIGTERM)]


def test_terminate_signal_permission_failure_fails_closed():
    sys_f, recorded = _clean()
    sender = RecordingSender(exc=PermissionError("no permission to signal"))

    receipt = pp.terminate_gracefully(
        recorded, sys_f.probes(), sender=sender, grace_seconds=0.0
    )
    assert receipt.outcome == pp.OUTCOME_FAILED
    assert receipt.reason == "signal_permission"


def test_terminate_sender_unexpected_exception_names_the_type():
    sys_f, recorded = _clean()
    sender = RecordingSender(exc=RuntimeError("sender exploded"))
    receipt = pp.terminate_gracefully(
        recorded, sys_f.probes(), sender=sender, grace_seconds=0.0
    )
    assert receipt.outcome == pp.OUTCOME_FAILED
    assert receipt.reason == "signal_error:RuntimeError"


def test_terminate_target_already_gone_at_send_time():
    sys_f, recorded = _clean()
    sender = RecordingSender(exc=ProcessLookupError())
    receipt = pp.terminate_gracefully(
        recorded, sys_f.probes(), sender=sender, grace_seconds=0.0
    )
    assert receipt.outcome == pp.OUTCOME_EXITED
    assert receipt.reason == "already_gone"


# ── force path: explicit gate + fresh revalidation + SIGKILL only when open ──

def test_force_terminate_without_gate_sends_nothing():
    sys_f, recorded = _clean()
    sender = RecordingSender()
    receipt = pp.force_terminate(recorded, sys_f.probes(), sender=sender)
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "force_not_permitted"
    assert sender.calls == []


def test_force_terminate_with_closed_gate_sends_nothing():
    sys_f, recorded = _clean()
    sender = RecordingSender()
    receipt = pp.force_terminate(
        recorded, sys_f.probes(), sender=sender,
        force_gate=pp.ForceGate(allowed=False, reason="operator said no"),
    )
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "force_not_permitted"
    assert sender.calls == []


def test_force_terminate_with_open_gate_sends_sigkill_and_reports_exit():
    sys_f, recorded = _clean()
    sender = RecordingSender(on_send=lambda pid, sig: setattr(sys_f, "exists_flag", False))
    receipt = pp.force_terminate(
        recorded, sys_f.probes(), sender=sender, force_gate=_open_gate()
    )
    assert sender.calls == [(recorded.pid, signal_mod.SIGKILL)]
    assert receipt.outcome == pp.OUTCOME_EXITED
    assert receipt.reason == "killed"
    assert receipt.action == pp.ACTION_FORCE
    assert receipt.signal == signal_mod.SIGKILL


def test_force_terminate_revalidates_before_killing():
    """Force must also re-probe: recycled PID before SIGKILL → nothing sent."""
    sys_f, recorded = _clean()
    sender = RecordingSender()
    sys_f.create_time = "CT-2-RECYCLED"
    receipt = pp.force_terminate(
        recorded, sys_f.probes(), sender=sender, force_gate=_open_gate()
    )
    assert receipt.outcome == pp.OUTCOME_DENIED
    assert receipt.reason == "pid_reuse"
    assert sender.calls == []


def test_force_terminate_survivor_is_reported_as_failure():
    sys_f, recorded = _clean()
    sender = RecordingSender()  # target ignores SIGKILL (fake)
    receipt = pp.force_terminate(
        recorded, sys_f.probes(), sender=sender, force_gate=_open_gate()
    )
    assert receipt.outcome == pp.OUTCOME_FAILED
    assert receipt.reason == "still_alive"


# ── positive control: real probes against a subprocess this test created ─────

def test_positive_control_real_subprocess_full_contract():
    """Prove default_probes() + observe() + decide() + terminate_gracefully()
    work against a REAL live process — one this test spawns and owns.

    The signal sender is a recorder, so the child is never signalled; the
    contract's revalidation is exercised end-to-end on real kernel facts
    (create-time, executable, UID) with zero live-state mutation.
    """
    proc = subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdin.readline()"],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        probes = pp.default_probes()
        obs = pp.observe(proc.pid, probes)
        assert obs.error is None, obs
        assert obs.create_time, obs
        assert obs.exe, obs
        assert obs.uid is not None, obs
        assert abs(probes.now() - obs.observed_at) < pp.MAX_OBSERVATION_AGE_SECONDS

        recorded = pp.RecordedIdentity(
            pid=proc.pid,
            create_time=obs.create_time,
            exe=obs.exe,
            uid=obs.uid,
            evidence=_evidence(status="idle", kind="user"),
        )

        decision = pp.decide(obs, recorded, pp.ACTION_TERMINATE, probes)
        assert decision.outcome == pp.OUTCOME_ALLOWED, decision
        assert decision.classification == pp.CLASS_USER
        assert decision.protections == (), decision

        sender = RecordingSender()
        receipt = pp.terminate_gracefully(
            recorded, probes, sender=sender, grace_seconds=0.0
        )
        assert sender.calls == [(proc.pid, signal_mod.SIGTERM)]
        assert receipt.outcome == pp.OUTCOME_SURVIVED
        assert proc.poll() is None, "child must be alive: no real signal was sent"
    finally:
        try:
            if proc.poll() is None and proc.stdin is not None:
                proc.stdin.write("\n")
                proc.stdin.flush()
        except (BrokenPipeError, OSError):
            pass
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)
        if proc.poll() is None:  # safety net only; must not fire in practice
            proc.kill()
            proc.wait(timeout=5)


def test_positive_control_dead_pid_never_allowed():
    """Real probes against a PID that cannot be alive: fail closed, no fakes."""
    probes = pp.default_probes()
    # 2**30 is outside any sane pid_max on macOS/Linux → ESRCH, not EOVERFLOW.
    dead_pid = 2**30
    obs = pp.observe(dead_pid, probes)
    assert obs.error is not None, obs
    recorded = pp.RecordedIdentity(
        pid=dead_pid,
        create_time="whatever",
        exe="/usr/bin/whatever",
        uid=501,
        evidence=_evidence(),
    )
    receipt = pp.decide(obs, recorded, pp.ACTION_TERMINATE, probes)
    assert receipt.outcome == pp.OUTCOME_DENIED
    # Any honest failure surface is acceptable — allowing it is not.
    assert receipt.reason.startswith(
        ("process_gone", "missing_", "probe_error:", "probe_permission")
    ), receipt
