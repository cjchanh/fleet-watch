from __future__ import annotations

from fleet_watch import process_policy as policy


def _probes(*, create="ct-1", executable="/tmp/disposable", command="disposable --job", alive=True, uid=501):
    return {
        "exists": lambda pid: alive,
        "create_time": lambda pid: create,
        "executable": lambda pid: executable,
        "uid": lambda pid: uid,
        "command": lambda pid: command,
        "inspect": lambda pid: {"ppid": 1, "pgid": pid, "tty": "??"},
    }


def _identity(**kwargs):
    return policy.snapshot_process(
        42,
        now=100.0,
        probes=_probes(**kwargs),
    )


def test_pid_create_time_executable_and_disposable_registration_are_required():
    identity = _identity()
    registration = {
        "pid": 42,
        "create_time": "ct-1",
        "executable": "/tmp/disposable",
        "uid": 501,
        "owner_id": "campaign",
        "service_class": "disposable",
    }
    candidate = {
        "owner_identity_proven": False,
        "lease_active": False,
        "parent_alive": False,
        "session_alive": False,
        "stdio_peer_alive": False,
        "active_work": False,
        "inspection_complete": True,
        "classification": "orphan_confirmed",
    }
    decision = policy.evaluate(
        identity,
        candidate=candidate,
        disposable_registration=registration,
        now=100.1,
        operator_confirmed=True,
    )
    assert decision.allowed is True
    assert decision.action == "terminate"
    assert decision.receipt()["policy_version"] == policy.POLICY_VERSION


def test_pid_reuse_stale_observation_and_protected_work_are_denied():
    identity = _identity()
    registration = {
        "create_time": "different",
        "executable": "/tmp/disposable",
        "uid": 501,
        "owner_id": "campaign",
        "service_class": "disposable",
    }
    candidate = {
        "owner_dead": True,
        "inspection_complete": True,
        "classification": "orphan_confirmed",
    }
    assert policy.evaluate(identity, candidate=candidate, disposable_registration=registration, now=100.1).reason == "pid_reuse_detected"
    stale = _identity()
    assert policy.evaluate(stale, candidate=candidate, disposable_registration={**registration, "create_time": "ct-1"}, now=200).reason == "observation_stale"
    protected = _identity(executable="/System/Library/Frameworks/WindowServer", command="WindowServer")
    assert policy.evaluate(protected, candidate=candidate, disposable_registration=registration, now=100.1).reason.startswith("protected_")


def test_revalidation_graceful_exit_and_force_denial():
    identity = _identity()
    registration = {
        "create_time": "ct-1",
        "executable": "/tmp/disposable",
        "uid": 501,
        "owner_id": "campaign",
        "service_class": "disposable",
    }
    candidate = {
        "owner_dead": True,
        "lease_active": False,
        "parent_alive": False,
        "session_alive": False,
        "stdio_peer_alive": False,
        "active_work": False,
        "inspection_complete": True,
        "classification": "orphan_confirmed",
    }
    decision = policy.evaluate(identity, candidate=candidate, disposable_registration=registration, now=100, operator_confirmed=True)
    signals = []
    clock = [100.0]
    alive = [True]
    def send(pid, sig):
        signals.append((pid, sig))
    def exists(pid):
        return alive[0]
    def now_fn():
        return clock[0]
    def sleep(seconds):
        clock[0] += seconds
    # Exit after the first poll.
    def exists_then_exit(pid):
        value = alive[0]
        alive[0] = False
        return value
    result = policy.revalidate_and_terminate(
        decision,
        candidate=candidate,
        disposable_registration=registration,
        probes=_probes(),
        send_signal=send,
        exists=exists_then_exit,
        sleep=sleep,
        now=now_fn,
        grace_seconds=0.2,
    )
    assert result.outcome == "exited"
    assert signals[0][1] == policy.signal.SIGTERM
    assert len(signals) == 1

    # A process that remains alive is denied force escalation by default.
    alive[0] = True
    clock[0] = 100.0
    result = policy.revalidate_and_terminate(
        decision,
        candidate=candidate,
        disposable_registration=registration,
        probes=_probes(),
        send_signal=send,
        exists=lambda pid: alive[0],
        sleep=sleep,
        now=now_fn,
        grace_seconds=0.1,
    )
    assert result.outcome == "still_running"
    assert all(sig != policy.signal.SIGKILL for _pid, sig in signals)
