"""Tests for the referee — claim logic and budget enforcement."""

import os
import socket
import sqlite3

from fleet_watch import events, referee, registry


def _fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 0)"
    )
    conn.commit()
    return conn


def _free_ephemeral_port() -> int:
    """Bind 0, read the OS-assigned free port, release it. Race-tolerant for tests."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_port_available():
    conn = _fresh_conn()
    port = _free_ephemeral_port()
    d = referee.check_port(conn, port)
    assert d.allowed is True
    assert d.reason == "port available"


def test_port_taken():
    conn = _fresh_conn()
    port = _free_ephemeral_port()
    registry.register_process(conn, pid=1234, name="mlx", workstream="ws", port=port)
    d = referee.check_port(conn, port)
    assert d.allowed is False
    assert d.holder is not None
    assert d.holder["pid"] == 1234


def test_port_held_by_os_unregistered_is_denied():
    """Registry-empty + OS-held must refuse — the 8765 fail-open class.

    Proof shape (production): fleet guard --json --port 8765 returned
    allowed:true while socket.bind raised [Errno 48]. This test holds a real
    listener, leaves the registry empty, and asserts check_port refuses with
    the OS reason. Mutation-tested: without os_port_held in check_port, this
    assertion fails (allowed becomes True).
    """
    conn = _fresh_conn()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = int(listener.getsockname()[1])
    try:
        # Positive control: OS really holds it.
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.bind(("127.0.0.1", port))
            raise AssertionError("positive control failed: OS did not hold port")
        except OSError:
            pass
        finally:
            probe.close()

        d = referee.check_port(conn, port)
        assert d.allowed is False, (
            "registry was empty but OS held the port — must refuse, not "
            f"clear (got allowed={d.allowed!r} reason={d.reason!r})"
        )
        assert "not in fleet registry" in d.reason
        assert "registry absence is not availability" in d.reason
        assert d.holder is None
    finally:
        listener.close()


def test_os_port_held_true_when_listening():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = int(listener.getsockname()[1])
    try:
        assert referee.os_port_held(port) is True
    finally:
        listener.close()


def test_os_port_held_false_when_free():
    port = _free_ephemeral_port()
    assert referee.os_port_held(port) is False


def test_os_port_held_refuses_invalid_port():
    """Bad port numbers are not 'available' — fail closed."""
    assert referee.os_port_held(0) is True
    assert referee.os_port_held(-1) is True
    assert referee.os_port_held(70000) is True


def test_repo_available():
    conn = _fresh_conn()
    d = referee.check_repo(conn, "/tmp/test-repo")
    assert d.allowed is True


def test_repo_denied_by_external_resource():
    conn = _fresh_conn()
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-other",
        workstream="paper",
        name="Thunder abc123",
        repo_dir="/tmp/test-repo",
        status="RUNNING",
    )
    d = referee.check_repo(conn, "/tmp/test-repo")
    assert d.allowed is False
    assert d.holder is not None
    assert d.holder["provider"] == "thunder"


def test_repo_allowed_for_current_external_owner_session():
    conn = _fresh_conn()
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-current",
        workstream="paper",
        name="Thunder abc123",
        repo_dir="/tmp/test-repo",
        status="RUNNING",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-current")
    assert d.allowed is True


def test_repo_allows_active_cooperative_session_lease():
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn,
        "sess-other",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")
    assert d.allowed is True
    assert d.holders[0]["session_id"] == "sess-other"
    assert d.safe_mode == "declare --write-scope before editing"


def test_repo_denied_by_exclusive_session_lease():
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn,
        "sess-exclusive",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
        repo_lock_mode="exclusive",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")
    assert d.allowed is False
    assert d.holder is not None
    assert d.holder["session_id"] == "sess-exclusive"
    assert d.holder["repo_lock_mode"] == "exclusive"


def test_repo_denied_by_overlapping_write_scope():
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn,
        "sess-tools",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
        write_scopes=["tools/playwright"],
    )
    d = referee.check_repo_with_session(
        conn,
        "/tmp/test-repo",
        current_session_id="sess-mine",
        write_scopes=["tools/playwright/edit_post.py"],
    )
    assert d.allowed is False
    assert d.holder is not None
    assert d.holder["session_id"] == "sess-tools"
    assert any(path.endswith("tools/playwright") for path in d.overlap_paths)


def test_repo_allows_disjoint_write_scope():
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn,
        "sess-docs",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
        write_scopes=["docs"],
    )
    d = referee.check_repo_with_session(
        conn,
        "/tmp/test-repo",
        current_session_id="sess-mine",
        write_scopes=["tools/playwright"],
    )
    assert d.allowed is True
    assert d.holders[0]["session_id"] == "sess-docs"
    assert d.safe_mode == "cooperative-write"


def test_repo_allowed_for_current_session_lease():
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn,
        "sess-current",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-current")
    assert d.allowed is True
    assert "owned by current session" in d.reason


def test_repo_releases_cooperative_lease_with_dead_owner(monkeypatch):
    """Path C (DECOUPLE): a cooperative lease whose owner is PROVEN DEAD is
    released immediately, independent of heartbeat freshness. A dead owner is
    not a live advisory holder — the lease is closed and surfaced as stale.
    (Replaces the prior test asserting the buggy behavior of keeping a fresh
    dead-owner lease ACTIVE.)"""
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: False)
    # Fresh heartbeat (5s) — old behavior kept it ACTIVE; Path C closes it.
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 5 if ts else None)
    registry.upsert_session_lease(
        conn,
        "sess-dead-owner",
        owner_pid=99999,
        repo_dir="/tmp/test-repo",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")
    assert d.allowed is True
    assert d.holders == []
    assert d.stale_holders[0]["session_id"] == "sess-dead-owner"
    lease = registry.get_session_lease(conn, "sess-dead-owner")
    assert lease["status"] == "CLOSED"


def test_repo_allowed_when_owner_dead_and_heartbeat_stale(monkeypatch):
    """A lease whose owner PID is dead AND heartbeat is stale does not block the repo."""
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: False)
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 999 if ts else None)
    registry.upsert_session_lease(
        conn,
        "sess-dead-stale",
        owner_pid=99999,
        repo_dir="/tmp/test-repo",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")
    assert d.allowed is True
    assert d.stale_holders[0]["session_id"] == "sess-dead-stale"


def test_repo_cleans_ownerless_stale_session_lease(monkeypatch):
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 999 if ts else None)
    registry.upsert_session_lease(
        conn,
        "sess-ownerless-stale",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
    )

    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")

    assert d.allowed is True
    assert d.stale_holders[0]["session_id"] == "sess-ownerless-stale"
    lease = registry.get_session_lease(conn, "sess-ownerless-stale")
    assert lease["status"] == "CLOSED"


def test_repo_allowed_for_current_local_owner_session():
    """Same-session bypass works for local process repo locks, not just external."""
    conn = _fresh_conn()
    registry.register_process(
        conn,
        pid=os.getpid(),
        name="writer",
        workstream="test",
        repo_dir="/tmp/test-repo",
        session_id="sess-current",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-current")
    assert d.allowed is True
    assert "owned by current session" in d.reason


def test_repo_denied_for_different_local_session():
    """Different session is denied even for local processes."""
    conn = _fresh_conn()
    registry.register_process(
        conn,
        pid=os.getpid(),
        name="writer",
        workstream="test",
        repo_dir="/tmp/test-repo",
        session_id="sess-other",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")
    assert d.allowed is False


def test_gpu_budget_fits():
    conn = _fresh_conn()
    d = referee.check_gpu_budget(conn, 50000)
    assert d.allowed is True


def test_gpu_budget_overflow():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="a", workstream="ws", gpu_mb=100000)
    d = referee.check_gpu_budget(conn, 50000)
    assert d.allowed is False
    assert "exceeded" in d.reason


def test_gpu_zero_always_ok():
    conn = _fresh_conn()
    d = referee.check_gpu_budget(conn, 0)
    assert d.allowed is True


def test_preflight_all_clear():
    conn = _fresh_conn()
    failures = referee.preflight_register(conn, port=8100, gpu_mb=1000, repo_dir="/tmp/r")
    assert failures == []


def test_preflight_port_conflict():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="a", workstream="ws", port=8100)
    failures = referee.preflight_register(conn, port=8100)
    assert len(failures) == 1
    assert "8100" in failures[0].reason


def test_preflight_multiple_failures():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="a", workstream="ws", port=8100, gpu_mb=120000)
    failures = referee.preflight_register(conn, port=8100, gpu_mb=50000)
    assert len(failures) == 2


def test_claim_port_logs_event():
    conn = _fresh_conn()
    referee.claim_port(conn, 8100)
    evts = events.get_events(conn, hours=1, event_type="CLAIM")
    assert len(evts) == 1


def test_claim_port_conflict_logs_event():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="a", workstream="ws", port=8100)
    referee.claim_port(conn, 8100)
    evts = events.get_events(conn, hours=1, event_type="CONFLICT")
    assert len(evts) == 1


def test_preempt_higher_priority():
    conn = _fresh_conn()
    registry.register_process(conn, pid=2147483646, name="low", workstream="ws", port=8100, priority=2)
    # Use grace=0 for test speed — PID won't exist anyway
    d = referee.preempt_port(conn, 8100, new_priority=5, reason="test", grace_seconds=0)
    assert d.allowed is True
    # Port should be free now
    assert registry.get_process_by_port(conn, 8100) is None


def test_preempt_lower_priority_denied():
    conn = _fresh_conn()
    registry.register_process(conn, pid=2147483646, name="high", workstream="ws", port=8100, priority=5)
    d = referee.preempt_port(conn, 8100, new_priority=3, reason="test", grace_seconds=0)
    assert d.allowed is False


def test_preempt_empty_port():
    conn = _fresh_conn()
    d = referee.preempt_port(conn, 8100, new_priority=5, reason="test")
    assert d.allowed is True
    assert "free" in d.reason


def test_suggest_ports_skips_taken_and_requested():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="held", workstream="ws", port=8000)
    registry.register_process(conn, pid=2, name="held2", workstream="ws", port=8100)

    ports = referee.suggest_ports(
        conn,
        preferred_ports=[8000, 8001, 8100, 8899],
        requested_port=8899,
    )

    assert 8000 not in ports
    assert 8100 not in ports
    assert 8899 not in ports
    assert ports[0] == 8001
