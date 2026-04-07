"""Tests for the referee — claim logic and budget enforcement."""

import os
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


def test_port_available():
    conn = _fresh_conn()
    d = referee.check_port(conn, 8100)
    assert d.allowed is True


def test_port_taken():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="mlx", workstream="ws", port=8100)
    d = referee.check_port(conn, 8100)
    assert d.allowed is False
    assert d.holder is not None
    assert d.holder["pid"] == 1234


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
