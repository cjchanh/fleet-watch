"""Tests for the process registry."""

import os
import sqlite3

from fleet_watch import registry


def _fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 0)"
    )
    conn.commit()
    return conn


def test_register_and_get():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="test", workstream="ws")
    proc = registry.get_process(conn, 1234)
    assert proc is not None
    assert proc["name"] == "test"
    assert proc["workstream"] == "ws"
    assert proc["priority"] == 3


def test_register_with_port():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="mlx", workstream="ws", port=8100)
    proc = registry.get_process_by_port(conn, 8100)
    assert proc is not None
    assert proc["pid"] == 1234


def test_port_uniqueness():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="a", workstream="ws", port=8100)
    try:
        registry.register_process(conn, pid=5678, name="b", workstream="ws", port=8100)
        assert False, "Should have raised IntegrityError"
    except sqlite3.IntegrityError:
        pass


def test_repo_uniqueness():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="a", workstream="ws", repo_dir="/tmp/test-repo")
    try:
        registry.register_process(conn, pid=5678, name="b", workstream="ws", repo_dir="/tmp/test-repo")
        assert False, "Should have raised IntegrityError"
    except sqlite3.IntegrityError:
        pass


def test_gpu_budget_tracking():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="mlx", workstream="ws", gpu_mb=54000)
    budget = registry.get_gpu_budget(conn)
    assert budget["allocated_mb"] == 54000
    assert budget["available_mb"] == 131072 - 16384 - 54000


def test_release_restores_gpu():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="mlx", workstream="ws", gpu_mb=54000)
    registry.release_process(conn, 1234)
    budget = registry.get_gpu_budget(conn)
    assert budget["allocated_mb"] == 0


def test_heartbeat():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="test", workstream="ws")
    proc_before = registry.get_process(conn, 1234)
    assert registry.heartbeat(conn, 1234) is True
    proc_after = registry.get_process(conn, 1234)
    assert proc_after["last_heartbeat"] >= proc_before["last_heartbeat"]


def test_heartbeat_unknown_pid():
    conn = _fresh_conn()
    assert registry.heartbeat(conn, 9999) is False


def test_release_unknown_pid():
    conn = _fresh_conn()
    assert registry.release_process(conn, 9999) is None


def test_get_all_processes():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="a", workstream="ws1", priority=2)
    registry.register_process(conn, pid=2, name="b", workstream="ws2", priority=4)
    procs = registry.get_all_processes(conn)
    assert len(procs) == 2
    # Higher priority first
    assert procs[0]["priority"] == 4


def test_claimed_ports():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="a", workstream="ws", port=8100)
    registry.register_process(conn, pid=2, name="b", workstream="ws", port=8899)
    ports = registry.get_claimed_ports(conn)
    assert ports == {8100: 1, 8899: 2}


def test_clean_dead_pids():
    conn = _fresh_conn()
    # Register with a PID that definitely doesn't exist
    registry.register_process(conn, pid=2147483647, name="dead", workstream="ws", gpu_mb=1000)
    cleaned = registry.clean_dead_pids(conn)
    assert len(cleaned) == 1
    assert cleaned[0]["pid"] == 2147483647
    # GPU budget should be restored
    budget = registry.get_gpu_budget(conn)
    assert budget["allocated_mb"] == 0


def test_invalid_priority():
    conn = _fresh_conn()
    try:
        registry.register_process(conn, pid=1, name="test", workstream="ws", priority=0)
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


def test_invalid_restart_policy():
    conn = _fresh_conn()
    try:
        registry.register_process(conn, pid=1, name="test", workstream="ws", restart_policy="INVALID")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass
