"""Tests for the process registry."""

import json
import os
import sqlite3
from pathlib import Path

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


def test_register_process_creates_auto_session_lease():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="test", workstream="ws")
    lease = registry.get_session_lease(conn, "cli-1234")
    assert lease is not None
    assert lease["status"] == "ACTIVE"
    assert lease["owner_pid"] == 1234


def test_classify_process_orphan_confirmed_when_stale_and_closed(monkeypatch):
    conn = _fresh_conn()
    registry.register_process(conn, pid=5555, name="stale", workstream="ws")
    assert registry.close_session_lease(conn, "cli-5555") is True

    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid == 5555)
    monkeypatch.setattr(
        registry,
        "_inspect_process",
        lambda pid: {
            "pid": pid,
            "alive": True,
            "inspectable": True,
            "ppid": 1,
            "pgid": 5555,
            "tty": "?",
        } if pid == 5555 else None,
    )
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 600 if ts else None)

    items = registry.get_process_classifications(conn)
    assert len(items) == 1
    assert items[0]["classification"] == "orphan_confirmed"
    assert items[0]["safe_to_reap"] is True


def test_classify_discovered_process_without_lease_is_disconnected(monkeypatch):
    conn = _fresh_conn()
    registry.register_process(
        conn,
        pid=6666,
        name="observed",
        workstream="ws",
        manage_session_lease=False,
    )

    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid in {6666, 7777})
    monkeypatch.setattr(
        registry,
        "_inspect_process",
        lambda pid: {
            "pid": pid,
            "alive": True,
            "inspectable": True,
            "ppid": 7777 if pid == 6666 else 1,
            "pgid": 6666,
            "tty": "ttys000",
        } if pid in {6666, 7777} else None,
    )
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 30 if ts else None)

    items = registry.get_process_classifications(conn)
    assert len(items) == 1
    assert items[0]["classification"] == "disconnected"
    assert items[0]["session_lease_present"] is False


def test_classify_stale_detached_process_without_lease_stays_disconnected(monkeypatch):
    conn = _fresh_conn()
    registry.register_process(
        conn,
        pid=7777,
        name="observed-stale",
        workstream="ws",
        manage_session_lease=False,
    )

    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid == 7777)
    monkeypatch.setattr(
        registry,
        "_inspect_process",
        lambda pid: {
            "pid": pid,
            "alive": True,
            "inspectable": True,
            "ppid": 1,
            "pgid": 7777,
            "tty": "??",
        } if pid == 7777 else None,
    )
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 600 if ts else None)

    items = registry.get_process_classifications(conn)
    assert len(items) == 1
    assert items[0]["classification"] == "disconnected"
    assert items[0]["safe_to_reap"] is False


def test_parent_chain_detached_walks_ancestors(monkeypatch):
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid in {100, 90, 80})

    def fake_inspect(pid):
        mapping = {
            100: {"pid": 100, "alive": True, "inspectable": True, "ppid": 90, "pgid": 80, "tty": "??"},
            90: {"pid": 90, "alive": True, "inspectable": True, "ppid": 80, "pgid": 80, "tty": "??"},
            80: {"pid": 80, "alive": True, "inspectable": True, "ppid": 1, "pgid": 80, "tty": "ttys003"},
        }
        return mapping.get(pid)

    monkeypatch.setattr(registry, "_inspect_process", fake_inspect)

    assert registry._is_parent_chain_detached(100) is False


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


def test_connect_applies_configured_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")
    (tmp_path / "config.json").write_text(
        json.dumps({"gpu_total_mb": 65536, "gpu_reserve_mb": 8192})
    )

    conn = registry.connect()
    budget = registry.get_gpu_budget(conn)

    assert budget["total_mb"] == 65536
    assert budget["reserve_mb"] == 8192


def test_register_and_get_external_resource():
    conn = _fresh_conn()
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-1",
        workstream="paper",
        name="Thunder abc123",
        repo_dir="/tmp/fleet-watch",
        status="RUNNING",
        metadata={"id": "0"},
    )
    resource = registry.get_external_resource(conn, provider="thunder", external_id="abc123")
    assert resource is not None
    assert resource["provider"] == "thunder"
    assert resource["external_id"] == "abc123"
    assert resource["repo_dir"] == str(Path("/tmp/fleet-watch").resolve())
    assert resource["metadata"]["id"] == "0"


def test_release_external_resource():
    conn = _fresh_conn()
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-1",
        workstream="paper",
        name="Thunder abc123",
    )
    released = registry.release_external_resource(conn, provider="thunder", external_id="abc123")
    assert released is not None
    assert released["external_id"] == "abc123"
    assert registry.get_external_resource(conn, provider="thunder", external_id="abc123") is None


def test_replace_provider_resources_preserves_claim_metadata():
    conn = _fresh_conn()
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-1",
        workstream="paper",
        name="Claimed resource",
        repo_dir="/tmp/fleet-watch",
        endpoint="http://127.0.0.1:8000/v1/chat/completions",
        metadata={"id": "0"},
    )
    registry.replace_provider_resources(
        conn,
        provider="thunder",
        resources=[
            {
                "resource_type": "instance",
                "external_id": "abc123",
                "name": "Thunder abc123",
                "status": "RUNNING",
                "metadata": {"id": "0", "gpuType": "A100"},
                "cleanup_cmd": "tnr delete 0 --yes",
            }
        ],
    )
    resource = registry.get_external_resource(conn, provider="thunder", external_id="abc123")
    assert resource is not None
    assert resource["session_id"] == "sess-1"
    assert resource["repo_dir"] == str(Path("/tmp/fleet-watch").resolve())
    assert resource["metadata"]["gpuType"] == "A100"
