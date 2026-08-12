"""Tests for auto-discovery sync and config behavior."""

import json
import sqlite3
import time

from fleet_watch import discover, events, registry
from fleet_watch import syshealth


def _patch_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")


def _fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 0)"
    )
    conn.commit()
    return conn


def test_load_config_writes_default_when_missing(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)

    config = discover.load_config()

    assert (tmp_path / "config.json").exists()
    assert config["preferred_ports"]


def test_sync_reports_skipped_conflict(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="held", workstream="ws", port=8100)

    monkeypatch.setattr(
        discover,
        "discover",
        lambda config=None: [
            discover.DiscoveredProcess(
                pid=2,
                port=8100,
                listener_owned=True,
                name="new",
                workstream="ws",
                model=None,
                gpu_mb=0,
                priority=3,
                restart_policy="ALERT_ONLY",
                command="python -m new",
            )
        ],
    )

    result = discover.sync(conn)
    conflict_events = events.get_events(conn, hours=1, event_type="CONFLICT")

    assert result["added"] == []
    assert result["skipped"][0]["pid"] == 2
    assert conflict_events


def test_sync_thunder_auto_sync(tmp_path, monkeypatch):
    """Discovery cycle syncs Thunder instances when tnr is available."""
    _patch_paths(monkeypatch, tmp_path)
    conn = _fresh_conn()

    tnr_payload = json.dumps([
        {"id": "0", "uuid": "mmtezz03", "name": "mmtezz03", "status": "RUNNING"},
        {"id": "1", "uuid": "tcrsdox3", "name": "tcrsdox3", "status": "RUNNING"},
    ])

    class FakeResult:
        def __init__(self):
            self.stdout = "Fetching instances...\n" + tnr_payload
            self.stderr = ""
            self.returncode = 0

    monkeypatch.setattr(
        discover.subprocess,
        "run",
        lambda *args, **kwargs: FakeResult(),
    )
    # Stub local discovery to return nothing
    monkeypatch.setattr(discover, "discover", lambda config=None: [])

    result = discover.sync(conn)

    assert result["thunder_synced"] == 2
    resources = registry.get_all_external_resources(conn)
    assert len(resources) == 2
    assert resources[0]["provider"] == "thunder"


def test_sync_thunder_unavailable_is_silent(tmp_path, monkeypatch):
    """Discovery cycle continues without error when tnr is not installed."""
    _patch_paths(monkeypatch, tmp_path)
    conn = _fresh_conn()

    original_run = discover.subprocess.run

    def _mock_run(cmd, **kwargs):
        if cmd[0] == "tnr":
            raise FileNotFoundError("tnr not found")
        return original_run(cmd, **kwargs)

    monkeypatch.setattr(discover.subprocess, "run", _mock_run)
    monkeypatch.setattr(discover, "discover", lambda config=None: [])

    result = discover.sync(conn)

    assert result["thunder_synced"] == 0
    assert registry.get_all_external_resources(conn) == []


# --- parse_tnr_instances_output ---


class TestParseTnrInstancesOutput:
    def test_no_instances_in_stderr(self):
        result = discover.parse_tnr_instances_output(
            "", "Fetching instances...\nNo instances found.\n"
        )
        assert result == []

    def test_no_instances_in_stdout(self):
        result = discover.parse_tnr_instances_output(
            "Fetching instances...\nNo instances found.\n", ""
        )
        assert result == []

    def test_valid_json_in_stdout(self):
        result = discover.parse_tnr_instances_output(
            '[{"id":"0","status":"RUNNING"}]', "Fetching instances...\n"
        )
        assert result == [{"id": "0", "status": "RUNNING"}]

    def test_json_with_preamble(self):
        result = discover.parse_tnr_instances_output(
            'Fetching instances...\n[{"id":"1","status":"STARTING"}]', ""
        )
        assert result == [{"id": "1", "status": "STARTING"}]

    def test_garbage_returns_none(self):
        result = discover.parse_tnr_instances_output(
            "Something weird happened", "error text"
        )
        assert result is None

    def test_empty_returns_none(self):
        result = discover.parse_tnr_instances_output("", "")
        assert result is None

    def test_stderr_bracket_not_parsed_as_json(self):
        """stderr containing '[' must not be mistaken for an instance list."""
        result = discover.parse_tnr_instances_output(
            "", 'error: [broken stuff]\nNo instances found.'
        )
        assert result == []

    def test_stderr_bracket_no_sentinel_returns_none(self):
        result = discover.parse_tnr_instances_output("", "error: [broken stuff]")
        assert result is None


# --- _prefer_listener_owned_processes ---


def _proc(pid: int, port: int | None, listener_owned: bool, name: str = "Svc") -> discover.DiscoveredProcess:
    return discover.DiscoveredProcess(
        pid=pid, port=port, listener_owned=listener_owned, name=name,
        workstream="ws", model=None, gpu_mb=0, priority=3,
        restart_policy="ALERT_ONLY", command="python test.py",
    )


class TestPreferListenerOwned:
    def test_listener_beats_parent(self):
        parent = _proc(829, 4343, False, "Reranker")
        child = _proc(1242, 4343, True, "Reranker")
        result = discover._prefer_listener_owned_processes([parent, child])
        assert len(result) == 1
        assert result[0].pid == 1242

    def test_both_listeners_lower_pid_wins(self):
        a = _proc(100, 8080, True)
        b = _proc(200, 8080, True)
        result = discover._prefer_listener_owned_processes([b, a])
        assert len(result) == 1
        assert result[0].pid == 100

    def test_no_port_passes_through(self):
        parent = _proc(829, 4343, False, "Reranker")
        child = _proc(1242, 4343, True, "Reranker")
        worker = _proc(999, None, False, "Worker")
        result = discover._prefer_listener_owned_processes([parent, child, worker])
        assert len(result) == 2
        pids = {p.pid for p in result}
        assert pids == {999, 1242}

    def test_different_keys_not_merged(self):
        a = _proc(100, 8080, True, "SvcA")
        b = _proc(200, 8080, True, "SvcB")
        result = discover._prefer_listener_owned_processes([a, b])
        assert len(result) == 2


# --- Thunder empty-output clears stale rows ---


def test_sync_thunder_empty_clears_stale(tmp_path, monkeypatch):
    """When tnr returns 'No instances found', stale Thunder rows are cleared."""
    _patch_paths(monkeypatch, tmp_path)
    conn = _fresh_conn()

    registry.register_external_resource(
        conn, provider="thunder", resource_type="instance",
        external_id="stale-abc", workstream="thunder", name="Thunder stale-abc",
        status="RUNNING",
    )
    assert len(registry.get_all_external_resources(conn)) == 1

    class FakeResult:
        def __init__(self):
            self.stdout = "Fetching instances...\nNo instances found.\n"
            self.stderr = ""
            self.returncode = 0

    monkeypatch.setattr(
        discover.subprocess, "run",
        lambda *args, **kwargs: FakeResult(),
    )
    monkeypatch.setattr(discover, "discover", lambda config=None: [])

    result = discover.sync(conn)

    assert result["thunder_synced"] == 0
    assert registry.get_all_external_resources(conn) == []


# --- Listener reassignment in sync ---


def test_sync_reassigns_parent_to_listener(tmp_path, monkeypatch):
    """sync() replaces a registered parent PID with the actual listener child."""
    _patch_paths(monkeypatch, tmp_path)
    conn = _fresh_conn()

    registry.register_process(
        conn, pid=829, name="Reranker", workstream="ws", port=4343,
    )
    assert registry.get_process(conn, 829) is not None

    monkeypatch.setattr(
        discover, "discover",
        lambda config=None: [
            discover.DiscoveredProcess(
                pid=1242, port=4343, listener_owned=True, name="Reranker",
                workstream="ws", model=None, gpu_mb=0, priority=3,
                restart_policy="ALERT_ONLY", command="python reranker.py",
            ),
        ],
    )

    def _fake_thunder(*args, **kwargs):
        raise FileNotFoundError("tnr not found")

    monkeypatch.setattr(discover.subprocess, "run", _fake_thunder)

    result = discover.sync(conn)

    assert registry.get_process(conn, 829) is None
    assert registry.get_process(conn, 1242) is not None
    assert registry.get_process(conn, 1242)["port"] == 4343
    assert any(c["pid"] == 829 for c in result["cleaned"])
    assert any(a["pid"] == 1242 for a in result["added"])


def test_clean_stale_session_leases_closes_dead_stale(monkeypatch):
    """Stale session leases with dead owners are closed during discover."""
    conn = _fresh_conn()
    registry.upsert_session_lease(conn, "stale-sess", owner_pid=99999)
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid != 99999)
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 999 if ts else None)

    cleaned = discover._clean_stale_session_leases(conn)

    assert cleaned == 1
    lease = registry.get_session_lease(conn, "stale-sess")
    assert lease["status"] == "CLOSED"


def test_clean_stale_session_leases_skips_fresh_heartbeat(monkeypatch):
    """Leases with dead owner but fresh heartbeat are NOT closed."""
    conn = _fresh_conn()
    registry.upsert_session_lease(conn, "fresh-sess", owner_pid=99999)
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid != 99999)
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 30 if ts else None)

    cleaned = discover._clean_stale_session_leases(conn)

    assert cleaned == 0
    lease = registry.get_session_lease(conn, "fresh-sess")
    assert lease["status"] == "ACTIVE"


def test_clean_stale_session_leases_skips_alive_owner():
    """Leases with alive owners are never touched."""
    conn = _fresh_conn()
    import os
    registry.upsert_session_lease(conn, "alive-sess", owner_pid=os.getpid())

    cleaned = discover._clean_stale_session_leases(conn)

    assert cleaned == 0
    lease = registry.get_session_lease(conn, "alive-sess")
    assert lease["status"] == "ACTIVE"


def test_gpu_memory_monitor_logs_transition_alerts(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = _fresh_conn()

    prior = {
        "timestamp_s": time.time() - 60,
        "memory": syshealth.MemoryState(
            8192, 4000, 1000, 2000, 0, 1000, pageouts=100, swapins=0
        ).to_dict(),
        "overcommit_pids": [],
        "thrashing": False,
    }
    discover.gpu_monitor_state_path().write_text(json.dumps(prior) + "\n")

    monkeypatch.setattr(
        discover.syshealth,
        "get_memory_state",
        lambda: syshealth.MemoryState(
            8192, 4500, 900, 1800, 0, 1000, pageouts=70000, swapins=100
        ),
    )
    monkeypatch.setattr(
        discover.syshealth,
        "get_gpu_workload_footprints",
        lambda processes: [
            syshealth.ProcessFootprint(
                pid=1234, name="cake", resident_mb=7000, dirty_mb=5000, swapped_mb=512
            )
        ],
    )

    snapshot = discover._run_gpu_memory_monitor(
        conn,
        [
            discover.DiscoveredProcess(
                pid=1234,
                port=None,
                listener_owned=False,
                name="cake",
                workstream="inference",
                model="qwen2.5-7B-Q4_K_M.gguf",
                gpu_mb=4096,
                priority=3,
                restart_policy="ALERT_ONLY",
                command="./cake --model qwen2.5-7B-Q4_K_M.gguf",
            )
        ],
        config={},
    )

    alerts = snapshot["alerts"]
    assert len(alerts) == 2
    assert {alert["type"] for alert in alerts} == {
        "pageout_thrashing",
        "process_footprint_overcommit",
    }

    logged = events.get_events(conn, hours=1, event_type="GPU_MEMORY_PRESSURE")
    assert len(logged) == 2
