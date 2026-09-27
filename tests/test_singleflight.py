from __future__ import annotations

import threading
import time

import pytest

from fleet_watch import discover, registry
from fleet_watch.singleflight import SingleFlight, SingleFlightBusy


def test_single_flight_rejects_overlap_without_starting_second_worker(tmp_path):
    gate = threading.Event()
    entered = threading.Event()
    calls: list[str] = []
    flight = SingleFlight(tmp_path / "cycle.lock", timeout_seconds=0.05)

    def first():
        with flight:
            calls.append("first")
            entered.set()
            assert gate.wait(1.0)

    thread = threading.Thread(target=first)
    thread.start()
    assert entered.wait(1.0)
    with pytest.raises(SingleFlightBusy):
        with flight:
            calls.append("second")
    gate.set()
    thread.join(1.0)
    assert not thread.is_alive()
    assert calls == ["first"]


def test_single_flight_releases_after_exception(tmp_path):
    flight = SingleFlight(tmp_path / "cycle.lock", timeout_seconds=0.1)
    with pytest.raises(RuntimeError):
        with flight:
            raise RuntimeError("probe failed")
    with flight:
        pass


def test_discovery_result_is_list_compatible_and_carries_status():
    result = discover.DiscoveryResult([], status="UNKNOWN", errors=[{"probe": "ps"}])
    assert list(result) == []
    assert result.status == "UNKNOWN"
    assert result.errors == [{"probe": "ps"}]


def test_sync_unknown_collection_does_not_mutate_registry(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = _fresh_conn()
    registry.register_process(conn, pid=4242, name="keep", workstream="test")
    before = registry.get_process(conn, 4242)
    monkeypatch.setattr(
        discover,
        "discover",
        lambda config=None: discover.DiscoveryResult(
            [], status="UNKNOWN", errors=[{"probe": "ps", "error": "timeout"}]
        ),
    )

    result = discover.sync(conn)

    assert result["status"] == "UNKNOWN"
    assert result["reason"] == "probe_failure"
    assert registry.get_process(conn, 4242) == before


def test_sync_lock_contention_is_unknown_not_empty_success(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = _fresh_conn()
    flight = SingleFlight(tmp_path / ".discover.lock", timeout_seconds=0.05)
    gate = threading.Event()
    entered = threading.Event()

    def holder():
        with flight:
            entered.set()
            assert gate.wait(1.0)

    thread = threading.Thread(target=holder)
    thread.start()
    assert entered.wait(1.0)
    try:
        result = discover.sync(conn, lock_timeout_seconds=0.05)
    finally:
        gate.set()
        thread.join(1.0)

    assert result["status"] == "UNKNOWN"
    assert result["reason"] == "discovery_in_progress"
    assert result["added"] == []
    assert result["cleaned"] == []


def _patch_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")


def _fresh_conn():
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 0)"
    )
    conn.commit()
    return conn
