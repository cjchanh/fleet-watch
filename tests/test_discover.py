"""Tests for auto-discovery sync and config behavior."""

import sqlite3

from fleet_watch import discover, events, registry


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
