from __future__ import annotations

import sqlite3
import subprocess

from fleet_watch import discover, registry


def _patch_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 0)"
    )
    conn.commit()
    return conn


def test_total_ps_failure_is_unknown_not_empty_success(monkeypatch, tmp_path):
    _patch_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(discover, "_get_listeners_with_status", lambda **kwargs: ({}, "OK", []))
    monkeypatch.setattr(
        discover.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(args[0], kwargs.get("timeout", 1))
        ),
    )
    result = discover.discover(config={"patterns": []})
    assert result.status == "UNKNOWN"
    assert result.errors


def test_unknown_discovery_does_not_clean_registered_state(monkeypatch, tmp_path):
    _patch_paths(monkeypatch, tmp_path)
    conn = _conn()
    registry.register_process(conn, pid=321, name="keep", workstream="test")
    monkeypatch.setattr(
        discover,
        "discover",
        lambda config=None: discover.DiscoveryResult(
            [], status="UNKNOWN", errors=[{"probe": "ps", "error": "timeout"}]
        ),
    )
    result = discover.sync(conn)
    assert result["status"] == "UNKNOWN"
    assert registry.get_process(conn, 321) is not None
