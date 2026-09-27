from __future__ import annotations

import os
import sqlite3
import sys

import pytest

from fleet_watch import registry


def _conn():
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 0)"
    )
    conn.commit()
    return conn


def test_disposable_registration_requires_live_identity():
    conn = _conn()
    row = registry.register_disposable_workload(
        conn,
        pid=os.getpid(),
        executable=sys.executable,
        owner_id="campaign-test",
        service_class="disposable",
    )
    assert row["pid"] == os.getpid()
    assert row["owner_id"] == "campaign-test"
    assert registry.get_disposable_registration(conn, os.getpid())["pid"] == os.getpid()


def test_disposable_registration_rejects_missing_or_reused_identity(monkeypatch):
    conn = _conn()
    monkeypatch.setattr(registry, "_process_executable", lambda pid: None)
    with pytest.raises(ValueError, match="identity"):
        registry.register_disposable_workload(
            conn,
            pid=os.getpid(),
            executable=sys.executable,
            owner_id="campaign-test",
            service_class="disposable",
        )
    assert registry.get_disposable_registration(conn, os.getpid()) is None
