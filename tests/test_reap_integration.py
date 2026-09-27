from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import time

from fleet_watch import registry
from fleet_watch.cli_support import _terminate_orphan


def test_reap_path_terminates_only_explicitly_registered_campaign_child(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")
    conn = registry.connect()
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        registration = registry.register_disposable_workload(
            conn,
            pid=proc.pid,
            executable=sys.executable,
            owner_id="campaign-reap-test",
        )
        candidate = {
            "classification": "disposable_orphan",
            "owner_dead": True,
            "lease_active": False,
            "parent_alive": False,
            "session_alive": False,
            "stdio_peer_alive": False,
            "active_work": False,
            "inspection_complete": True,
        }
        assert _terminate_orphan(
            proc.pid,
            candidate=candidate,
            disposable_registration=registration,
            operator_confirmed=True,
        )
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and proc.poll() is None:
            time.sleep(0.02)
        assert proc.poll() is not None
    finally:
        conn.close()
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=2)
