from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

from fleet_watch import process_policy as policy
from fleet_watch import registry


def test_real_disposable_subprocess_terminates_after_fresh_revalidation(tmp_path):
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        # The campaign is the sole owner/creator of this test subprocess.
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.executescript(registry.SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
            "VALUES (1, 131072, 16384, 0)"
        )
        conn.commit()
        registration = registry.register_disposable_workload(
            conn,
            pid=proc.pid,
            executable=sys.executable,
            owner_id="campaign-integration-test",
            service_class="disposable",
        )
        conn.close()
        identity = policy.snapshot_process(proc.pid)
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
        decision = policy.evaluate(
            identity,
            candidate=candidate,
            disposable_registration=registration,
            operator_confirmed=True,
        )
        assert decision.allowed is True
        result = policy.revalidate_and_terminate(
            decision,
            candidate=candidate,
            disposable_registration=registration,
            grace_seconds=1.0,
        )
        assert result.outcome == "exited"
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and proc.poll() is None:
            time.sleep(0.02)
        assert proc.poll() is not None
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=2)
