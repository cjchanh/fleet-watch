"""Tests for the MCP orphan detector (NS-17 / B3). The load-bearing invariant:
a live-session MCP server is NEVER classified as an orphan."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fleet_watch.discovery import mcp_orphan_detector as M


def proc(pid, ppid, rss_mb=70):
    return {"pid": pid, "ppid": ppid, "rss_mb": rss_mb,
            "cmd": "python3 mcp_compile_server.py"}


class TestClassify(unittest.TestCase):
    def test_live_session_server_never_orphan(self):
        # Parent alive -> keeper. This is the invariant.
        live = {100}
        orphans, keepers = M.classify_mcp_orphans(
            [proc(200, 100)], pid_alive=lambda pid: pid in live)
        self.assertEqual(orphans, [])
        self.assertEqual(len(keepers), 1)

    def test_dead_session_server_is_orphan(self):
        orphans, keepers = M.classify_mcp_orphans(
            [proc(200, 999)], pid_alive=lambda pid: False)
        self.assertEqual(len(orphans), 1)
        self.assertEqual(keepers, [])

    def test_reparented_ppid1_is_orphan(self):
        # ppid<=1 means the parent died and it reparented to init -> orphan,
        # regardless of what pid_alive says about pid 1.
        orphans, _ = M.classify_mcp_orphans(
            [proc(200, 1)], pid_alive=lambda pid: True)
        self.assertEqual(len(orphans), 1)

    def test_mixed_only_dead_sessions_flagged(self):
        live = {100}  # session 100 alive, 999 dead
        procs = [proc(201, 100), proc(202, 100), proc(203, 999)]
        orphans, keepers = M.classify_mcp_orphans(
            procs, pid_alive=lambda pid: pid in live)
        self.assertEqual({o["pid"] for o in orphans}, {203})
        self.assertEqual({k["pid"] for k in keepers}, {201, 202})


class TestDetectResult(unittest.TestCase):
    def test_recovered_mb_and_kill_command(self):
        # Inject by monkeypatching _get_mcp_processes.
        orig = M._get_mcp_processes
        try:
            M._get_mcp_processes = lambda: [proc(201, 100, 50), proc(202, 999, 80)]
            res = M.detect(pid_alive=lambda pid: pid == 100)
            self.assertTrue(res.orphans_detected)
            self.assertEqual(res.orphan_pids, [202])
            self.assertEqual(res.live_pids, [201])
            self.assertEqual(res.estimated_recovered_mb, 80)
            self.assertIn("kill 202", res.suggested_kill_command)
            self.assertEqual(res.mcp_process_count, 2)
        finally:
            M._get_mcp_processes = orig

    def test_no_orphans_clean(self):
        orig = M._get_mcp_processes
        try:
            M._get_mcp_processes = lambda: [proc(201, 100)]
            res = M.detect(pid_alive=lambda pid: True)
            self.assertFalse(res.orphans_detected)
            self.assertEqual(res.suggested_kill_command, "")
        finally:
            M._get_mcp_processes = orig

    def test_regex_matches_cds_mcp_servers(self):
        for name in ("mcp_compile_server.py", "mcp_engines_server.py",
                     "mcp_graph_context_server.py", "mcp_sitrep_server.py"):
            self.assertRegex(name, M._MCP_SERVER_RE)


if __name__ == "__main__":
    unittest.main(verbosity=2)
