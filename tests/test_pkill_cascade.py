"""Tests for fleet pkill cascade wrapper (H4)."""

from __future__ import annotations

from fleet_watch import pkill as pkill_module
from fleet_watch.pkill import (
    PkillResult,
    PkillTarget,
    execute_pkill,
)


class TestPkillResult:
    def test_dry_run_by_default(self):
        r = PkillResult(pattern="test")
        assert r.dry_run is True
        assert r.confirmed is False
        assert not r.killed_any

    def test_killed_any_detects_kills(self):
        r = PkillResult(pattern="test", pids_killed=[100, 200])
        assert r.killed_any

    def test_to_dict(self):
        r = PkillResult(
            pattern="ollama-runner",
            cascade=True,
            confirmed=True,
            dry_run=False,
            pids_killed=[100],
            children_killed=[200, 300],
            total_rss_freed_mb=1024,
        )
        d = r.to_dict()
        assert d["pattern"] == "ollama-runner"
        assert d["dry_run"] is False
        assert d["pids_killed"] == [100]
        assert d["children_killed"] == [200, 300]
        assert d["total_rss_freed_mb"] == 1024


class TestPkillTarget:
    def test_to_dict(self):
        child = PkillTarget(pid=200, name="child", cmdline="runner child",
                            rss_mb=100, ppid=100)
        parent = PkillTarget(pid=100, name="parent", cmdline="runner parent",
                             rss_mb=200, ppid=1, children=[child])
        d = parent.to_dict()
        assert d["pid"] == 100
        assert len(d["children"]) == 1
        assert d["children"][0]["pid"] == 200


class TestExecutePkill:
    def test_dry_run_does_not_kill(self):
        result = execute_pkill(pattern="nonexistent_pattern_xyz",
                                cascade=False, confirm=False)
        # Dry-run with no match should return errors
        assert result.dry_run
        assert not result.confirmed
        assert len(result.pids_killed) == 0

    def test_no_match_reports_error(self):
        result = execute_pkill(pattern="nonexistent_pattern_xyz",
                                cascade=False, confirm=True)
        assert len(result.errors) > 0

    def test_confirm_flag_propagates(self, monkeypatch):
        terminated: list[int] = []
        monkeypatch.setattr(
            pkill_module,
            "_find_matching_pids",
            lambda pattern: [{"pid": 424242}],
        )
        monkeypatch.setattr(
            pkill_module,
            "_get_process_detail",
            lambda pid: {
                "pid": pid,
                "ppid": 1,
                "rss_mb": 1,
                "name": "fixture",
                "cmdline": "fixture test process",
            },
        )
        monkeypatch.setattr(
            pkill_module,
            "_terminate_process",
            lambda pid: (terminated.append(pid), True)[1],
        )

        result = execute_pkill(pattern="test", cascade=False, confirm=True)
        assert result.confirmed
        assert not result.dry_run
        assert terminated == [424242]

    def test_cascade_flag_propagates(self):
        result = execute_pkill(pattern="test", cascade=True, confirm=False)
        assert result.cascade

    def test_dry_run_default(self):
        result = execute_pkill(pattern="test")
        assert result.dry_run
        assert not result.confirmed

    def test_to_dict_roundtrip(self):
        r = PkillResult(pattern="test_pattern")
        d = r.to_dict()
        assert d["pattern"] == "test_pattern"
        assert d["dry_run"] is True
        assert d["target_count"] == 0


class TestPkillTargetTree:
    def test_single_target_no_children(self):
        t = PkillTarget(pid=100, name="proc", cmdline="cmd", rss_mb=50)
        d = t.to_dict()
        assert d["pid"] == 100
        assert d["children"] == []

    def test_nested_children(self):
        grandchild = PkillTarget(pid=300, name="gc", cmdline="", rss_mb=10, ppid=200)
        child = PkillTarget(pid=200, name="child", cmdline="", rss_mb=20,
                            ppid=100, children=[grandchild])
        parent = PkillTarget(pid=100, name="parent", cmdline="", rss_mb=30,
                             children=[child])

        d = parent.to_dict()
        assert d["pid"] == 100
        assert len(d["children"]) == 1
        assert d["children"][0]["pid"] == 200
        assert len(d["children"][0]["children"]) == 1
        assert d["children"][0]["children"][0]["pid"] == 300
