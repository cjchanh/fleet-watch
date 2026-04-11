"""Tests for runaway process detection, CLI command, and guard integration."""

import json
import os
import sqlite3

from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import events, registry, runaway


def _patch_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")


# --- runaway.py unit tests ---


class TestParseEtime:
    def test_minutes_seconds(self):
        assert runaway._parse_etime("05:30") == 330

    def test_hours_minutes_seconds(self):
        assert runaway._parse_etime("02:10:05") == 7805

    def test_days_hours_minutes_seconds(self):
        assert runaway._parse_etime("1-00:00:00") == 86400

    def test_just_seconds(self):
        assert runaway._parse_etime("45") == 45

    def test_zero(self):
        assert runaway._parse_etime("00:00") == 0

    def test_complex_days(self):
        assert runaway._parse_etime("2-03:15:30") == 2 * 86400 + 3 * 3600 + 15 * 60 + 30


class TestScanRunaways:
    def test_scan_finds_high_cpu_processes(self, monkeypatch):
        """Processes above CPU threshold with sufficient runtime are flagged."""
        fake_output = (
            "  PID  %CPU     ELAPSED COMMAND\n"
            "12345  99.5    10:00 /opt/homebrew/lib/node_modules/@openai/codex/node_modules/@openai/codex-darwin-arm64/vendor/aarch64-apple-darwin/codex/codex\n"
            "12346   5.0    20:00 /usr/bin/python3 server.py\n"
            "12347  95.0    00:30 /tmp/short-lived\n"  # Too short runtime
        )

        import subprocess as real_subprocess

        def mock_run(cmd, **kwargs):
            if cmd[0] == "ps" and "-eo" in cmd:
                return type("R", (), {"stdout": fake_output, "returncode": 0})()
            return real_subprocess.run(cmd, **kwargs)

        monkeypatch.setattr(runaway.subprocess, "run", mock_run)

        results = runaway.scan_runaways(cpu_threshold=90.0, sustained_seconds=60)

        assert len(results) == 1
        assert results[0].pid == 12345
        assert results[0].cpu_pct == 99.5
        assert results[0].runtime_seconds == 600
        assert "codex/codex" in results[0].command

    def test_scan_empty_when_no_high_cpu(self, monkeypatch):
        """No processes flagged when all are below threshold."""
        fake_output = (
            "  PID  %CPU     ELAPSED COMMAND\n"
            "  100  10.0    30:00 /usr/bin/python3\n"
            "  200  50.0    15:00 node server.js\n"
        )

        def mock_run(cmd, **kwargs):
            return type("R", (), {"stdout": fake_output, "returncode": 0})()

        monkeypatch.setattr(runaway.subprocess, "run", mock_run)

        results = runaway.scan_runaways(cpu_threshold=90.0, sustained_seconds=60)
        assert results == []

    def test_scan_handles_ps_failure(self, monkeypatch):
        """Gracefully returns empty list on ps failure."""
        def mock_run(cmd, **kwargs):
            return type("R", (), {"stdout": "", "returncode": 1})()

        monkeypatch.setattr(runaway.subprocess, "run", mock_run)

        results = runaway.scan_runaways()
        assert results == []


class TestRunawayProcess:
    def test_to_dict(self):
        proc = runaway.RunawayProcess(
            pid=123, name="codex", cpu_pct=99.0,
            runtime_seconds=600, command="codex/codex",
        )
        d = proc.to_dict()
        assert d["pid"] == 123
        assert d["name"] == "codex"
        assert d["cpu_pct"] == 99.0
        assert d["runtime_seconds"] == 600
        assert d["command"] == "codex/codex"


class TestDaemonRunawayTracker:
    def _make_mock_scan(self, monkeypatch, processes):
        """Mock scan_runaways to return given processes."""
        monkeypatch.setattr(
            runaway,
            "scan_runaways",
            lambda cpu_threshold=90.0, sustained_seconds=60: processes,
        )

    def test_no_flag_before_threshold(self, monkeypatch):
        """Process must hit DAEMON_CONSECUTIVE_TICKS before being flagged."""
        proc = runaway.RunawayProcess(
            pid=100, name="codex", cpu_pct=99.0,
            runtime_seconds=600, command="codex/codex",
        )
        self._make_mock_scan(monkeypatch, [proc])
        tracker = runaway.DaemonRunawayTracker()

        # First two ticks: not yet flagged
        result1 = tracker.tick()
        assert result1 == []
        assert tracker.tick_counts[100] == 1

        result2 = tracker.tick()
        assert result2 == []
        assert tracker.tick_counts[100] == 2

    def test_flag_on_threshold_tick(self, monkeypatch):
        """Process is flagged exactly when consecutive ticks hits the threshold."""
        proc = runaway.RunawayProcess(
            pid=100, name="codex", cpu_pct=99.0,
            runtime_seconds=600, command="codex/codex",
        )
        self._make_mock_scan(monkeypatch, [proc])
        tracker = runaway.DaemonRunawayTracker()

        for _ in range(runaway.DAEMON_CONSECUTIVE_TICKS - 1):
            tracker.tick()

        result = tracker.tick()
        assert len(result) == 1
        assert result[0].pid == 100

    def test_no_double_flag(self, monkeypatch):
        """Process is flagged once, not every subsequent tick."""
        proc = runaway.RunawayProcess(
            pid=100, name="codex", cpu_pct=99.0,
            runtime_seconds=600, command="codex/codex",
        )
        self._make_mock_scan(monkeypatch, [proc])
        tracker = runaway.DaemonRunawayTracker()

        for _ in range(runaway.DAEMON_CONSECUTIVE_TICKS):
            tracker.tick()

        # Fourth tick: should NOT re-flag
        result = tracker.tick()
        assert result == []

    def test_reset_on_cpu_drop(self, monkeypatch):
        """Counter resets when process drops below threshold."""
        proc = runaway.RunawayProcess(
            pid=100, name="codex", cpu_pct=99.0,
            runtime_seconds=600, command="codex/codex",
        )
        tracker = runaway.DaemonRunawayTracker()

        # Two ticks with high CPU
        monkeypatch.setattr(
            runaway, "scan_runaways",
            lambda cpu_threshold=90.0, sustained_seconds=60: [proc],
        )
        tracker.tick()
        tracker.tick()
        assert tracker.tick_counts[100] == 2

        # CPU drops
        monkeypatch.setattr(
            runaway, "scan_runaways",
            lambda cpu_threshold=90.0, sustained_seconds=60: [],
        )
        tracker.tick()
        assert 100 not in tracker.tick_counts

    def test_get_active_warnings_returns_flagged(self, monkeypatch):
        """get_active_warnings returns processes at or above threshold."""
        proc = runaway.RunawayProcess(
            pid=100, name="codex", cpu_pct=99.0,
            runtime_seconds=600, command="codex/codex",
        )
        self._make_mock_scan(monkeypatch, [proc])
        tracker = runaway.DaemonRunawayTracker()

        for _ in range(runaway.DAEMON_CONSECUTIVE_TICKS):
            tracker.tick()

        warnings = tracker.get_active_warnings()
        assert len(warnings) == 1
        assert warnings[0]["pid"] == 100
        assert warnings[0]["consecutive_ticks"] >= runaway.DAEMON_CONSECUTIVE_TICKS

    def test_get_active_warnings_empty_before_threshold(self, monkeypatch):
        """get_active_warnings is empty before threshold is reached."""
        proc = runaway.RunawayProcess(
            pid=100, name="codex", cpu_pct=99.0,
            runtime_seconds=600, command="codex/codex",
        )
        self._make_mock_scan(monkeypatch, [proc])
        tracker = runaway.DaemonRunawayTracker()
        tracker.tick()

        assert tracker.get_active_warnings() == []


class TestTrackerPersistence:
    def test_save_and_load(self, tmp_path):
        """Tracker state survives save/load cycle."""
        tracker = runaway.DaemonRunawayTracker()
        tracker.tick_counts = {100: 2, 200: 1}
        tracker.last_cpu = {100: 99.0, 200: 95.0}
        tracker.last_runtime = {100: 600, 200: 300}

        path = tmp_path / "runaway_tracker.json"
        tracker.save(path)

        loaded = runaway.DaemonRunawayTracker.load(path)
        assert loaded.tick_counts == {100: 2, 200: 1}
        assert loaded.last_cpu == {100: 99.0, 200: 95.0}
        assert loaded.last_runtime == {100: 600, 200: 300}

    def test_load_missing_file_returns_empty(self, tmp_path):
        """Loading from nonexistent file returns empty tracker."""
        path = tmp_path / "nonexistent.json"
        loaded = runaway.DaemonRunawayTracker.load(path)
        assert loaded.tick_counts == {}

    def test_load_corrupt_file_returns_empty(self, tmp_path):
        """Loading from corrupt file returns empty tracker."""
        path = tmp_path / "corrupt.json"
        path.write_text("not json")
        loaded = runaway.DaemonRunawayTracker.load(path)
        assert loaded.tick_counts == {}


# --- CLI tests ---


class TestRunawayCLI:
    def test_dry_run_lists_flagged(self, monkeypatch):
        """fleet runaway without --kill shows flagged processes."""
        flagged = [
            runaway.RunawayProcess(
                pid=12345, name="codex", cpu_pct=99.5,
                runtime_seconds=3600, command="codex/codex --sandbox",
            ),
        ]
        monkeypatch.setattr(runaway, "scan_runaways", lambda **kw: flagged)

        runner = CliRunner()
        result = runner.invoke(cli_module.cli, ["runaway", "--json"])

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["confirmed"] is False
        assert payload["flagged_count"] == 1
        assert payload["flagged"][0]["pid"] == 12345
        assert payload["killed"] == []

    def test_kill_mode_sends_kill_and_logs(self, tmp_path, monkeypatch):
        """fleet runaway --kill terminates processes and logs events."""
        _patch_paths(monkeypatch, tmp_path)
        flagged = [
            runaway.RunawayProcess(
                pid=12345, name="codex", cpu_pct=99.5,
                runtime_seconds=3600, command="codex/codex --sandbox",
            ),
        ]
        monkeypatch.setattr(runaway, "scan_runaways", lambda **kw: flagged)

        killed_pids: list[int] = []

        def fake_kill(pid):
            killed_pids.append(pid)
            return True

        monkeypatch.setattr(runaway, "kill_runaway", fake_kill)

        runner = CliRunner()
        result = runner.invoke(cli_module.cli, ["runaway", "--kill", "--json"])

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["confirmed"] is True
        assert payload["killed"][0]["pid"] == 12345
        assert killed_pids == [12345]

        # Verify event was logged
        conn = registry.connect()
        logged = events.get_events(conn, hours=1, event_type="RUNAWAY_KILL")
        assert len(logged) == 1
        assert logged[0]["pid"] == 12345
        conn.close()

    def test_kill_failure_reports_failed(self, tmp_path, monkeypatch):
        """fleet runaway --kill reports failures when kill fails."""
        _patch_paths(monkeypatch, tmp_path)
        flagged = [
            runaway.RunawayProcess(
                pid=99999, name="stubborn", cpu_pct=98.0,
                runtime_seconds=7200, command="/bin/stubborn",
            ),
        ]
        monkeypatch.setattr(runaway, "scan_runaways", lambda **kw: flagged)
        monkeypatch.setattr(runaway, "kill_runaway", lambda pid: False)

        runner = CliRunner()
        result = runner.invoke(cli_module.cli, ["runaway", "--kill", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["failed"][0]["pid"] == 99999

    def test_no_runaways_clean(self, monkeypatch):
        """fleet runaway with no flagged processes reports clean."""
        monkeypatch.setattr(runaway, "scan_runaways", lambda **kw: [])

        runner = CliRunner()
        result = runner.invoke(cli_module.cli, ["runaway"])

        assert result.exit_code == 0
        assert "No runaway processes detected" in result.output

    def test_custom_thresholds(self, monkeypatch):
        """fleet runaway accepts custom --cpu-threshold and --sustained-seconds."""
        captured_kwargs = {}

        def capture_scan(**kwargs):
            captured_kwargs.update(kwargs)
            return []

        monkeypatch.setattr(runaway, "scan_runaways", capture_scan)

        runner = CliRunner()
        result = runner.invoke(
            cli_module.cli,
            ["runaway", "--cpu-threshold", "80", "--sustained-seconds", "120", "--json"],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["cpu_threshold"] == 80.0
        assert payload["sustained_seconds"] == 120


class TestGuardRunawayIntegration:
    def test_guard_includes_runaways_when_present(self, tmp_path, monkeypatch):
        """fleet guard --json includes runaways key when runaways exist."""
        _patch_paths(monkeypatch, tmp_path)
        flagged = [
            runaway.RunawayProcess(
                pid=55555, name="codex", cpu_pct=99.0,
                runtime_seconds=1800, command="codex/codex",
            ),
        ]
        monkeypatch.setattr(runaway, "scan_runaways", lambda **kw: flagged)

        runner = CliRunner()
        result = runner.invoke(cli_module.cli, ["guard", "--json"])

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert "runaways" in payload
        assert payload["runaways"][0]["pid"] == 55555
        # Runaways are advisory, not blocking
        assert payload["allowed"] is True

    def test_guard_omits_runaways_when_none(self, tmp_path, monkeypatch):
        """fleet guard --json omits runaways key when no runaways."""
        _patch_paths(monkeypatch, tmp_path)
        monkeypatch.setattr(runaway, "scan_runaways", lambda **kw: [])

        runner = CliRunner()
        result = runner.invoke(cli_module.cli, ["guard", "--json"])

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert "runaways" not in payload


class TestDiscoveryPattern:
    def test_codex_native_binary_pattern_matches(self):
        """The Codex agent discovery pattern matches the native binary path."""
        import re
        pattern = None
        for p in cli_module.discover_mod.DEFAULT_CONFIG["patterns"]:
            if p["name_template"] == "Codex agent":
                pattern = p
                break

        assert pattern is not None
        assert pattern["workstream"] == "codex"

        native_path = (
            "/opt/homebrew/lib/node_modules/@openai/codex/"
            "node_modules/@openai/codex-darwin-arm64/"
            "vendor/aarch64-apple-darwin/codex/codex"
        )
        assert re.search(pattern["process_match"], native_path)

    def test_codex_native_binary_pattern_does_not_match_wrapper(self):
        """The Codex agent pattern must NOT match the node wrapper."""
        import re
        pattern = None
        for p in cli_module.discover_mod.DEFAULT_CONFIG["patterns"]:
            if p["name_template"] == "Codex agent":
                pattern = p
                break

        wrapper_cmd = "node /opt/homebrew/bin/codex"
        assert not re.search(pattern["process_match"], wrapper_cmd)

    def test_codex_native_binary_pattern_does_not_match_codex_cli(self):
        """The pattern must NOT match plain 'codex' without path ending."""
        import re
        pattern = None
        for p in cli_module.discover_mod.DEFAULT_CONFIG["patterns"]:
            if p["name_template"] == "Codex agent":
                pattern = p
                break

        assert not re.search(pattern["process_match"], "/usr/local/bin/codex --help")
        assert not re.search(pattern["process_match"], "codex --sandbox")


class TestDaemonRunawayLogging:
    def test_discover_logs_runaway_event(self, tmp_path, monkeypatch):
        """fleet discover logs RUNAWAY_DETECTED when tracker threshold is hit."""
        _patch_paths(monkeypatch, tmp_path)

        # Stub discover sync and reporter
        monkeypatch.setattr(cli_module.discover_mod, "sync", lambda conn, config=None: {
            "added": [], "cleaned": [], "skipped": [],
            "thunder_synced": 0, "session_leases_cleaned": 0,
        })
        monkeypatch.setattr(cli_module.discover_mod, "load_config", lambda: {})
        monkeypatch.setattr(
            cli_module.reporter, "write_report",
            lambda conn: (tmp_path / "r.md", tmp_path / "r.json"),
        )
        monkeypatch.setattr(
            cli_module.syshealth, "get_session_processes",
            lambda patterns=None: [],
        )

        # Make scan_runaways return a consistent runaway
        proc = runaway.RunawayProcess(
            pid=77777, name="codex", cpu_pct=99.0,
            runtime_seconds=600, command="codex/codex",
        )
        monkeypatch.setattr(
            runaway, "scan_runaways",
            lambda cpu_threshold=90.0, sustained_seconds=60: [proc],
        )

        runner = CliRunner()
        # Run discover DAEMON_CONSECUTIVE_TICKS times
        for _ in range(runaway.DAEMON_CONSECUTIVE_TICKS):
            result = runner.invoke(cli_module.cli, ["discover"])
            assert result.exit_code == 0

        # Verify RUNAWAY_DETECTED event was logged
        conn = registry.connect()
        logged = events.get_events(conn, hours=1, event_type="RUNAWAY_DETECTED")
        assert len(logged) == 1
        assert logged[0]["pid"] == 77777
        detail = logged[0]["detail"]
        assert detail["cpu_pct"] == 99.0
        assert detail["runtime_seconds"] == 600
        conn.close()

    def test_discover_auto_kills_runaway(self, tmp_path, monkeypatch):
        """fleet discover kills runaway processes by default (auto_kill=True)."""
        _patch_paths(monkeypatch, tmp_path)

        monkeypatch.setattr(cli_module.discover_mod, "sync", lambda conn, config=None: {
            "added": [], "cleaned": [], "skipped": [],
            "thunder_synced": 0, "session_leases_cleaned": 0,
        })
        monkeypatch.setattr(cli_module.discover_mod, "load_config", lambda: {})
        monkeypatch.setattr(
            cli_module.reporter, "write_report",
            lambda conn: (tmp_path / "r.md", tmp_path / "r.json"),
        )
        monkeypatch.setattr(
            cli_module.syshealth, "get_session_processes",
            lambda patterns=None: [],
        )

        proc = runaway.RunawayProcess(
            pid=77777, name="codex", cpu_pct=99.0,
            runtime_seconds=600, command="codex/codex",
        )
        monkeypatch.setattr(
            runaway, "scan_runaways",
            lambda cpu_threshold=90.0, sustained_seconds=60: [proc],
        )

        killed_pids: list[int] = []
        monkeypatch.setattr(runaway, "kill_runaway", lambda pid: (killed_pids.append(pid), True)[1])

        runner = CliRunner()
        for _ in range(runaway.DAEMON_CONSECUTIVE_TICKS):
            runner.invoke(cli_module.cli, ["discover"])

        assert killed_pids == [77777]

        conn = registry.connect()
        kill_events = events.get_events(conn, hours=1, event_type="RUNAWAY_KILL")
        assert len(kill_events) == 1
        assert kill_events[0]["pid"] == 77777
        conn.close()

    def test_discover_no_auto_kill_flag(self, tmp_path, monkeypatch):
        """fleet discover --no-auto-kill suppresses kills, only logs warnings."""
        _patch_paths(monkeypatch, tmp_path)

        monkeypatch.setattr(cli_module.discover_mod, "sync", lambda conn, config=None: {
            "added": [], "cleaned": [], "skipped": [],
            "thunder_synced": 0, "session_leases_cleaned": 0,
        })
        monkeypatch.setattr(cli_module.discover_mod, "load_config", lambda: {})
        monkeypatch.setattr(
            cli_module.reporter, "write_report",
            lambda conn: (tmp_path / "r.md", tmp_path / "r.json"),
        )
        monkeypatch.setattr(
            cli_module.syshealth, "get_session_processes",
            lambda patterns=None: [],
        )

        proc = runaway.RunawayProcess(
            pid=77777, name="codex", cpu_pct=99.0,
            runtime_seconds=600, command="codex/codex",
        )
        monkeypatch.setattr(
            runaway, "scan_runaways",
            lambda cpu_threshold=90.0, sustained_seconds=60: [proc],
        )

        killed_pids: list[int] = []
        monkeypatch.setattr(runaway, "kill_runaway", lambda pid: (killed_pids.append(pid), True)[1])

        runner = CliRunner()
        for _ in range(runaway.DAEMON_CONSECUTIVE_TICKS):
            result = runner.invoke(cli_module.cli, ["discover", "--no-auto-kill"])
            assert result.exit_code == 0

        assert killed_pids == []

        conn = registry.connect()
        detected = events.get_events(conn, hours=1, event_type="RUNAWAY_DETECTED")
        assert len(detected) == 1
        kill_events = events.get_events(conn, hours=1, event_type="RUNAWAY_KILL")
        assert len(kill_events) == 0
        conn.close()

    def test_discover_kill_failure_logs_failed_event(self, tmp_path, monkeypatch):
        """RUNAWAY_KILL_FAILED event is logged when kill_runaway returns False."""
        _patch_paths(monkeypatch, tmp_path)

        monkeypatch.setattr(cli_module.discover_mod, "sync", lambda conn, config=None: {
            "added": [], "cleaned": [], "skipped": [],
            "thunder_synced": 0, "session_leases_cleaned": 0,
        })
        monkeypatch.setattr(cli_module.discover_mod, "load_config", lambda: {})
        monkeypatch.setattr(
            cli_module.reporter, "write_report",
            lambda conn: (tmp_path / "r.md", tmp_path / "r.json"),
        )
        monkeypatch.setattr(
            cli_module.syshealth, "get_session_processes",
            lambda patterns=None: [],
        )

        proc = runaway.RunawayProcess(
            pid=88888, name="stubborn", cpu_pct=98.0,
            runtime_seconds=1200, command="/bin/stubborn",
        )
        monkeypatch.setattr(
            runaway, "scan_runaways",
            lambda cpu_threshold=90.0, sustained_seconds=60: [proc],
        )
        monkeypatch.setattr(runaway, "kill_runaway", lambda pid: False)

        runner = CliRunner()
        for _ in range(runaway.DAEMON_CONSECUTIVE_TICKS):
            runner.invoke(cli_module.cli, ["discover"])

        conn = registry.connect()
        failed = events.get_events(conn, hours=1, event_type="RUNAWAY_KILL_FAILED")
        assert len(failed) == 1
        assert failed[0]["pid"] == 88888
        conn.close()


class TestKillRunawayGuards:
    def test_refuses_low_pid(self):
        """kill_runaway refuses PIDs below MIN_SAFE_PID."""
        assert runaway.kill_runaway(1) is False
        assert runaway.kill_runaway(50) is False
        assert runaway.kill_runaway(99) is False

    def test_refuses_zero_and_negative(self):
        """kill_runaway refuses PID 0 and negative PIDs."""
        assert runaway.kill_runaway(0) is False
        assert runaway.kill_runaway(-1) is False

    def test_refuses_own_pid(self):
        """kill_runaway refuses to kill its own process."""
        assert runaway.kill_runaway(os.getpid()) is False

    def test_refuses_parent_pid(self):
        """kill_runaway refuses to kill its parent process."""
        assert runaway.kill_runaway(os.getppid()) is False


class TestCodexPatternWithArgs:
    def test_matches_binary_with_cli_args(self):
        """Codex pattern matches the binary path when followed by CLI arguments."""
        import re
        pattern = None
        for p in cli_module.discover_mod.DEFAULT_CONFIG["patterns"]:
            if p["name_template"] == "Codex agent":
                pattern = p
                break

        cmd_with_args = (
            "/opt/homebrew/lib/node_modules/@openai/codex/"
            "node_modules/@openai/codex-darwin-arm64/"
            "vendor/aarch64-apple-darwin/codex/codex -a never -s danger-full-access"
        )
        assert re.search(pattern["process_match"], cmd_with_args)
