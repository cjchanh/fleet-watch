"""Tests for CLI commands — check, context, discover."""

import json
import sqlite3

from click.testing import CliRunner

from fleet_watch import registry
from fleet_watch.cli import cli


def _setup_db(tmp_path):
    """Create a fresh DB and patch registry to use it."""
    db_path = tmp_path / "test.db"
    conn = registry.connect(db_path)
    return conn, db_path


def test_status_empty():
    runner = CliRunner()
    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0
    assert "No active processes" in result.output or "GPU:" in result.output


def test_context_output_is_json():
    runner = CliRunner()
    result = runner.invoke(cli, ["context"])
    assert result.exit_code == 0
    parsed = json.loads(result.output)
    assert "occupied_ports" in parsed
    assert "safe_ports" in parsed
    assert "gpu_available_mb" in parsed
    assert "locked_repos" in parsed
    assert "process_count" in parsed
    assert "processes" in parsed


def test_context_has_correct_types():
    runner = CliRunner()
    result = runner.invoke(cli, ["context"])
    parsed = json.loads(result.output)
    assert isinstance(parsed["occupied_ports"], list)
    assert isinstance(parsed["safe_ports"], list)
    assert isinstance(parsed["gpu_available_mb"], int)
    assert isinstance(parsed["gpu_budget_pct"], int)
    assert isinstance(parsed["locked_repos"], list)
    assert isinstance(parsed["processes"], list)


def test_check_port_available():
    runner = CliRunner()
    # Port 59999 is almost certainly not in use
    result = runner.invoke(cli, ["check", "--port", "59999"])
    assert result.exit_code == 0
    assert "available" in result.output


def test_check_gpu_available():
    runner = CliRunner()
    result = runner.invoke(cli, ["check", "--gpu", "1024"])
    assert result.exit_code == 0
    assert "available" in result.output


def test_check_requires_argument():
    runner = CliRunner()
    result = runner.invoke(cli, ["check"])
    assert result.exit_code == 2


def test_discover_runs():
    runner = CliRunner()
    result = runner.invoke(cli, ["discover"])
    assert result.exit_code == 0
    # Should either add processes, clean dead ones, or report no changes
    assert any(x in result.output for x in ["+", "-", "No changes"])


def test_history_runs():
    runner = CliRunner()
    result = runner.invoke(cli, ["history"])
    assert result.exit_code == 0


def test_clean_runs():
    runner = CliRunner()
    result = runner.invoke(cli, ["clean"])
    assert result.exit_code == 0


def test_report_writes_files():
    runner = CliRunner()
    result = runner.invoke(cli, ["report"])
    assert result.exit_code == 0
    assert "STATE_REPORT.md" in result.output
    assert "state.json" in result.output
