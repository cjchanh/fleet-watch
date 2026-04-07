"""CLI contract tests for agent-facing Fleet Watch surfaces."""

import json
import os

from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import registry


def _patch_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")


def test_guard_json_denies_taken_port(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = registry.connect()
    registry.register_process(conn, pid=os.getpid(), name="mlx", workstream="ws", port=8100)
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--port", "8100", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["allowed"] is False
    assert payload["checks"]["port"]["holder"]["pid"] == os.getpid()
    assert payload["checks"]["port"]["suggested_ports"]


def test_check_exit_code_is_zero_when_resource_is_available(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    runner = CliRunner()

    result = runner.invoke(cli_module.cli, ["check", "--port", "8100", "--gpu", "1024"])

    assert result.exit_code == 0
    assert "Port 8100: available" in result.output


def test_context_alias_returns_guard_json(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    runner = CliRunner()

    result = runner.invoke(cli_module.cli, ["context"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["allowed"] is True
    assert "state" in payload
    assert "external_resources" in payload["state"]


def test_install_launchd_writes_real_executable_path(tmp_path, monkeypatch):
    output_path = tmp_path / "io.fleet-watch.plist"
    monkeypatch.setattr(cli_module.shutil, "which", lambda name: "/tmp/fleet")
    runner = CliRunner()

    result = runner.invoke(
        cli_module.cli,
        [
            "install-launchd",
            "--interval",
            "30",
            "--output",
            str(output_path),
            "--no-load",
        ],
    )

    assert result.exit_code == 0
    plist = output_path.read_text()
    assert "/tmp/fleet" in plist
    assert "<integer>30</integer>" in plist


def test_session_start_and_close_updates_lease(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    runner = CliRunner()

    start = runner.invoke(
        cli_module.cli,
        ["session", "start", "--session-id", "sess-1", "--owner-pid", str(os.getpid())],
    )
    assert start.exit_code == 0

    conn = registry.connect()
    lease = registry.get_session_lease(conn, "sess-1")
    assert lease is not None
    assert lease["status"] == "ACTIVE"
    conn.close()

    close = runner.invoke(cli_module.cli, ["session", "close", "--session-id", "sess-1"])
    assert close.exit_code == 0

    conn = registry.connect()
    lease = registry.get_session_lease(conn, "sess-1")
    assert lease is not None
    assert lease["status"] == "CLOSED"
    conn.close()


def test_reconcile_json_reports_process_classification(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = registry.connect()
    registry.register_process(conn, pid=os.getpid(), name="mlx", workstream="ws")
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["reconcile", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "summary" in payload
    assert payload["processes"]
    assert payload["processes"][0]["classification"] == "live"


def test_guard_repo_denied_by_external_resource(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = registry.connect()
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-other",
        workstream="paper",
        name="Thunder abc123",
        repo_dir=str(tmp_path),
        status="RUNNING",
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--repo", str(tmp_path), "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["checks"]["repo"]["allowed"] is False


def test_guard_repo_allows_current_external_owner_session(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = registry.connect()
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-current",
        workstream="paper",
        name="Thunder abc123",
        repo_dir=str(tmp_path),
        status="RUNNING",
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        ["guard", "--repo", str(tmp_path), "--session-id", "sess-current", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["checks"]["repo"]["allowed"] is True
