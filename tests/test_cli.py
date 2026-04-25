"""CLI contract tests for agent-facing Fleet Watch surfaces."""

import json
import os
import sqlite3

from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import registry
from fleet_watch import syshealth


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


def test_guard_repo_denied_by_session_lease_includes_unblock_command(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    conn = registry.connect()
    registry.upsert_session_lease(
        conn,
        "sess-editor",
        owner_pid=None,
        repo_dir=str(repo),
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--repo", str(repo), "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    repo_check = payload["checks"]["repo"]
    assert repo_check["allowed"] is False
    assert repo_check["holder"]["session_id"] == "sess-editor"
    assert repo_check["unblock_command"] == "fleet session close --session-id sess-editor"


def test_share_repo_closes_documents_session_lease_and_logs_event(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    docs_root = tmp_path / "Documents"
    repo = docs_root / "Substack"
    repo.mkdir(parents=True)
    monkeypatch.setattr(cli_module, "_documents_root", lambda: docs_root.resolve())

    conn = registry.connect()
    registry.upsert_session_lease(
        conn,
        "sess-editor",
        owner_pid=None,
        repo_dir=str(repo),
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["share-repo", str(repo)])

    assert result.exit_code == 0
    assert "Released session lease sess-editor" in result.output

    conn = registry.connect()
    lease = registry.get_session_lease(conn, "sess-editor")
    session_close_events = cli_module.events.get_events(conn, hours=1, event_type="SESSION_CLOSE")
    conn.close()

    assert lease is not None
    assert lease["status"] == "CLOSED"
    assert session_close_events
    assert session_close_events[0]["detail"]["source"] == "share-repo"

    guard = runner.invoke(cli_module.cli, ["guard", "--repo", str(repo), "--json"])
    assert guard.exit_code == 0
    payload = json.loads(guard.output)
    assert payload["allowed"] is True


def test_share_repo_rejects_non_documents_paths(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    docs_root = tmp_path / "Documents"
    repo = tmp_path / "Workspace" / "active" / "engineering"
    repo.mkdir(parents=True)
    monkeypatch.setattr(cli_module, "_documents_root", lambda: docs_root.resolve())

    conn = registry.connect()
    registry.upsert_session_lease(
        conn,
        "sess-engineering",
        owner_pid=None,
        repo_dir=str(repo),
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["share-repo", str(repo)])

    assert result.exit_code == 2
    assert "share-repo is limited" in result.output

    conn = registry.connect()
    lease = registry.get_session_lease(conn, "sess-engineering")
    conn.close()
    assert lease is not None
    assert lease["status"] == "ACTIVE"


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


def test_guard_gpu_without_model_does_not_false_deny(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_memory_state",
        lambda: syshealth.MemoryState(8192, 4000, 1000, 2000, 0, 1000),
    )

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--gpu", "1024", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["checks"]["gpu"]["allowed"] is True
    assert "working_set" not in payload["checks"]["gpu"]


def test_guard_logs_working_set_denial_event(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_memory_state",
        lambda: syshealth.MemoryState(8192, 4000, 1000, 2000, 0, 1000),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "guard",
            "--gpu",
            "4096",
            "--framework",
            "candle",
            "--model",
            "qwen2.5-7B-Q4_K_M.gguf",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["checks"]["gpu"]["reason"] == "working_set_exceeds_physical_ram"

    conn = registry.connect()
    try:
        events = cli_module.events.get_events(conn, hours=1, event_type="GPU_WORKING_SET_DENY")
    finally:
        conn.close()
    assert events


def test_guard_human_output_disambiguates_budget_vs_physical_ram(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_memory_state",
        lambda: syshealth.MemoryState(8192, 4000, 1000, 2000, 0, 1000),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "guard",
            "--gpu",
            "4096",
            "--framework",
            "candle",
            "--model",
            "qwen2.5-7B-Q4_K_M.gguf",
        ],
    )

    assert result.exit_code == 1
    assert "Physical RAM available after reserve: 6144MB" in result.output
    assert "GPU budget available:" in result.output


def test_health_json_reports_session_attention(monkeypatch):
    monkeypatch.setattr(cli_module.discover_mod, "load_config", lambda: {})
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_memory_state",
        lambda: syshealth.MemoryState(131072, 40000, 20000, 30000, 10000, 15000),
    )
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_session_processes",
        lambda patterns=None: [
            syshealth.SessionProcess(
                pid=61042,
                name="Codex",
                kind="codex",
                rss_mb=84,
                cpu_pct=61.7,
                started="1:57PM",
                tty="??",
                command="codex",
                ppid=61009,
                pgid=61009,
                group_leader_pid=61009,
                member_pids=[61041, 61042],
                member_count=2,
                parent_chain_detached=True,
                classification="detached_hot",
                attention=True,
                evidence=["launcher ancestry detached", "cpu 61.7%"],
            ),
        ],
    )
    monkeypatch.setattr(cli_module.syshealth, "get_idle_processes", lambda **kwargs: [])

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["health", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["sessions"][0]["classification"] == "detached_hot"
    assert payload["sessions"][0]["attention"] is True
    assert payload["sessions"][0]["member_count"] == 2


def test_health_human_notifies_on_detached_hot_sessions(monkeypatch):
    """Non-JSON health output triggers macOS notification for attention sessions."""
    monkeypatch.setattr(cli_module.discover_mod, "load_config", lambda: {})
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_memory_state",
        lambda: syshealth.MemoryState(131072, 40000, 20000, 30000, 10000, 15000),
    )

    hot_session = syshealth.SessionProcess(
        pid=77001,
        name="Codex",
        kind="codex",
        rss_mb=90,
        cpu_pct=45.0,
        started="3:00PM",
        tty="??",
        command="codex",
        member_pids=[77001],
        member_count=1,
        parent_chain_detached=True,
        classification="detached_hot",
        attention=True,
        evidence=["launcher ancestry detached", "cpu 45.0%"],
    )
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_session_processes",
        lambda patterns=None: [hot_session],
    )
    monkeypatch.setattr(cli_module.syshealth, "get_idle_processes", lambda **kwargs: [])

    osascript_calls: list[list[str]] = []
    real_subprocess_run = cli_module.subprocess.run

    def capture_run(cmd, **kwargs):
        if cmd and cmd[0] == "osascript":
            osascript_calls.append(cmd)
            return
        return real_subprocess_run(cmd, **kwargs)

    monkeypatch.setattr(cli_module.subprocess, "run", capture_run)

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["health"])

    assert result.exit_code == 0
    assert len(osascript_calls) == 1
    script = osascript_calls[0][2]
    assert "Attention Required" in script
    assert "45%" in script


def test_check_repo_uses_env_session_id_for_same_session_bypass(tmp_path, monkeypatch):
    """Regression gate: fleet check --repo resolves FLEET_SESSION_ID from env."""
    _patch_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("FLEET_SESSION_ID", "sess-check-env")
    conn = registry.connect()
    registry.upsert_session_lease(
        conn,
        "sess-check-env",
        owner_pid=os.getpid(),
        repo_dir=str(tmp_path),
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["check", "--repo", str(tmp_path)])

    assert result.exit_code == 0
    assert "available" in result.output


def test_reap_sessions_dry_run_lists_candidates(monkeypatch):
    monkeypatch.setattr(cli_module.discover_mod, "load_config", lambda: {})
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_session_processes",
        lambda patterns=None: [
            syshealth.SessionProcess(
                pid=99901,
                name="Codex",
                kind="codex",
                rss_mb=80,
                cpu_pct=55.0,
                started="1:00PM",
                tty="??",
                command="codex",
                member_pids=[99901, 99902],
                member_count=2,
                parent_chain_detached=True,
                classification="detached_hot",
                attention=True,
                evidence=["launcher ancestry detached", "cpu 55.0%"],
            ),
        ],
    )

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["reap-sessions", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["confirmed"] is False
    assert payload["candidate_count"] == 1
    assert payload["candidates"][0]["pid"] == 99901


def test_reap_sessions_confirm_kills_member_pids(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(cli_module.discover_mod, "load_config", lambda: {})

    monkeypatch.setattr(
        cli_module.syshealth,
        "get_session_processes",
        lambda patterns=None: [
            syshealth.SessionProcess(
                pid=99901,
                name="Codex",
                kind="codex",
                rss_mb=80,
                cpu_pct=55.0,
                started="1:00PM",
                tty="??",
                command="codex",
                member_pids=[99901, 99902],
                member_count=2,
                parent_chain_detached=True,
                classification="detached_hot",
                attention=True,
                evidence=["launcher ancestry detached", "cpu 55.0%"],
            ),
        ],
    )

    terminated_pids: list[int] = []

    def fake_terminate(pid, grace_seconds=1.5):
        terminated_pids.append(pid)
        return True

    monkeypatch.setattr(cli_module, "_terminate_orphan", fake_terminate)

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["reap-sessions", "--confirm", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["confirmed"] is True
    assert payload["killed"][0]["pid"] == 99901
    assert sorted(terminated_pids) == [99901, 99902]


def test_guard_repo_denied_by_active_session_lease(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = registry.connect()
    registry.upsert_session_lease(
        conn,
        "sess-other",
        owner_pid=os.getpid(),
        repo_dir=str(tmp_path),
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--repo", str(tmp_path), "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["checks"]["repo"]["allowed"] is False


def test_session_ensure_retries_on_db_locked(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    call_count = {"n": 0}
    real_upsert = registry.upsert_session_lease

    def flaky_upsert(conn, session_id, **kwargs):
        call_count["n"] += 1
        if call_count["n"] <= 2:
            raise sqlite3.OperationalError("database is locked")
        return real_upsert(conn, session_id, **kwargs)

    monkeypatch.setattr(registry, "upsert_session_lease", flaky_upsert)

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        ["session", "ensure", "--session-id", "retry-test", "--owner-pid", str(os.getpid()),
         "--retries", "3", "--retry-delay", "0.01"],
    )

    assert result.exit_code == 0
    assert "active" in result.output
    assert call_count["n"] == 3

    conn = registry.connect()
    lease = registry.get_session_lease(conn, "retry-test")
    assert lease is not None
    assert lease["status"] == "ACTIVE"
    conn.close()


def test_session_ensure_fail_open_on_exhausted_retries(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)

    def always_fail(conn, session_id, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(registry, "upsert_session_lease", always_fail)

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        ["session", "ensure", "--session-id", "doomed", "--owner-pid", str(os.getpid()),
         "--retries", "2", "--retry-delay", "0.01"],
    )

    assert result.exit_code == 0
    assert "UNTRACKED" in result.output


def test_discover_notifies_on_detached_hot_sessions(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(cli_module.discover_mod, "sync", lambda conn, config=None: {
        "added": [], "cleaned": [], "skipped": [], "thunder_synced": 0, "session_leases_cleaned": 0,
    })
    monkeypatch.setattr(cli_module.discover_mod, "load_config", lambda: {})
    monkeypatch.setattr(cli_module.reporter, "write_report", lambda conn: (tmp_path / "r.md", tmp_path / "r.json"))

    hot_session = syshealth.SessionProcess(
        pid=88001,
        name="Codex",
        kind="codex",
        rss_mb=100,
        cpu_pct=70.0,
        started="2:00PM",
        tty="??",
        command="codex",
        member_pids=[88001],
        member_count=1,
        parent_chain_detached=True,
        classification="detached_hot",
        attention=True,
        evidence=["launcher ancestry detached", "cpu 70.0%"],
    )
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_session_processes",
        lambda patterns=None: [hot_session],
    )

    osascript_calls: list[list[str]] = []
    real_subprocess_run = cli_module.subprocess.run

    def capture_run(cmd, **kwargs):
        if cmd and cmd[0] == "osascript":
            osascript_calls.append(cmd)
            return
        return real_subprocess_run(cmd, **kwargs)

    monkeypatch.setattr(cli_module.subprocess, "run", capture_run)

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["discover"])

    assert result.exit_code == 0
    assert len(osascript_calls) == 1
    script = osascript_calls[0][2]
    assert "Attention Required" in script
    assert "1 detached hot session(s)" in script
    assert "70%" in script


def test_guard_repo_uses_env_session_id_for_same_session_bypass(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    monkeypatch.setenv("FLEET_SESSION_ID", "sess-current")
    conn = registry.connect()
    registry.upsert_session_lease(
        conn,
        "sess-current",
        owner_pid=os.getpid(),
        repo_dir=str(tmp_path),
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--repo", str(tmp_path), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["checks"]["repo"]["allowed"] is True
