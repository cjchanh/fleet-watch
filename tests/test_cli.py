"""CLI contract tests for agent-facing Fleet Watch surfaces."""

import json
import os
import sqlite3
from pathlib import Path

from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import registry
from fleet_watch import syshealth


def _patch_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_memory_state",
        lambda: syshealth.MemoryState(131072, 20000, 60000, 20000, 0, 10000),
    )
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_swap_state",
        lambda: syshealth.SwapState(8192, 1024, 7168),
    )
    monkeypatch.setattr(cli_module.syshealth, "get_vm_pressure_level", lambda: 1)
    monkeypatch.setattr(cli_module.syshealth, "get_total_memory_mb", lambda: 131072)


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


def test_session_list_json_shows_active_lease(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    runner = CliRunner()
    runner.invoke(
        cli_module.cli,
        ["session", "start", "--session-id", "sess-live", "--owner-pid", str(os.getpid())],
    )

    result = runner.invoke(cli_module.cli, ["session", "list", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["count"] == 1
    assert payload["session_leases"][0]["session_id"] == "sess-live"
    assert payload["session_leases"][0]["status"] == "ACTIVE"


def test_session_list_empty_is_clean(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["session", "list"])
    assert result.exit_code == 0
    assert "No active session leases." in result.output


def test_session_list_active_excludes_closed_but_all_includes(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    runner = CliRunner()
    runner.invoke(
        cli_module.cli,
        ["session", "start", "--session-id", "sess-x", "--owner-pid", str(os.getpid())],
    )
    runner.invoke(cli_module.cli, ["session", "close", "--session-id", "sess-x"])

    active = runner.invoke(cli_module.cli, ["session", "list", "--json"])
    assert json.loads(active.output)["count"] == 0  # closed lease excluded by default

    everything = runner.invoke(cli_module.cli, ["session", "list", "--all", "--json"])
    payload = json.loads(everything.output)
    assert payload["count"] == 1
    assert payload["session_leases"][0]["status"] == "CLOSED"


def test_session_start_records_write_scope(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    runner = CliRunner()

    start = runner.invoke(
        cli_module.cli,
        [
            "session",
            "start",
            "--session-id",
            "sess-scope",
            "--owner-pid",
            str(os.getpid()),
            "--repo",
            str(repo),
            "--write-scope",
            "tools/playwright",
        ],
    )
    assert start.exit_code == 0

    conn = registry.connect()
    lease = registry.get_session_lease(conn, "sess-scope")
    conn.close()

    assert lease is not None
    assert lease["repo_lock_mode"] == "cooperative"
    assert lease["write_scopes"] == [str((repo / "tools/playwright").resolve())]


def test_guard_repo_allows_cooperative_session_lease_and_reports_holder(tmp_path, monkeypatch):
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

    assert result.exit_code == 0
    payload = json.loads(result.output)
    repo_check = payload["checks"]["repo"]
    assert repo_check["allowed"] is True
    assert repo_check["holder"] is None
    assert repo_check["holders"][0]["session_id"] == "sess-editor"
    assert repo_check["safe_mode"] == "declare --write-scope before editing"


def test_guard_repo_denied_by_exclusive_session_lease_includes_unblock_command(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    conn = registry.connect()
    registry.upsert_session_lease(
        conn,
        "sess-editor",
        owner_pid=None,
        repo_dir=str(repo),
        repo_lock_mode="exclusive",
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--repo", str(repo), "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    repo_check = payload["checks"]["repo"]
    assert repo_check["allowed"] is False
    assert repo_check["holder"]["session_id"] == "sess-editor"
    assert repo_check["holder"]["repo_lock_mode"] == "exclusive"
    assert repo_check["unblock_command"] == "fleet session close --session-id sess-editor"


def test_guard_repo_cleans_stale_dead_exclusive_session_before_payload(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    dead_pid = 2147483646
    conn = registry.connect()
    registry.upsert_session_lease(
        conn,
        "sess-stale",
        owner_pid=dead_pid,
        repo_dir=str(repo),
        repo_lock_mode="exclusive",
    )
    conn.close()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid != dead_pid)
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 999 if ts else None)

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        ["guard", "--repo", str(repo), "--exclusive-repo-lock", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    repo_check = payload["checks"]["repo"]
    assert payload["allowed"] is True
    assert repo_check["allowed"] is True
    assert repo_check["holder"] is None
    assert [holder["session_id"] for holder in repo_check["stale_holders"]] == ["sess-stale"]
    assert str(repo.resolve()) not in payload["state"]["locked_repos"]

    conn = registry.connect()
    lease = registry.get_session_lease(conn, "sess-stale")
    row = conn.execute(
        "SELECT pid, workstream, detail FROM events WHERE event_type = 'CLEAN'"
    ).fetchone()
    conn.close()

    assert lease is not None
    assert lease["status"] == "CLOSED"
    assert row is not None
    assert row[0] == dead_pid
    assert row[1] == "session"
    detail = json.loads(row[2])
    assert detail["source"] == "guard"
    # Path C: proven-dead owner is reaped via the dead-owner arm.
    assert detail["reason"] == "dead_session_owner"
    assert detail["session_id"] == "sess-stale"
    assert detail["repo_dir"] == str(repo.resolve())


def test_guard_repo_reports_stale_cleaned_lease_even_when_active_holder_denies(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    stale_pid = 2147483646
    active_pid = 2147483645
    conn = registry.connect()
    registry.upsert_session_lease(
        conn,
        "sess-stale",
        owner_pid=stale_pid,
        repo_dir=str(repo),
        repo_lock_mode="exclusive",
    )
    registry.upsert_session_lease(
        conn,
        "sess-active",
        owner_pid=active_pid,
        repo_dir=str(repo),
        repo_lock_mode="exclusive",
    )
    conn.close()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid == active_pid)
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 999 if ts else None)

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--repo", str(repo), "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    repo_check = payload["checks"]["repo"]
    assert repo_check["allowed"] is False
    assert repo_check["holder"]["session_id"] == "sess-active"
    assert [holder["session_id"] for holder in repo_check["stale_holders"]] == ["sess-stale"]

    conn = registry.connect()
    assert registry.get_session_lease(conn, "sess-stale")["status"] == "CLOSED"
    assert registry.get_session_lease(conn, "sess-active")["status"] == "ACTIVE"
    conn.close()


def test_guard_repo_denies_overlapping_write_scope(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    conn = registry.connect()
    registry.upsert_session_lease(
        conn,
        "sess-tools",
        owner_pid=None,
        repo_dir=str(repo),
        write_scopes=["tools/playwright"],
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        ["guard", "--repo", str(repo), "--write-scope", "tools/playwright/edit_post.py", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    repo_check = payload["checks"]["repo"]
    assert repo_check["allowed"] is False
    assert repo_check["holder"]["session_id"] == "sess-tools"
    assert any(path.endswith("tools/playwright") for path in repo_check["overlap_paths"])


def test_guard_repo_allows_disjoint_write_scope(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    conn = registry.connect()
    registry.upsert_session_lease(
        conn,
        "sess-docs",
        owner_pid=None,
        repo_dir=str(repo),
        write_scopes=["docs"],
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        ["guard", "--repo", str(repo), "--write-scope", "tools/playwright", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    repo_check = payload["checks"]["repo"]
    assert repo_check["allowed"] is True
    assert repo_check["holders"][0]["session_id"] == "sess-docs"
    assert repo_check["safe_mode"] == "cooperative-write"


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
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_swap_state",
        lambda: syshealth.SwapState(8192, 1024, 7168),
    )

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--gpu", "1024", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["checks"]["gpu"]["allowed"] is True
    assert "working_set" not in payload["checks"]["gpu"]


def test_guard_gpu_uses_kernel_pressure_over_computed_proxy(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    # Replay the live 128GB-host incident: the computed proxy reaches 81%, but
    # macOS reports NORMAL pressure and roughly 57GB remains available.
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_memory_state",
        lambda: syshealth.MemoryState(131072, 38971, 41263, 15709, 55059, 12443),
    )
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_swap_state",
        lambda: syshealth.SwapState(4096, 2777, 1318, encrypted=True),
    )

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--gpu", "3072", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["allowed"] is True
    assert payload["checks"]["swap_pressure"]["allowed"] is True
    assert payload["checks"]["memory_pressure"]["allowed"] is True
    assert payload["checks"]["memory_pressure"]["blockers"] == []


def test_guard_blocks_worker_launch_on_swap_pressure(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    # Memory genuinely pressured (available < floor) so the swap blockers are
    # corroborated and fire. swap pressure ALONE no longer blocks (recalibrated
    # for big-RAM hosts where a small dynamic swapfile fills without real pressure).
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_memory_state",
        lambda: syshealth.MemoryState(131072, 118000, 2000, 2000, 8000, 1072),
    )
    monkeypatch.setattr(
        cli_module.syshealth,
        "get_swap_state",
        lambda: syshealth.SwapState(34560, 33459, 1101, encrypted=True),
    )

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--repo", str(tmp_path), "--gpu", "8192", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["allowed"] is False
    pressure = payload["checks"]["memory_pressure"]
    assert pressure["allowed"] is False
    assert pressure["blockers"][0]["code"] == "SWAP_PRESSURE_HIGH"
    assert pressure["blockers"][0]["required_below_pct"] == 85


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


def test_guard_repo_allows_active_cooperative_session_lease(tmp_path, monkeypatch):
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

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["checks"]["repo"]["allowed"] is True
    assert payload["checks"]["repo"]["holders"][0]["session_id"] == "sess-other"


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


# ── fleet session check (single-writer preflight) smoke tests ───────────────
from fleet_watch import session_coupling as _sc  # noqa: E402


def test_session_check_allows_when_no_other_session(monkeypatch):
    monkeypatch.setattr(_sc, "load_active_leases", lambda: [])
    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["session", "check", "--repo", "/tmp/repoX", "--me", "me"])
    assert result.exit_code == 0
    assert "ALLOW" in result.output


def test_session_check_conflicts_on_other_live_session(monkeypatch):
    live = {"session_id": "OTHER", "owner_pid": os.getpid(), "repo_dir": "/tmp/repoX",
            "status": "ACTIVE", "last_heartbeat_at": "x", "shutdown_at": None,
            "repo_lock_mode": "cooperative"}
    monkeypatch.setattr(_sc, "load_active_leases", lambda: [live])
    monkeypatch.setattr(_sc, "default_age_of", lambda hb: 5.0)  # fresh heartbeat
    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli, ["session", "check", "--repo", "/tmp/repoX", "--me", "me", "--json"])
    assert result.exit_code == 3
    payload = json.loads(result.output)
    assert payload["decision"] == "CONFLICT"
    assert payload["conflicts"][0]["session_id"] == "OTHER"


def test_session_check_failclosed_when_registry_unavailable(monkeypatch):
    monkeypatch.setattr(_sc, "load_active_leases", lambda: None)
    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["session", "check", "--repo", "/tmp/repoX"])
    assert result.exit_code == 4
    assert "UNKNOWN" in result.output


def _census_snapshot(loaded_pid=4242):
    from fleet_watch.census import probes as census_probes

    return census_probes.SystemSnapshot(
        machine=census_probes.MachineInfo(
            host="testhost", os="Darwin 25.5.0", cores=8, ram_gb=16
        ),
        user_agents=[
            census_probes.ParsedPlist(
                path=Path("/tmp/LaunchAgents/com.x.job.plist"),
                label="com.x.job",
                target="/usr/bin/true",
                triggers=("RunAtLoad",),
                keys=("Label", "Program"),
            )
        ],
        launchctl={
            "com.x.job": census_probes.LaunchctlEntry("com.x.job", loaded_pid, 0)
        },
    )


def test_census_writes_a_valid_receipt_and_reports_its_path(tmp_path, monkeypatch):
    from fleet_watch import census as census_mod

    monkeypatch.setattr(
        census_mod, "collect_snapshot", lambda *a, **k: _census_snapshot()
    )
    monkeypatch.setattr(cli_module, "_census_registry_rows", lambda: [])

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli, ["census", "--receipt-dir", str(tmp_path), "--json"]
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["schema_version"] == census_mod.SCHEMA_VERSION
    assert payload["totals"]["items"] >= 1
    assert (tmp_path / "latest.json").exists()
    assert census_mod.validate(json.loads((tmp_path / "latest.json").read_text())) == []


def test_census_refuses_and_exits_nonzero_when_every_probe_returns_nothing(
    tmp_path, monkeypatch
):
    from fleet_watch import census as census_mod
    from fleet_watch.census import probes as census_probes

    empty = census_probes.SystemSnapshot(
        machine=census_probes.MachineInfo(host="testhost", os="Darwin 25.5.0")
    )
    monkeypatch.setattr(census_mod, "collect_snapshot", lambda *a, **k: empty)
    monkeypatch.setattr(cli_module, "_census_registry_rows", lambda: [])

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["census", "--receipt-dir", str(tmp_path)])

    assert result.exit_code == 1
    assert "REFUSAL" in result.output
    assert not (tmp_path / "latest.json").exists()


def test_census_emit_launchd_plist_installs_nothing(monkeypatch):
    called = []
    monkeypatch.setattr(
        cli_module.subprocess, "run", lambda *a, **k: called.append(a) or None
    )

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["census", "--emit-launchd-plist"])

    assert result.exit_code == 0
    assert "io.fleet-watch.census" in result.output
    assert called == []
