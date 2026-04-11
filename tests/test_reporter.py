"""Tests for the state reporter."""

import os
import json
import sqlite3
from pathlib import Path

from fleet_watch import events, registry, reporter


def _fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 0)"
    )
    conn.commit()
    return conn


def test_build_state_empty():
    conn = _fresh_conn()
    state = reporter.build_state(conn)
    assert state["agent_interface"] == "fleet guard --json"
    assert state["process_count"] == 0
    assert state["gpu_budget"]["allocated_mb"] == 0
    assert "safe_ports" in state
    assert state["external_resources"] == []


def test_build_state_with_process():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="mlx", workstream="sov", port=8100, gpu_mb=54000)
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-1",
        workstream="paper",
        name="Thunder abc123",
        repo_dir="/tmp/fleet-watch",
        status="RUNNING",
    )
    state = reporter.build_state(conn)
    assert state["process_count"] == 1
    assert state["gpu_budget"]["allocated_mb"] == 54000
    assert 8100 in state["ports_claimed"]
    assert len(state["external_resources"]) == 1


def test_build_guard_state_includes_active_session_repo_lock():
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn,
        "sess-1",
        owner_pid=os.getpid(),
        repo_dir="/tmp/fleet-watch",
    )
    state = reporter.build_guard_state(conn)
    assert state["repos_locked"][str(Path("/tmp/fleet-watch").resolve())] == os.getpid()


def test_markdown_report_structure():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="mlx", workstream="sov", port=8100)
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-1",
        workstream="paper",
        name="Thunder abc123",
        status="RUNNING",
    )
    md = reporter.generate_markdown(reporter.build_state(conn))
    assert "# Fleet Watch" in md
    assert "### sov" in md
    assert "### paper" in md
    assert "mlx" in md
    assert "Resource Budget" in md
    assert "Ownership" in md


def test_markdown_empty():
    conn = _fresh_conn()
    md = reporter.generate_markdown(reporter.build_state(conn))
    assert "No active workstreams." in md


def test_json_report_parseable():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="test", workstream="ws")
    json_str = reporter.generate_json(reporter.build_state(conn))
    parsed = json.loads(json_str)
    assert parsed["process_count"] == 1
    assert "gpu_budget" in parsed


def test_markdown_reports_attention_required_sessions(monkeypatch):
    conn = _fresh_conn()
    monkeypatch.setattr(
        reporter.syshealth,
        "get_session_processes",
        lambda patterns=None: [
            reporter.syshealth.SessionProcess(
                pid=61042,
                name="Codex",
                kind="codex",
                rss_mb=84,
                cpu_pct=61.7,
                started="1:57PM",
                tty="??",
                command="codex",
                member_count=2,
                attention=True,
                classification="detached_hot",
                evidence=["launcher ancestry detached", "cpu 61.7%"],
            ),
        ],
    )
    monkeypatch.setattr(reporter.syshealth, "get_idle_processes", lambda **kwargs: [])

    md = reporter.generate_markdown(reporter.build_state(conn))

    assert "Attention Required Sessions" in md
    assert "detached_hot" in md


def test_write_report(tmp_path):
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="test", workstream="ws")
    md_path, json_path = reporter.write_report(conn, output_dir=tmp_path)
    assert md_path.exists()
    assert json_path.exists()
    assert "Fleet Watch" in md_path.read_text()
    parsed = json.loads(json_path.read_text())
    assert parsed["process_count"] == 1


def test_changelog_records_process_added(tmp_path):
    """Changelog records when a new process appears."""
    conn = _fresh_conn()
    # First report: empty
    reporter.write_report(conn, output_dir=tmp_path)
    # Add a process
    registry.register_process(conn, pid=9999, name="new-mlx", workstream="ws", gpu_mb=8192)
    # Second report: should record the addition
    reporter.write_report(conn, output_dir=tmp_path)
    log = (tmp_path / "state_changelog.jsonl").read_text().strip().splitlines()
    assert len(log) >= 1
    entry = json.loads(log[-1])
    assert "delta" in entry
    added = entry["delta"].get("processes_added", [])
    assert any(p["name"] == "new-mlx" for p in added)


def test_changelog_records_process_removed(tmp_path):
    """Changelog records when a process disappears."""
    conn = _fresh_conn()
    registry.register_process(conn, pid=9999, name="old-mlx", workstream="ws")
    reporter.write_report(conn, output_dir=tmp_path)
    # Remove the process
    registry.release_process(conn, 9999)
    reporter.write_report(conn, output_dir=tmp_path)
    log = (tmp_path / "state_changelog.jsonl").read_text().strip().splitlines()
    entry = json.loads(log[-1])
    removed = entry["delta"].get("processes_removed", [])
    assert any(p["name"] == "old-mlx" for p in removed)


def test_changelog_records_gpu_change(tmp_path):
    """Changelog records GPU budget changes."""
    conn = _fresh_conn()
    reporter.write_report(conn, output_dir=tmp_path)
    registry.register_process(conn, pid=9999, name="gpu-hog", workstream="ws", gpu_mb=50000)
    reporter.write_report(conn, output_dir=tmp_path)
    log = (tmp_path / "state_changelog.jsonl").read_text().strip().splitlines()
    entry = json.loads(log[-1])
    gpu = entry["delta"].get("gpu_allocated_mb")
    assert gpu is not None
    assert gpu["old"] == 0
    assert gpu["new"] == 50000


def test_changelog_no_entry_when_no_change(tmp_path):
    """Changelog doesn't append when nothing changed."""
    conn = _fresh_conn()
    reporter.write_report(conn, output_dir=tmp_path)
    reporter.write_report(conn, output_dir=tmp_path)
    log_path = tmp_path / "state_changelog.jsonl"
    if log_path.exists():
        lines = log_path.read_text().strip().splitlines()
        # At most the initial state diff, no duplicate
        assert len(lines) <= 1


def test_changelog_decays_old_entries(tmp_path):
    """Changelog trims oldest entries when exceeding max."""
    conn = _fresh_conn()
    log_path = tmp_path / "state_changelog.jsonl"
    # Write more than CHANGELOG_MAX_LINES entries directly
    with log_path.open("w") as f:
        for i in range(reporter.CHANGELOG_MAX_LINES + 100):
            f.write(json.dumps({"timestamp": f"T{i}", "delta": {"i": i}}) + "\n")
    # Trigger decay by appending one more
    reporter._append_changelog(log_path, {"timestamp": "T-final", "delta": {}})
    lines = log_path.read_text().strip().splitlines()
    assert len(lines) <= reporter.CHANGELOG_MAX_LINES
    # Most recent entry is preserved
    assert "T-final" in lines[-1]
