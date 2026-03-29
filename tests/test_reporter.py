"""Tests for the state reporter."""

import json
import sqlite3

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
    assert state["process_count"] == 0
    assert state["gpu_budget"]["allocated_mb"] == 0


def test_build_state_with_process():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="mlx", workstream="sov", port=8100, gpu_mb=54000)
    state = reporter.build_state(conn)
    assert state["process_count"] == 1
    assert state["gpu_budget"]["allocated_mb"] == 54000
    assert 8100 in state["ports_claimed"]


def test_markdown_report_structure():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="mlx", workstream="sov", port=8100)
    md = reporter.generate_markdown(reporter.build_state(conn))
    assert "# Fleet Watch State Report" in md
    assert "Active Processes (1)" in md
    assert "mlx" in md
    assert "Resource Budget" in md


def test_markdown_empty():
    conn = _fresh_conn()
    md = reporter.generate_markdown(reporter.build_state(conn))
    assert "No active processes." in md


def test_json_report_parseable():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="test", workstream="ws")
    json_str = reporter.generate_json(reporter.build_state(conn))
    parsed = json.loads(json_str)
    assert parsed["process_count"] == 1
    assert "gpu_budget" in parsed


def test_write_report(tmp_path):
    conn = _fresh_conn()
    registry.register_process(conn, pid=1234, name="test", workstream="ws")
    md_path, json_path = reporter.write_report(conn, output_dir=tmp_path)
    assert md_path.exists()
    assert json_path.exists()
    assert "Fleet Watch" in md_path.read_text()
    parsed = json.loads(json_path.read_text())
    assert parsed["process_count"] == 1
