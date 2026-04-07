"""Contract freeze tests for stable JSON interfaces."""

import os

from fleet_watch import cli as cli_module
from fleet_watch import registry, reporter


def _patch_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")


def test_guard_json_contract_shape(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = registry.connect()
    registry.register_process(
        conn,
        pid=os.getpid(),
        name="mlx",
        workstream="ws",
        port=8100,
        gpu_mb=4096,
    )

    payload = cli_module._build_guard_payload(
        conn,
        port=8100,
        repo_dir=tmp_path,
        gpu_mb=1024,
    )

    assert set(payload.keys()) == {"allowed", "request", "checks", "state"}
    assert set(payload["request"].keys()) == {"port", "repo_dir", "gpu_mb"}
    assert set(payload["checks"].keys()) == {"port", "repo", "gpu"}
    assert set(payload["checks"]["port"].keys()) == {
        "allowed",
        "reason",
        "holder",
        "suggested_ports",
    }
    assert set(payload["checks"]["repo"].keys()) == {"allowed", "reason", "holder"}
    assert set(payload["checks"]["gpu"].keys()) == {
        "allowed",
        "reason",
        "requested_mb",
        "available_mb",
        "suggested_max_mb",
    }
    assert set(payload["state"].keys()) == {
        "process_count",
        "occupied_ports",
        "safe_ports",
        "locked_repos",
        "gpu_budget",
        "external_resources",
    }
    assert set(payload["state"]["gpu_budget"].keys()) == {
        "total_mb",
        "reserve_mb",
        "allocated_mb",
        "available_mb",
    }


def test_guard_state_contract_shape(tmp_path, monkeypatch):
    """Guard state has no syshealth keys — fast path only."""
    _patch_paths(monkeypatch, tmp_path)
    conn = registry.connect()
    state = reporter.build_guard_state(conn)
    assert set(state.keys()) == {
        "agent_interface",
        "generated_utc",
        "processes",
        "external_resources",
        "process_count",
        "gpu_budget",
        "ports_claimed",
        "preferred_ports",
        "safe_ports",
        "repos_locked",
    }
    # No syshealth keys
    assert "system_memory" not in state
    assert "sessions" not in state
    assert "idle_processes" not in state


def test_state_json_contract_shape(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = registry.connect()
    registry.register_process(
        conn,
        pid=1234,
        name="mlx",
        workstream="ws",
        port=8100,
        gpu_mb=4096,
    )

    state = reporter.build_state(conn)

    assert set(state.keys()) == {
        "agent_interface",
        "generated_utc",
        "processes",
        "external_resources",
        "process_count",
        "gpu_budget",
        "ports_claimed",
        "preferred_ports",
        "safe_ports",
        "repos_locked",
        "stale_processes",
        "recent_events",
        "conflicts_prevented_24h",
        "system_memory",
        "sessions",
        "idle_processes",
    }
    assert set(state["gpu_budget"].keys()) == {
        "total_mb",
        "reserve_mb",
        "allocated_mb",
        "available_mb",
    }
