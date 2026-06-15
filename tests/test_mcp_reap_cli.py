"""NS-17 B3 (kill half): `fleet reap --include-mcp` opt-in reaping of dead-session
MCP servers. No real process is killed — _terminate_orphan is monkeypatched."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from click.testing import CliRunner

from fleet_watch import cli
from fleet_watch.discovery.mcp_orphan_detector import MCPOrphanResult


class FakeConn:
    def close(self):
        pass


def _setup(monkeypatch, mcp_pids):
    monkeypatch.setattr(cli, "_get_conn", lambda: FakeConn())
    monkeypatch.setattr(cli.registry, "get_reapable_processes", lambda conn: [])
    monkeypatch.setattr(cli.registry, "_pid_exists", lambda pid: False)
    monkeypatch.setattr(
        cli.mcp_orphan_detector, "detect",
        lambda: MCPOrphanResult(
            mcp_process_count=10, orphans_detected=bool(mcp_pids),
            orphan_pids=list(mcp_pids), estimated_recovered_mb=70,
            suggested_kill_command="kill " + " ".join(map(str, mcp_pids)),
        ),
    )
    calls: list[int] = []
    monkeypatch.setattr(cli, "_terminate_orphan", lambda pid, **k: (calls.append(pid), True)[1])
    monkeypatch.setattr(cli.events, "log_event", lambda *a, **k: None)
    monkeypatch.setattr(cli.reporter, "write_report", lambda conn: None)
    return calls


def test_reap_without_flag_ignores_mcp(monkeypatch):
    calls = _setup(monkeypatch, [201, 202])
    res = CliRunner().invoke(cli.reap, [])
    assert "No orphan-confirmed processes" in res.output  # MCP NOT included
    assert calls == []


def test_reap_include_mcp_dryrun_lists_no_kill(monkeypatch):
    calls = _setup(monkeypatch, [201, 202])
    res = CliRunner().invoke(cli.reap, ["--include-mcp"])
    assert "201" in res.output and "202" in res.output
    assert calls == []  # dry-run kills nothing


def test_reap_include_mcp_confirm_kills(monkeypatch):
    calls = _setup(monkeypatch, [201, 202])
    res = CliRunner().invoke(cli.reap, ["--include-mcp", "--confirm"])
    assert set(calls) == {201, 202}  # both terminated
    assert res.exit_code == 0


def test_reap_confirm_without_flag_kills_nothing(monkeypatch):
    # Existing behavior preserved: --confirm alone, no MCP, empty registry → no kills.
    calls = _setup(monkeypatch, [201])
    CliRunner().invoke(cli.reap, ["--confirm"])
    assert calls == []


def test_mcp_reap_candidates_shape(monkeypatch):
    monkeypatch.setattr(cli.mcp_orphan_detector, "detect",
                        lambda: MCPOrphanResult(orphan_pids=[5], orphans_detected=True))
    cands = cli._mcp_reap_candidates()
    assert len(cands) == 1
    assert cands[0]["source"] == "mcp" and cands[0]["pid"] == 5


def test_mcp_reap_candidates_failsoft(monkeypatch):
    def boom():
        raise RuntimeError("detector down")
    monkeypatch.setattr(cli.mcp_orphan_detector, "detect", boom)
    assert cli._mcp_reap_candidates() == []  # never breaks reap


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
