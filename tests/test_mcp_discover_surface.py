"""NS-17 B3 (surfacing half): `fleet discover` reports MCP orphans read-only.
Tests the pure formatter _mcp_surface_lines (no I/O, no kill)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fleet_watch import cli
from fleet_watch.discovery.mcp_orphan_detector import MCPOrphanResult


def test_no_servers_no_lines():
    r = MCPOrphanResult(mcp_process_count=0)
    assert cli._mcp_surface_lines(r) == []


def test_servers_no_orphans():
    r = MCPOrphanResult(mcp_process_count=6, orphans_detected=False, live_pids=[1, 2, 3, 4, 5, 6])
    lines = cli._mcp_surface_lines(r)
    assert len(lines) == 1
    assert "6 server(s) tracked, 0 orphans" in lines[0]


def test_orphans_surfaced_with_kill_command():
    r = MCPOrphanResult(
        mcp_process_count=8, orphans_detected=True,
        orphan_pids=[201, 202], estimated_recovered_mb=140,
        suggested_kill_command="kill 201 202",
    )
    lines = cli._mcp_surface_lines(r)
    assert any("2 dead-session orphan(s)" in x for x in lines)
    assert any("~140MB" in x for x in lines)
    assert any("kill 201 202" in x for x in lines)


def test_no_kill_line_when_command_empty():
    r = MCPOrphanResult(mcp_process_count=3, orphans_detected=True,
                        orphan_pids=[9], estimated_recovered_mb=10,
                        suggested_kill_command="")
    lines = cli._mcp_surface_lines(r)
    # orphan summary present, but no 'suggested:' line when command is empty
    assert any("dead-session orphan" in x for x in lines)
    assert not any(x.strip().startswith("suggested:") for x in lines)


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
