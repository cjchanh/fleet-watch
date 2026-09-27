"""Launchd-supervised MCP daemons are keepers, never orphans (Fix 1 of the
2026-09-26 orphan recon).

The daemon line "MCP: 8 dead-session orphan(s)" was a false-positive churn:
those 8 pids are `com.cds.mcp-gateway*` launchd jobs with `KeepAlive=true`.
ppid=1 is how launchd parents its jobs, and manual kill is the exact event the
supervisor respawns from. The detector now resolves launchctl PID->Label and
holds supervised rows out of the orphan set.

Kill-direction rule (build_candidates): when the supervision map cannot be
read, every reparented row is held — a ppid<=1 row cannot be told apart from a
supervised daemon, and a reclaim path must never act on an unreadable
distinction. The advisory detect() path keeps surfacing in that case (it never
kills), matching its historical output.

PLANTED_BAD fixture: PLANTED_BAD_KEEPALIVE_GATEWAY — a supervised KeepAlive
gateway presented as an orphan candidate. The kill path must REJECT it. If it
ever reaches the candidate set, this batch is void.
"""
from __future__ import annotations

from fleet_watch.discovery import mcp_orphan_detector as M


def row(pid: int, ppid: int, cmd: str = "python /x/mcp_demo_server.py", rss: int = 10):
    return {"pid": pid, "ppid": ppid, "rss_mb": rss, "cmd": cmd}


GATEWAY = "/Users/cj/.codex/scripts/mcp_session_memory_server.py"


def test_planted_bad_keepalive_gateway_is_rejected(monkeypatch):
    """PLANTED_BAD: a supervised KeepAlive gateway must never be a candidate."""
    monkeypatch.setattr(
        M, "_launchd_supervised_pids", lambda: {1048: "com.cds.mcp-gateway"}
    )
    candidates = M.build_candidates([row(1048, 1, cmd=f"python {GATEWAY}")])
    assert candidates == [], "supervised gateway reached the kill path (batch void)"


def test_supervised_gateway_is_not_an_orphan_in_detect(monkeypatch):
    monkeypatch.setattr(
        M, "_launchd_supervised_pids", lambda: {1048: "com.cds.mcp-gateway"}
    )
    monkeypatch.setattr(
        M, "_get_mcp_processes_with_status",
        lambda: ([row(1048, 1, cmd=f"python {GATEWAY}")], None),
    )
    monkeypatch.setattr(M, "_get_mcp_processes", lambda: [row(1048, 1, cmd=f"python {GATEWAY}")])
    result = M.detect()
    assert result.orphan_pids == []
    assert 1048 in result.live_pids
    assert result.orphans_detected is False


def test_true_orphan_ppid1_still_flagged(monkeypatch):
    """Control: a reparented row the supervisor does NOT own is still an orphan."""
    monkeypatch.setattr(M, "_launchd_supervised_pids", lambda: {})
    assert M.build_candidates([row(2001, 1)])[0].pid == 2001
    monkeypatch.setattr(
        M, "_get_mcp_processes_with_status", lambda: ([row(2001, 1)], None)
    )
    monkeypatch.setattr(M, "_get_mcp_processes", lambda: [row(2001, 1)])
    assert M.detect().orphan_pids == [2001]


def test_unreadable_map_fails_closed_for_kill_path(monkeypatch):
    """No supervision map: every reparented row is held from the kill path."""
    monkeypatch.setattr(M, "_launchd_supervised_pids", lambda: None)
    assert M.build_candidates([row(3001, 1), row(3002, 1)]) == []


def test_unreadable_map_still_allows_dead_parent_orphans(monkeypatch):
    """Control: parent-proven-dead evidence stands on its own."""
    monkeypatch.setattr(M, "_launchd_supervised_pids", lambda: None)
    monkeypatch.setattr(M, "_pid_alive", lambda pid: False)
    candidates = M.build_candidates([row(3003, 4242)], exists=lambda pid: False)
    assert [c.pid for c in candidates] == [3003]
    assert candidates[0].owner.reparented is False


def test_unreadable_map_keeps_advisory_surfacing(monkeypatch):
    """detect() never hides rows when the map is unreadable (it never kills)."""
    monkeypatch.setattr(M, "_launchd_supervised_pids", lambda: None)
    monkeypatch.setattr(M, "_get_mcp_processes_with_status", lambda: ([row(4001, 1)], None))
    monkeypatch.setattr(M, "_get_mcp_processes", lambda: [row(4001, 1)])
    assert M.detect().orphan_pids == [4001]


def test_live_parent_keeper_still_kept(monkeypatch):
    """Control: a session child with a live parent is a keeper either way."""
    monkeypatch.setattr(M, "_launchd_supervised_pids", lambda: {})
    assert M.build_candidates([row(5001, 5000)], exists=lambda pid: True) == []
