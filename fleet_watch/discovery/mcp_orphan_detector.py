"""MCP-server orphan detector for Fleet Watch (NS-17 / B3).

Each Claude session spawns ~6 CDS MCP stdio servers. When a terminal/session
dies, its servers can linger (73 servers / ~3.1 GB observed from ~8 sessions,
several stale >19h). This detector finds MCP servers whose owning session is
dead and surfaces them for reaping.

Surfacing only — never auto-kills (mirrors orphan_detector's contract; the
actual reap stays the operator / `fleet reap` path, honoring 'never kill running
work without permission'). The load-bearing invariant: a server whose parent
session is ALIVE is never flagged as an orphan.
"""
from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass, field
from typing import Any, Callable

# CDS MCP stdio server scripts (compile/engines/sitrep/session_memory/lexicon/graph
# and any future mcp_*_server.py). Matched against the process command line.
_MCP_SERVER_RE = re.compile(r"mcp_[a-z0-9_]*server\.py|mcp_graph_context_server\.py")


@dataclass
class MCPOrphanResult:
    orphans_detected: bool = False
    mcp_process_count: int = 0
    orphan_pids: list[int] = field(default_factory=list)
    live_pids: list[int] = field(default_factory=list)
    estimated_recovered_mb: int = 0
    suggested_kill_command: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "orphans_detected": self.orphans_detected,
            "mcp_process_count": self.mcp_process_count,
            "orphan_pids": self.orphan_pids,
            "live_pids": self.live_pids,
            "estimated_recovered_mb": self.estimated_recovered_mb,
            "suggested_kill_command": self.suggested_kill_command,
            "error": self.error,
        }


def _pid_alive(pid: int) -> bool:
    if pid <= 1:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists but not ours to signal
    return True


def classify_mcp_orphans(
    procs: list[dict[str, Any]],
    pid_alive: Callable[[int], bool] = _pid_alive,
) -> tuple[list[dict], list[dict]]:
    """Pure split into (orphans, keepers). A process is an orphan iff its parent
    is reparented (ppid<=1) OR the parent process is dead. If the parent (owning
    session) is alive, it is ALWAYS a keeper — never reaped. This is the
    invariant that keeps a live session's servers safe."""
    orphans: list[dict] = []
    keepers: list[dict] = []
    for p in procs:
        ppid = int(p.get("ppid", 0) or 0)
        if ppid <= 1 or not pid_alive(ppid):
            orphans.append(p)
        else:
            keepers.append(p)
    return orphans, keepers


def _get_mcp_processes() -> list[dict[str, Any]]:
    """List live MCP server processes via ps. Returns [{pid, ppid, rss_mb, cmd}]."""
    try:
        out = subprocess.run(
            ["ps", "-eo", "pid,ppid,rss,command"],
            capture_output=True, text=True, timeout=5,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    procs: list[dict[str, Any]] = []
    for line in out.splitlines()[1:]:
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        pid_s, ppid_s, rss_s, cmd = parts
        if not _MCP_SERVER_RE.search(cmd):
            continue
        try:
            procs.append({
                "pid": int(pid_s), "ppid": int(ppid_s),
                "rss_mb": int(rss_s) // 1024, "cmd": cmd,
            })
        except ValueError:
            continue
    return procs


def detect(pid_alive: Callable[[int], bool] = _pid_alive) -> MCPOrphanResult:
    """Scan for dead-session MCP servers. Surfacing only — never kills."""
    procs = _get_mcp_processes()
    orphans, keepers = classify_mcp_orphans(procs, pid_alive)
    orphan_pids = [p["pid"] for p in orphans]
    recovered = sum(p.get("rss_mb", 0) for p in orphans)
    kill_cmd = ("kill " + " ".join(str(p) for p in orphan_pids)) if orphan_pids else ""
    return MCPOrphanResult(
        orphans_detected=bool(orphan_pids),
        mcp_process_count=len(procs),
        orphan_pids=orphan_pids,
        live_pids=[p["pid"] for p in keepers],
        estimated_recovered_mb=recovered,
        suggested_kill_command=kill_cmd,
    )
