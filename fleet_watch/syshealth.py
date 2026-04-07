"""System health — RAM pressure, session inventory, idle detection."""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from typing import Any


@dataclass
class MemoryState:
    """System memory snapshot in MB."""
    total_mb: int
    active_mb: int
    inactive_mb: int
    free_mb: int
    compressed_mb: int
    wired_mb: int

    @property
    def available_mb(self) -> int:
        return self.free_mb + self.inactive_mb

    @property
    def pressure_pct(self) -> int:
        """Memory pressure as percentage. >80% is constrained."""
        used = self.active_mb + self.wired_mb + self.compressed_mb
        return int(used / max(self.total_mb, 1) * 100)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_mb": self.total_mb,
            "active_mb": self.active_mb,
            "inactive_mb": self.inactive_mb,
            "free_mb": self.free_mb,
            "compressed_mb": self.compressed_mb,
            "wired_mb": self.wired_mb,
            "available_mb": self.available_mb,
            "pressure_pct": self.pressure_pct,
        }


def get_memory_state() -> MemoryState:
    """Read system memory state via vm_stat + sysctl."""
    total_mb = 0
    try:
        out = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True, text=True, timeout=3,
        )
        if out.returncode == 0:
            total_mb = int(out.stdout.strip()) // (1024 * 1024)
    except Exception:
        total_mb = 131072  # 128GB fallback

    pages: dict[str, int] = {}
    try:
        out = subprocess.run(
            ["vm_stat"], capture_output=True, text=True, timeout=3,
        )
        # vm_stat uses 16384-byte pages on Apple Silicon
        page_size = 16384
        for line in out.stdout.splitlines():
            match = re.match(r"(.+?):\s+(\d+)", line)
            if match:
                pages[match.group(1).strip()] = int(match.group(2))
        if "Mach Virtual Memory Statistics" in out.stdout:
            ps_match = re.search(r"page size of (\d+) bytes", out.stdout)
            if ps_match:
                page_size = int(ps_match.group(1))
    except Exception:
        return MemoryState(total_mb, 0, 0, total_mb, 0, 0)

    def mb(key: str) -> int:
        return pages.get(key, 0) * page_size // (1024 * 1024)

    return MemoryState(
        total_mb=total_mb,
        active_mb=mb("Pages active"),
        inactive_mb=mb("Pages inactive"),
        free_mb=mb("Pages free"),
        compressed_mb=mb("Pages stored in compressor"),
        wired_mb=mb("Pages wired down"),
    )


@dataclass
class SessionProcess:
    """A Claude Code or Codex session process."""
    pid: int
    name: str
    kind: str  # "claude-code" or "codex"
    rss_mb: int
    cpu_pct: float
    started: str
    tty: str
    command: str


def get_session_processes() -> list[SessionProcess]:
    """Discover running Claude Code and Codex processes."""
    try:
        out = subprocess.run(
            ["ps", "aux"],
            capture_output=True, text=True, timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []

    sessions: list[SessionProcess] = []
    for line in out.stdout.splitlines()[1:]:
        parts = line.split(None, 10)
        if len(parts) < 11:
            continue

        cmd = parts[10]
        kind = None
        name = None

        # Claude Code CLI sessions
        if "/claude" in cmd and ("--dangerously-skip-permissions" in cmd or "--effort" in cmd):
            kind = "claude-code"
            name = "Claude Code"
        # Codex sandbox processes
        elif "/codex" in cmd and "codex" in parts[10].lower():
            kind = "codex"
            name = "Codex"

        if kind is None:
            continue

        try:
            pid = int(parts[1])
            cpu_pct = float(parts[2])
            rss_mb = int(parts[5]) // 1024
            tty = parts[6]
            started = parts[8]
        except (ValueError, IndexError):
            continue

        sessions.append(SessionProcess(
            pid=pid,
            name=name,
            kind=kind,
            rss_mb=rss_mb,
            cpu_pct=cpu_pct,
            started=started,
            tty=tty,
            command=cmd[:200],
        ))

    return sessions


def get_idle_processes(threshold_cpu: float = 1.0) -> list[dict[str, Any]]:
    """Find registered-style processes that are alive but consuming near-zero CPU.

    Returns process info dicts for anything matching known AI workload patterns
    that's below the CPU threshold — likely idle and reclaimable.
    """
    idle_patterns = [
        re.compile(r"reranker"),
        re.compile(r"socat.*TCP-LISTEN"),
        re.compile(r"mlx_lm.*server"),
        re.compile(r"mlx_vlm.*server"),
        re.compile(r"uvicorn"),
    ]

    try:
        out = subprocess.run(
            ["ps", "aux"],
            capture_output=True, text=True, timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []

    idle: list[dict[str, Any]] = []
    for line in out.stdout.splitlines()[1:]:
        parts = line.split(None, 10)
        if len(parts) < 11:
            continue

        cmd = parts[10]
        matched = False
        for pattern in idle_patterns:
            if pattern.search(cmd):
                matched = True
                break
        if not matched:
            continue

        try:
            cpu_pct = float(parts[2])
            if cpu_pct > threshold_cpu:
                continue
            pid = int(parts[1])
            rss_mb = int(parts[5]) // 1024
            started = parts[8]
        except (ValueError, IndexError):
            continue

        idle.append({
            "pid": pid,
            "command": cmd[:200],
            "cpu_pct": cpu_pct,
            "rss_mb": rss_mb,
            "started": started,
        })

    return idle
