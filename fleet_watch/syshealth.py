"""System health — RAM pressure, session inventory, idle detection.

All detection patterns are config-driven via ~/.fleet-watch/config.json.
No product names, tool names, or install paths are hardcoded.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass, field
from typing import Any

from fleet_watch import registry


# --- Default patterns (overridable via config.json) ---

DEFAULT_SESSION_PATTERNS: list[dict[str, str]] = [
    {
        "name": "Claude Code",
        "kind": "claude-code",
        "process_match": r"/claude\b.*--",
    },
    {
        "name": "Codex",
        "kind": "codex",
        "process_match": r"/codex\b",
    },
]

DEFAULT_IDLE_PATTERNS: list[str] = [
    r"reranker",
    r"socat.*TCP-LISTEN",
    r"mlx_lm.*server",
    r"mlx_vlm.*server",
    r"uvicorn",
    r"gunicorn",
    r"vllm.*serve",
]

DEFAULT_IDLE_CPU_THRESHOLD = 1.0
DEFAULT_SESSION_HOT_CPU_THRESHOLD = 20.0

DEFAULT_PRESSURE_THRESHOLDS = {
    "elevated": 70,
    "critical": 85,
}


# --- Memory ---

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
    def is_available(self) -> bool:
        """True if memory telemetry was successfully collected."""
        return self.total_mb > 0

    @property
    def pressure_pct(self) -> int:
        """Memory pressure as percentage. -1 if telemetry unavailable."""
        if not self.is_available:
            return -1
        used = self.active_mb + self.wired_mb + self.compressed_mb
        return int(used / max(self.total_mb, 1) * 100)

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": self.is_available,
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
    """Read system memory state via vm_stat + sysctl (macOS).

    Returns zeroed MemoryState on non-macOS or on failure.
    """
    total_mb = _get_total_memory_mb()
    if total_mb == 0:
        return MemoryState(0, 0, 0, 0, 0, 0)

    pages: dict[str, int] = {}
    page_size = 16384  # default, overridden by vm_stat header
    try:
        out = subprocess.run(
            ["vm_stat"], capture_output=True, text=True, timeout=3,
        )
        if out.returncode != 0:
            return MemoryState(total_mb, 0, 0, total_mb, 0, 0)
        for line in out.stdout.splitlines():
            match = re.match(r"(.+?):\s+(\d+)", line)
            if match:
                pages[match.group(1).strip()] = int(match.group(2))
        ps_match = re.search(r"page size of (\d+) bytes", out.stdout)
        if ps_match:
            page_size = int(ps_match.group(1))
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
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


def _get_total_memory_mb() -> int:
    """Get total physical memory in MB. Returns 0 on failure."""
    try:
        out = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True, text=True, timeout=3,
        )
        if out.returncode == 0:
            return int(out.stdout.strip()) // (1024 * 1024)
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError, ValueError):
        pass
    return 0


# --- Session discovery ---

@dataclass
class SessionProcess:
    """A discovered CLI session process."""
    pid: int
    name: str
    kind: str
    rss_mb: int
    cpu_pct: float
    started: str
    tty: str
    command: str
    ppid: int | None = None
    pgid: int | None = None
    group_leader_pid: int | None = None
    member_pids: list[int] = field(default_factory=list)
    member_count: int = 1
    parent_chain_detached: bool | None = None
    classification: str = "attached"
    attention: bool = False
    evidence: list[str] = field(default_factory=list)


def get_session_processes(
    patterns: list[dict[str, str]] | None = None,
) -> list[SessionProcess]:
    """Discover running CLI session processes by config-driven patterns.

    Each pattern dict has: name, kind, process_match (regex).
    """
    compiled = _compile_session_patterns(
        patterns if patterns is not None else DEFAULT_SESSION_PATTERNS
    )
    if not compiled:
        return []

    lines = _ps_aux_lines()
    raw_matches: list[dict[str, Any]] = []

    for parts in lines:
        cmd = parts[10]
        matched_name = None
        matched_kind = None

        for regex, name, kind in compiled:
            if regex.search(cmd):
                matched_name = name
                matched_kind = kind
                break

        if matched_kind is None:
            continue

        try:
            pid = int(parts[1])
            cpu_pct = float(parts[2])
            rss_mb = int(parts[5]) // 1024
            tty = parts[6]
            started = parts[8]
        except (ValueError, IndexError):
            continue

        info = registry._inspect_process(pid) or {}
        raw_matches.append({
            "pid": pid,
            "name": matched_name,
            "kind": matched_kind,
            "rss_mb": rss_mb,
            "cpu_pct": cpu_pct,
            "started": started,
            "tty": tty,
            "command": cmd[:200],
            "ppid": info.get("ppid"),
            "pgid": info.get("pgid"),
        })

    pgid_groups: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for item in raw_matches:
        group_pid = item["pgid"] or item["pid"]
        pgid_groups.setdefault((item["kind"], group_pid), []).append(item)

    grouped: dict[tuple[str, int, int], list[dict[str, Any]]] = {}
    for (kind, pgid), members in pgid_groups.items():
        by_pid: dict[int, dict[str, Any]] = {m["pid"]: m for m in members}
        for m in members:
            cursor = m["pid"]
            ppid = m["ppid"]
            while ppid in by_pid:
                cursor = ppid
                ppid = by_pid[cursor]["ppid"]
            # cursor is the topmost member; ppid is its external parent.
            # Siblings spawned by the same external parent share ppid here,
            # so use ppid as the family key when it exists.
            family = ppid if ppid is not None else cursor
            grouped.setdefault((kind, pgid, family), []).append(m)

    sessions: list[SessionProcess] = []
    for (_, group_pid, _root), members in grouped.items():
        representative = max(
            members,
            key=lambda item: (item["cpu_pct"], item["rss_mb"], -item["pid"]),
        )
        leader_pid = group_pid or representative["pid"]
        leader_info = registry._inspect_process(leader_pid) or {}
        tty = next(
            (
                candidate["tty"]
                for candidate in members
                if candidate["tty"] not in {"?", "??"}
            ),
            leader_info.get("tty") or representative["tty"],
        )
        detached = registry._is_parent_chain_detached(leader_pid)
        total_cpu = round(sum(item["cpu_pct"] for item in members), 1)
        total_rss = sum(item["rss_mb"] for item in members)
        evidence: list[str] = []
        classification = "attached"
        attention = False

        if detached is True:
            evidence.append("launcher ancestry detached")
            if total_cpu >= DEFAULT_SESSION_HOT_CPU_THRESHOLD:
                classification = "detached_hot"
                attention = True
                evidence.append(f"cpu {total_cpu:.1f}%")
            else:
                classification = "detached"
        elif detached is False:
            evidence.append("launcher ancestry attached")
        else:
            evidence.append("launcher ancestry unknown")

        if len(members) > 1:
            evidence.append(f"{len(members)} matched processes collapsed")

        sessions.append(SessionProcess(
            pid=representative["pid"],
            name=representative["name"],
            kind=representative["kind"],
            rss_mb=total_rss,
            cpu_pct=total_cpu,
            started=representative["started"],
            tty=tty,
            command=representative["command"],
            ppid=leader_info.get("ppid", representative["ppid"]),
            pgid=leader_info.get("pgid", representative["pgid"]),
            group_leader_pid=leader_pid,
            member_pids=sorted(item["pid"] for item in members),
            member_count=len(members),
            parent_chain_detached=detached,
            classification=classification,
            attention=attention,
            evidence=evidence,
        ))

    return sessions


def _compile_session_patterns(
    patterns: list[dict[str, str]],
) -> list[tuple[re.Pattern, str, str]]:
    """Compile session pattern dicts to (regex, name, kind) tuples."""
    compiled = []
    for p in patterns:
        try:
            compiled.append((
                re.compile(p["process_match"]),
                p["name"],
                p["kind"],
            ))
        except (KeyError, re.error):
            continue
    return compiled


# --- Idle detection ---

def get_idle_processes(
    patterns: list[str] | None = None,
    threshold_cpu: float | None = None,
) -> list[dict[str, Any]]:
    """Find processes matching patterns that consume near-zero CPU.

    Patterns and threshold are config-driven.
    """
    pattern_list = patterns if patterns is not None else DEFAULT_IDLE_PATTERNS
    cpu_limit = threshold_cpu if threshold_cpu is not None else DEFAULT_IDLE_CPU_THRESHOLD

    compiled = []
    for p in pattern_list:
        try:
            compiled.append(re.compile(p))
        except re.error:
            continue
    if not compiled:
        return []

    lines = _ps_aux_lines()
    idle: list[dict[str, Any]] = []

    for parts in lines:
        cmd = parts[10]
        if not any(regex.search(cmd) for regex in compiled):
            continue

        try:
            cpu_pct = float(parts[2])
            if cpu_pct > cpu_limit:
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


# --- Shared ---

def _ps_aux_lines() -> list[list[str]]:
    """Run ps aux and return parsed lines (11+ fields each)."""
    try:
        out = subprocess.run(
            ["ps", "aux"], capture_output=True, text=True, timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
        return []
    if out.returncode != 0:
        return []
    result = []
    for line in out.stdout.splitlines()[1:]:
        parts = line.split(None, 10)
        if len(parts) >= 11:
            result.append(parts)
    return result


def load_health_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Extract health-specific config from the main Fleet Watch config.

    Expected config keys (all optional, defaults used if absent):
      session_patterns: [{name, kind, process_match}, ...]
      idle_patterns: ["regex", ...]
      idle_cpu_threshold: float
      pressure_thresholds: {elevated: int, critical: int}
    """
    if config is None:
        return {
            "session_patterns": DEFAULT_SESSION_PATTERNS,
            "idle_patterns": DEFAULT_IDLE_PATTERNS,
            "idle_cpu_threshold": DEFAULT_IDLE_CPU_THRESHOLD,
            "pressure_thresholds": DEFAULT_PRESSURE_THRESHOLDS,
        }
    return {
        "session_patterns": config.get("session_patterns", DEFAULT_SESSION_PATTERNS),
        "idle_patterns": config.get("idle_patterns", DEFAULT_IDLE_PATTERNS),
        "idle_cpu_threshold": config.get("idle_cpu_threshold", DEFAULT_IDLE_CPU_THRESHOLD),
        "pressure_thresholds": config.get("pressure_thresholds", DEFAULT_PRESSURE_THRESHOLDS),
    }


def pressure_label(pressure_pct: int, thresholds: dict[str, int] | None = None) -> str:
    """Return OK / ELEVATED / CRITICAL / UNAVAILABLE based on pressure percentage."""
    if pressure_pct < 0:
        return "UNAVAILABLE"
    t = thresholds or DEFAULT_PRESSURE_THRESHOLDS
    if pressure_pct >= t.get("critical", 85):
        return "CRITICAL"
    if pressure_pct >= t.get("elevated", 70):
        return "ELEVATED"
    return "OK"
