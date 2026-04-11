"""Runaway process detection — sustained high-CPU process scanning.

Detects processes exceeding CPU thresholds for sustained periods.
Used by both the `fleet runaway` CLI command and the daemon tick cycle.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_CPU_THRESHOLD = 90.0
DEFAULT_SUSTAINED_SECONDS = 60

# Daemon uses stricter thresholds: 95% CPU for 3 consecutive ticks (3 min at 60s)
DAEMON_CPU_THRESHOLD = 95.0
DAEMON_CONSECUTIVE_TICKS = 3


@dataclass
class RunawayProcess:
    """A process flagged as runaway."""
    pid: int
    name: str
    cpu_pct: float
    runtime_seconds: int
    command: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "pid": self.pid,
            "name": self.name,
            "cpu_pct": self.cpu_pct,
            "runtime_seconds": self.runtime_seconds,
            "command": self.command,
        }


def _parse_etime(etime_str: str) -> int:
    """Parse ps elapsed time format to seconds.

    Formats: "MM:SS", "HH:MM:SS", "D-HH:MM:SS", or just "SS".
    """
    etime_str = etime_str.strip()
    if not etime_str:
        return 0
    days = 0
    if "-" in etime_str:
        day_part, etime_str = etime_str.split("-", 1)
        try:
            days = int(day_part)
        except ValueError:
            return 0

    parts = etime_str.split(":")
    try:
        if len(parts) == 3:
            hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
        elif len(parts) == 2:
            hours = 0
            minutes, seconds = int(parts[0]), int(parts[1])
        elif len(parts) == 1:
            hours = 0
            minutes = 0
            seconds = int(parts[0])
        else:
            return 0
    except ValueError:
        return 0

    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def scan_runaways(
    cpu_threshold: float = DEFAULT_CPU_THRESHOLD,
    sustained_seconds: int = DEFAULT_SUSTAINED_SECONDS,
) -> list[RunawayProcess]:
    """Scan all processes for CPU usage above threshold sustained for given duration.

    Uses `ps -eo pid,pcpu,etime,command` for a single-pass snapshot.
    A process qualifies as runaway if:
    1. Current CPU% >= cpu_threshold
    2. Process has been running >= sustained_seconds
    """
    # Get all processes with cpu, elapsed time, pid, and command
    try:
        out = subprocess.run(
            ["ps", "-eo", "pid,pcpu,etime,command"],
            capture_output=True, text=True, timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
        return []

    if out.returncode != 0:
        return []

    runaways: list[RunawayProcess] = []
    for line in out.stdout.splitlines()[1:]:
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue

        try:
            pid = int(parts[0])
            cpu_pct = float(parts[1])
            etime_str = parts[2]
            command = parts[3]
        except (ValueError, IndexError):
            continue

        if cpu_pct < cpu_threshold:
            continue

        runtime = _parse_etime(etime_str)
        if runtime < sustained_seconds:
            continue

        # Derive a name from the command
        cmd_parts = command.split()
        if cmd_parts:
            basename = cmd_parts[0].rstrip("/").split("/")[-1]
            name = basename[:40]
        else:
            name = "unknown"

        runaways.append(RunawayProcess(
            pid=pid,
            name=name,
            cpu_pct=cpu_pct,
            runtime_seconds=runtime,
            command=command[:200],
        ))

    return runaways


def kill_runaway(pid: int) -> bool:
    """Send SIGKILL to a runaway process. Returns True if process is gone."""
    if pid <= 0:
        return False
    if pid == os.getpid() or pid == os.getppid():
        return False
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False

    # Brief wait to confirm
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        time.sleep(0.05)

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


@dataclass
class DaemonRunawayTracker:
    """Tracks high-CPU processes across daemon ticks for sustained detection.

    A process must exceed DAEMON_CPU_THRESHOLD for DAEMON_CONSECUTIVE_TICKS
    consecutive ticks before it is flagged as a runaway warning.
    """
    # {pid: consecutive_tick_count}
    tick_counts: dict[int, int] = field(default_factory=dict)
    # {pid: last_cpu_pct} for reporting
    last_cpu: dict[int, float] = field(default_factory=dict)
    # {pid: last_runtime_seconds} for reporting
    last_runtime: dict[int, int] = field(default_factory=dict)

    def tick(self) -> list[RunawayProcess]:
        """Run one daemon tick. Returns newly-flagged runaways (those hitting the threshold)."""
        current = scan_runaways(
            cpu_threshold=DAEMON_CPU_THRESHOLD,
            sustained_seconds=0,  # Runtime check is not needed; tick count covers sustained detection
        )
        current_pids = {r.pid for r in current}
        current_map = {r.pid: r for r in current}

        # Update tick counts (guard against PID reuse — reset if runtime dropped)
        new_counts: dict[int, int] = {}
        for pid in current_pids:
            prev_runtime = self.last_runtime.get(pid)
            if prev_runtime is not None and current_map[pid].runtime_seconds < prev_runtime - 10:
                new_counts[pid] = 1  # PID reuse detected — reset
            else:
                new_counts[pid] = self.tick_counts.get(pid, 0) + 1
            self.last_cpu[pid] = current_map[pid].cpu_pct
            self.last_runtime[pid] = current_map[pid].runtime_seconds

        # Identify pids that just hit the threshold
        newly_flagged: list[RunawayProcess] = []
        for pid, count in new_counts.items():
            if count == DAEMON_CONSECUTIVE_TICKS:
                proc = current_map[pid]
                newly_flagged.append(proc)

        # Clear pids that dropped below threshold
        self.tick_counts = new_counts

        # Clean stale entries from last_cpu/last_runtime
        for pid in list(self.last_cpu.keys()):
            if pid not in current_pids:
                del self.last_cpu[pid]
        for pid in list(self.last_runtime.keys()):
            if pid not in current_pids:
                del self.last_runtime[pid]

        return newly_flagged

    def get_active_warnings(self) -> list[dict[str, Any]]:
        """Return all processes currently at or above the consecutive tick threshold."""
        warnings: list[dict[str, Any]] = []
        for pid, count in self.tick_counts.items():
            if count >= DAEMON_CONSECUTIVE_TICKS:
                warnings.append({
                    "pid": pid,
                    "cpu_pct": self.last_cpu.get(pid, 0.0),
                    "runtime_seconds": self.last_runtime.get(pid, 0),
                    "consecutive_ticks": count,
                })
        return warnings

    def save(self, path: Path) -> None:
        """Persist tracker state to disk for cross-invocation continuity."""
        data = {
            "tick_counts": {str(k): v for k, v in self.tick_counts.items()},
            "last_cpu": {str(k): v for k, v in self.last_cpu.items()},
            "last_runtime": {str(k): v for k, v in self.last_runtime.items()},
        }
        try:
            path.write_text(json.dumps(data, separators=(",", ":")) + "\n")
        except OSError:
            pass  # Never block on persistence failure

    @classmethod
    def load(cls, path: Path) -> "DaemonRunawayTracker":
        """Load tracker state from disk. Returns empty tracker on failure."""
        tracker = cls()
        try:
            data = json.loads(path.read_text())
            tracker.tick_counts = {int(k): v for k, v in data.get("tick_counts", {}).items()}
            tracker.last_cpu = {int(k): v for k, v in data.get("last_cpu", {}).items()}
            tracker.last_runtime = {int(k): v for k, v in data.get("last_runtime", {}).items()}
        except (OSError, json.JSONDecodeError, KeyError, ValueError, TypeError):
            return cls()
        return tracker
