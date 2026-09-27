"""Runaway process detection — sustained high-CPU process scanning.

Detects processes exceeding CPU thresholds for sustained periods.
Used by both the `fleet runaway` CLI command and the daemon tick cycle.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass, field
from math import ceil, isfinite
from pathlib import Path
from typing import Any

from fleet_watch.constants import PS_BIN

DEFAULT_CPU_THRESHOLD = 90.0
DEFAULT_SUSTAINED_SECONDS = 60

# Daemon uses stricter thresholds: 95% CPU for 3 consecutive ticks (3 min at 60s)
DAEMON_CPU_THRESHOLD = 95.0
DAEMON_CONSECUTIVE_TICKS = 3

# ps reports 100% for one fully occupied logical CPU. Aggregate pressure is
# advisory and is expressed relative to the host's full logical CPU capacity.
AGGREGATE_CAPACITY_THRESHOLD_PCT = 25.0
AGGREGATE_CONTRIBUTOR_CPU_FLOOR_PCT = 5.0
AGGREGATE_MIN_CONTRIBUTOR_FRACTION = 0.5


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


@dataclass(frozen=True)
class AggregatePressureGroup:
    """One executable identity consuming material host capacity in aggregate.

    This is load evidence only. Parent PID, runtime, and process count do not
    prove orphanhood or ownership, so aggregate groups are never signal eligible.
    """

    identity: str
    aggregate_cpu_pct: float
    capacity_pct: float
    contributor_count: int
    logical_cpu_count: int
    parent_one_count: int
    pids: list[int]
    orphan_status: str = "UNKNOWN"
    signal_eligible: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": "aggregate_process_pressure",
            "identity": self.identity,
            "aggregate_cpu_pct": self.aggregate_cpu_pct,
            "capacity_pct": self.capacity_pct,
            "contributor_count": self.contributor_count,
            "logical_cpu_count": self.logical_cpu_count,
            "parent_one_count": self.parent_one_count,
            "pids": self.pids,
            "orphan_status": self.orphan_status,
            "signal_eligible": self.signal_eligible,
        }


@dataclass(frozen=True)
class AggregatePressureScan:
    """Aggregate collection result with explicit uncertainty."""

    status: str
    groups: list[AggregatePressureGroup]
    reason: str | None = None


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
            [PS_BIN, "-eo", "pid,pcpu,etime,command"],
            capture_output=True, text=True, timeout=5, check=False,
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


def scan_aggregate_pressure(
    logical_cpu_count: int | None = None,
    capacity_threshold_pct: float = AGGREGATE_CAPACITY_THRESHOLD_PCT,
    contributor_cpu_floor_pct: float = AGGREGATE_CONTRIBUTOR_CPU_FLOOR_PCT,
    sustained_seconds: int = DEFAULT_SUSTAINED_SECONDS,
) -> AggregatePressureScan:
    """Detect distributed process pressure that per-process thresholds miss.

    Parent-1 processes are grouped by executable basename after one bounded ps
    snapshot. Parent 1 narrows the candidate shape but does not prove orphanhood.
    A group must have at least half as many contributors as logical CPUs and
    consume at least ``capacity_threshold_pct`` of total CPU capacity.
    """
    cpu_count = logical_cpu_count if logical_cpu_count is not None else os.cpu_count()
    valid_parameters = (
        isinstance(cpu_count, int)
        and not isinstance(cpu_count, bool)
        and cpu_count > 0
        and isfinite(capacity_threshold_pct)
        and capacity_threshold_pct >= 0
        and isfinite(contributor_cpu_floor_pct)
        and contributor_cpu_floor_pct >= 0
        and isinstance(sustained_seconds, int)
        and sustained_seconds >= 0
    )
    if not valid_parameters:
        return AggregatePressureScan(
            status="UNKNOWN",
            groups=[],
            reason="invalid_scan_parameters",
        )

    try:
        result = subprocess.run(
            [PS_BIN, "-ww", "-eo", "pid=,ppid=,pcpu=,etime=,comm="],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return AggregatePressureScan(status="UNKNOWN", groups=[], reason="ps_timeout")
    except FileNotFoundError:
        return AggregatePressureScan(
            status="UNKNOWN", groups=[], reason="ps_unavailable"
        )
    except PermissionError:
        return AggregatePressureScan(
            status="UNKNOWN", groups=[], reason="ps_permission_denied"
        )
    except OSError:
        return AggregatePressureScan(status="UNKNOWN", groups=[], reason="ps_os_error")

    if result.returncode != 0:
        return AggregatePressureScan(
            status="UNKNOWN",
            groups=[],
            reason=f"ps_exit_{result.returncode}",
        )

    contributors: dict[str, list[tuple[int, int, float]]] = defaultdict(list)
    seen_pids: set[int] = set()
    valid_sample_count = 0
    for line in (result.stdout or "").splitlines():
        parts = line.split(None, 4)
        if len(parts) < 5:
            continue
        try:
            pid = int(parts[0])
            parent_pid = int(parts[1])
            cpu_pct = float(parts[2])
        except (TypeError, ValueError):
            continue
        if pid <= 0 or parent_pid < 0 or not isfinite(cpu_pct) or cpu_pct < 0:
            continue
        runtime_seconds = _parse_etime(parts[3])
        if runtime_seconds == 0 and parts[3].strip() not in {
            "0",
            "00",
            "0:00",
            "00:00",
            "0:00:00",
            "00:00:00",
        }:
            continue
        comm = parts[4].strip()
        if not comm:
            continue
        valid_sample_count += 1
        if pid in seen_pids:
            continue
        seen_pids.add(pid)
        if parent_pid != 1:
            continue
        if cpu_pct < contributor_cpu_floor_pct:
            continue
        if runtime_seconds < sustained_seconds:
            continue
        identity = Path(comm.rstrip("/")).name[:40]
        if not identity:
            continue
        contributors[identity].append((pid, parent_pid, cpu_pct))

    if valid_sample_count == 0:
        return AggregatePressureScan(
            status="UNKNOWN",
            groups=[],
            reason="ps_no_valid_samples",
        )

    minimum_contributors = max(
        4,
        ceil(cpu_count * AGGREGATE_MIN_CONTRIBUTOR_FRACTION),
    )
    groups: list[AggregatePressureGroup] = []
    for identity, samples in contributors.items():
        if len(samples) < minimum_contributors:
            continue
        aggregate_cpu_pct = round(sum(sample[2] for sample in samples), 1)
        capacity_pct = round(aggregate_cpu_pct / cpu_count, 1)
        if capacity_pct < capacity_threshold_pct:
            continue
        groups.append(
            AggregatePressureGroup(
                identity=identity,
                aggregate_cpu_pct=aggregate_cpu_pct,
                capacity_pct=capacity_pct,
                contributor_count=len(samples),
                logical_cpu_count=cpu_count,
                parent_one_count=sum(1 for _, ppid, _ in samples if ppid == 1),
                pids=sorted(sample[0] for sample in samples),
            )
        )

    groups.sort(key=lambda group: (-group.capacity_pct, group.identity))
    return AggregatePressureScan(status="OK", groups=groups)


MIN_SAFE_PID = 100  # Never kill kernel threads or core system daemons


def kill_runaway(
    pid: int,
    *,
    candidate: dict[str, Any] | None = None,
    disposable_registration: dict[str, Any] | None = None,
    operator_confirmed: bool = False,
    allow_force: bool = False,
) -> bool:
    """Compatibility wrapper around the shared fail-closed process policy.

    CPU/runtime and a bare PID are never authorization.  Callers must pass the
    candidate evidence and explicit disposable registration; force remains
    separately gated.
    """
    from fleet_watch import process_policy

    identity = process_policy.snapshot_process(pid)
    decision = process_policy.evaluate(
        identity,
        candidate=candidate,
        disposable_registration=disposable_registration,
        operator_confirmed=operator_confirmed,
        allow_force=allow_force,
    )
    if not decision.allowed:
        return False
    outcome = process_policy.revalidate_and_terminate(
        decision,
        candidate=candidate,
        disposable_registration=disposable_registration,
        allow_force=allow_force,
    )
    return outcome.outcome == "exited"


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
    # Executable identities currently above aggregate pressure threshold.
    aggregate_active_keys: set[str] = field(default_factory=set)

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

    def observe_aggregate_pressure(
        self,
        groups: list[AggregatePressureGroup],
    ) -> list[AggregatePressureGroup]:
        """Return groups entering pressure and reset eligibility after recovery."""
        current = {group.identity for group in groups}
        entered = [
            group for group in groups if group.identity not in self.aggregate_active_keys
        ]
        self.aggregate_active_keys = current
        return entered

    def save(self, path: Path) -> None:
        """Persist tracker state to disk for cross-invocation continuity."""
        data = {
            "tick_counts": {str(k): v for k, v in self.tick_counts.items()},
            "last_cpu": {str(k): v for k, v in self.last_cpu.items()},
            "last_runtime": {str(k): v for k, v in self.last_runtime.items()},
            "aggregate_active_keys": sorted(self.aggregate_active_keys),
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
            active_keys = data.get("aggregate_active_keys", [])
            if not isinstance(active_keys, list) or not all(
                isinstance(key, str) for key in active_keys
            ):
                return cls()
            tracker.aggregate_active_keys = set(active_keys)
        except (OSError, json.JSONDecodeError, KeyError, ValueError, TypeError):
            return cls()
        return tracker
