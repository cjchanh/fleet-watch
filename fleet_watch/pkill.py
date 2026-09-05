"""Operator-authorized cascade process kill for Fleet Watch.

Provides the `fleet pkill` subcommand: pattern-matched kill with dry-run
default, mandatory --confirm flag, and optional --cascade to kill children.

Emits FLEET_PKILL_EXECUTED event on confirmed execution.
Never invokable from other Fleet Watch code paths — always operator-typed.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PkillTarget:
    """A process identified for potential kill."""

    pid: int
    name: str
    cmdline: str
    rss_mb: int = 0
    ppid: int | None = None
    children: list[PkillTarget] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pid": self.pid,
            "name": self.name,
            "cmdline": self.cmdline[:200],
            "rss_mb": self.rss_mb,
            "ppid": self.ppid,
            "children": [c.to_dict() for c in self.children],
        }


@dataclass
class PkillResult:
    """Result of a fleet pkill operation."""

    pattern: str
    cascade: bool = False
    confirmed: bool = False
    dry_run: bool = True
    targets: list[PkillTarget] = field(default_factory=list)
    pids_killed: list[int] = field(default_factory=list)
    children_killed: list[int] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    total_rss_freed_mb: int = 0

    @property
    def killed_any(self) -> bool:
        return len(self.pids_killed) > 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "pattern": self.pattern,
            "cascade": self.cascade,
            "confirmed": self.confirmed,
            "dry_run": self.dry_run,
            "target_count": len(self.targets),
            "targets": [t.to_dict() for t in self.targets],
            "pids_killed": self.pids_killed,
            "children_killed": self.children_killed,
            "errors": self.errors,
            "total_rss_freed_mb": self.total_rss_freed_mb,
        }


def _find_matching_pids(pattern: str) -> list[dict[str, Any]]:
    """Find process PIDs matching a pattern via pgrep -f."""
    try:
        out = subprocess.run(
            ["pgrep", "-f", pattern],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
        return []
    if out.returncode != 0:
        return []

    results: list[dict[str, Any]] = []
    for line in out.stdout.strip().splitlines():
        try:
            results.append({"pid": int(line.strip())})
        except ValueError:
            continue
    return results


def _get_process_detail(pid: int) -> dict[str, Any] | None:
    """Get name, cmdline, RSS, and PPID for a PID."""
    try:
        out = subprocess.run(
            ["ps", "-p", str(pid), "-o", "pid=", "-o", "ppid=", "-o", "rss=", "-o", "comm=", "-o", "args="],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
        return None
    if out.returncode != 0 or not out.stdout.strip():
        return None
    parts = out.stdout.strip().split(None, 4)
    if len(parts) < 4:
        return None
    try:
        return {
            "pid": int(parts[0]),
            "ppid": int(parts[1]) if parts[1].isdigit() else None,
            "rss_mb": int(parts[2]) // 1024,
            "name": parts[3],
            "cmdline": parts[4] if len(parts) > 4 else parts[3],
        }
    except (ValueError, IndexError):
        return None


def _get_children(ppid: int, depth: int = 2) -> list[dict[str, Any]]:
    """Get child processes up to a given depth."""
    if depth <= 0:
        return []
    try:
        out = subprocess.run(
            ["pgrep", "-P", str(ppid)],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
        return []
    if out.returncode != 0:
        return []

    children: list[dict[str, Any]] = []
    for line in out.stdout.strip().splitlines():
        try:
            child_pid = int(line.strip())
        except ValueError:
            continue
        detail = _get_process_detail(child_pid)
        if detail is None:
            continue
        grandchildren = _get_children(child_pid, depth - 1)
        detail["children"] = grandchildren
        children.append(detail)
    return children


def _build_targets(
    matching_pids: list[dict[str, Any]],
    cascade: bool = False,
) -> list[PkillTarget]:
    """Build PkillTarget tree from matched PIDs."""
    targets: list[PkillTarget] = []
    seen: set[int] = set()

    for match in matching_pids:
        pid = match["pid"]
        if pid in seen:
            continue
        seen.add(pid)

        detail = _get_process_detail(pid)
        if detail is None:
            continue

        child_targets: list[PkillTarget] = []
        if cascade:
            child_details = _get_children(pid, depth=2)
            child_seen: set[int] = set()
            for child in child_details:
                _add_child_targets(child, child_targets, child_seen)

        targets.append(
            PkillTarget(
                pid=detail["pid"],
                name=detail.get("name", f"PID {detail['pid']}"),
                cmdline=detail.get("cmdline", ""),
                rss_mb=detail.get("rss_mb", 0),
                ppid=detail.get("ppid"),
                children=child_targets,
            )
        )

    return targets


def _add_child_targets(
    child_detail: dict[str, Any],
    targets: list[PkillTarget],
    seen: set[int],
) -> None:
    """Recursively add child targets."""
    if child_detail["pid"] in seen:
        return
    seen.add(child_detail["pid"])

    grandchild_targets: list[PkillTarget] = []
    for gc in child_detail.get("children", []):
        _add_child_targets(gc, grandchild_targets, seen)

    targets.append(
        PkillTarget(
            pid=child_detail["pid"],
            name=child_detail.get("name", f"PID {child_detail['pid']}"),
            cmdline=child_detail.get("cmdline", ""),
            rss_mb=child_detail.get("rss_mb", 0),
            ppid=child_detail.get("ppid"),
            children=grandchild_targets,
        )
    )


def _terminate_process(pid: int, grace_seconds: float = 1.5) -> bool:
    """Send SIGTERM then SIGKILL to a process. Returns True on success."""
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.1)

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.1)
    return False


def execute_pkill(
    pattern: str,
    cascade: bool = False,
    confirm: bool = False,
) -> PkillResult:
    """Find and optionally kill processes matching a pattern.

    Parameters:
        pattern: pgrep -f pattern to match against
        cascade: if True, also kill child processes (depth 2)
        confirm: if False, dry-run only; if True, execute kills
    """
    result = PkillResult(
        pattern=pattern,
        cascade=cascade,
        confirmed=confirm,
        dry_run=not confirm,
    )

    matching = _find_matching_pids(pattern)
    if not matching:
        result.errors.append(f"no processes matching pattern '{pattern}'")
        return result

    result.targets = _build_targets(matching, cascade=cascade)

    if not confirm:
        return result

    for target in result.targets:
        if target.children:
            for child in target.children:
                _collect_and_kill(child, result)

        ok = _terminate_process(target.pid)
        if ok:
            result.pids_killed.append(target.pid)
            result.total_rss_freed_mb += target.rss_mb
        else:
            result.errors.append(
                f"failed to kill PID {target.pid} ({target.name})"
            )

    return result


def _collect_and_kill(target: PkillTarget, result: PkillResult) -> None:
    """Kill a target and its children recursively."""
    for child in target.children:
        _collect_and_kill(child, result)

    ok = _terminate_process(target.pid)
    if ok:
        result.children_killed.append(target.pid)
        result.total_rss_freed_mb += target.rss_mb
    else:
        result.errors.append(
            f"failed to kill child PID {target.pid} ({target.name})"
        )
