"""Auto-discover ollama runner subprocesses and compute actual GPU consumption.

On every fleet status / fleet guard invocation, walks children of any
registered ollama serve PID and registers synthetic fleet entries for each
ollama runner child with live RSS, model hash, and parent PID.

These entries are transient view items — never persisted to registry.db.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass, field
from typing import Any


@dataclass
class OllamaRunner:
    """A discovered ollama runner subprocess."""

    pid: int
    parent_pid: int
    rss_mb: int
    model_hash: str
    port: int | None
    cmdline: str


@dataclass
class OllamaRunnerReport:
    """Result of an ollama runner discovery scan."""

    serve_pid: int
    runners: list[OllamaRunner] = field(default_factory=list)
    total_runner_rss_mb: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "serve_pid": self.serve_pid,
            "runner_count": len(self.runners),
            "total_runner_rss_mb": self.total_runner_rss_mb,
            "runners": [
                {
                    "pid": r.pid,
                    "parent_pid": r.parent_pid,
                    "rss_mb": r.rss_mb,
                    "model_hash": r.model_hash,
                    "port": r.port,
                }
                for r in self.runners
            ],
            "error": self.error,
        }


def _get_ollama_serve_pids() -> list[int]:
    """Return PIDs of all running ollama serve processes."""
    try:
        out = subprocess.run(
            ["pgrep", "-f", "ollama serve"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
        return []
    if out.returncode != 0:
        return []
    pids = []
    for line in out.stdout.strip().splitlines():
        try:
            pids.append(int(line.strip()))
        except ValueError:
            continue
    return pids


def _get_child_pids(parent_pid: int) -> list[int]:
    """Return direct child PIDs of a given parent."""
    try:
        out = subprocess.run(
            ["pgrep", "-P", str(parent_pid)],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
        return []
    if out.returncode != 0:
        return []
    pids = []
    for line in out.stdout.strip().splitlines():
        try:
            pids.append(int(line.strip()))
        except ValueError:
            continue
    return pids


def _get_process_info(pid: int) -> dict[str, Any] | None:
    """Get RSS, cmdline, and ppid for a process via ps."""
    try:
        out = subprocess.run(
            ["ps", "-p", str(pid), "-o", "rss=", "-o", "args="],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
        return None
    if out.returncode != 0 or not out.stdout.strip():
        return None
    line = out.stdout.strip()
    parts = line.split(None, 1)
    if len(parts) < 2:
        return None
    try:
        rss_kb = int(parts[0])
    except ValueError:
        return None
    cmdline = parts[1]
    return {"rss_kb": rss_kb, "cmdline": cmdline}


_RUNNER_CMDLINE_RE = re.compile(r"ollama[_\- ]runner")


def _is_ollama_runner(cmdline: str) -> bool:
    return bool(_RUNNER_CMDLINE_RE.search(cmdline))


_MODEL_RE = re.compile(r"--model\s+(\S+)")


def _extract_model_hash(cmdline: str) -> str:
    match = _MODEL_RE.search(cmdline)
    if match:
        return match.group(1)
    return "unknown"


_PORT_RE = re.compile(r"--port\s+(\d+)")


def _extract_port(cmdline: str) -> int | None:
    match = _PORT_RE.search(cmdline)
    if match:
        return int(match.group(1))
    return None


def discover_ollama_runners() -> list[OllamaRunnerReport]:
    """Walk ollama serve children and discover all runner subprocesses.

    Returns one report per ollama serve instance. Only the direct children
    of each serve PID are inspected.
    """
    serve_pids = _get_ollama_serve_pids()
    if not serve_pids:
        return []

    reports: list[OllamaRunnerReport] = []
    for serve_pid in serve_pids:
        report = OllamaRunnerReport(serve_pid=serve_pid)
        child_pids = _get_child_pids(serve_pid)

        runners: list[OllamaRunner] = []
        for child_pid in child_pids:
            info = _get_process_info(child_pid)
            if info is None:
                continue
            if not _is_ollama_runner(info["cmdline"]):
                continue

            rss_mb = info["rss_kb"] // 1024
            model_hash = _extract_model_hash(info["cmdline"])
            port = _extract_port(info["cmdline"])

            runners.append(
                OllamaRunner(
                    pid=child_pid,
                    parent_pid=serve_pid,
                    rss_mb=rss_mb,
                    model_hash=model_hash,
                    port=port,
                    cmdline=info["cmdline"],
                )
            )

        report.runners = runners
        report.total_runner_rss_mb = sum(r.rss_mb for r in runners)
        reports.append(report)

    return reports


def total_actual_gpu_mb(reports: list[OllamaRunnerReport]) -> int:
    """Sum of all runner RSS across all ollama serve instances."""
    return sum(r.total_runner_rss_mb for r in reports)


def runner_entries_for_status(
    reports: list[OllamaRunnerReport],
) -> list[dict[str, Any]]:
    """Convert runner reports into synthetic fleet status entries."""
    entries: list[dict[str, Any]] = []
    for report in reports:
        for runner in report.runners:
            entries.append(
                {
                    "pid": runner.pid,
                    "name": f"ollama runner :{runner.port or '11434'}",
                    "workstream": "inference",
                    "port": runner.port,
                    "gpu_mb": runner.rss_mb,
                    "rss_mb": runner.rss_mb,
                    "model_hash": runner.model_hash,
                    "parent_pid": runner.parent_pid,
                    "synthetic": True,
                    "priority": 4,
                    "model": runner.model_hash,
                }
            )
    return entries
