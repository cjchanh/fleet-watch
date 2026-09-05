"""Orphan-runner detector for Fleet Watch.

Compares the Ollama API /api/ps model list against actual ollama runner
processes found via ps. When the runner count exceeds the known model count,
surfaces ORPHAN_RUNNERS_DETECTED with orphan PIDs, estimated recovered memory,
and a suggested kill command.

Surfacing only — never auto-kills.
"""

from __future__ import annotations

import json
import re
import subprocess
import urllib.request
from dataclasses import dataclass, field
from typing import Any


@dataclass
class OrphanDetectionResult:
    """Result of an orphan-runner detection scan."""

    orphans_detected: bool = False
    known_model_count: int = 0
    runner_process_count: int = 0
    known_model_names: list[str] = field(default_factory=list)
    orphan_pids: list[int] = field(default_factory=list)
    estimated_recovered_mb: int = 0
    suggested_kill_command: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "orphans_detected": self.orphans_detected,
            "known_model_count": self.known_model_count,
            "runner_process_count": self.runner_process_count,
            "known_model_names": self.known_model_names,
            "orphan_pids": self.orphan_pids,
            "estimated_recovered_mb": self.estimated_recovered_mb,
            "suggested_kill_command": self.suggested_kill_command,
            "error": self.error,
        }


_RUNNER_RE = re.compile(r"ollama[ _-]runner")


def _get_known_models(port: int = 11434) -> list[str]:
    """Query Ollama /api/ps for known-loaded model names."""
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/ps",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=3) as resp:  # nosec B310 - loopback-only probe; tests/test_no_external_egress.py enforces the host set
            data = json.loads(resp.read())
            return [m.get("name", "unknown") for m in data.get("models", [])]
    except Exception:
        return []


def _get_runner_processes() -> list[dict[str, Any]]:
    """Find ollama runner processes via ps aux."""
    try:
        out = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
        return []
    if out.returncode != 0:
        return []

    runners: list[dict[str, Any]] = []
    for line in out.stdout.splitlines():
        if not _RUNNER_RE.search(line):
            continue
        parts = line.split(None, 10)
        if len(parts) < 6:
            continue
        try:
            pid = int(parts[1])
            rss_mb = int(parts[5]) // 1024
        except (ValueError, IndexError):
            continue
        runners.append(
            {
                "pid": pid,
                "rss_mb": rss_mb,
                "cmdline": parts[10] if len(parts) > 10 else "",
            }
        )
    return runners


def detect_orphans(
    known_models: list[str] | None = None,
    runners: list[dict[str, Any]] | None = None,
) -> OrphanDetectionResult:
    """Detect orphan ollama runners by comparing known models vs running processes.

    Parameters are injectable for deterministic unit testing.
    """
    if known_models is None:
        known_models = _get_known_models()
    if runners is None:
        runners = _get_runner_processes()

    result = OrphanDetectionResult(
        known_model_count=len(known_models),
        runner_process_count=len(runners),
        known_model_names=list(known_models),
    )

    if result.runner_process_count <= result.known_model_count:
        return result

    result.orphans_detected = True

    # Identify orphans: runners without a matching known model.
    # Strategy: match runners whose cmdline model hash substring appears in
    # known model names. Unmatched runners are orphans.
    _MODEL_RE = re.compile(r"--model\s+(\S+)")

    matched: set[int] = set()
    for runner in runners:
        cmd = runner.get("cmdline", "")
        m = _MODEL_RE.search(cmd)
        if not m:
            continue
        model_hash = m.group(1)
        for known_name in known_models:
            # The model hash is a truncated hex digest; the known name
            # contains the full model name. Check substring match.
            if model_hash.lower() in known_name.lower() or any(
                segment in known_name.lower() for segment in model_hash.split("/")[-1:]
            ):
                matched.add(runner["pid"])
                break

    orphan_runners = [r for r in runners if r["pid"] not in matched]
    result.orphan_pids = sorted(r["pid"] for r in orphan_runners)
    result.estimated_recovered_mb = sum(r["rss_mb"] for r in orphan_runners)

    if result.orphan_pids:
        result.suggested_kill_command = f"kill {' '.join(str(p) for p in result.orphan_pids)}"

    return result
