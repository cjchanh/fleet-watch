"""Bounded process-group probe execution for FleetWatch collectors."""

from __future__ import annotations

import os
import signal
import subprocess
from dataclasses import dataclass
from typing import Mapping, Sequence


@dataclass(frozen=True)
class IsolatedResult:
    argv: tuple[str, ...]
    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool


def _terminate_owned_group(proc: subprocess.Popen[str]) -> None:
    """Terminate only the process group created by this Popen call."""
    try:
        if os.getpgid(proc.pid) != proc.pid:
            return
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        return
    try:
        proc.wait(timeout=0.2)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        if os.getpgid(proc.pid) == proc.pid:
            os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        return
    try:
        proc.wait(timeout=0.5)
    except subprocess.TimeoutExpired:
        pass


def run_isolated(
    argv: Sequence[str],
    *,
    timeout_seconds: float,
    env: Mapping[str, str] | None = None,
    text: bool = True,
    errors: str | None = None,
) -> IsolatedResult:
    """Run a no-shell probe in a new session and bound its whole process group."""
    args = tuple(str(item) for item in argv)
    proc = subprocess.Popen(
        args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=text,
        errors=errors,
        env=dict(env) if env is not None else None,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=max(0.01, timeout_seconds))
        return IsolatedResult(args, proc.returncode, stdout or "", stderr or "", False)
    except subprocess.TimeoutExpired:
        _terminate_owned_group(proc)
        try:
            stdout, stderr = proc.communicate(timeout=0.5)
        except subprocess.TimeoutExpired:
            stdout, stderr = "", ""
        return IsolatedResult(args, proc.returncode, stdout or "", stderr or "", True)
