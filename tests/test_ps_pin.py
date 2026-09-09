"""Pinned binaries must be absolute system paths, never PATH."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from fleet_watch.constants import (
    LSOF_BIN,
    NETSTAT_BIN,
    PGREP_BIN,
    PS_BIN,
    SS_BIN,
    SYSCTL_BIN,
    VM_STAT_BIN,
)

REPO = Path(__file__).resolve().parent.parent
PACKAGE = REPO / "fleet_watch"

_PINNED = (PS_BIN, LSOF_BIN, SS_BIN, NETSTAT_BIN, PGREP_BIN, SYSCTL_BIN, VM_STAT_BIN)
_BARE_NAMES = ("ps", "lsof", "ss", "netstat", "pgrep", "sysctl", "vm_stat")


def test_ps_bin_is_absolute_system_binary() -> None:
    assert PS_BIN in {"/bin/ps", "/usr/bin/ps"}
    assert Path(PS_BIN).is_absolute()


def test_pinned_bins_are_absolute_well_known_paths() -> None:
    allowed_roots = ("/bin/", "/usr/bin/", "/usr/sbin/")
    for bin_path in _PINNED:
        assert bin_path.startswith(allowed_roots), bin_path
        assert Path(bin_path).is_absolute()


def test_no_path_resolved_ps_in_package() -> None:
    offenders: list[str] = []
    for path in PACKAGE.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for name in _BARE_NAMES:
            for needle in (f'["{name}"', f"['{name}'", f',"{name}"', f", '{name}'"):
                if needle in text:
                    offenders.append(f"{path.relative_to(REPO)}: {needle}")
    assert offenders == []


@pytest.mark.skipif(sys.platform not in {"darwin", "linux"}, reason="unix ps")
def test_ps_bin_runs_and_reports_current_pid() -> None:
    pid = os.getpid()
    proc = subprocess.run(
        [PS_BIN, "-p", str(pid), "-o", "pid="],
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert proc.returncode == 0, proc.stderr
    assert str(pid) in proc.stdout
