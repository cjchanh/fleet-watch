"""Pinned local binaries.

Process inspection must not honor a caller-controlled PATH. A ``ps`` shim
on PATH can fake create-times and release a live exclusive lease. The same
applies to listener and memory probes on the lease/admission path.

Each pin prefers ``/bin/<name>``, then ``/usr/bin/<name>``, then
``/usr/sbin/<name>``. Darwin ships ``lsof`` / ``netstat`` / ``sysctl`` in
``/usr/sbin``; Ubuntu 24.04 merges ``/bin`` → ``/usr/bin`` and keeps
``sysctl`` in ``/usr/sbin``. A missing binary stays at ``/bin/<name>`` so
the call site still raises ``FileNotFoundError`` and degrades the same way
as an absent pin.
"""

from __future__ import annotations

import os


def _resolve_bin(name: str) -> str:
    for candidate in (f"/bin/{name}", f"/usr/bin/{name}", f"/usr/sbin/{name}"):
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return f"/bin/{name}"


PS_BIN = _resolve_bin("ps")
LSOF_BIN = _resolve_bin("lsof")
SS_BIN = _resolve_bin("ss")
NETSTAT_BIN = _resolve_bin("netstat")
PGREP_BIN = _resolve_bin("pgrep")
SYSCTL_BIN = _resolve_bin("sysctl")
VM_STAT_BIN = _resolve_bin("vm_stat")
