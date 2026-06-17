"""session_coupling.py — read-only single-writer awareness over live session leases.

The gap this closes: a session lease carries ``owner_pid`` + ``last_heartbeat_at``
(liveness) but ``repo_dir`` is frequently null, so
``registry.get_active_session_leases_by_repo`` finds no conflicts and the
single-writer rule (the one that prevents two agents co-mutating the same repo)
is unenforceable in practice. This module makes a lease's repo resolvable EVEN
WHEN ``repo_dir`` is null — by deriving it from the owner PID's working
directory — and answers two read-only questions:

  * ``who_is_live(repo)``        -> live sessions whose repo == repo
  * ``single_writer_check(...)`` -> ``ALLOW`` | ``CONFLICT`` | ``UNKNOWN``

A session is LIVE iff: status ``ACTIVE``, owner PID alive, AND heartbeat fresher
than ``DEFAULT_STALE_SECONDS`` (the referee's own staleness rule). Heartbeat
freshness also defeats PID recycling — a recycled PID never heartbeats Fleet.

Fail-closed: if the lease set cannot be obtained, ``single_writer_check`` returns
``UNKNOWN`` (never ``ALLOW``) — an unresolvable coordination state must not
green-light a write, per Fleet Watch's fail-closed product invariant.

Boundary: stdlib only (plus ``fleet_watch.registry`` for the lease source and
age helper). No cross-repo imports, no network. Strictly read-only — never
opens, refreshes, closes, or mutates a lease.
"""
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

DEFAULT_STALE_SECONDS = 180
_EXIT = {"ALLOW": 0, "CONFLICT": 3, "UNKNOWN": 4}


# ── repo canonicalization + resolution ──────────────────────────────────────
def canonical_repo(path: Optional[str]) -> Optional[str]:
    """Absolute, symlink-resolved path string (joins ``/a/b`` and ``/a/b/``)."""
    if not path:
        return None
    try:
        return str(Path(path).expanduser().resolve())
    except OSError:
        return None


def git_root(path: str) -> Optional[str]:
    """Nearest enclosing git repo root of ``path``; the path itself if none
    (a non-git dir is still a single-writer 'repo unit')."""
    if not path:
        return None
    p = Path(path)
    for d in [p, *p.parents]:
        if (d / ".git").exists():
            return str(d)
    return str(p)


def _pid_cwd(pid: int) -> Optional[str]:
    """Working directory of a live PID via ``lsof`` (macOS/BSD). None if
    unresolvable — caller treats unresolvable as 'repo unknown', not 'no repo'."""
    try:
        out = subprocess.run(
            ["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"],
            capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    for line in out.stdout.splitlines():
        if line.startswith("n"):
            return line[1:].strip() or None
    return None


def _pid_alive(pid: Optional[int]) -> bool:
    if not pid or int(pid) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists but owned by another user
    except OSError:
        return False
    return True


def lease_repo(
    lease: dict,
    *,
    cwd_resolver: Callable[[int], Optional[str]] = _pid_cwd,
    root_resolver: Callable[[str], Optional[str]] = git_root,
) -> Optional[str]:
    """The repo a lease governs: its declared ``repo_dir`` if set, else derived
    from the owner PID's cwd (the fix for the null-``repo_dir`` blind spot)."""
    declared = canonical_repo(lease.get("repo_dir"))
    if declared:
        return canonical_repo(root_resolver(declared))
    pid = lease.get("owner_pid")
    if pid:
        cwd = cwd_resolver(int(pid))
        if cwd:
            return canonical_repo(root_resolver(canonical_repo(cwd) or cwd))
    return None


# ── liveness ────────────────────────────────────────────────────────────────
def is_live(
    lease: dict,
    *,
    age_of: Callable[[Optional[str]], Optional[float]],
    pid_alive: Callable[[Optional[int]], bool] = _pid_alive,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> bool:
    """ACTIVE + PID alive + heartbeat fresher than ``stale_seconds``."""
    if lease.get("status") != "ACTIVE" or lease.get("shutdown_at"):
        return False
    if not pid_alive(lease.get("owner_pid")):
        return False
    age = age_of(lease.get("last_heartbeat_at"))
    return age is not None and age <= stale_seconds


@dataclass
class Verdict:
    decision: str  # ALLOW | CONFLICT | UNKNOWN
    repo: Optional[str]
    reason: str
    conflicts: list = field(default_factory=list)

    @property
    def exit_code(self) -> int:
        return _EXIT[self.decision]


def who_is_live(
    repo: str,
    leases: list,
    *,
    age_of: Callable[[Optional[str]], Optional[float]],
    pid_alive: Callable[[Optional[int]], bool] = _pid_alive,
    cwd_resolver: Callable[[int], Optional[str]] = _pid_cwd,
    root_resolver: Callable[[str], Optional[str]] = git_root,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> list:
    """Live sessions whose resolved repo matches ``repo``."""
    target = canonical_repo(root_resolver(canonical_repo(repo) or "")) if repo else None
    if not target:
        return []
    out = []
    for lease in leases or []:
        if not is_live(lease, age_of=age_of, pid_alive=pid_alive, stale_seconds=stale_seconds):
            continue
        lr = lease_repo(lease, cwd_resolver=cwd_resolver, root_resolver=root_resolver)
        if lr and lr == target:
            out.append({
                "session_id": lease.get("session_id"),
                "owner_pid": lease.get("owner_pid"),
                "repo": lr,
                "repo_lock_mode": lease.get("repo_lock_mode"),
                "last_heartbeat_at": lease.get("last_heartbeat_at"),
            })
    return out


def single_writer_check(
    repo: str,
    my_session_id: Optional[str],
    leases: Optional[list],
    *,
    age_of: Callable[[Optional[str]], Optional[float]],
    pid_alive: Callable[[Optional[int]], bool] = _pid_alive,
    cwd_resolver: Callable[[int], Optional[str]] = _pid_cwd,
    root_resolver: Callable[[str], Optional[str]] = git_root,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> Verdict:
    """Is it safe for ``my_session_id`` to write ``repo`` right now?

    ALLOW   no other live session on the repo.
    CONFLICT another live session holds the repo (the collision to avoid).
    UNKNOWN  leases unobtainable or repo unresolvable -> fail-closed.
    """
    target = canonical_repo(root_resolver(canonical_repo(repo) or "")) if repo else None
    if leases is None:
        return Verdict("UNKNOWN", target, "lease set unavailable (fail-closed)")
    if not target:
        return Verdict("UNKNOWN", None, "repo path unresolvable (fail-closed)")
    live = who_is_live(
        repo, leases, age_of=age_of, pid_alive=pid_alive,
        cwd_resolver=cwd_resolver, root_resolver=root_resolver, stale_seconds=stale_seconds,
    )
    others = [s for s in live if s.get("session_id") != my_session_id]
    if others:
        return Verdict("CONFLICT", target, f"{len(others)} other live session(s) on {target}", others)
    return Verdict("ALLOW", target, "no other live writer on repo")


# ── production wiring (Fleet registry; injected away in tests) ──────────────
def load_active_leases() -> Optional[list]:
    """Active session leases from the Fleet registry, or None if unreachable
    (so the caller fails closed to UNKNOWN)."""
    try:
        from fleet_watch import registry

        conn = registry.connect()
        try:
            return registry.list_active_session_leases(conn)
        finally:
            conn.close()
    except Exception:
        return None


def default_age_of(heartbeat_at: Optional[str]) -> Optional[float]:
    try:
        from fleet_watch import registry

        return registry._age_seconds(heartbeat_at)
    except Exception:
        return None
