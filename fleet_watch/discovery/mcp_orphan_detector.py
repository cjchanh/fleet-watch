"""MCP-server orphan detector for Fleet Watch (NS-17 / B3).

Each Claude session spawns ~6 CDS MCP stdio servers. When a terminal/session
dies, its servers can linger (73 servers / ~3.1 GB observed from ~8 sessions,
several stale >19h). This detector finds MCP servers whose owning session is
dead and surfaces them for reaping.

Surfacing only — never auto-kills (mirrors orphan_detector's contract; the
actual reap stays the operator / `fleet reap` path, honoring 'never kill running
work without permission'). The load-bearing invariant: a server whose parent
session is ALIVE is never flagged as an orphan.
"""
from __future__ import annotations

import os
import re
import subprocess
from dataclasses import asdict, dataclass, field
from typing import Any, Callable

from fleet_watch.constants import PS_BIN

# CDS MCP stdio server scripts (compile/engines/sitrep/session_memory/lexicon/graph
# and any future mcp_*_server.py). Matched against the process command line.
_MCP_SERVER_RE = re.compile(r"mcp_[a-z0-9_]*server\.py|mcp_graph_context_server\.py")


@dataclass
class MCPOrphanResult:
    orphans_detected: bool = False
    mcp_process_count: int = 0
    orphan_pids: list[int] = field(default_factory=list)
    live_pids: list[int] = field(default_factory=list)
    estimated_recovered_mb: int = 0
    suggested_kill_command: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "orphans_detected": self.orphans_detected,
            "mcp_process_count": self.mcp_process_count,
            "orphan_pids": self.orphan_pids,
            "live_pids": self.live_pids,
            "estimated_recovered_mb": self.estimated_recovered_mb,
            "suggested_kill_command": self.suggested_kill_command,
            "error": self.error,
        }


def _pid_alive(pid: int) -> bool:
    if pid <= 1:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists but not ours to signal
    return True


def _launchd_supervised_pids() -> dict[int, str] | None:
    """PID -> launchd label for processes a supervisor owns (KeepAlive jobs).

    Returns ``None`` when the supervision map cannot be read. A supervised
    daemon (ppid=1 is how launchd parents its jobs) is never an orphan: killing
    it is the exact event a ``KeepAlive=true`` supervisor respawns from.
    """
    try:
        result = subprocess.run(
            ["/bin/launchctl", "list"],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    supervised: dict[int, str] = {}
    for line in result.stdout.splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        pid_s, _last_exit, label = parts[0], parts[1], "\t".join(parts[2:])
        if pid_s == "-":
            continue
        try:
            supervised[int(pid_s)] = label
        except ValueError:
            continue
    return supervised


def _supervision_partition(
    procs: list[dict[str, Any]],
    supervised: dict[int, str] | None,
    *,
    fail_closed: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split rows into (held_from_orphan_set, pass_to_classification).

    * map readable: held = launchd-supervised pids (their ppid=1 is supervision,
      not abandonment).
    * map unreadable and ``fail_closed``: held = every ppid<=1 row — a
      reparented row cannot be told apart from a supervised daemon, and a
      kill-facing path must never act on an unreadable distinction.
    * map unreadable and not ``fail_closed`` (advisory surfacing): nothing is
      held; behavior matches the historical advisory output.
    """
    if supervised is not None:
        held = [p for p in procs if int(p.get("pid", 0) or 0) in supervised]
        passed = [p for p in procs if int(p.get("pid", 0) or 0) not in supervised]
        return held, passed
    if fail_closed:
        held = [p for p in procs if int(p.get("ppid", 0) or 0) <= 1]
        passed = [p for p in procs if int(p.get("ppid", 0) or 0) > 1]
        return held, passed
    return [], list(procs)


def classify_mcp_orphans(
    procs: list[dict[str, Any]],
    pid_alive: Callable[[int], bool] = _pid_alive,
) -> tuple[list[dict], list[dict]]:
    """Pure split into (orphans, keepers). A process is an orphan iff its parent
    is reparented (ppid<=1) OR the parent process is dead. If the parent (owning
    session) is alive, it is ALWAYS a keeper — never reaped. This is the
    invariant that keeps a live session's servers safe."""
    orphans: list[dict] = []
    keepers: list[dict] = []
    for p in procs:
        ppid = int(p.get("ppid", 0) or 0)
        if ppid <= 1 or not pid_alive(ppid):
            orphans.append(p)
        else:
            keepers.append(p)
    return orphans, keepers


def _get_mcp_processes_with_status() -> tuple[list[dict[str, Any]], str | None]:
    """List MCP processes while preserving a failed ps probe as UNKNOWN."""
    try:
        result = subprocess.run(
            [PS_BIN, "-eo", "pid,ppid,rss,command"],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return [], f"ps_{type(exc).__name__}"
    if result.returncode != 0:
        return [], "ps_nonzero_exit"
    procs: list[dict[str, Any]] = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        pid_s, ppid_s, rss_s, cmd = parts
        if not _MCP_SERVER_RE.search(cmd):
            continue
        try:
            procs.append({
                "pid": int(pid_s), "ppid": int(ppid_s),
                "rss_mb": int(rss_s) // 1024, "cmd": cmd,
            })
        except ValueError:
            continue
    return procs, None


def _get_mcp_processes() -> list[dict[str, Any]]:
    """Compatibility wrapper returning only process rows."""
    procs, _error = _get_mcp_processes_with_status()
    return procs


_DEFAULT_GET_MCP_PROCESSES = _get_mcp_processes


def detect(pid_alive: Callable[[int], bool] = _pid_alive) -> MCPOrphanResult:
    """Scan for dead-session MCP servers. Surfacing only — never kills."""
    if _get_mcp_processes is not _DEFAULT_GET_MCP_PROCESSES:
        procs, error = _get_mcp_processes(), None
    else:
        procs, error = _get_mcp_processes_with_status()
    if error:
        return MCPOrphanResult(error=error)
    # Advisory surfacing: supervised daemons are noise, not orphans, but an
    # unreadable supervision map must not hide rows from the report (this path
    # never kills). The kill-facing path (`build_candidates`) fails closed.
    _held, pass_rows = _supervision_partition(
        procs, _launchd_supervised_pids(), fail_closed=False,
    )
    orphans, keepers = classify_mcp_orphans(pass_rows, pid_alive)
    keepers = keepers + _held
    orphan_pids = [p["pid"] for p in orphans]
    recovered = sum(p.get("rss_mb", 0) for p in orphans)
    kill_cmd = ("kill " + " ".join(str(p) for p in orphan_pids)) if orphan_pids else ""
    return MCPOrphanResult(
        orphans_detected=bool(orphan_pids),
        mcp_process_count=len(procs),
        orphan_pids=orphan_pids,
        live_pids=[p["pid"] for p in keepers],
        estimated_recovered_mb=recovered,
        suggested_kill_command=kill_cmd,
    )


# ═══════════════════════════════════════════════════════════════════════════
# EVIDENCE-CAPTURING CANDIDATE SET — the observation half of the verified
# `fleet reap --mcp --verify` reclaim path.
#
# APPEND-ONLY: nothing above this line changed. `detect()` stays exactly as it
# was (advisory-only, 4-column ps) so the daemon's surfacing surface keeps its
# current bytes; this section adds a SEPARATE capture so a new probe shape can
# never degrade the advisory path.
#
# WHAT IS CAPTURED AND WHY (the evidence a kill decision will later re-prove):
#   * the server's own kernel create-time, so a recycled PID cannot inherit the
#     earlier decision;
#   * the owning session's PID and its kernel create-time AT SCAN TIME, so the
#     decision can prove the original owner is gone rather than assume it;
#   * a reparented flag (ppid <= 1) recording a chain that is already detached.
#
# Everything here is read-only: no signal, no registry write, no side effect.
# ═══════════════════════════════════════════════════════════════════════════

# `lstart` is included so the candidate's own start time arrives in ONE bulk ps
# instead of one call per server.
_SCAN_FIELDS = "pid,ppid,rss,lstart,command"


def _fixed_env() -> dict[str, str]:
    """TZ/locale-invariant rendering for the bulk scan.

    ``ps -o lstart=`` renders in the CALLER's timezone/locale, so an
    interactive shell and a launchd daemon read different strings for the same
    live process. Forcing ``LC_ALL=C``/``TZ=UTC`` here makes the bulk scan's
    ``lstart`` byte-identical to ``registry._pid_create_time`` (which forces the
    same environment), so the two can be compared for equality. The value is
    used ONLY for equality, never displayed.
    """
    return {**os.environ, "LC_ALL": "C", "TZ": "UTC"}


@dataclass(frozen=True)
class MCPOwnerSnapshot:
    """The owning session as it looked AT SCAN TIME (the anti-reuse anchor)."""

    session_pid: int = 0
    session_create_time: str | None = None
    reparented: bool = False
    alive_at_scan: bool | None = None  # None: no owner PID to test (reparented)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MCPCandidate:
    """One dead-session MCP server plus the owner evidence captured with it.

    ``create_time`` is the server's OWN kernel create-time from the scan. A
    missing value is not a default: it means identity is unprovable and every
    downstream decision must DENY.
    """

    pid: int
    ppid: int
    rss_mb: int
    cmd: str
    create_time: str | None
    owner: MCPOwnerSnapshot
    evidence: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "pid": self.pid,
            "ppid": self.ppid,
            "rss_mb": self.rss_mb,
            "cmd": self.cmd,
            "create_time": self.create_time,
            "owner": self.owner.to_dict(),
            "evidence": list(self.evidence),
            "source": "mcp",
        }


def scan_mcp_processes() -> tuple[list[dict[str, Any]], str | None]:
    """One bulk ps that also captures each server's kernel create-time.

    A failed probe is preserved as an UNKNOWN error string, never as an empty
    "no orphans" reading — an unscannable host must not look clean.
    """
    try:
        result = subprocess.run(
            [PS_BIN, "-eo", _SCAN_FIELDS],
            capture_output=True, text=True, timeout=5, check=False,
            env=_fixed_env(),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return [], f"ps_{type(exc).__name__}"
    if result.returncode != 0:
        return [], "ps_nonzero_exit"
    rows: list[dict[str, Any]] = []
    for line in result.stdout.splitlines()[1:]:
        # ``lstart`` renders as exactly five tokens ("Sat Sep 26 16:14:03 2026"),
        # so the columns cannot be split naively from the left — the command
        # would be sliced into the timestamp. Split the first two fields, then
        # the five lstart tokens, and take the remainder VERBATIM (command lines
        # carry meaningful internal spacing).
        head = line.split(None, 2)
        if len(head) < 3:
            continue
        pid_s, ppid_s, rest = head
        tail = rest.split(None, 6)
        if len(tail) < 7:
            continue
        rss_s = tail[0]
        lstart = " ".join(tail[1:6])
        cmd = tail[6]
        if not _MCP_SERVER_RE.search(cmd):
            continue
        try:
            rows.append({
                "pid": int(pid_s),
                "ppid": int(ppid_s),
                "rss_mb": int(rss_s) // 1024,
                "cmd": cmd,
                "create_time": lstart.strip() or None,
            })
        except ValueError:
            continue
    return rows, None


def _owner_create_time(pid: int) -> str | None:
    """Kernel create-time of a PID, via the registry's TZ-invariant reader.

    Imported lazily: the registry is the canonical owner of this rendering and
    duplicating it here would create a second, drifting definition.
    """
    from fleet_watch import registry
    return registry._pid_create_time(pid)


def build_candidates(
    rows: list[dict[str, Any]],
    *,
    exists: Callable[[int], bool] = _pid_alive,
    create_time: Callable[[int], str | None] | None = None,
) -> list[MCPCandidate]:
    """Turn raw scan rows into evidence-carrying candidates.

    The split goes through :func:`classify_mcp_orphans`, so the load-bearing
    invariant is inherited rather than re-implemented: a server whose parent is
    ALIVE is never a candidate.
    """
    owner_ct = create_time or _owner_create_time
    # Kill-facing: a launchd-supervised daemon is NEVER a reclaim candidate
    # (its ppid=1 is supervision, and killing it is what KeepAlive respawns
    # from), and when the supervision map is unreadable every reparented row is
    # held — the kill direction must not act on an unreadable distinction.
    _held, pass_rows = _supervision_partition(
        rows, _launchd_supervised_pids(), fail_closed=True,
    )
    orphans, _keepers = classify_mcp_orphans(pass_rows, exists)
    candidates: list[MCPCandidate] = []
    for row in orphans:
        pid = int(row["pid"])
        ppid = int(row.get("ppid", 0) or 0)
        reparented = ppid <= 1
        owner_snapshot = MCPOwnerSnapshot(
            session_pid=ppid,
            session_create_time=None,
            reparented=reparented,
            alive_at_scan=None if reparented else False,
        )
        if not reparented:
            # Capture the owner's identity at scan time. An unreadable value
            # stays None so the decision denies instead of guessing.
            try:
                owner_snapshot = MCPOwnerSnapshot(
                    session_pid=ppid,
                    session_create_time=owner_ct(ppid),
                    reparented=False,
                    alive_at_scan=bool(exists(ppid)),
                )
            except (OSError, ValueError, TypeError):
                owner_snapshot = MCPOwnerSnapshot(
                    session_pid=ppid, session_create_time=None,
                    reparented=False, alive_at_scan=None,
                )
        if reparented:
            evidence = (
                f"parent chain detached at scan (ppid={ppid})",
                f"rss={row.get('rss_mb', 0)}MB",
            )
        elif owner_snapshot.alive_at_scan:
            evidence = (
                f"owning session pid {ppid} still alive at scan time",
                f"rss={row.get('rss_mb', 0)}MB",
            )
        else:
            evidence = (
                f"owning session pid {ppid} absent at scan time",
                f"owner_create_time={owner_snapshot.session_create_time}",
                f"rss={row.get('rss_mb', 0)}MB",
            )
        candidates.append(MCPCandidate(
            pid=pid,
            ppid=ppid,
            rss_mb=int(row.get("rss_mb", 0) or 0),
            cmd=str(row.get("cmd", "")),
            create_time=row.get("create_time") or None,
            owner=owner_snapshot,
            evidence=evidence,
        ))
    return candidates


def scan_candidates(
    *,
    exists: Callable[[int], bool] = _pid_alive,
    create_time: Callable[[int], str | None] | None = None,
    rows: list[dict[str, Any]] | None = None,
) -> tuple[list[MCPCandidate], str | None]:
    """Read-only candidate capture for the verified reclaim path.

    ``rows`` lets a caller (or a test) supply an already-read process table
    instead of re-running ``ps``. Returns ``(candidates, error)``; ``error`` is
    never ``None``-by-default-on-failure — an unscannable host yields no
    candidates AND a reason, so the caller reports UNKNOWN instead of "clean".
    """
    if rows is None:
        rows, error = scan_mcp_processes()
        if error:
            return [], error
    return build_candidates(rows, exists=exists, create_time=create_time), None
