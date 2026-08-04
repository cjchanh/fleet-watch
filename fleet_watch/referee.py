"""Referee — claim logic, budget enforcement, preemption for Fleet Watch."""

from __future__ import annotations

import errno
import os
import re
import signal
import socket
import sqlite3
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fleet_watch import events, registry


@dataclass
class Decision:
    """Outcome of a claim or guard decision."""
    allowed: bool
    reason: str
    holder: dict[str, Any] | None = None
    holders: list[dict[str, Any]] = field(default_factory=list)
    overlap_paths: list[str] = field(default_factory=list)
    stale_holders: list[dict[str, Any]] = field(default_factory=list)
    safe_mode: str | None = None


def _session_holder_from_lease(lease: dict[str, Any]) -> dict[str, Any]:
    return {
        "pid": lease.get("owner_pid"),
        "name": f"session {lease['session_id']}",
        "workstream": "session",
        "priority": 3,
        "port": None,
        "repo_dir": lease.get("repo_dir"),
        "gpu_mb": 0,
        "session_id": lease["session_id"],
        "repo_lock_mode": lease.get("repo_lock_mode", "cooperative"),
        "write_scopes": lease.get("write_scopes", []),
    }


def normalize_write_scopes(repo_dir: str | None, write_scopes: list[str] | tuple[str, ...] | None) -> list[str]:
    """Resolve write scopes to stable paths for overlap checks."""
    if not write_scopes:
        return []
    base = Path(repo_dir).expanduser().resolve() if repo_dir else None
    resolved: list[str] = []
    for raw in write_scopes:
        path = Path(raw).expanduser()
        if not path.is_absolute() and base is not None:
            path = base / path
        value = str(path.resolve())
        if value not in resolved:
            resolved.append(value)
    return resolved


def _paths_overlap(left: str, right: str) -> bool:
    left_path = Path(left)
    right_path = Path(right)
    if left_path == right_path:
        return True
    try:
        left_path.relative_to(right_path)
        return True
    except ValueError:
        pass
    try:
        right_path.relative_to(left_path)
        return True
    except ValueError:
        return False


def _overlap_paths(requested: list[str], held: list[str]) -> list[str]:
    overlaps: list[str] = []
    for request_scope in requested:
        for held_scope in held:
            if _paths_overlap(request_scope, held_scope):
                if request_scope not in overlaps:
                    overlaps.append(request_scope)
                if held_scope not in overlaps:
                    overlaps.append(held_scope)
    return overlaps


PORT_FREE = "free"
PORT_HELD = "held"
PORT_PRIVILEGED = "privileged"
PORT_UNDETERMINED = "undetermined"

# Socket-creation errnos that mean "this host does not speak this address
# family" — the only class of creation failure that may be skipped. Every
# other creation errno (EMFILE/ENFILE/ENOBUFS/ENOMEM) means the probe itself
# failed and the port's state is UNKNOWN, which is not the same as free.
_FAMILY_UNSUPPORTED = frozenset(
    getattr(errno, name)
    for name in ("EAFNOSUPPORT", "EPROTONOSUPPORT", "EPFNOSUPPORT", "ESOCKTNOSUPPORT")
    if hasattr(errno, name)
)


@dataclass(frozen=True)
class PortProbe:
    """What the OS socket table says about one TCP port on loopback.

    ``status`` separates the three answers the old boolean conflated:
    ``held`` (a live listener owns it), ``privileged`` (bind was refused for
    lack of privilege — we learned nothing about a listener), and
    ``undetermined`` (the probe itself failed). Only ``free`` permits a claim;
    everything else refuses, but the *reason* now matches the measurement.
    """

    status: str
    detail: str

    @property
    def claimable(self) -> bool:
        """True only when the OS positively proved the port bindable."""
        return self.status == PORT_FREE


def probe_port(port: int) -> PortProbe:
    """Ask the OS whether this TCP port can still be bound on loopback.

    Why this exists: ``check_port`` previously returned "port available" when
    ``registry.get_process_by_port`` was empty. That measured the claim table,
    not the socket table — so any unregistered listener (proof: Decision Card
    Kernel on 8765, ``fleet guard --json --port 8765`` → allowed:true while
    ``socket.bind(("127.0.0.1", 8765))`` raised [Errno 48]) made the guard
    fail-open in the direction operators rely on ("allowed:false → stop").

    Registry absence is not availability. The OS bind is the authority on
    whether a port can still be claimed. Loopback only: this is a local
    single-user tool; non-loopback holders are out of scope for this probe.

    The tri-state exists because a bool cannot carry a failed measurement.
    Two measured defects in the boolean version:

    1. ``except OSError: continue`` on socket CREATION was commented as
       "AF_INET6 unsupported" but caught every creation failure for both
       families; with both continuing, the loop fell through to
       ``return False`` = available. Reproduced under fd exhaustion: with
       RLIMIT_NOFILE lowered to 64 and fds consumed to EMFILE (errno 24),
       ``os_port_held(P)`` returned False for a port with a LIVE listener —
       fail-open on the exact path the fix was built to close.
    2. ``os_port_held(80)`` returned True as an unprivileged user because
       bind raised EACCES, and the caller reported the port "held by the OS".
       Nothing was measured about a holder; the emitted reason was untrue.
    """
    if not isinstance(port, int) or isinstance(port, bool) or port < 1 or port > 65535:
        # Invalid port is not "available" — fail closed for bad input.
        return PortProbe(PORT_UNDETERMINED, f"{port!r} is not a valid TCP port number")

    probed_any = False
    for family, address in (
        (socket.AF_INET, "127.0.0.1"),
        (socket.AF_INET6, "::1"),
    ):
        try:
            sock = socket.socket(family, socket.SOCK_STREAM)
        except OSError as exc:
            if exc.errno in _FAMILY_UNSUPPORTED:
                # Host does not speak this family (common for AF_INET6) — the
                # other family can still answer. Not a failed measurement.
                continue
            # Anything else (EMFILE and friends) means we could not measure.
            # Falling through to "free" here is the fd-exhaustion fail-open.
            return PortProbe(
                PORT_UNDETERMINED,
                f"socket creation failed: {exc.strerror or exc} (errno {exc.errno})",
            )
        try:
            # SO_REUSEADDR left at default 0 so an existing listener is not
            # masked by a second bind that would otherwise succeed.
            sock.bind((address, port))
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EPERM):
                # Privileged port, unprivileged prober. We did NOT learn that
                # a listener holds it — only that we may not ask.
                return PortProbe(
                    PORT_PRIVILEGED,
                    f"bind to {address}:{port} refused for lack of privilege "
                    f"(errno {exc.errno})",
                )
            if exc.errno in (errno.EADDRINUSE, errno.EADDRNOTAVAIL):
                if exc.errno == errno.EADDRNOTAVAIL:
                    # This family's loopback address does not exist here (no
                    # ::1). Nothing learned about the port — try the other.
                    continue
                return PortProbe(
                    PORT_HELD,
                    f"{address}:{port} is in use by a live listener "
                    f"(errno {exc.errno})",
                )
            return PortProbe(
                PORT_UNDETERMINED,
                f"bind to {address}:{port} failed: {exc.strerror or exc} "
                f"(errno {exc.errno})",
            )
        else:
            probed_any = True
        finally:
            sock.close()

    if not probed_any:
        # Every family was skipped. Zero measurements is not evidence of free.
        return PortProbe(
            PORT_UNDETERMINED, "no supported loopback address family could be probed"
        )
    return PortProbe(PORT_FREE, f"loopback bind to port {port} succeeded")


def os_port_held(port: int) -> bool:
    """True when this TCP port may not be claimed on loopback.

    Thin bool view of :func:`probe_port` kept for callers that only need the
    verdict. Note the asymmetry: this is False ONLY when the OS positively
    proved the port bindable. Held, privileged, and undetermined all read as
    True, because "I could not measure it" must never be reported as free.
    """
    return not probe_port(port).claimable


# Listener enumeration. Ordered ss -> netstat -> lsof: the first source that
# answers with any rows wins. Ordering is load-bearing for correctness, not
# just preference — measured on this host, unprivileged
# `lsof -iTCP -sTCP:LISTEN` showed 0 of the root-owned listeners, while
# `netstat -anv -p tcp` showed every one of them with its PID. lsof last means
# a partial view is only ever consulted when nothing better answered.
_LISTENER_SOURCES: tuple[tuple[str, list[str]], ...] = (
    ("ss", ["ss", "-ltnp"]),
    ("netstat", ["netstat", "-anv", "-p", "tcp"]),
    ("lsof", ["lsof", "-iTCP", "-sTCP:LISTEN", "-P", "-n", "-F", "pn"]),
)


def _parse_ss(output: str) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int]] = []
    for line in output.splitlines():
        if "LISTEN" not in line or "pid=" not in line:
            continue
        port_match = re.search(r":(\d+)\s", line)
        if not port_match:
            continue
        port = int(port_match.group(1))
        # findall, not search: one listening socket can be shared by several
        # PIDs (pre-forked workers, SO_REUSEPORT). Taking only the first would
        # make ownership verification deny the real co-owners.
        for pid in re.findall(r"pid=(\d+)", line):
            pairs.append((int(pid), port))
    return pairs


def _parse_netstat(output: str) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int]] = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 11 or parts[0] not in {"tcp4", "tcp6", "tcp46"}:
            continue
        if parts[5] != "LISTEN":
            continue
        port_match = re.search(r"\.(\d+)$", parts[3])
        pid_match = re.search(r":(\d+)$", parts[10])
        if not port_match or not pid_match:
            continue
        pairs.append((int(pid_match.group(1)), int(port_match.group(1))))
    return pairs


def _parse_lsof(output: str) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int]] = []
    current_pid: int | None = None
    for line in output.splitlines():
        if line.startswith("p"):
            try:
                current_pid = int(line[1:])
            except ValueError:
                current_pid = None
        elif line.startswith("n") and current_pid is not None:
            # Anchored at end-of-line, not `:(\d+)\b` — an IPv6 address like
            # "n[::1]:8100" makes the unanchored form match ":1]" and report
            # port 1.
            match = re.search(r":(\d+)$", line.strip())
            if match:
                pairs.append((current_pid, int(match.group(1))))
    return pairs


def socket_table_listeners() -> list[tuple[int, int]] | None:
    """Return every ``(pid, port)`` TCP LISTEN pair the OS will show us.

    ``None`` means no enumeration tool ran at all — distinct from ``[]``,
    which means a tool ran and saw nothing listening. Callers must not read
    ``None`` as "nothing is listening".
    """
    parsers = {"ss": _parse_ss, "netstat": _parse_netstat, "lsof": _parse_lsof}
    ran_any = False
    for name, argv in _LISTENER_SOURCES:
        try:
            completed = subprocess.run(
                argv, capture_output=True, text=True, timeout=5
            )
        except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError, OSError):
            continue
        if completed.returncode != 0:
            continue
        ran_any = True
        pairs = parsers[name](completed.stdout)
        if pairs:
            return pairs
    return [] if ran_any else None


def os_port_owner_pids(port: int) -> set[int] | None:
    """PIDs the OS attributes to this listening port, or ``None`` if unknown.

    ``None`` = the socket table could not be read. An empty set = the table
    was read and attributed the port to nobody visible. Both are refusals for
    an ownership claim, never approvals: every uncertainty in this lookup
    resolves toward DENY, so an incomplete source (unprivileged lsof hiding
    another user's socket) can only produce a false refusal, never a false
    grant.
    """
    pairs = socket_table_listeners()
    if pairs is None:
        return None
    return {pid for pid, listen_port in pairs if listen_port == port}


def check_port(
    conn: sqlite3.Connection, port: int, *, owner_pid: int | None = None
) -> Decision:
    """Return whether a port is available for a new claim.

    Two independent authorities, both required:
    1. Fleet registry — no registered process already claimed this port.
    2. OS socket table — loopback bind must succeed (see ``os_port_held``).

    Either alone is insufficient: an unregistered listener must still block,
    and a registered-but-dead claim is handled by the registry path's holder
    record (discover/clean reaps dead PIDs elsewhere).

    ``owner_pid`` is for callers that are not asking "may I bind this?" but
    "may I record that I already hold this?" — ``fleet discover``, which has
    just FOUND a live listener, and ``fleet register --pid``, the documented
    explicit-registration path. For those the OS holding the port is the
    premise, not a conflict. Without it the OS probe made discover unable to
    register any listener at all, because every listener holds its own port,
    and `fleet register --pid <self> --port <held>` was refused with
    "port held by the OS" — the registry could no longer learn the one fact
    that lets ``guard`` name a holder PID.

    The declaration is VERIFIED, not trusted: the socket table must actually
    attribute this port to ``owner_pid``. Trusting it would have re-opened
    the fail-open one level up — any caller passing an arbitrary --pid could
    switch the OS check off and write a false holder into the registry, which
    then reads back as an authoritative claim. The registry half still applies
    in full: a DIFFERENT registered process claiming the port is a conflict.
    """
    holder = registry.get_process_by_port(conn, port)
    if holder is not None:
        return Decision(
            allowed=False,
            reason=f"port {port} claimed by PID {holder['pid']} ({holder['name']})",
            holder=holder,
        )

    probe = probe_port(port)
    if probe.claimable:
        return Decision(allowed=True, reason="port available")

    if owner_pid is not None:
        owners = os_port_owner_pids(port)
        if owners is not None and owner_pid in owners:
            return Decision(
                allowed=True,
                reason=(
                    f"port {port} available to PID {owner_pid} — socket table "
                    f"confirms it is the holder, and no other process has "
                    f"registered a claim"
                ),
            )
        if owners is None:
            detail = (
                "the socket table could not be read, so the claim is "
                "unverifiable"
            )
        elif owners:
            detail = (
                f"the socket table attributes it to PID(s) "
                f"{sorted(owners)}, not to PID {owner_pid}"
            )
        else:
            detail = (
                f"the socket table attributes it to no visible process, so "
                f"PID {owner_pid}'s claim is unconfirmed"
            )
        return Decision(
            allowed=False,
            reason=(
                f"port {port} is not claimable by PID {owner_pid}: "
                f"{probe.detail}; {detail}"
            ),
            holder=None,
        )

    if probe.status == PORT_HELD:
        reason = (
            f"port {port} held by the OS (not in fleet registry) — "
            "registry absence is not availability"
        )
    elif probe.status == PORT_PRIVILEGED:
        # NOT "held by the OS": nothing was measured about a holder. Saying
        # "held" here emitted a true DENY with a false reason.
        reason = (
            f"port {port} could not be tested — {probe.detail}; refusing "
            "rather than reporting an untested port as available"
        )
    else:
        reason = (
            f"port {port} availability is undetermined — {probe.detail}; "
            "refusing rather than guessing"
        )
    return Decision(allowed=False, reason=reason, holder=None)


def check_repo(conn: sqlite3.Connection, repo_dir: str) -> Decision:
    """Return whether a repo path is available without session context.

    PROVENANCE — read before trusting an ``allowed=True`` from this call.
    The answer is derived from the Fleet Watch registry (process rows,
    external-resource rows, session leases) plus a liveness probe
    (``os.kill(pid, 0)``) of holders the registry already knows about. There
    is no OS-level equivalent of the port bind here: a checkout, editor, or
    agent writing to this repo without a Fleet Watch registration is
    INVISIBLE to this decision and will be reported as "repo available".

    So the guardrail is asymmetric, and only one half is load-bearing:
    ``allowed: false`` is evidence (a holder was found) and means stop;
    ``allowed: true`` means "no REGISTERED holder", not "no holder".
    Behaviour is unchanged by this note — the disclosure is the fix.
    """
    return check_repo_with_session(conn, repo_dir, current_session_id=None)


def check_repo_with_session(
    conn: sqlite3.Connection,
    repo_dir: str,
    current_session_id: str | None,
    write_scopes: list[str] | tuple[str, ...] | None = None,
    exclusive: bool = False,
) -> Decision:
    """Return whether a repo path is available for the current session.

    PROVENANCE: registry-only, same asymmetry as :func:`check_repo` — an
    unregistered writer on this path cannot be seen by this decision, so
    ``allowed: true`` means "no registered holder", not "nobody is writing".
    """
    resolved_repo_dir = str(Path(repo_dir).resolve())
    requested_scopes = normalize_write_scopes(resolved_repo_dir, write_scopes)
    holder = registry.get_process_by_repo(conn, resolved_repo_dir)
    if holder is None:
        external_holders = registry.get_external_resources_by_repo(conn, repo_dir)
        if not external_holders:
            session_leases = registry.get_active_session_leases_by_repo(conn, repo_dir)
            owned_by_current_session = False
            advisory_holders: list[dict[str, Any]] = []
            stale_holders: list[dict[str, Any]] = []
            for lease in session_leases:
                if current_session_id and lease["session_id"] == current_session_id:
                    owned_by_current_session = True
                    continue

                owner_pid = lease.get("owner_pid")
                heartbeat_age = registry._age_seconds(lease.get("last_heartbeat_at"))
                owner_missing = owner_pid is None
                # Path C (DECOUPLE): proven owner death and heartbeat-TTL expiry
                # are INDEPENDENT sufficient triggers for release — OR, not AND.
                # ``_lease_owner_alive`` is create-time aware, so a recycled PID
                # (PID reuse) reads as dead even though the integer still exists.
                owner_dead = owner_pid is not None and not registry._lease_owner_alive(
                    lease
                )
                ttl_expired = (
                    heartbeat_age is not None
                    and heartbeat_age > registry.DEFAULT_STALE_SECONDS
                )
                # Conservative arm: a null-PID lease only releases on TTL expiry
                # (missing PID + fresh heartbeat keeps blocking — fail-closed).
                if owner_dead or (owner_missing and ttl_expired):
                    registry.close_session_lease(conn, lease["session_id"])
                    events.log_event(
                        conn,
                        "CLEAN",
                        pid=owner_pid,
                        workstream="session",
                        detail={
                            "reason": "dead_session_owner" if owner_dead else "ownerless_stale_session",
                            "repo_dir": resolved_repo_dir,
                            "session_id": lease["session_id"],
                        },
                    )
                    stale_holders.append(_session_holder_from_lease(lease))
                    continue

                lease_holder = _session_holder_from_lease(lease)
                lease_mode = lease.get("repo_lock_mode", "cooperative")
                held_scopes = lease.get("write_scopes", [])
                overlaps = _overlap_paths(requested_scopes, held_scopes)
                if exclusive or lease_mode == "exclusive":
                    reason = (
                        f"repo {resolved_repo_dir} locked by exclusive session {lease['session_id']}"
                        if lease_mode == "exclusive"
                        else f"exclusive repo lock blocked by active session {lease['session_id']}"
                    )
                    return Decision(
                        allowed=False,
                        reason=reason,
                        holder=lease_holder,
                        holders=[lease_holder],
                        overlap_paths=overlaps,
                        stale_holders=stale_holders,
                    )
                if requested_scopes and held_scopes and overlaps:
                    return Decision(
                        allowed=False,
                        reason=f"repo {resolved_repo_dir} write scope overlaps active session {lease['session_id']}",
                        holder=lease_holder,
                        holders=[lease_holder],
                        overlap_paths=overlaps,
                        stale_holders=stale_holders,
                    )
                advisory_holders.append(lease_holder)
            if owned_by_current_session:
                return Decision(
                    allowed=True,
                    reason="repo available (owned by current session)",
                    stale_holders=stale_holders,
                    safe_mode="same-session",
                )
            if advisory_holders:
                if requested_scopes:
                    reason = "repo available; cooperative sessions have no overlapping write scopes"
                    safe_mode = "cooperative-write"
                else:
                    reason = "repo available; cooperative sessions present"
                    safe_mode = "declare --write-scope before editing"
                return Decision(
                    allowed=True,
                    reason=reason,
                    holders=advisory_holders,
                    stale_holders=stale_holders,
                    safe_mode=safe_mode,
                )
            reason = "repo available"
            if stale_holders:
                reason = "repo available (stale session lease cleared)"
            return Decision(allowed=True, reason=reason, stale_holders=stale_holders)
        for external in external_holders:
            if current_session_id and external["session_id"] == current_session_id:
                continue
            return Decision(
                allowed=False,
                reason=(
                    f"repo {resolved_repo_dir} locked by external "
                    f"{external['provider']} resource {external['external_id']} ({external['name']})"
                ),
                holder=external,
            )
        return Decision(allowed=True, reason="repo available (owned by current session)")
    # Check if holder PID is still alive
    try:
        os.kill(holder["pid"], 0)
    except ProcessLookupError:
        # Holder is dead — auto-release
        registry.release_process(conn, holder["pid"])
        events.log_event(conn, "CLEAN", pid=holder["pid"], workstream=holder["workstream"],
                         detail={"reason": "dead_pid", "repo_dir": repo_dir})
        return Decision(allowed=True, reason="repo available (stale lock cleared)")
    except PermissionError:
        pass  # Process exists

    # Same-session bypass for local processes
    if current_session_id and holder.get("session_id") == current_session_id:
        return Decision(allowed=True, reason="repo available (owned by current session)")

    return Decision(
        allowed=False,
        reason=f"repo {resolved_repo_dir} locked by PID {holder['pid']} ({holder['name']})",
        holder=holder,
    )


def check_gpu_budget(conn: sqlite3.Connection, gpu_mb: int) -> Decision:
    """Return whether a raw GPU budget claim fits the current ledger.

    PROVENANCE — the numbers in this decision's ``reason`` are LEDGER
    arithmetic, not GPU telemetry. ``allocated_mb`` is the sum of what
    registered processes DECLARED they would use; ``available_mb`` is
    ``total_mb - reserve_mb - allocated_mb``. Nothing here reads the device.

    Two consequences the operator must not have to infer from the number:
    an unregistered workload consuming VRAM is invisible and does not reduce
    ``available_mb``; and a registered process that declared 8000MB while
    actually using 20000MB is counted at its declaration. ``allowed: true``
    therefore means "fits the declared ledger", not "the GPU has room".
    ``allowed: false`` is the load-bearing half and means stop.
    Behaviour is unchanged by this note — the disclosure is the fix.
    """
    if gpu_mb <= 0:
        return Decision(allowed=True, reason="no GPU claim")
    budget = registry.get_gpu_budget(conn)
    if gpu_mb <= budget["available_mb"]:
        return Decision(allowed=True, reason=f"{gpu_mb}MB fits in {budget['available_mb']}MB available")
    return Decision(
        allowed=False,
        reason=(
            f"GPU budget exceeded: requesting {gpu_mb}MB but only "
            f"{budget['available_mb']}MB available "
            f"({budget['allocated_mb']}MB allocated of "
            f"{budget['total_mb'] - budget['reserve_mb']}MB allocatable)"
        ),
    )


def summarize_holder(holder: dict[str, Any] | None) -> dict[str, Any] | None:
    """Reduce a holder record to the stable public JSON contract shape."""
    if holder is None:
        return None
    summary = {
        "pid": holder.get("pid"),
        "name": holder["name"],
        "workstream": holder["workstream"],
        "priority": holder["priority"],
        "port": holder.get("port"),
        "repo_dir": holder["repo_dir"],
        "gpu_mb": holder["gpu_mb"],
    }
    for key in ("session_id", "provider", "external_id", "resource_type", "repo_lock_mode", "write_scopes"):
        if key in holder:
            summary[key] = holder.get(key)
    return summary


def suggest_ports(
    conn: sqlite3.Connection,
    preferred_ports: list[int],
    requested_port: int | None = None,
    limit: int = 5,
) -> list[int]:
    """Suggest candidate ports the OS confirms are bindable right now.

    Both authorities apply, same as ``check_port``: no registry claim AND a
    successful loopback bind. Registry-only suggestion was measured handing
    back a port the same process had just denied — in one run, ``check_port``
    returned allowed=False for a live listener on 55692 while
    ``suggest_ports`` returned 55692 as suggestion #1. A suggestion that
    cannot be bound is worse than no suggestion: it is a denial followed by
    advice to do the thing that just failed.

    Only ``PORT_FREE`` candidates are offered. Privileged and undetermined
    ports are skipped rather than suggested — an unverifiable port is not a
    safe recommendation.
    """
    occupied = set(registry.get_claimed_ports(conn).keys())
    suggestions: list[int] = []

    def _offerable(candidate: int) -> bool:
        if candidate == requested_port or candidate in occupied:
            return False
        if candidate in suggestions:
            return False
        return probe_port(candidate).claimable

    for port in preferred_ports:
        if not _offerable(port):
            continue
        suggestions.append(port)
        if len(suggestions) >= limit:
            return suggestions

    if requested_port is None:
        start = 8000
    else:
        start = max(1024, requested_port - 5)

    for port in range(start, start + 200):
        if not _offerable(port):
            continue
        suggestions.append(port)
        if len(suggestions) >= limit:
            break

    return suggestions


def preflight_register(
    conn: sqlite3.Connection,
    port: int | None = None,
    gpu_mb: int = 0,
    repo_dir: str | None = None,
    current_session_id: str | None = None,
    write_scopes: list[str] | tuple[str, ...] | None = None,
    exclusive_repo_lock: bool = False,
    owner_pid: int | None = None,
) -> list[Decision]:
    """Run all checks before registration. Returns list of failed decisions (empty = all clear)."""
    failures: list[Decision] = []

    if port is not None:
        d = check_port(conn, port, owner_pid=owner_pid)
        if not d.allowed:
            failures.append(d)

    if gpu_mb > 0:
        d = check_gpu_budget(conn, gpu_mb)
        if not d.allowed:
            failures.append(d)

    if repo_dir is not None:
        d = check_repo_with_session(
            conn,
            repo_dir,
            current_session_id=current_session_id,
            write_scopes=write_scopes,
            exclusive=exclusive_repo_lock,
        )
        if not d.allowed:
            failures.append(d)

    return failures


def claim_port(conn: sqlite3.Connection, port: int) -> Decision:
    """Standalone port claim check (no registration)."""
    decision = check_port(conn, port)
    if decision.allowed:
        events.log_event(conn, "CLAIM", detail={"resource": "port", "port": port})
    else:
        events.log_event(conn, "CONFLICT", detail={"resource": "port", "port": port,
                                                     "holder_pid": decision.holder["pid"] if decision.holder else None})
    return decision


def claim_repo(conn: sqlite3.Connection, repo_dir: str) -> Decision:
    """Standalone repo claim check (no registration)."""
    decision = check_repo(conn, repo_dir)
    if decision.allowed:
        events.log_event(conn, "CLAIM", detail={"resource": "repo", "repo_dir": repo_dir})
    else:
        events.log_event(conn, "CONFLICT", detail={"resource": "repo", "repo_dir": repo_dir,
                                                     "holder_pid": decision.holder["pid"] if decision.holder else None})
    return decision


def preempt_port(
    conn: sqlite3.Connection,
    port: int,
    new_priority: int,
    reason: str,
    grace_seconds: int = 30,
) -> Decision:
    """Preempt a port from a lower-priority registered holder.

    Preemption is registry-scoped by construction: it works by signalling a
    PID Fleet Watch recorded. An unregistered listener has no record, so
    there is no PID to signal and nothing to preempt.

    The empty-registry case therefore consults the OS before answering. It
    previously returned allowed=True "port already free" whenever the
    registry row was missing — the same registry-absence-means-free defect
    ``check_port`` had, and worse here, because the caller acts on it by
    binding a port that a live listener still owns.
    """
    holder = registry.get_process_by_port(conn, port)
    if holder is None:
        probe = probe_port(port)
        if probe.claimable:
            return Decision(allowed=True, reason="port already free")
        owners = os_port_owner_pids(port)
        if owners:
            attribution = (
                f"unregistered PID(s) {sorted(owners)} hold it; Fleet Watch "
                "will not signal a process it does not manage"
            )
        else:
            attribution = (
                "no registered holder exists to preempt and the OS holder "
                "could not be identified"
            )
        return Decision(
            allowed=False,
            reason=f"cannot preempt port {port}: {probe.detail}; {attribution}",
        )

    if new_priority <= holder["priority"]:
        return Decision(
            allowed=False,
            reason=f"cannot preempt: new priority {new_priority} <= holder priority {holder['priority']}",
            holder=holder,
        )

    # Log the preemption
    events.log_event(
        conn, "PREEMPT",
        pid=holder["pid"],
        workstream=holder["workstream"],
        detail={
            "port": port,
            "holder_pid": holder["pid"],
            "holder_priority": holder["priority"],
            "new_priority": new_priority,
            "reason": reason,
            "grace_seconds": grace_seconds,
        },
    )

    # Send SIGTERM to the holder
    try:
        os.kill(holder["pid"], signal.SIGTERM)
    except ProcessLookupError:
        pass  # Already dead

    # Wait for grace period
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        try:
            os.kill(holder["pid"], 0)
            time.sleep(1)
        except ProcessLookupError:
            break  # Process exited

    # Force-release claims
    registry.release_process(conn, holder["pid"])

    return Decision(
        allowed=True,
        reason=f"preempted PID {holder['pid']} ({holder['name']}) for: {reason}",
        holder=holder,
    )
