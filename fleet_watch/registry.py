"""SQLite registry for Fleet Watch process tracking."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FLEET_DIR = Path.home() / ".fleet-watch"
DB_PATH = FLEET_DIR / "registry.db"
DEFAULT_GPU_TOTAL_MB = 131072
DEFAULT_GPU_RESERVE_MB = 16384
DEFAULT_STALE_SECONDS = 180
DEFAULT_SESSION_LEASE_CLEANUP_LIMIT = 50
# Bound on any PPID walk used for an authorization decision. Real process trees
# on this host are single digits deep; the budget exists so an inspection error
# or a PPID cycle terminates as "uninspectable" (fail-closed) instead of
# spinning up unbounded `ps` calls inside a gate.
LINEAGE_MAX_HOPS = 32

SCHEMA = """
CREATE TABLE IF NOT EXISTS processes (
    pid         INTEGER PRIMARY KEY,
    session_id  TEXT NOT NULL,
    workstream  TEXT NOT NULL,
    name        TEXT NOT NULL,
    priority    INTEGER NOT NULL DEFAULT 3,
    port        INTEGER,
    gpu_mb      INTEGER DEFAULT 0,
    repo_dir    TEXT,
    model       TEXT,
    restart_policy TEXT NOT NULL DEFAULT 'ALERT_ONLY',
    start_cmd      TEXT,
    start_time     TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL,
    expected_duration_min INTEGER,
    -- Kernel create-time of `pid`, captured at registration. Used ONLY for
    -- equality against a later read, which proves whether the integer PID still
    -- names the same process (see registry._owner_still_alive). Declared LAST so
    -- a fresh DB and a DB migrated by _ensure_column agree on column order —
    -- _row_to_dict maps `SELECT *` positionally.
    start_create_time TEXT,
    UNIQUE(port),
    UNIQUE(repo_dir)
);

CREATE TABLE IF NOT EXISTS session_leases (
    session_id          TEXT PRIMARY KEY,
    owner_pid           INTEGER,
    owner_ppid          INTEGER,
    owner_pgid          INTEGER,
    owner_tty           TEXT,
    owner_create_time   TEXT,
    repo_dir            TEXT,
    repo_lock_mode      TEXT NOT NULL DEFAULT 'cooperative',
    write_scopes        TEXT,
    fencing_epoch       INTEGER NOT NULL DEFAULT 1,
    started_at          TEXT NOT NULL,
    last_heartbeat_at   TEXT NOT NULL,
    shutdown_at         TEXT,
    status              TEXT NOT NULL DEFAULT 'ACTIVE'
);

CREATE TABLE IF NOT EXISTS external_resources (
    provider        TEXT NOT NULL,
    resource_type   TEXT NOT NULL,
    external_id     TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    workstream      TEXT NOT NULL,
    name            TEXT NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 3,
    gpu_mb          INTEGER DEFAULT 0,
    repo_dir        TEXT,
    model           TEXT,
    status          TEXT NOT NULL DEFAULT 'ACTIVE',
    started_by      TEXT,
    owner_tool      TEXT,
    endpoint        TEXT,
    cleanup_cmd     TEXT,
    safe_to_delete  INTEGER NOT NULL DEFAULT 0,
    metadata        TEXT,
    start_time      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    PRIMARY KEY(provider, external_id)
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    pid         INTEGER,
    workstream  TEXT,
    detail      TEXT,
    prev_hash   TEXT,
    hash        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gpu_budget (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    total_mb        INTEGER NOT NULL DEFAULT 131072,
    reserve_mb      INTEGER NOT NULL DEFAULT 16384,
    allocated_mb    INTEGER NOT NULL DEFAULT 0
);
"""

RESTART_POLICIES = frozenset({
    "RESTART_ALWAYS",
    "RESTART_ON_FAILURE",
    "RESTART_NEVER",
    "ALERT_ONLY",
})

SESSION_LEASE_STATUSES = frozenset({
    "ACTIVE",
    "CLOSED",
})

SESSION_REPO_LOCK_MODES = frozenset({
    "cooperative",
    "exclusive",
})

EXCLUSIVE_REPO_INDEX = "ux_session_leases_exclusive_repo"
REGISTRY_WARNINGS: list[dict[str, Any]] = []

PROCESS_STATES = frozenset({
    "live",
    "disconnected",
    "stale_candidate",
    "orphan_confirmed",
    "exited",
})

# B608 guard: the ONLY columns ``heartbeat_external_resource`` may ever set.
# Its UPDATE statement is assembled from this module's own constants — never
# from external input — and every column name is validated against this set
# before the statement is built.
_EXTERNAL_RESOURCE_UPDATABLE_COLUMNS = frozenset({
    "last_seen",
    "status",
    "metadata",
})


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dir() -> Path:
    """Create the Fleet Watch state directory if needed and return it."""
    FLEET_DIR.mkdir(parents=True, exist_ok=True)
    return FLEET_DIR


def _resolve_repo_dir(repo_dir: str | None) -> str | None:
    return str(Path(repo_dir).resolve()) if repo_dir else None


def repo_dirs_overlap(left: str | None, right: str | None) -> bool:
    """True when resolved paths are equal or one is a directory prefix of the other."""
    if not left or not right:
        return False
    left_resolved = str(Path(left).resolve())
    right_resolved = str(Path(right).resolve())
    if left_resolved == right_resolved:
        return True
    left_prefix = left_resolved + os.sep
    right_prefix = right_resolved + os.sep
    return left_resolved.startswith(right_prefix) or right_resolved.startswith(left_prefix)


def _registry_db_key(db_path: Path | str | None = None) -> str:
    return str(Path(db_path or DB_PATH).expanduser().resolve())


def registry_warnings_for(db_path: Path | str | None = None) -> list[dict[str, Any]]:
    """Warnings recorded for one registry file; later connects to other DBs do not clear these."""
    key = _registry_db_key(db_path)
    return [dict(warning) for warning in REGISTRY_WARNINGS if warning.get("db_path") == key]


def _resolve_write_scopes(repo_dir: str | None, write_scopes: list[str] | tuple[str, ...] | None) -> list[str]:
    if not write_scopes:
        return []
    base = Path(repo_dir).expanduser().resolve() if repo_dir else None
    if base is None:
        raise ValueError("write scopes require a repository root")
    resolved: list[str] = []
    for raw in write_scopes:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = base / path
        resolved_path = path.resolve()
        try:
            resolved_path.relative_to(base)
        except ValueError as exc:
            raise ValueError(
                f"write scope escapes repository root: {raw!r} is outside {base}"
            ) from exc
        value = str(resolved_path)
        if value not in resolved:
            resolved.append(value)
    return resolved


def _encode_write_scopes(scopes: list[str]) -> str | None:
    return json.dumps(scopes, separators=(",", ":")) if scopes else None


def _decode_write_scopes(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, str)]


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _ensure_exclusive_repo_index(conn: sqlite3.Connection, db_path: Path | str) -> None:
    """Create the partial unique index for ACTIVE exclusive repo leases.

    If creation fails because an existing registry already holds duplicate
    ACTIVE exclusive rows for one repo, do NOT close or edit any lease:
    leave the index absent, record the condition in ``REGISTRY_WARNINGS``,
    and keep the transactional grant path as the enforcement.
    """
    key = _registry_db_key(db_path)
    try:
        conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {EXCLUSIVE_REPO_INDEX} "
            "ON session_leases(repo_dir) "
            "WHERE status = 'ACTIVE' AND repo_lock_mode = 'exclusive'"
        )
    except sqlite3.IntegrityError:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        warning = {
            "code": "exclusive_repo_index_absent",
            "reason": (
                "duplicate ACTIVE exclusive session leases prevent unique index; "
                "transactional grant remains the enforcement"
            ),
            "db_path": key,
        }
        if warning not in REGISTRY_WARNINGS:
            REGISTRY_WARNINGS.append(warning)
        return
    REGISTRY_WARNINGS[:] = [
        warning
        for warning in REGISTRY_WARNINGS
        if not (
            warning.get("code") == "exclusive_repo_index_absent"
            and warning.get("db_path") == key
        )
    ]


def _age_seconds(iso_ts: str | None) -> int | None:
    if not iso_ts:
        return None
    ts = datetime.fromisoformat(iso_ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return int((datetime.now(timezone.utc) - ts).total_seconds())


def _pid_exists(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _pid_create_time(pid: int | None) -> str | None:
    """Return a stable create-time string for a live PID via ``ps -o lstart=``.

    The kernel start time is constant for the life of a process and changes the
    instant a PID is recycled, so a recorded-vs-live mismatch positively proves
    the original owner is gone — defeating PID reuse without waiting out the TTL.
    Returns ``None`` when the PID is dead or create-time is unresolvable (the
    caller then degrades to PID-existence only — conservative, never fail-open).

    ENVIRONMENT INVARIANCE (catastrophic two-writer guard): ``ps -o lstart=``
    renders its timestamp in the *caller's* timezone and locale, so the SAME
    live PID yields different strings to an interactive shell (local TZ) and the
    launchd ``fleet discover`` daemon (UTC). The recorded value is used ONLY for
    equality in ``_lease_owner_alive`` (never displayed), so we force a fixed
    rendering — ``LC_ALL=C`` and ``TZ=UTC`` — in the subprocess environment.
    Capture and check then produce byte-identical strings regardless of the
    caller's environment, so a LIVE owner is never misread as dead across a
    TZ/locale boundary (which would release its exclusive lease => two writers).
    """
    if pid is None or pid <= 0:
        return None
    if not _pid_exists(pid):
        return None
    # Force an environment-independent rendering so the create-time recorded at
    # lease open compares equal to the create-time read at the liveness check
    # regardless of the caller's TZ / locale. Equality is the only use of this
    # value (it is never displayed), so a fixed canonical rendering is correct.
    fixed_env = {**os.environ, "LC_ALL": "C", "TZ": "UTC"}
    try:
        result = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
            env=fixed_env,
        )
    except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired, OSError):
        return None
    line = result.stdout.strip()
    if result.returncode != 0 or not line:
        return None
    return line


def _owner_still_alive(pid: int | None, recorded_create_time: str | None) -> bool:
    """Positively confirm ``pid`` still names the SAME process it named when
    ``recorded_create_time`` was captured.

    PID existence alone cannot answer this: the OS recycles PIDs, so a dead
    owner's integer can be handed to an unrelated process and read as "alive".
    A recorded create-time that no longer matches positively proves the original
    owner is gone.

    Every uncertainty resolves toward "alive" (keep blocking), never toward
    release: a missing create-time (pre-migration row) or an unreadable live
    create-time degrades to PID existence, which is the prior behaviour. Death
    is only ever declared on positive evidence.

    Single implementation shared by the session-lease path and the process path
    — the two used to differ, and the process path was the one without it.
    """
    if pid is None:
        return False
    if not _pid_exists(pid):
        return False
    if not recorded_create_time:
        return True
    live = _pid_create_time(pid)
    if live is None:
        return True
    return live == recorded_create_time


def _owner_identity_proven(
    pid: int | None,
    recorded_create_time: str | None,
) -> bool | None:
    """Return positive process identity proof for authorization decisions.

    Tri-state, unlike :func:`_owner_still_alive`, which resolves uncertainty
    toward "alive" because its job is to keep a lease BLOCKING. Authorization
    cannot reuse that bias: "probably the owner" must not open a privilege.

    ``True``  — the PID exists and its kernel create-time matches the one
                recorded at lease open (identity positively proven).
    ``False`` — the PID is gone, or the create-time no longer matches, which
                positively proves the original owner is dead (PID reuse).
    ``None``  — identity is unprovable (no recorded create-time, or ``ps`` is
                unavailable/unreadable). Callers MUST fail closed.
    """
    if pid is None or pid <= 0 or not _pid_exists(pid):
        return False
    if not recorded_create_time:
        return None
    live_create_time = _pid_create_time(pid)
    if live_create_time is None:
        return None
    return live_create_time == recorded_create_time


def _lease_owner_alive(lease: dict[str, Any] | None) -> bool:
    """Positively confirm a lease's owner process is the SAME process that opened
    it — PID exists AND, when a create-time was recorded, the live PID's
    create-time still matches.

    A recorded create-time that no longer matches means the OS recycled the PID
    onto an unrelated process: the original owner is dead. Returns ``False`` for
    a null owner_pid (ownership is then handled by the conservative TTL arm).
    """
    if lease is None:
        return False
    return _owner_still_alive(lease.get("owner_pid"), lease.get("owner_create_time"))


def process_owner_alive(process: dict[str, Any] | None) -> bool:
    """Confirm a registered PROCESS row's PID still names the same process.

    The process-row analogue of :func:`_lease_owner_alive`. Callers that act on
    a registry row's PID — refusing a repo in its name, or SIGTERMing it —
    must use this rather than a bare ``os.kill(pid, 0)``, which cannot tell a
    live owner from a recycled integer.
    """
    if process is None:
        return False
    return _owner_still_alive(process.get("pid"), process.get("start_create_time"))


def current_fencing_epoch(conn: sqlite3.Connection, session_id: str) -> int | None:
    """Return the lease's current monotonic fencing epoch, or ``None`` if absent.

    HONESTY NOTE (Path C, Layer C): this is NOT a storage-level fencing token in
    the Kleppmann sense. Fleet Watch is advisory pre-flight — it never sits in
    the filesystem write path, so there is no enforcement point at which a stale
    token can be MECHANICALLY rejected by the storage system. The epoch is a
    real, persisted, monotonic grant counter: a caller that snapshotted epoch N
    can call ``fencing_token_valid`` later to detect that the lease was re-granted
    (epoch advanced) and self-abort. That detection is voluntary, not enforced.
    On a single machine the load-bearing guards against the stale-owner failure
    are create-time identity + TTL (Layers A/B); the epoch is the minimal honest
    primitive a future gated writer could enforce against, nothing more.
    """
    row = conn.execute(
        "SELECT fencing_epoch FROM session_leases WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def fencing_token_valid(
    conn: sqlite3.Connection, session_id: str, presented_epoch: int
) -> bool:
    """True iff ``presented_epoch`` matches the lease's CURRENT epoch.

    A holder whose epoch is behind the current one took a stale snapshot (the
    lease was re-granted to a newer owner) and must not act. Returns ``False``
    when the lease is absent — fail-closed, never validate an unknown token.
    """
    current = current_fencing_epoch(conn, session_id)
    if current is None:
        return False
    return presented_epoch == current


def _inspect_process(pid: int | None) -> dict[str, Any] | None:
    if pid is None or pid <= 0:
        return None
    if not _pid_exists(pid):
        return None

    try:
        result = subprocess.run(
            ["ps", "-o", "ppid=", "-o", "pgid=", "-o", "tty=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired) as exc:
        return {
            "pid": pid,
            "alive": True,
            "inspectable": False,
            "ppid": None,
            "pgid": None,
            "tty": None,
            "error": str(exc),
        }

    line = result.stdout.strip()
    if result.returncode != 0 or not line:
        return {
            "pid": pid,
            "alive": True,
            "inspectable": False,
            "ppid": None,
            "pgid": None,
            "tty": None,
            "error": result.stderr.strip() or "ps inspection failed",
        }

    parts = line.split(None, 2)
    if len(parts) < 2:
        return {
            "pid": pid,
            "alive": True,
            "inspectable": False,
            "ppid": None,
            "pgid": None,
            "tty": None,
            "error": f"unexpected ps output: {line}",
        }

    tty = parts[2] if len(parts) >= 3 else "?"
    try:
        return {
            "pid": pid,
            "alive": True,
            "inspectable": True,
            "ppid": int(parts[0]),
            "pgid": int(parts[1]),
            "tty": tty,
        }
    except ValueError:
        return {
            "pid": pid,
            "alive": True,
            "inspectable": False,
            "ppid": None,
            "pgid": None,
            "tty": tty,
            "error": f"unexpected ps output: {line}",
        }


def collect_owner_metadata(owner_pid: int | None) -> dict[str, Any]:
    """Gather ``ps``-derived owner fields. Does not touch the registry DB."""
    inspect = _inspect_process(owner_pid)
    return {
        "owner_pid": owner_pid,
        "owner_ppid": inspect.get("ppid") if inspect else None,
        "owner_pgid": inspect.get("pgid") if inspect else None,
        "owner_tty": inspect.get("tty") if inspect else None,
        "owner_create_time": _pid_create_time(owner_pid),
    }


def _process_uid(pid: int | None) -> int | None:
    """Return the numeric owning uid of a live PID, or ``None`` when unresolvable.

    ``None`` is an authorization DENY signal, never "same uid" — an unreadable
    uid is exactly the case an attacker would engineer.
    """
    if pid is None or pid <= 0 or not _pid_exists(pid):
        return None
    try:
        result = subprocess.run(
            ["ps", "-o", "uid=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
            env={**os.environ, "LC_ALL": "C"},
        )
    except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired, OSError):
        return None
    line = result.stdout.strip()
    if result.returncode != 0 or not line:
        return None
    try:
        return int(line.split()[0])
    except (ValueError, IndexError):
        return None


def _process_command(pid: int | None) -> str | None:
    """Full argv of a live PID, or ``None`` when unresolvable.

    ``None`` is a DENY signal for every caller, never "nothing interesting" — an
    unreadable command is exactly what an agent hiding from the roster produces.
    """
    if pid is None or pid <= 0 or not _pid_exists(pid):
        return None
    try:
        result = subprocess.run(
            ["ps", "-o", "command=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
            env={**os.environ, "LC_ALL": "C"},
        )
    except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired, OSError):
        return None
    line = result.stdout.strip()
    if result.returncode != 0 or not line:
        return None
    return line


def _agent_runtime_roster() -> list[dict[str, str]]:
    """The one roster, read lazily.

    ``syshealth`` imports this module, so the dependency can only run this
    direction at call time. An import failure returns ``None`` upward and every
    caller denies — a roster that cannot be loaded must never read as "no agent".
    """
    from fleet_watch import syshealth  # local: syshealth imports registry

    return syshealth.DEFAULT_SESSION_PATTERNS


def _command_is_agent_runtime(command: str, roster: list[dict[str, str]]) -> bool:
    """True when ``command`` looks like ANY roster runtime.

    Deliberately over-matches relative to the census: an argv token whose
    basename equals a roster ``binary`` counts, and so does the census regex.
    Over-matching costs an operator one denied close; under-matching would hand
    an agent the operator's own revocation authority.
    """
    for entry in roster:
        pattern = entry.get("process_match")
        if pattern:
            try:
                if re.search(pattern, command):
                    return True
            except re.error:
                pass
        binary = entry.get("binary")
        if not binary:
            continue
        for token in command.split():
            if os.path.basename(token) == binary:
                return True
            # A runtime's own directory in the path counts: `/opt/codex/bin/x`
            # and `~/.claude/hooks/x.py` are both agent-side scripts. This is
            # the fail-closed direction — it costs an operator running such a
            # script one denied close (lineage still decides), where the reverse
            # error would hand an agent the operator's revocation authority.
            if (
                f"/{binary}/" in token
                or f"/.{binary}/" in token
                or token.startswith((f"{binary}/", f".{binary}/"))
            ):
                return True
    return False


def _agent_runtime_in_ancestry(
    pid: int | None,
    max_hops: int = LINEAGE_MAX_HOPS,
) -> bool | None:
    """Is ``pid`` — or any ancestor of it — an agent runtime?

    ``True`` = an agent was found. ``False`` = the ancestry was walked to init
    with no match. ``None`` = the walk could not be completed (unreadable
    command, uninspectable process, PPID cycle, hop budget exhausted) and every
    caller MUST fail closed.
    """
    if pid is None or pid <= 0:
        return None
    try:
        roster = _agent_runtime_roster()
    except Exception:  # noqa: BLE001 - an unloadable roster must not read as "no agent"
        return None
    if not roster:
        return None

    current_pid = pid
    seen: set[int] = set()
    for _ in range(max_hops):
        if current_pid in seen:
            return None
        seen.add(current_pid)

        command = _process_command(current_pid)
        if command is None:
            return None
        if _command_is_agent_runtime(command, roster):
            return True

        info = _inspect_process(current_pid)
        if info is None or not info.get("inspectable"):
            return None
        parent_pid = info.get("ppid")
        if not isinstance(parent_pid, int):
            return None
        if parent_pid in (0, 1):
            return False
        current_pid = parent_pid
    return None


def _lineage_proven(
    start_pid: int | None,
    target_pid: int | None,
    max_hops: int = LINEAGE_MAX_HOPS,
) -> bool | None:
    """Prove ``target_pid`` is ``start_pid`` itself or an ANCESTOR of it, by PPID.

    ``True`` is positive proof. ``False`` is a fully inspected non-match (the
    walk reached init without meeting the target). ``None`` means the lineage
    could not be inspected — ``ps`` unavailable, output unparseable, a PPID
    cycle, or the hop budget exhausted — and callers MUST fail closed.

    Direction is the caller's choice, which is what makes both an ancestor and a
    descendant check expressible with one bounded walk:
      * descendant arm — ``_lineage_proven(requester, owner)``
      * ancestor arm  — ``_lineage_proven(owner, requester)``

    PGID and TTY are recorded audit evidence only; neither grants authority.
    """
    if start_pid is None or start_pid <= 0:
        return None
    if target_pid is None or target_pid <= 0:
        return False
    if start_pid == target_pid:
        return True

    current_pid = start_pid
    seen: set[int] = set()
    for _ in range(max_hops):
        if current_pid in seen:
            return None
        seen.add(current_pid)

        info = _inspect_process(current_pid)
        if info is None or not info.get("inspectable"):
            return None
        parent_pid = info.get("ppid")
        if not isinstance(parent_pid, int):
            return None
        if parent_pid == target_pid:
            return True
        if parent_pid in (0, 1):
            return False
        current_pid = parent_pid
    return None


def authorize_session_close(
    conn: sqlite3.Connection,
    session_id: str,
    requester_pid: int | None,
) -> tuple[bool, str]:
    """Decide whether ``requester_pid`` may close ``session_id``'s lease.

    Closing a lease is a PRIVILEGE REVOCATION: an ACTIVE lease is what makes
    ``fleet guard --json`` answer DENY for every other agent, so a close turns
    that DENY into ALLOW. A session id is a public locator (``fleet session
    list`` prints it), never a bearer token — possession grants nothing.

    Authorized, and only these:
      * the owner PID itself;
      * a DESCENDANT of the owner (the session's own ``bash``/``fleet`` child);
      * an ANCESTOR of the owner (the Terminal shell that spawned the session)
        — an owner cannot outrank the shell that created it, and denying this
        forces unaudited hand-deletion of governance state;
      * the OPERATOR SEAT — a requester of the same uid with NO agent runtime
        anywhere in its ancestry, i.e. a human at any of their own terminals.
        Lineage alone denied a second Terminal tab, which is a sibling of the
        owner's parent, not an ancestor. Agents are excluded by construction
        (a Claude/Codex/OpenCode/Grok process anywhere up the chain fails the
        test), so this never lets one agent revoke another's lease;
      * anyone at all when the owner is PROVABLY dead — reaping a dead lease is
        not a privilege, and fleet-watch invariant 3 forbids a dead owner from
        holding a repo for up to the TTL.

    Every other outcome denies, including every uncertainty: unresolvable uid,
    uninspectable lineage, unprovable owner identity, a PPID cycle, or a lease
    with a NULL ``owner_pid`` (whose liveness belongs to the referee's TTL arm,
    not to this path). There is deliberately no ``--force`` and no environment
    override — an override is a fail-open path by construction.

    Returns ``(allowed, reason)``; the reason is always safe to print.
    """
    lease = get_session_lease(conn, session_id)
    if lease is None:
        return False, "session lease not found"
    if requester_pid is None or requester_pid <= 0:
        return False, "requester pid is unknown (fail-closed)"

    owner_pid = lease.get("owner_pid")
    if not isinstance(owner_pid, int) or owner_pid <= 0:
        return False, "session lease has no owner pid; close is TTL-governed (fail-closed)"

    owner_identity = _owner_identity_proven(owner_pid, lease.get("owner_create_time"))
    if owner_identity is False:
        return True, f"session owner pid {owner_pid} is provably dead; reaping"
    if owner_identity is None:
        return False, "session owner identity is uninspectable (fail-closed)"

    owner_uid = _process_uid(owner_pid)
    requester_uid = _process_uid(requester_pid)
    if owner_uid is None or requester_uid is None:
        return False, "requester or owner uid is unresolvable (fail-closed)"
    if owner_uid != requester_uid:
        return False, "requester uid does not match the session owner uid"

    descendant = _lineage_proven(requester_pid, owner_pid)
    if descendant is True:
        return True, "requester is the session owner or a descendant of it"
    ancestor = _lineage_proven(owner_pid, requester_pid)
    if ancestor is True:
        return True, "requester is an ancestor of the session owner"

    # Operator seat (2026-09-03). Lineage alone denied the operator's OWN hand:
    # a second Terminal tab is a SIBLING of the owner's parent, so a human at
    # their own machine could not close their own lease and was pushed to
    # deleting governance state by hand. A same-uid requester with NO agent
    # runtime anywhere in its ancestry is that hand. An agent — or any child of
    # one — never reaches this arm, so one agent still cannot revoke another's
    # lease. Ordered last: it only ever converts a would-be DENY into an ALLOW,
    # and only after every identity and uid check above has already passed.
    agent_in_ancestry = _agent_runtime_in_ancestry(requester_pid)
    if agent_in_ancestry is False:
        return True, "requester is the operator seat (same uid, no agent runtime in its ancestry)"
    if agent_in_ancestry is None:
        return False, "requester ancestry is uninspectable for agent runtimes (fail-closed)"

    if descendant is None or ancestor is None:
        return False, "requester lineage is uninspectable (fail-closed)"
    return False, (
        "requester is an agent runtime and is not the session owner, a descendant, "
        "or an ancestor"
    )


def describe_session_close_authority(
    conn: sqlite3.Connection,
    session_id: str,
) -> dict[str, Any]:
    """Predict what ``fleet session close`` will decide for this lease.

    Advice that names a command the gate will refuse is worse than no advice:
    it sends an operator to a DENY and teaches them the guard is noise. So the
    remedy text is derived from :func:`authorize_session_close`'s OWN
    predicates rather than restated beside them — the two cannot drift because
    there is only one implementation of "is the owner provably dead" and one of
    "does this lease even have an owner".

    Requester-independent by construction: a requester PID is not known when a
    ``fleet guard`` denial is rendered, so this answers the question that can
    be answered without one — WHO may close it — and leaves the lineage walk
    to the close path itself.

    ``status`` is one of:
      ``absent``        — no such lease; nothing to close.
      ``ttl_only``      — NULL ``owner_pid``: close fails closed for EVERY
                          requester; only heartbeat-TTL expiry clears it.
      ``reapable``      — the owner is provably dead; anyone may close it.
      ``lineage_only``  — the owner is live and proven; only the owner, a
                          descendant, or an ancestor may close it.
      ``uninspectable`` — owner identity is unprovable; close fails closed for
                          every requester until it can be inspected.
    """
    lease = get_session_lease(conn, session_id)
    if lease is None:
        return {"session_id": session_id, "status": "absent", "owner_pid": None}

    owner_pid = lease.get("owner_pid")
    if not isinstance(owner_pid, int) or owner_pid <= 0:
        return {"session_id": session_id, "status": "ttl_only", "owner_pid": None}

    identity = _owner_identity_proven(owner_pid, lease.get("owner_create_time"))
    if identity is False:
        return {"session_id": session_id, "status": "reapable", "owner_pid": owner_pid}
    if identity is None:
        return {
            "session_id": session_id,
            "status": "uninspectable",
            "owner_pid": owner_pid,
        }
    return {"session_id": session_id, "status": "lineage_only", "owner_pid": owner_pid}


def _is_parent_chain_detached(pid: int) -> bool | None:
    info = _inspect_process(pid)
    if info is None:
        return True
    if not info.get("inspectable"):
        return None

    seen: set[int] = {pid}
    current = info
    while True:
        parent_pid = current["ppid"]
        if parent_pid in (0, 1):
            return True
        if parent_pid in seen:
            return None
        if not _pid_exists(parent_pid):
            return True

        parent_info = _inspect_process(parent_pid)
        if parent_info is None:
            return True
        if not parent_info.get("inspectable"):
            return None

        parent_tty = (parent_info.get("tty") or "").strip()
        if parent_tty and parent_tty not in {"?", "??"}:
            return False

        seen.add(parent_pid)
        current = parent_info


def _configured_budget_defaults() -> tuple[int, int]:
    config_path = FLEET_DIR / "config.json"
    total_mb = DEFAULT_GPU_TOTAL_MB
    reserve_mb = DEFAULT_GPU_RESERVE_MB

    if not config_path.exists():
        return total_mb, reserve_mb

    try:
        config = json.loads(config_path.read_text())
    except json.JSONDecodeError:
        return total_mb, reserve_mb

    try:
        total_mb = int(config.get("gpu_total_mb", total_mb))
        reserve_mb = int(config.get("gpu_reserve_mb", reserve_mb))
    except (TypeError, ValueError):
        return DEFAULT_GPU_TOTAL_MB, DEFAULT_GPU_RESERVE_MB

    if total_mb <= 0 or reserve_mb < 0 or reserve_mb >= total_mb:
        return DEFAULT_GPU_TOTAL_MB, DEFAULT_GPU_RESERVE_MB

    return total_mb, reserve_mb


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    """Open the registry database and ensure the schema is initialized."""
    path = db_path or DB_PATH
    ensure_dir()
    total_mb, reserve_mb = _configured_budget_defaults()
    conn = sqlite3.connect(str(path), timeout=10)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    _ensure_column(conn, "session_leases", "repo_lock_mode", "TEXT NOT NULL DEFAULT 'cooperative'")
    _ensure_column(conn, "session_leases", "write_scopes", "TEXT")
    # Path C migration (backward-compatible): create-time identity defeats PID
    # reuse; fencing_epoch is a monotonic per-lease token issued at grant. Both
    # default safely on pre-existing rows (NULL create-time degrades to PID
    # existence; epoch defaults to 1).
    _ensure_column(conn, "session_leases", "owner_create_time", "TEXT")
    _ensure_column(conn, "session_leases", "fencing_epoch", "INTEGER NOT NULL DEFAULT 1")
    # Same migration for PROCESS rows. The session-lease path has defeated PID
    # reuse since Path C; the process path could not even in principle, because
    # the evidence was never recorded — ``processes`` had no create-time column,
    # so ``check_repo``'s ``os.kill(pid, 0)`` could only ask "does this integer
    # name a live process", never "is it still the SAME process". Appended last
    # (ALTER TABLE ADD COLUMN), which is where ``_row_to_dict`` expects it.
    _ensure_column(conn, "processes", "start_create_time", "TEXT")
    _ensure_exclusive_repo_index(conn, path)
    # Ensure gpu_budget singleton exists
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, ?, ?, 0)",
        (total_mb, reserve_mb),
    )
    conn.execute(
        "UPDATE gpu_budget SET total_mb = ?, reserve_mb = ? WHERE id = 1",
        (total_mb, reserve_mb),
    )
    conn.commit()
    return conn


def upsert_session_lease(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    owner_pid: int | None = None,
    repo_dir: str | None = None,
    status: str = "ACTIVE",
    repo_lock_mode: str | None = None,
    write_scopes: list[str] | tuple[str, ...] | None = None,
    owner_metadata: dict[str, Any] | None = None,
    commit: bool = True,
) -> None:
    """Create or refresh a session lease with current owner metadata."""
    if status not in SESSION_LEASE_STATUSES:
        raise ValueError(f"Invalid session lease status: {status}")
    if repo_lock_mode is not None and repo_lock_mode not in SESSION_REPO_LOCK_MODES:
        raise ValueError(f"Invalid repo lock mode: {repo_lock_mode}")

    now = _now_iso()
    resolved_repo = _resolve_repo_dir(repo_dir)
    prior = get_session_lease(conn, session_id)
    resolved_mode = repo_lock_mode or (prior.get("repo_lock_mode") if prior else "cooperative")
    prior_scopes = prior.get("write_scopes", []) if prior else []
    resolved_scopes = (
        _resolve_write_scopes(resolved_repo, write_scopes)
        if write_scopes is not None
        else prior_scopes
    )
    if owner_metadata is None:
        owner_metadata = collect_owner_metadata(owner_pid)
    owner_pid = owner_metadata.get("owner_pid", owner_pid)
    owner_ppid = owner_metadata.get("owner_ppid")
    owner_pgid = owner_metadata.get("owner_pgid")
    owner_tty = owner_metadata.get("owner_tty")
    owner_create_time = owner_metadata.get("owner_create_time")
    # Fencing: a (re-)grant issues a monotonically increasing epoch so a stale
    # holder's token can be rejected at any future enforcement point. A new
    # owner taking over the same session id strictly bumps the prior epoch.
    next_epoch = (int(prior.get("fencing_epoch") or 0) + 1) if prior else 1

    conn.execute(
        """
        INSERT INTO session_leases (
            session_id, owner_pid, owner_ppid, owner_pgid, owner_tty,
            owner_create_time, repo_dir, repo_lock_mode, write_scopes,
            fencing_epoch, started_at, last_heartbeat_at, shutdown_at, status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
        ON CONFLICT(session_id) DO UPDATE SET
            owner_pid = COALESCE(excluded.owner_pid, session_leases.owner_pid),
            owner_ppid = COALESCE(excluded.owner_ppid, session_leases.owner_ppid),
            owner_pgid = COALESCE(excluded.owner_pgid, session_leases.owner_pgid),
            owner_tty = COALESCE(excluded.owner_tty, session_leases.owner_tty),
            owner_create_time = excluded.owner_create_time,
            repo_dir = COALESCE(excluded.repo_dir, session_leases.repo_dir),
            repo_lock_mode = excluded.repo_lock_mode,
            write_scopes = excluded.write_scopes,
            fencing_epoch = excluded.fencing_epoch,
            last_heartbeat_at = excluded.last_heartbeat_at,
            shutdown_at = NULL,
            status = excluded.status
        """,
        (
            session_id,
            owner_pid,
            owner_ppid,
            owner_pgid,
            owner_tty,
            owner_create_time,
            resolved_repo,
            resolved_mode,
            _encode_write_scopes(resolved_scopes),
            next_epoch,
            now,
            now,
            status,
        ),
    )
    if commit:
        conn.commit()


def heartbeat_session_lease(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    owner_pid: int | None = None,
    repo_dir: str | None = None,
    repo_lock_mode: str | None = None,
    write_scopes: list[str] | tuple[str, ...] | None = None,
) -> bool:
    """Refresh an existing session lease or create it when owner_pid is given."""
    if repo_lock_mode is not None and repo_lock_mode not in SESSION_REPO_LOCK_MODES:
        raise ValueError(f"Invalid repo lock mode: {repo_lock_mode}")
    lease = get_session_lease(conn, session_id)
    if lease is None:
        if owner_pid is None:
            return False
        upsert_session_lease(
            conn,
            session_id,
            owner_pid=owner_pid,
            repo_dir=repo_dir,
            repo_lock_mode=repo_lock_mode,
            write_scopes=write_scopes,
        )
        return True

    now = _now_iso()
    pid_to_use = owner_pid if owner_pid is not None else lease.get("owner_pid")
    inspect = _inspect_process(pid_to_use)
    owner_ppid = inspect.get("ppid") if inspect else lease.get("owner_ppid")
    owner_pgid = inspect.get("pgid") if inspect else lease.get("owner_pgid")
    owner_tty = inspect.get("tty") if inspect else lease.get("owner_tty")
    owner_create_time = _pid_create_time(pid_to_use) or lease.get(
        "owner_create_time"
    )
    resolved_repo = _resolve_repo_dir(repo_dir) or lease.get("repo_dir")
    resolved_mode = repo_lock_mode or lease.get("repo_lock_mode", "cooperative")
    resolved_scopes = (
        _resolve_write_scopes(resolved_repo, write_scopes)
        if write_scopes is not None
        else lease.get("write_scopes", [])
    )
    cursor = conn.execute(
        """
        UPDATE session_leases
        SET owner_pid = COALESCE(?, owner_pid),
            owner_ppid = ?,
            owner_pgid = ?,
            owner_tty = ?,
            owner_create_time = ?,
            repo_dir = COALESCE(?, repo_dir),
            repo_lock_mode = ?,
            write_scopes = ?,
            last_heartbeat_at = ?,
            shutdown_at = NULL,
            status = 'ACTIVE'
        WHERE session_id = ?
        """,
        (
            pid_to_use,
            owner_ppid,
            owner_pgid,
            owner_tty,
            owner_create_time,
            resolved_repo,
            resolved_mode,
            _encode_write_scopes(resolved_scopes),
            now,
            session_id,
        ),
    )
    conn.commit()
    return cursor.rowcount > 0


def _close_session_lease_row(conn: sqlite3.Connection, session_id: str) -> bool:
    now = _now_iso()
    cursor = conn.execute(
        """
        UPDATE session_leases
        SET shutdown_at = ?,
            last_heartbeat_at = ?,
            status = 'CLOSED'
        WHERE session_id = ?
        """,
        (now, now, session_id),
    )
    return cursor.rowcount > 0


def close_session_lease(conn: sqlite3.Connection, session_id: str) -> bool:
    """Mark a session lease as closed."""
    closed = _close_session_lease_row(conn, session_id)
    conn.commit()
    return closed


def _exclusive_grant_conflict(
    conn: sqlite3.Connection,
    session_id: str,
    resolved_repo: str | None,
) -> str | None:
    """Return a DENY reason if a live holder blocks an exclusive grant."""
    if not resolved_repo:
        return None
    for lease in list_active_session_leases(conn):
        if lease["session_id"] == session_id:
            continue
        held = lease.get("repo_dir")
        if not held:
            continue
        lease_mode = lease.get("repo_lock_mode", "cooperative")
        owner_pid = lease.get("owner_pid")
        heartbeat_age = _age_seconds(lease.get("last_heartbeat_at"))
        owner_missing = owner_pid is None
        owner_dead = owner_pid is not None and not _lease_owner_alive(lease)
        ttl_expired = (
            heartbeat_age is not None and heartbeat_age > DEFAULT_STALE_SECONDS
        )
        if owner_dead or (owner_missing and ttl_expired):
            _close_session_lease_row(conn, lease["session_id"])
            continue
        if lease_mode != "exclusive" and ttl_expired:
            continue
        if lease_mode == "exclusive":
            if not repo_dirs_overlap(resolved_repo, held):
                continue
            return (
                f"repo {resolved_repo} locked by exclusive session "
                f"{lease['session_id']}"
            )
        if held != resolved_repo:
            continue
        return (
            f"exclusive repo lock blocked by active session {lease['session_id']}"
        )
    return None


def grant_exclusive_lease(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    owner_metadata: dict[str, Any],
    repo_dir: str | None = None,
    write_scopes: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Atomically re-check exclusivity and grant. Fail-closed on lock.

    Owner metadata must already be gathered — ``ps`` stays outside the
    transaction. On ``sqlite3.OperationalError`` after ``busy_timeout``, DENY.
    """
    resolved_repo = _resolve_repo_dir(repo_dir)
    resolved_scopes = (
        _resolve_write_scopes(resolved_repo, write_scopes)
        if write_scopes is not None
        else None
    )
    try:
        if conn.in_transaction:
            conn.commit()
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        return {"allowed": False, "reason": f"registry locked: {exc}"}
    try:
        if not resolved_repo:
            prior = get_session_lease(conn, session_id)
            if prior:
                resolved_repo = prior.get("repo_dir")
        conflict = _exclusive_grant_conflict(conn, session_id, resolved_repo)
        if conflict is not None:
            conn.rollback()
            return {"allowed": False, "reason": conflict}
        upsert_session_lease(
            conn,
            session_id,
            owner_pid=owner_metadata.get("owner_pid"),
            repo_dir=resolved_repo,
            repo_lock_mode="exclusive",
            write_scopes=resolved_scopes,
            owner_metadata=owner_metadata,
            commit=False,
        )
        conn.commit()
        return {"allowed": True, "reason": "granted"}
    except sqlite3.OperationalError as exc:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        return {"allowed": False, "reason": f"registry locked: {exc}"}
    except sqlite3.IntegrityError:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        held = resolved_repo or repo_dir or ""
        return {
            "allowed": False,
            "reason": f"repo {held} exclusive lock already held",
        }
    except Exception:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        raise


def get_session_lease(conn: sqlite3.Connection, session_id: str) -> dict[str, Any] | None:
    """Return one session lease by id, if present."""
    row = conn.execute(
        """
        SELECT session_id, owner_pid, owner_ppid, owner_pgid, owner_tty,
               owner_create_time, repo_dir, repo_lock_mode, write_scopes,
               fencing_epoch, started_at, last_heartbeat_at, shutdown_at, status
        FROM session_leases
        WHERE session_id = ?
        """,
        (session_id,),
    ).fetchone()
    if not row:
        return None
    return _session_lease_row_to_dict(row)


def list_session_leases(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return all session leases ordered by creation time."""
    rows = conn.execute(
        """
        SELECT session_id, owner_pid, owner_ppid, owner_pgid, owner_tty,
               owner_create_time, repo_dir, repo_lock_mode, write_scopes,
               fencing_epoch, started_at, last_heartbeat_at, shutdown_at, status
        FROM session_leases
        ORDER BY started_at ASC, session_id ASC
        """
    ).fetchall()
    return [_session_lease_row_to_dict(row) for row in rows]


def list_active_session_leases(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return only active, non-shutdown session leases."""
    rows = conn.execute(
        """
        SELECT session_id, owner_pid, owner_ppid, owner_pgid, owner_tty,
               owner_create_time, repo_dir, repo_lock_mode, write_scopes,
               fencing_epoch, started_at, last_heartbeat_at, shutdown_at, status
        FROM session_leases
        WHERE status = 'ACTIVE' AND shutdown_at IS NULL
        ORDER BY last_heartbeat_at DESC
        """
    ).fetchall()
    return [_session_lease_row_to_dict(row) for row in rows]


def get_session_lease_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Return compact live/history counts without materializing closed leases."""
    active = int(
        conn.execute(
            "SELECT COUNT(*) FROM session_leases "
            "WHERE status = 'ACTIVE' AND shutdown_at IS NULL"
        ).fetchone()[0]
    )
    total = int(conn.execute("SELECT COUNT(*) FROM session_leases").fetchone()[0])
    return {"active": active, "closed": total - active, "total": total}


def register_process(
    conn: sqlite3.Connection,
    pid: int,
    name: str,
    workstream: str,
    session_id: str | None = None,
    port: int | None = None,
    gpu_mb: int = 0,
    repo_dir: str | None = None,
    model: str | None = None,
    priority: int = 3,
    restart_policy: str = "ALERT_ONLY",
    start_cmd: str | None = None,
    expected_duration_min: int | None = None,
    manage_session_lease: bool = True,
) -> None:
    """Register a process row and update related budget/session state."""
    if restart_policy not in RESTART_POLICIES:
        raise ValueError(f"Invalid restart policy: {restart_policy}")
    if not 1 <= priority <= 5:
        raise ValueError(f"Priority must be 1-5, got {priority}")

    now = _now_iso()
    sid = session_id or f"cli-{pid}"

    # Resolve repo_dir to absolute path
    resolved_repo = _resolve_repo_dir(repo_dir)

    conn.execute(
        """INSERT INTO processes
           (pid, session_id, workstream, name, priority, port, gpu_mb, repo_dir,
            model, restart_policy, start_cmd, start_time, last_heartbeat,
            expected_duration_min, start_create_time)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (pid, sid, workstream, name, priority, port, gpu_mb, resolved_repo,
         model, restart_policy, start_cmd, now, now, expected_duration_min,
         # Kernel create-time of the process being registered, captured at
         # registration. Its ONLY use is equality against a later read, which
         # positively proves whether the integer PID still names the same
         # process — see ``_owner_still_alive``.
         _pid_create_time(pid)),
    )
    # Update GPU budget
    if gpu_mb > 0:
        conn.execute(
            "UPDATE gpu_budget SET allocated_mb = allocated_mb + ? WHERE id = 1",
            (gpu_mb,),
        )
    if manage_session_lease and get_session_lease(conn, sid) is None:
        inspect = _inspect_process(pid) or {}
        conn.execute(
            """
            INSERT INTO session_leases (
                session_id, owner_pid, owner_ppid, owner_pgid, owner_tty,
                owner_create_time, repo_dir,
                started_at, last_heartbeat_at, shutdown_at, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'ACTIVE')
            """,
            (
                sid,
                pid,
                inspect.get("ppid"),
                inspect.get("pgid"),
                inspect.get("tty"),
                _pid_create_time(pid),
                resolved_repo,
                now,
                now,
            ),
        )
    conn.commit()


def release_process(conn: sqlite3.Connection, pid: int) -> dict[str, Any] | None:
    """Release a registered process and decrement its GPU budget claim."""
    row = conn.execute(
        "SELECT pid, session_id, name, workstream, gpu_mb FROM processes WHERE pid = ?", (pid,)
    ).fetchone()
    if not row:
        return None

    gpu_mb = row[4] or 0
    conn.execute("DELETE FROM processes WHERE pid = ?", (pid,))
    if gpu_mb > 0:
        conn.execute(
            "UPDATE gpu_budget SET allocated_mb = MAX(0, allocated_mb - ?) WHERE id = 1",
            (gpu_mb,),
        )
    session_id = row[1]
    if session_id == f"cli-{pid}":
        remaining = conn.execute(
            "SELECT COUNT(*) FROM processes WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]
        external_remaining = conn.execute(
            "SELECT COUNT(*) FROM external_resources WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]
        if remaining == 0 and external_remaining == 0:
            now = _now_iso()
            conn.execute(
                """
                UPDATE session_leases
                SET shutdown_at = COALESCE(shutdown_at, ?),
                    last_heartbeat_at = ?,
                    status = 'CLOSED'
                WHERE session_id = ?
                """,
                (now, now, session_id),
            )
    conn.commit()
    return {"pid": row[0], "name": row[2], "workstream": row[3], "gpu_mb": gpu_mb}


def release_port(conn: sqlite3.Connection, port: int) -> dict[str, Any] | None:
    """Release the process currently holding a given port, if any."""
    row = conn.execute(
        "SELECT pid, name, workstream, gpu_mb FROM processes WHERE port = ?", (port,)
    ).fetchone()
    if not row:
        return None
    return release_process(conn, row[0])


def heartbeat(conn: sqlite3.Connection, pid: int) -> bool:
    """Refresh a process heartbeat and its owned lease when applicable."""
    now = _now_iso()
    row = conn.execute(
        "SELECT session_id, repo_dir FROM processes WHERE pid = ?",
        (pid,),
    ).fetchone()
    if row is None:
        return False
    cursor = conn.execute(
        "UPDATE processes SET last_heartbeat = ? WHERE pid = ?", (now, pid)
    )
    lease = get_session_lease(conn, row[0])
    if lease is not None and lease.get("owner_pid") == pid:
        inspect = _inspect_process(pid)
        conn.execute(
            """
            UPDATE session_leases
            SET owner_ppid = ?,
                owner_pgid = ?,
                owner_tty = ?,
                repo_dir = COALESCE(?, repo_dir),
                last_heartbeat_at = ?,
                shutdown_at = NULL,
                status = 'ACTIVE'
            WHERE session_id = ?
            """,
            (
                (inspect or {}).get("ppid"),
                (inspect or {}).get("pgid"),
                (inspect or {}).get("tty"),
                row[1],
                now,
                row[0],
            ),
        )
    conn.commit()
    return cursor.rowcount > 0


def get_process(conn: sqlite3.Connection, pid: int) -> dict[str, Any] | None:
    """Return one registered process by pid."""
    row = conn.execute("SELECT * FROM processes WHERE pid = ?", (pid,)).fetchone()
    if not row:
        return None
    return _row_to_dict(row, conn)


def get_all_processes(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return all registered processes ordered by priority and age."""
    rows = conn.execute("SELECT * FROM processes ORDER BY priority DESC, start_time ASC").fetchall()
    return [_row_to_dict(r, conn) for r in rows]


def get_process_by_port(conn: sqlite3.Connection, port: int) -> dict[str, Any] | None:
    """Return the registered process claiming a port, if any."""
    row = conn.execute("SELECT * FROM processes WHERE port = ?", (port,)).fetchone()
    if not row:
        return None
    return _row_to_dict(row, conn)


def get_process_by_repo(conn: sqlite3.Connection, repo_dir: str) -> dict[str, Any] | None:
    """Return the registered process locking a repo path, if any."""
    resolved = str(Path(repo_dir).resolve())
    row = conn.execute("SELECT * FROM processes WHERE repo_dir = ?", (resolved,)).fetchone()
    if not row:
        return None
    return _row_to_dict(row, conn)


def get_gpu_budget(conn: sqlite3.Connection) -> dict[str, int]:
    """Return the current total, reserve, allocated, and available GPU budget."""
    row = conn.execute("SELECT total_mb, reserve_mb, allocated_mb FROM gpu_budget WHERE id = 1").fetchone()
    total, reserve, allocated = row
    return {
        "total_mb": total,
        "reserve_mb": reserve,
        "allocated_mb": allocated,
        "available_mb": total - reserve - allocated,
    }


def get_stale_processes(
    conn: sqlite3.Connection,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> list[dict[str, Any]]:
    """Return processes whose heartbeat age exceeds the stale threshold."""
    return [
        proc
        for proc in get_process_classifications(conn, stale_seconds=stale_seconds)
        if (proc.get("heartbeat_age_seconds") or 0) > stale_seconds
    ]


def get_reapable_processes(
    conn: sqlite3.Connection,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> list[dict[str, Any]]:
    """Return processes classified as safe to reap."""
    return [
        proc
        for proc in get_process_classifications(conn, stale_seconds=stale_seconds)
        if proc["classification"] == "orphan_confirmed"
    ]


def clean_dead_pids(
    conn: sqlite3.Connection,
    exclude_pids: set[int] | None = None,
) -> list[dict[str, Any]]:
    """Remove entries for PIDs that no longer exist.

    *exclude_pids*, when provided, are skipped — they were just confirmed
    alive by the discovery scan and should not be reaped.
    """
    rows = conn.execute("SELECT pid, name, workstream, gpu_mb FROM processes").fetchall()
    skip = exclude_pids or set()
    cleaned = []
    for pid, name, ws, gpu_mb in rows:
        if pid in skip:
            continue
        if not _pid_exists(pid):
            release_process(conn, pid)
            cleaned.append({"pid": pid, "name": name, "workstream": ws})
    return cleaned


def clean_stale_session_leases(
    conn: sqlite3.Connection,
    limit: int = DEFAULT_SESSION_LEASE_CLEANUP_LIMIT,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> list[dict[str, Any]]:
    """Close ACTIVE session leases that are stale by EITHER sufficient trigger.

    Path C (DECOUPLE): a lease is reapable when the owner is PROVEN DEAD
    (``_lease_owner_alive`` False — PID gone or create-time mismatch from PID
    reuse) OR when an ownerless/dead lease's heartbeat has exceeded the TTL. A
    proven-dead owner is reaped IMMEDIATELY, independent of heartbeat age, so a
    dead session never holds a repo for up to ``stale_seconds``. A null-PID
    lease with a fresh heartbeat is conservatively left in place (TTL arm only).
    """
    if limit <= 0:
        return []

    cleaned: list[dict[str, Any]] = []
    for lease in list_session_leases(conn):
        if len(cleaned) >= limit:
            break
        if lease["status"] != "ACTIVE" or lease.get("shutdown_at") is not None:
            continue
        owner_pid = lease.get("owner_pid")
        heartbeat_age = _age_seconds(lease.get("last_heartbeat_at"))

        if owner_pid is not None and not _lease_owner_alive(lease):
            # Proven death (PID gone or recycled) — independent sufficient
            # trigger, no TTL wait.
            reason = "dead_session_owner"
        elif heartbeat_age is not None and heartbeat_age > stale_seconds and (
            owner_pid is None or not _lease_owner_alive(lease)
        ):
            # TTL arm: ownerless/dead lease whose heartbeat has expired.
            reason = "stale_dead_session_lease"
        else:
            continue

        try:
            closed = close_session_lease(conn, lease["session_id"])
        except (sqlite3.Error, OSError):
            continue
        if not closed:
            continue
        cleaned.append(
            {
                "reason": reason,
                "session_id": lease["session_id"],
                "owner_pid": owner_pid,
                "repo_dir": lease.get("repo_dir"),
                "repo_lock_mode": lease.get("repo_lock_mode", "cooperative"),
                "heartbeat_age_seconds": heartbeat_age,
            }
        )
    return cleaned


def get_claimed_ports(conn: sqlite3.Connection) -> dict[int, int]:
    """Return {port: pid} for all claimed ports."""
    rows = conn.execute("SELECT port, pid FROM processes WHERE port IS NOT NULL").fetchall()
    return {port: pid for port, pid in rows}


def get_locked_repos(conn: sqlite3.Connection) -> dict[str, int]:
    """Return {repo_dir: pid} for all locked repos."""
    rows = conn.execute("SELECT repo_dir, pid FROM processes WHERE repo_dir IS NOT NULL").fetchall()
    return {repo: pid for repo, pid in rows}


def _session_lease_blocks_repo(
    lease: dict[str, Any],
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> bool:
    if lease.get("status") != "ACTIVE" or lease.get("shutdown_at") is not None:
        return False

    owner_pid = lease.get("owner_pid")
    if owner_pid is not None:
        # Proven death (PID gone or recycled) never blocks — Path C decouple.
        if not _lease_owner_alive(lease):
            return False
        return True

    heartbeat_age = _age_seconds(lease.get("last_heartbeat_at"))
    return heartbeat_age is not None and heartbeat_age <= stale_seconds


def get_active_session_leases_by_repo(
    conn: sqlite3.Connection,
    repo_dir: str,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> list[dict[str, Any]]:
    """Return active session leases associated with a repo path."""
    resolved = str(Path(repo_dir).resolve())
    rows = conn.execute(
        """
        SELECT session_id, owner_pid, owner_ppid, owner_pgid, owner_tty,
               owner_create_time, repo_dir, repo_lock_mode, write_scopes,
               fencing_epoch, started_at, last_heartbeat_at, shutdown_at, status
        FROM session_leases
        WHERE repo_dir = ? AND status = 'ACTIVE' AND shutdown_at IS NULL
        ORDER BY last_heartbeat_at DESC
        """,
        (resolved,),
    ).fetchall()
    return [_session_lease_row_to_dict(row) for row in rows]


def get_effective_locked_repos(
    conn: sqlite3.Connection,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> dict[str, int | None]:
    """Return repo locks from processes plus active blocking session leases."""
    locks = get_locked_repos(conn)
    for lease in list_session_leases(conn):
        repo_dir = lease.get("repo_dir")
        if not repo_dir or repo_dir in locks:
            continue
        if lease.get("repo_lock_mode") == "exclusive" and _session_lease_blocks_repo(lease, stale_seconds=stale_seconds):
            locks[repo_dir] = lease.get("owner_pid")
    return locks


def get_process_classifications(
    conn: sqlite3.Connection,
    stale_seconds: int = DEFAULT_STALE_SECONDS,
) -> list[dict[str, Any]]:
    """Classify each registered process by liveness and ownership evidence."""
    results: list[dict[str, Any]] = []
    for proc in get_all_processes(conn):
        process_alive = _pid_exists(proc["pid"])
        heartbeat_age = _age_seconds(proc.get("last_heartbeat"))
        stale = heartbeat_age is not None and heartbeat_age > stale_seconds
        lease = get_session_lease(conn, proc["session_id"])
        lease_present = lease is not None
        lease_active = bool(lease and lease["status"] == "ACTIVE" and lease["shutdown_at"] is None)
        owner_pid = lease.get("owner_pid") if lease else None
        owner_alive = _pid_exists(owner_pid) if owner_pid else None
        process_info = _inspect_process(proc["pid"]) if process_alive else None
        parent_chain_detached = (
            _is_parent_chain_detached(proc["pid"])
            if process_alive else True
        )
        evidence: list[str] = []

        if not process_alive:
            classification = "exited"
            evidence.append("registered PID is no longer running")
        elif lease_active and owner_alive and not stale:
            classification = "live"
            evidence.append(f"active session lease owner PID {owner_pid} is alive")
        elif lease_present and not lease_active and stale and parent_chain_detached is True:
            classification = "orphan_confirmed"
            evidence.append("process heartbeat expired")
            evidence.append("session lease is closed or owner is gone")
            evidence.append("parent chain is detached")
        elif stale:
            if lease_present:
                classification = "stale_candidate"
                evidence.append("process heartbeat expired")
                evidence.append(f"session lease status={lease['status']}")
                if owner_pid:
                    evidence.append(
                        "session owner alive"
                        if owner_alive else "session owner missing"
                    )
                if parent_chain_detached is None:
                    evidence.append("parent chain inspection unavailable")
                elif parent_chain_detached:
                    evidence.append("parent chain detached")
                else:
                    evidence.append("parent chain still attached")
            else:
                classification = "disconnected"
                evidence.append("process heartbeat expired")
                evidence.append("session lease missing")
                if parent_chain_detached is None:
                    evidence.append("parent chain inspection unavailable")
                elif parent_chain_detached:
                    evidence.append("parent chain detached")
                else:
                    evidence.append("parent chain still attached")
        else:
            classification = "disconnected"
            if not lease_present:
                evidence.append("session lease missing")
            elif not lease_active:
                evidence.append(f"session lease closed ({lease['status']})")
            elif owner_pid and not owner_alive:
                evidence.append(f"session owner PID {owner_pid} is not running")
            else:
                evidence.append("ownership evidence incomplete")

        item = dict(proc)
        item.update({
            "classification": classification,
            "heartbeat_age_seconds": heartbeat_age,
            "stale_seconds": heartbeat_age if stale else 0,
            "process_alive": process_alive,
            "session_lease_present": lease_present,
            "session_lease_status": lease["status"] if lease else "MISSING",
            "session_lease_owner_pid": owner_pid,
            "session_lease_owner_alive": owner_alive,
            "session_lease_last_heartbeat_age_seconds": (
                _age_seconds(lease.get("last_heartbeat_at")) if lease else None
            ),
            "session_lease_shutdown_at": lease.get("shutdown_at") if lease else None,
            "parent_pid": process_info.get("ppid") if process_info else None,
            "process_group_id": process_info.get("pgid") if process_info else None,
            "tty": process_info.get("tty") if process_info else None,
            "parent_chain_detached": parent_chain_detached,
            "safe_to_reap": classification == "orphan_confirmed",
            "evidence": evidence,
        })
        results.append(item)
    return results


def register_external_resource(
    conn: sqlite3.Connection,
    *,
    provider: str,
    resource_type: str,
    external_id: str,
    session_id: str | None = None,
    workstream: str,
    name: str,
    priority: int = 3,
    gpu_mb: int = 0,
    repo_dir: str | None = None,
    model: str | None = None,
    status: str = "ACTIVE",
    started_by: str | None = None,
    owner_tool: str | None = None,
    endpoint: str | None = None,
    cleanup_cmd: str | None = None,
    safe_to_delete: bool = False,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Register or refresh an external non-local resource row."""
    if not provider:
        raise ValueError("provider is required")
    if not resource_type:
        raise ValueError("resource_type is required")
    if not external_id:
        raise ValueError("external_id is required")
    if not 1 <= priority <= 5:
        raise ValueError(f"Priority must be 1-5, got {priority}")

    now = _now_iso()
    sid = session_id or f"{provider}-{external_id}"
    resolved_repo = _resolve_repo_dir(repo_dir)
    metadata_json = json.dumps(metadata or {}, separators=(",", ":"))

    conn.execute(
        """INSERT OR REPLACE INTO external_resources
           (provider, resource_type, external_id, session_id, workstream, name,
            priority, gpu_mb, repo_dir, model, status, started_by, owner_tool,
            endpoint, cleanup_cmd, safe_to_delete, metadata, start_time, last_seen)
           VALUES (
             ?, ?, ?, ?, ?, ?,
             ?, ?, ?, ?, ?, ?, ?,
             ?, ?, ?, ?,
             COALESCE(
               (SELECT start_time FROM external_resources WHERE provider = ? AND external_id = ?),
               ?
             ),
             ?
           )""",
        (
            provider,
            resource_type,
            external_id,
            sid,
            workstream,
            name,
            priority,
            gpu_mb,
            resolved_repo,
            model,
            status,
            started_by,
            owner_tool,
            endpoint,
            cleanup_cmd,
            1 if safe_to_delete else 0,
            metadata_json,
            provider,
            external_id,
            now,
            now,
        ),
    )
    lease = get_session_lease(conn, sid)
    if lease is None:
        conn.execute(
            """
            INSERT INTO session_leases (
                session_id, owner_pid, owner_ppid, owner_pgid, owner_tty, repo_dir,
                started_at, last_heartbeat_at, shutdown_at, status
            )
            VALUES (?, NULL, NULL, NULL, NULL, ?, ?, ?, NULL, 'ACTIVE')
            """,
            (sid, resolved_repo, now, now),
        )
        conn.commit()
        return

    conn.execute(
        """
        UPDATE session_leases
        SET repo_dir = COALESCE(?, repo_dir),
            last_heartbeat_at = ?,
            shutdown_at = NULL,
            status = 'ACTIVE'
        WHERE session_id = ?
        """,
        (resolved_repo, now, sid),
    )
    conn.commit()


def _validate_external_resource_update_columns(
    columns: list[str] | tuple[str, ...],
) -> None:
    """Reject any column outside ``_EXTERNAL_RESOURCE_UPDATABLE_COLUMNS``.

    ``heartbeat_external_resource`` builds its UPDATE SET clause from a dynamic
    column list, so the assembled statement is guarded here: every column name
    must be one of this module's own whitelisted constants, or the update is
    refused before the SQL is ever built. Values always travel as ``?``
    placeholders; only these validated internal names are interpolated.
    """
    unknown = sorted(set(columns) - _EXTERNAL_RESOURCE_UPDATABLE_COLUMNS)
    if unknown:
        raise ValueError(
            f"columns not in _EXTERNAL_RESOURCE_UPDATABLE_COLUMNS: {unknown}"
        )


def heartbeat_external_resource(
    conn: sqlite3.Connection,
    *,
    provider: str,
    external_id: str,
    status: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> bool:
    """Refresh last-seen state for an external resource."""
    now = _now_iso()
    columns = ["last_seen"]
    params: list[Any] = [now]
    if status is not None:
        columns.append("status")
        params.append(status)
    if metadata is not None:
        columns.append("metadata")
        params.append(json.dumps(metadata, separators=(",", ":")))
    # The SET clause is derived from the validated names, never from a parallel
    # list, so an unvalidated identifier cannot reach the statement.
    _validate_external_resource_update_columns(columns)
    fields = [f"{column} = ?" for column in columns]
    params.extend([provider, external_id])
    cursor = conn.execute(
        f"UPDATE external_resources SET {', '.join(fields)} WHERE provider = ? AND external_id = ?",  # nosec B608 - column names validated against _EXTERNAL_RESOURCE_UPDATABLE_COLUMNS
        params,
    )
    conn.commit()
    return cursor.rowcount > 0


def release_external_resource(
    conn: sqlite3.Connection,
    *,
    provider: str,
    external_id: str,
) -> dict[str, Any] | None:
    """Release an external resource and close its synthetic lease if needed."""
    row = conn.execute(
        """SELECT provider, resource_type, external_id, session_id, workstream, name,
                  priority, gpu_mb, repo_dir, model, status, started_by, owner_tool,
                  endpoint, cleanup_cmd, safe_to_delete, metadata, start_time, last_seen
           FROM external_resources
           WHERE provider = ? AND external_id = ?""",
        (provider, external_id),
    ).fetchone()
    if not row:
        return None
    conn.execute(
        "DELETE FROM external_resources WHERE provider = ? AND external_id = ?",
        (provider, external_id),
    )
    session_id = row[3]
    if session_id == f"{provider}-{external_id}":
        remaining = conn.execute(
            "SELECT COUNT(*) FROM external_resources WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]
        proc_remaining = conn.execute(
            "SELECT COUNT(*) FROM processes WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]
        if remaining == 0 and proc_remaining == 0:
            now = _now_iso()
            conn.execute(
                """
                UPDATE session_leases
                SET shutdown_at = COALESCE(shutdown_at, ?),
                    last_heartbeat_at = ?,
                    status = 'CLOSED'
                WHERE session_id = ?
                """,
                (now, now, session_id),
            )
    conn.commit()
    return _external_row_to_dict(row)


def get_external_resource(
    conn: sqlite3.Connection,
    *,
    provider: str,
    external_id: str,
) -> dict[str, Any] | None:
    """Return one external resource by provider/id."""
    row = conn.execute(
        """SELECT provider, resource_type, external_id, session_id, workstream, name,
                  priority, gpu_mb, repo_dir, model, status, started_by, owner_tool,
                  endpoint, cleanup_cmd, safe_to_delete, metadata, start_time, last_seen
           FROM external_resources
           WHERE provider = ? AND external_id = ?""",
        (provider, external_id),
    ).fetchone()
    if not row:
        return None
    return _external_row_to_dict(row)


def get_all_external_resources(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return all tracked external resources ordered by priority and age."""
    rows = conn.execute(
        """SELECT provider, resource_type, external_id, session_id, workstream, name,
                  priority, gpu_mb, repo_dir, model, status, started_by, owner_tool,
                  endpoint, cleanup_cmd, safe_to_delete, metadata, start_time, last_seen
           FROM external_resources
           ORDER BY priority DESC, start_time ASC"""
    ).fetchall()
    return [_external_row_to_dict(r) for r in rows]


def get_external_resources_by_repo(
    conn: sqlite3.Connection,
    repo_dir: str,
) -> list[dict[str, Any]]:
    """Return all external resources associated with a repo path."""
    resolved = str(Path(repo_dir).resolve())
    rows = conn.execute(
        """SELECT provider, resource_type, external_id, session_id, workstream, name,
                  priority, gpu_mb, repo_dir, model, status, started_by, owner_tool,
                  endpoint, cleanup_cmd, safe_to_delete, metadata, start_time, last_seen
           FROM external_resources
           WHERE repo_dir = ?
           ORDER BY priority DESC, start_time ASC""",
        (resolved,),
    ).fetchall()
    return [_external_row_to_dict(r) for r in rows]


def replace_provider_resources(
    conn: sqlite3.Connection,
    *,
    provider: str,
    resources: list[dict[str, Any]],
) -> None:
    """Replace one provider's discovered resource set with the latest snapshot."""
    existing = {
        item["external_id"]: item
        for item in get_all_external_resources(conn)
        if item["provider"] == provider
    }
    seen_ids = {item["external_id"] for item in resources}
    for resource in resources:
        prior = existing.get(resource["external_id"])
        register_external_resource(
            conn,
            provider=provider,
            resource_type=resource["resource_type"],
            external_id=resource["external_id"],
            session_id=(prior["session_id"] if prior else None),
            workstream=(prior["workstream"] if prior else resource.get("workstream", provider)),
            name=(prior["name"] if prior else resource["name"]),
            priority=(prior["priority"] if prior else resource.get("priority", 3)),
            gpu_mb=resource.get("gpu_mb", prior["gpu_mb"] if prior else 0),
            repo_dir=(prior["repo_dir"] if prior else resource.get("repo_dir")),
            model=(prior["model"] if prior else resource.get("model")),
            status=resource.get("status", "ACTIVE"),
            started_by=(prior["started_by"] if prior else resource.get("started_by")),
            owner_tool=(prior["owner_tool"] if prior else resource.get("owner_tool")),
            endpoint=(prior["endpoint"] if prior else resource.get("endpoint")),
            cleanup_cmd=(prior["cleanup_cmd"] if prior else resource.get("cleanup_cmd")),
            safe_to_delete=(prior["safe_to_delete"] if prior else resource.get("safe_to_delete", False)),
            metadata={
                **(prior["metadata"] if prior else {}),
                **resource.get("metadata", {}),
            },
        )
    for external_id in existing:
        if external_id not in seen_ids:
            release_external_resource(conn, provider=provider, external_id=external_id)


def _row_to_dict(row: tuple, conn: sqlite3.Connection) -> dict[str, Any]:
    cols = [
        "pid", "session_id", "workstream", "name", "priority",
        "port", "gpu_mb", "repo_dir", "model", "restart_policy",
        "start_cmd", "start_time", "last_heartbeat", "expected_duration_min",
        # Appended by _ensure_column migration, so it is the last SELECT * value.
        # zip() silently truncates to the shorter side: omitting this name did
        # not error, it just made the create-time evidence unreadable.
        "start_create_time",
    ]
    return dict(zip(cols, row))


def _session_lease_row_to_dict(row: tuple) -> dict[str, Any]:
    cols = [
        "session_id",
        "owner_pid",
        "owner_ppid",
        "owner_pgid",
        "owner_tty",
        "owner_create_time",
        "repo_dir",
        "repo_lock_mode",
        "write_scopes",
        "fencing_epoch",
        "started_at",
        "last_heartbeat_at",
        "shutdown_at",
        "status",
    ]
    data = dict(zip(cols, row))
    data["repo_lock_mode"] = data.get("repo_lock_mode") or "cooperative"
    data["write_scopes"] = _decode_write_scopes(data.get("write_scopes"))
    data["fencing_epoch"] = int(data.get("fencing_epoch") or 1)
    return data


def _external_row_to_dict(row: tuple) -> dict[str, Any]:
    cols = [
        "provider",
        "resource_type",
        "external_id",
        "session_id",
        "workstream",
        "name",
        "priority",
        "gpu_mb",
        "repo_dir",
        "model",
        "status",
        "started_by",
        "owner_tool",
        "endpoint",
        "cleanup_cmd",
        "safe_to_delete",
        "metadata",
        "start_time",
        "last_seen",
    ]
    data = dict(zip(cols, row))
    data["safe_to_delete"] = bool(data["safe_to_delete"])
    data["metadata"] = json.loads(data["metadata"]) if data["metadata"] else {}
    return data
