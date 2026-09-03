"""Adapter for the Claude single-writer hook's twin lease file.

A Fleet Watch session lease has a TWIN outside this package: the Claude
single-writer hook (``~/.claude/hooks/single_writer_guard.py``) writes a small
JSON record per claimed repo into ``~/.governance/state/claude-session-leases/``
and ``~/.claude/hooks/fleet_guard_hook.py`` keeps blocking while that file
exists and its ``owner_pid`` is alive.

Closing the registry lease used to leave that twin behind, so a session that had
legitimately released a repo stayed blocked and the operator had to ``rm`` the
file by hand — unaudited deletion inside a governance state directory. This
module removes exactly one twin, on an ALREADY-AUTHORIZED close.

Deliberate constraints (the delete path is the risk, not the close):

* the state directory is a module CONSTANT; the only override is the
  ``state_dir`` keyword parameter, which exists for tests. No environment
  variable, so no ambient reconfiguration of a delete path;
* the filename is DERIVED exactly as ``single_writer_guard._state_path`` derives
  it — ``{owner_pid or 'nopid'}-{sha256(repo)[:12]}.json``. Nothing is globbed
  and nothing is enumerated;
* the file's own ``session_id`` must equal the closed session's id before the
  unlink. Another session's twin in the same directory is never touched;
* the resolved path's parent must be the state directory itself;
* every failure is a no-op that still reports. Cleanup can never fail a close.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

# Mirrors single_writer_guard.STATE_DIR. Constant on purpose — see module docstring.
CLAUDE_SESSION_LEASE_DIR = Path.home() / ".governance" / "state" / "claude-session-leases"


def twin_lease_path(
    repo_dir: str | Path,
    owner_pid: int | None,
    *,
    state_dir: Path | None = None,
) -> Path:
    """Return the twin file path for ``repo_dir``/``owner_pid``.

    Byte-for-byte the derivation in ``single_writer_guard._state_path``: a
    12-hex-char sha256 prefix of the repo path string, prefixed by the owner PID
    (or the literal ``nopid``). The digest is a filename derivation, not a
    security primitive — the ``session_id`` content check below is what makes
    the unlink safe.
    """
    root = state_dir if state_dir is not None else CLAUDE_SESSION_LEASE_DIR
    digest = hashlib.sha256(str(repo_dir).encode("utf-8")).hexdigest()[:12]
    return root / f"{owner_pid or 'nopid'}-{digest}.json"


def clear_twin_lease(
    session_id: str,
    repo_dir: str | None,
    owner_pid: int | None,
    *,
    state_dir: Path | None = None,
) -> dict[str, Any]:
    """Remove the twin lease file for ``session_id``, if and only if it matches.

    Call ONLY after the close has been authorized. Returns an audit record
    ``{"cleared": bool, "reason": str, "path": str | None}`` suitable for a
    ``SESSION_CLOSE`` event detail. Never raises.
    """
    root = state_dir if state_dir is not None else CLAUDE_SESSION_LEASE_DIR
    if not repo_dir:
        return {"cleared": False, "reason": "no repo_dir on lease", "path": None}

    path = twin_lease_path(repo_dir, owner_pid, state_dir=root)
    record: dict[str, Any] = {"cleared": False, "reason": "absent", "path": str(path)}

    # Containment: never act on a path that is not a direct child of the state
    # directory, whatever the repo_dir string contained.
    try:
        if path.parent.resolve() != root.resolve():
            record["reason"] = "resolved outside state dir"
            return record
    except OSError:
        record["reason"] = "state dir unresolvable"
        return record

    if not path.is_file():
        return record

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        record["reason"] = "unreadable twin file"
        return record

    if not isinstance(payload, dict) or payload.get("session_id") != session_id:
        record["reason"] = "session id mismatch"
        return record

    try:
        path.unlink()
    except OSError as exc:
        record["reason"] = f"unlink failed: {exc}"
        return record

    record["cleared"] = True
    record["reason"] = "removed"
    return record
