"""Authorization for `fleet session close`, and the Claude twin-lease cleanup.

Closing a session lease is a PRIVILEGE REVOCATION: an ACTIVE lease is what makes
`fleet guard --json` answer DENY for every other agent, so a close flips that to
ALLOW. Before this module's subject existed, `close_session_lease()` had no
authorization at all — any local process holding a (publicly listed) session id
could revoke any other session's exclusive write custody.

Two regressions are pinned here:

  * OVER-OPEN (the real hazard): an unrelated live process must never close a
    live owner's lease.
  * OVER-TIGHT (the observed incident, 2026-09-02): the operator's own Terminal
    — an ANCESTOR of the session it spawned — was denied and had to hand-delete
    governance state. An owner cannot outrank the shell that created it.

Every test is hermetic: an in-memory / tmp_path DB, a fabricated process table,
and a tmp_path twin-lease directory. The live registry at ~/.fleet-watch and
the live ~/.governance state directory are never read or written.

Assertions check the PROTECTED OUTPUT — the lease `status` column and the twin
file's presence on disk — not merely the decision function's own return value.
A self-reported verdict is not evidence.
"""

import json
import os
import sqlite3

from click.testing import CliRunner

from fleet_watch import claude_lease_twin
from fleet_watch import cli as cli_module
from fleet_watch import registry

TEST_UID = 501

# Fabricated process tree shared by the lineage tests:
#
#   1 (init)
#   └── 100  terminal          <- ANCESTOR of the owner (the incident case)
#       ├── 200  owner          <- holds the lease
#       │   └── 300  mid
#       │       └── 400  descendant
#       └── 500  sibling        <- fully inspected non-relative
#   1
#   └── 600  unrelated
TREE = {
    100: 1,
    200: 100,
    300: 200,
    400: 300,
    500: 100,
    600: 1,
}


def _install_fake_process_table(monkeypatch, tree=None, uids=None, uninspectable=()):
    """Replace every kernel probe registry uses with a deterministic table."""
    tree = TREE if tree is None else tree
    uids = {} if uids is None else uids

    def pid_exists(pid):
        return pid in tree

    def create_time(pid):
        return f"ct-{pid}" if pid in tree else None

    def inspect(pid):
        if pid not in tree:
            return None
        if pid in uninspectable:
            return {
                "pid": pid,
                "alive": True,
                "inspectable": False,
                "ppid": None,
                "pgid": None,
                "tty": None,
                "error": "ps inspection failed",
            }
        return {
            "pid": pid,
            "alive": True,
            "inspectable": True,
            "ppid": tree[pid],
            "pgid": pid,
            "tty": "ttys000",
        }

    def process_uid(pid):
        if pid not in tree:
            return None
        return uids.get(pid, TEST_UID)

    monkeypatch.setattr(registry, "_pid_exists", pid_exists)
    monkeypatch.setattr(registry, "_pid_create_time", create_time)
    monkeypatch.setattr(registry, "_inspect_process", inspect)
    monkeypatch.setattr(registry, "_process_uid", process_uid)


def _fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.commit()
    return conn


def _open_lease(conn, session_id="sess-a", owner_pid=200, repo_dir="/tmp/lease-repo"):
    registry.upsert_session_lease(
        conn,
        session_id,
        owner_pid=owner_pid,
        repo_dir=repo_dir,
        repo_lock_mode="exclusive",
    )
    return registry.get_session_lease(conn, session_id)


def _status(conn, session_id="sess-a"):
    return registry.get_session_lease(conn, session_id)["status"]


# ── ALLOW: the lease's own lineage ───────────────────────────────────────────

def test_owner_closes_its_own_lease(monkeypatch):
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    _open_lease(conn)

    allowed, reason = registry.authorize_session_close(conn, "sess-a", 200)

    assert allowed is True, reason
    assert registry.close_session_lease(conn, "sess-a") is True
    assert _status(conn) == "CLOSED"


def test_descendant_of_owner_closes_lease(monkeypatch):
    """The session's own child shell running `fleet` is the common case."""
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    _open_lease(conn)

    allowed, reason = registry.authorize_session_close(conn, "sess-a", 400)

    assert allowed is True, reason
    assert "descendant" in reason


def test_ancestor_terminal_closes_lease(monkeypatch):
    """REGRESSION (2026-09-02): the Terminal that spawned the session was denied.

    The owner is a child of PID 100; the requester IS 100. Walking the requester
    upward can never reach the owner, so a descendant-only rule denies here —
    which is what forced an unaudited hand-delete of governance state.
    """
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    _open_lease(conn)

    # The descendant direction alone genuinely does not prove this relation.
    assert registry._lineage_proven(100, 200) is False

    allowed, reason = registry.authorize_session_close(conn, "sess-a", 100)

    assert allowed is True, reason
    assert "ancestor" in reason


def test_dead_owner_lease_is_closable_by_anyone(monkeypatch):
    """Reaping a dead lease is not a privilege (fleet-watch invariant 3).

    The owner PID is absent from the table => provably dead. An unrelated live
    process may close it; otherwise a dead owner holds the repo until the TTL.
    """
    conn = _fresh_conn()
    _install_fake_process_table(monkeypatch)
    _open_lease(conn)
    # Owner dies: rebuild the table without it, keeping the requester alive.
    _install_fake_process_table(monkeypatch, tree={k: v for k, v in TREE.items() if k != 200})

    allowed, reason = registry.authorize_session_close(conn, "sess-a", 600)

    assert allowed is True, reason
    assert "provably dead" in reason


def test_recycled_owner_pid_counts_as_dead(monkeypatch):
    """A create-time mismatch proves PID reuse: the original owner is gone."""
    conn = _fresh_conn()
    _install_fake_process_table(monkeypatch)
    _open_lease(conn)
    monkeypatch.setattr(registry, "_pid_create_time", lambda pid: f"recycled-{pid}")

    allowed, reason = registry.authorize_session_close(conn, "sess-a", 600)

    assert allowed is True, reason
    assert "provably dead" in reason


# ── DENY: everything that is not positive proof ──────────────────────────────

def test_unrelated_live_process_is_denied(monkeypatch):
    """THE core threat: cross-session revocation producing two writers."""
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    _open_lease(conn)

    allowed, reason = registry.authorize_session_close(conn, "sess-a", 600)

    assert allowed is False
    assert reason == "requester is not the session owner, a descendant, or an ancestor"
    assert _status(conn) == "ACTIVE"


def test_sibling_under_same_terminal_is_denied(monkeypatch):
    """Sharing an ancestor is not lineage. 500 and 200 are both children of 100."""
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    _open_lease(conn)

    allowed, reason = registry.authorize_session_close(conn, "sess-a", 500)

    assert allowed is False
    assert _status(conn) == "ACTIVE"


def test_different_uid_is_denied_despite_correct_lineage(monkeypatch):
    """Correct lineage does not survive a uid mismatch."""
    _install_fake_process_table(monkeypatch, uids={400: TEST_UID + 1})
    conn = _fresh_conn()
    _open_lease(conn)

    allowed, reason = registry.authorize_session_close(conn, "sess-a", 400)

    assert allowed is False
    assert "uid" in reason
    assert _status(conn) == "ACTIVE"


def test_unresolvable_uid_is_denied(monkeypatch):
    _install_fake_process_table(monkeypatch)
    monkeypatch.setattr(registry, "_process_uid", lambda pid: None)
    conn = _fresh_conn()
    _open_lease(conn)

    allowed, reason = registry.authorize_session_close(conn, "sess-a", 200)

    assert allowed is False
    assert "unresolvable" in reason


def test_lineage_inspection_error_is_denied(monkeypatch):
    """`ps` failing mid-walk must deny, never degrade to allow."""
    _install_fake_process_table(monkeypatch, uninspectable={300, 200})
    conn = _fresh_conn()
    _open_lease(conn)

    allowed, reason = registry.authorize_session_close(conn, "sess-a", 400)

    assert allowed is False
    assert "uninspectable" in reason
    assert _status(conn) == "ACTIVE"


def test_owner_identity_unprovable_is_denied(monkeypatch):
    """No readable create-time => identity unprovable => deny (defeats PID reuse)."""
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    _open_lease(conn)
    monkeypatch.setattr(registry, "_pid_create_time", lambda pid: None)

    allowed, reason = registry.authorize_session_close(conn, "sess-a", 200)

    assert allowed is False
    assert "owner identity is uninspectable" in reason


def test_null_owner_pid_with_fresh_heartbeat_is_denied(monkeypatch):
    """An ownerless lease's liveness belongs to the TTL arm, not to this path."""
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    registry.upsert_session_lease(conn, "sess-null", owner_pid=None, repo_dir="/tmp/r")

    allowed, reason = registry.authorize_session_close(conn, "sess-null", 200)

    assert allowed is False
    assert "no owner pid" in reason
    assert _status(conn, "sess-null") == "ACTIVE"


def test_missing_lease_is_denied(monkeypatch):
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()

    allowed, reason = registry.authorize_session_close(conn, "sess-ghost", 200)

    assert allowed is False
    assert reason == "session lease not found"


def test_unknown_requester_pid_is_denied(monkeypatch):
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    _open_lease(conn)

    assert registry.authorize_session_close(conn, "sess-a", None)[0] is False
    assert registry.authorize_session_close(conn, "sess-a", 0)[0] is False


def test_lineage_walk_is_bounded_and_cycle_safe(monkeypatch):
    """A PPID cycle or a chain deeper than the budget resolves to None, not True."""
    cyclic = {10: 11, 11: 12, 12: 10}
    _install_fake_process_table(monkeypatch, tree=cyclic)
    assert registry._lineage_proven(10, 999) is None

    deep = {i: i + 1 for i in range(1, 200)}
    deep[200] = 1
    _install_fake_process_table(monkeypatch, tree=deep)
    assert registry._lineage_proven(1, 199) is None
    assert registry.LINEAGE_MAX_HOPS == 32


# ── Twin-lease file clearing ─────────────────────────────────────────────────

def _write_twin(state_dir, repo_dir, owner_pid, session_id):
    state_dir.mkdir(parents=True, exist_ok=True)
    path = claude_lease_twin.twin_lease_path(repo_dir, owner_pid, state_dir=state_dir)
    path.write_text(
        json.dumps(
            {
                "schema_version": "cds-session-lease/v1",
                "session_id": session_id,
                "repo": str(repo_dir),
                "owner_pid": owner_pid,
                "lock_mode": "exclusive",
                "claimed_at": "2026-09-02T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    return path


def test_twin_path_matches_single_writer_guard_derivation(tmp_path):
    """The filename is derived, never guessed: `<pid>-<sha256(repo)[:12]>.json`."""
    import hashlib

    repo = "/Users/cj/Workspace/active/fleet-watch"
    digest = hashlib.sha256(repo.encode("utf-8")).hexdigest()[:12]
    assert (
        claude_lease_twin.twin_lease_path(repo, 21455, state_dir=tmp_path).name
        == f"21455-{digest}.json"
    )
    assert (
        claude_lease_twin.twin_lease_path(repo, None, state_dir=tmp_path).name
        == f"nopid-{digest}.json"
    )


def test_matching_twin_file_is_removed(tmp_path):
    path = _write_twin(tmp_path, "/tmp/lease-repo", 200, "sess-a")

    result = claude_lease_twin.clear_twin_lease(
        "sess-a", "/tmp/lease-repo", 200, state_dir=tmp_path
    )

    assert result["cleared"] is True
    assert result["reason"] == "removed"
    assert not path.exists()


def test_twin_file_with_other_session_id_is_left_alone(tmp_path):
    """Filename collision is not authority — the content must match."""
    path = _write_twin(tmp_path, "/tmp/lease-repo", 200, "sess-OTHER")

    result = claude_lease_twin.clear_twin_lease(
        "sess-a", "/tmp/lease-repo", 200, state_dir=tmp_path
    )

    assert result["cleared"] is False
    assert result["reason"] == "session id mismatch"
    assert path.exists()
    assert json.loads(path.read_text())["session_id"] == "sess-OTHER"


def test_other_sessions_twin_files_are_untouched(tmp_path):
    """Nothing is globbed: only the derived name is ever considered."""
    mine = _write_twin(tmp_path, "/tmp/lease-repo", 200, "sess-a")
    other_repo = _write_twin(tmp_path, "/tmp/another-repo", 200, "sess-b")
    other_pid = _write_twin(tmp_path, "/tmp/lease-repo", 999, "sess-c")

    claude_lease_twin.clear_twin_lease(
        "sess-a", "/tmp/lease-repo", 200, state_dir=tmp_path
    )

    assert not mine.exists()
    assert other_repo.exists()
    assert other_pid.exists()


def test_absent_twin_file_is_a_clean_noop(tmp_path):
    result = claude_lease_twin.clear_twin_lease(
        "sess-a", "/tmp/lease-repo", 200, state_dir=tmp_path
    )
    assert result["cleared"] is False
    assert result["reason"] == "absent"


def test_unreadable_twin_file_is_not_removed(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = claude_lease_twin.twin_lease_path("/tmp/lease-repo", 200, state_dir=tmp_path)
    path.write_text("{not json", encoding="utf-8")

    result = claude_lease_twin.clear_twin_lease(
        "sess-a", "/tmp/lease-repo", 200, state_dir=tmp_path
    )

    assert result["cleared"] is False
    assert result["reason"] == "unreadable twin file"
    assert path.exists()


def test_lease_without_repo_dir_is_a_noop(tmp_path):
    result = claude_lease_twin.clear_twin_lease("sess-a", None, 200, state_dir=tmp_path)
    assert result["cleared"] is False
    assert result["reason"] == "no repo_dir on lease"


# ── CLI surface ──────────────────────────────────────────────────────────────

def _patch_cli_db(monkeypatch, tmp_path):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")


def _tree_with_real_pids(extra_parent):
    """TREE plus this pytest process and its parent, hung off ``extra_parent``."""
    tree = dict(TREE)
    tree[os.getppid()] = extra_parent
    tree[os.getpid()] = os.getppid()
    return tree


def test_cli_close_denies_unrelated_requester_and_exits_nonzero(tmp_path, monkeypatch):
    """The lease must survive a denied close — verified on the row, not the exit code alone."""
    _patch_cli_db(monkeypatch, tmp_path)
    _install_fake_process_table(monkeypatch, tree=_tree_with_real_pids(1))
    conn = registry.connect()
    _open_lease(conn, repo_dir=str(tmp_path / "repo"))
    conn.close()

    result = CliRunner().invoke(
        cli_module.cli, ["session", "close", "--session-id", "sess-a"]
    )

    assert result.exit_code == 3
    assert "DENY: requester is not the session owner" in result.output
    conn = registry.connect()
    assert _status(conn) == "ACTIVE"
    conn.close()


def test_cli_close_allows_descendant_and_clears_twin(tmp_path, monkeypatch):
    _patch_cli_db(monkeypatch, tmp_path)
    _install_fake_process_table(monkeypatch, tree=_tree_with_real_pids(200))
    state_dir = tmp_path / "claude-session-leases"
    monkeypatch.setattr(claude_lease_twin, "CLAUDE_SESSION_LEASE_DIR", state_dir)

    repo_dir = str(tmp_path / "repo")
    conn = registry.connect()
    lease = _open_lease(conn, repo_dir=repo_dir)
    conn.close()
    twin = _write_twin(state_dir, lease["repo_dir"], 200, "sess-a")
    bystander = _write_twin(state_dir, lease["repo_dir"], 777, "sess-other")

    result = CliRunner().invoke(
        cli_module.cli, ["session", "close", "--session-id", "sess-a"]
    )

    assert result.exit_code == 0, result.output
    conn = registry.connect()
    assert _status(conn) == "CLOSED"
    closes = cli_module.events.get_events(conn, hours=1, event_type="SESSION_CLOSE")
    conn.close()
    assert not twin.exists()
    assert bystander.exists()
    assert closes[0]["detail"]["twin_lease"]["cleared"] is True


def test_cli_close_missing_session_still_exits_two(tmp_path, monkeypatch):
    _patch_cli_db(monkeypatch, tmp_path)
    _install_fake_process_table(monkeypatch, tree=_tree_with_real_pids(1))
    registry.connect().close()

    result = CliRunner().invoke(
        cli_module.cli, ["session", "close", "--session-id", "sess-ghost"]
    )

    assert result.exit_code == 2
    assert "not found" in result.output


# ── The advice half: what `fleet guard` may truthfully advertise ─────────────
#
# `fleet guard --json` renders `unblock_command` for an operator who has been
# denied a repo. Advice that names a command this module refuses is worse than
# silence: it routes the blocked agent into a second refusal. These pin that
# `describe_session_close_authority` reports the SAME world the gate enforces.


def test_describe_reports_lineage_only_for_a_live_proven_owner(monkeypatch):
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    _open_lease(conn)

    described = registry.describe_session_close_authority(conn, "sess-a")

    assert described == {
        "session_id": "sess-a",
        "status": "lineage_only",
        "owner_pid": 200,
    }
    # And the gate agrees: an unrelated live process is refused.
    assert registry.authorize_session_close(conn, "sess-a", 600)[0] is False
    assert _status(conn) == "ACTIVE"


def test_describe_reports_reapable_for_a_provably_dead_owner(monkeypatch):
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    _open_lease(conn)
    # The owner exits: its pid leaves the table entirely.
    _install_fake_process_table(monkeypatch, tree={k: v for k, v in TREE.items() if k != 200})

    described = registry.describe_session_close_authority(conn, "sess-a")

    assert described["status"] == "reapable"
    assert described["owner_pid"] == 200
    # And the gate agrees: an unrelated process MAY reap it.
    assert registry.authorize_session_close(conn, "sess-a", 600)[0] is True


def test_describe_reports_ttl_only_for_a_null_owner_lease(monkeypatch):
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn, "sess-null", owner_pid=None, repo_dir="/tmp/lease-repo"
    )

    described = registry.describe_session_close_authority(conn, "sess-null")

    assert described["status"] == "ttl_only"
    assert described["owner_pid"] is None
    # And the gate agrees: NOBODY may close it, including the process that
    # would otherwise be its own lineage.
    assert registry.authorize_session_close(conn, "sess-null", 200)[0] is False
    assert registry.authorize_session_close(conn, "sess-null", 600)[0] is False


def test_describe_reports_uninspectable_when_owner_identity_is_unprovable(monkeypatch):
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()
    _open_lease(conn)
    # The pid still exists, but its create-time can no longer be read — the
    # exact input `_owner_identity_proven` resolves to None (fail closed).
    monkeypatch.setattr(registry, "_pid_create_time", lambda pid: None)

    described = registry.describe_session_close_authority(conn, "sess-a")

    assert described["status"] == "uninspectable"
    assert registry.authorize_session_close(conn, "sess-a", 200)[0] is False


def test_describe_reports_absent_for_a_lease_that_is_not_registered(monkeypatch):
    _install_fake_process_table(monkeypatch)
    conn = _fresh_conn()

    described = registry.describe_session_close_authority(conn, "sess-ghost")

    assert described == {
        "session_id": "sess-ghost",
        "status": "absent",
        "owner_pid": None,
    }
