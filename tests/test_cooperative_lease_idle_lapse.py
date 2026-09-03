"""A COOPERATIVE lease whose owner is alive but silent must stop blocking.

Before this change ``check_repo_with_session`` released a lease only when
``owner_dead or (owner_missing and ttl_expired)`` — both arms require the owner
to be GONE. A lease whose PID is alive but whose heartbeat stopped therefore
blocked its scope forever. Observed 2026-09-03: a cooperative lease on
``~/.claude``, last heartbeat 03:31Z, idle 40+ minutes, owner alive, refusing
every whole-tree operation there with no expiry path.

The fix lapses such a lease FOR ARBITRATION only. It is not closed: the row
stays ACTIVE and the owner's next heartbeat revives it. EXCLUSIVE leases keep
the old behaviour — an alive owner blocks at any heartbeat age — because the
~/.claude SessionStart guard relies on that for fail-closed whole-tree
protection.
"""

import sqlite3

import pytest

from fleet_watch import referee, registry

REPO = "/tmp/fleet-lapse-test-repo"
FRESH = 5
STALE = registry.DEFAULT_STALE_SECONDS + 60


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 0)"
    )
    conn.commit()
    return conn


@pytest.fixture()
def alive_owner(monkeypatch):
    """Owner PID is ALIVE — the case neither release arm covers."""
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: True)
    monkeypatch.setattr(registry, "_pid_create_time", lambda pid: None)
    monkeypatch.setattr(registry, "_lease_owner_alive", lambda lease: True)


def _age(monkeypatch, seconds: int) -> None:
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: seconds if ts else None)


def _hold(conn, session_id: str, *, mode: str, scope: str) -> None:
    registry.upsert_session_lease(
        conn, session_id, owner_pid=4242, repo_dir=REPO,
        repo_lock_mode=mode, write_scopes=(scope,),
    )


def test_idle_cooperative_lease_lapses_and_stops_blocking(alive_owner, monkeypatch):
    conn = _conn()
    _hold(conn, "sess-idle-coop", mode="cooperative", scope=f"{REPO}/shared.py")
    _age(monkeypatch, STALE)

    d = referee.check_repo_with_session(
        conn, REPO, current_session_id="sess-mine",
        write_scopes=[f"{REPO}/shared.py"],
    )

    assert d.allowed is True, d.reason
    lapsed = [h for h in d.stale_holders if h["session_id"] == "sess-idle-coop"]
    assert lapsed, f"lapsed lease not reported: {d.stale_holders}"
    assert lapsed[0]["reason"] == "cooperative_lease_idle_lapsed"
    # It is a lapse, not a reap: the row must survive untouched.
    lease = registry.get_session_lease(conn, "sess-idle-coop")
    assert lease is not None and lease["status"] == "ACTIVE"
    assert lease["shutdown_at"] is None
    conn.close()


def test_fresh_cooperative_lease_still_blocks_an_overlapping_scope(alive_owner, monkeypatch):
    """NEGATIVE CONTROL. If overlap detection had simply broken, the test above
    would pass for the wrong reason. Same lease, fresh heartbeat, still DENY."""
    conn = _conn()
    _hold(conn, "sess-fresh-coop", mode="cooperative", scope=f"{REPO}/shared.py")
    _age(monkeypatch, FRESH)

    d = referee.check_repo_with_session(
        conn, REPO, current_session_id="sess-mine",
        write_scopes=[f"{REPO}/shared.py"],
    )

    assert d.allowed is False, d.reason
    assert "write scope overlaps" in d.reason
    assert d.holder["session_id"] == "sess-fresh-coop"
    conn.close()


def test_idle_cooperative_lease_does_not_block_an_exclusive_request(alive_owner, monkeypatch):
    """A lapsed lease is not a cooperative peer either — an exclusive claim
    that a fresh peer would block gets through once the peer goes silent."""
    conn = _conn()
    _hold(conn, "sess-idle-coop", mode="cooperative", scope=f"{REPO}/other.py")

    _age(monkeypatch, FRESH)
    fresh = referee.check_repo_with_session(
        conn, REPO, current_session_id="sess-mine", exclusive=True
    )
    assert fresh.allowed is False, "a FRESH cooperative peer must still block exclusive"

    _age(monkeypatch, STALE)
    lapsed = referee.check_repo_with_session(
        conn, REPO, current_session_id="sess-mine", exclusive=True
    )
    assert lapsed.allowed is True, lapsed.reason
    assert any(h["reason"] == "cooperative_lease_idle_lapsed"
               for h in lapsed.stale_holders), lapsed.stale_holders
    conn.close()


def test_idle_EXCLUSIVE_lease_still_blocks(alive_owner, monkeypatch):
    """THE FAIL-CLOSED ARM. Exclusive mode does not lapse: an alive owner keeps
    the whole tree regardless of heartbeat age."""
    conn = _conn()
    _hold(conn, "sess-idle-excl", mode="exclusive", scope=f"{REPO}/anything.py")
    _age(monkeypatch, STALE)

    d = referee.check_repo_with_session(
        conn, REPO, current_session_id="sess-mine",
        write_scopes=[f"{REPO}/unrelated.py"],
    )

    assert d.allowed is False, d.reason
    assert "exclusive session sess-idle-excl" in d.reason
    lease = registry.get_session_lease(conn, "sess-idle-excl")
    assert lease["status"] == "ACTIVE"
    conn.close()


def test_a_heartbeat_revives_a_lapsed_lease(alive_owner, monkeypatch):
    """Revival is the reason lapsing is safe: the idle owner loses nothing
    durable and needs no operator action to get its authority back."""
    conn = _conn()
    _hold(conn, "sess-revive", mode="cooperative", scope=f"{REPO}/shared.py")

    _age(monkeypatch, STALE)
    assert referee.check_repo_with_session(
        conn, REPO, current_session_id="sess-mine",
        write_scopes=[f"{REPO}/shared.py"],
    ).allowed is True

    # The owner comes back. `heartbeat_session_lease` refreshes the timestamp;
    # the age stub is what the referee reads, so it moves with it.
    assert registry.heartbeat_session_lease(conn, "sess-revive", owner_pid=4242) is True
    _age(monkeypatch, FRESH)

    revived = referee.check_repo_with_session(
        conn, REPO, current_session_id="sess-mine",
        write_scopes=[f"{REPO}/shared.py"],
    )
    assert revived.allowed is False, "a revived lease must block again"
    assert revived.holder["session_id"] == "sess-revive"
    conn.close()


def test_unreadable_heartbeat_keeps_blocking(alive_owner, monkeypatch):
    """Fail-closed direction. An age that cannot be computed is not 'expired'."""
    conn = _conn()
    _hold(conn, "sess-unknown-age", mode="cooperative", scope=f"{REPO}/shared.py")
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: None)

    d = referee.check_repo_with_session(
        conn, REPO, current_session_id="sess-mine",
        write_scopes=[f"{REPO}/shared.py"],
    )

    assert d.allowed is False, d.reason
    conn.close()


def test_lapse_reason_survives_the_public_json_contract(alive_owner, monkeypatch):
    """`fleet guard --json` passes stale_holders through summarize_holder. If
    the reason is dropped there, an operator sees a repo handed away with no
    explanation."""
    conn = _conn()
    _hold(conn, "sess-idle-coop", mode="cooperative", scope=f"{REPO}/shared.py")
    _age(monkeypatch, STALE)

    d = referee.check_repo_with_session(
        conn, REPO, current_session_id="sess-mine",
        write_scopes=[f"{REPO}/shared.py"],
    )
    summarized = [referee.summarize_holder(h) for h in d.stale_holders]

    assert any(h["reason"] == "cooperative_lease_idle_lapsed" for h in summarized), summarized
    conn.close()


def test_a_session_never_lapses_its_own_lease(alive_owner, monkeypatch):
    """The current session is short-circuited before any liveness evaluation,
    so an idle session still owns its own repo."""
    conn = _conn()
    _hold(conn, "sess-mine", mode="cooperative", scope=f"{REPO}/shared.py")
    _age(monkeypatch, STALE)

    d = referee.check_repo_with_session(
        conn, REPO, current_session_id="sess-mine",
        write_scopes=[f"{REPO}/shared.py"],
    )

    assert d.allowed is True
    assert d.safe_mode == "same-session"
    assert d.stale_holders == []
    conn.close()
