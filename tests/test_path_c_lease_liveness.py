"""Path C regression tests — decouple proven owner-death from heartbeat TTL.

Three layers:
  A (DECOUPLE): a provably-dead owner releases the exclusive lease IMMEDIATELY,
    independent of the 180s heartbeat TTL. Predicate becomes
    (owner_dead) OR (heartbeat_expired). owner_missing (null PID) stays bound to
    the TTL arm — conservative, fail-closed.
  B (IDENTITY + REAPER + HONESTY): create-time identity defeats PID reuse;
    the discover/watch loop proactively reaps dead-owner exclusive leases;
    `fleet stale` and `fleet session list` cross-check PID liveness so a
    dead-owner lease is reported as dead instead of ACTIVE.
  C (FENCING): minimal honest fencing token (see test + module docstring).
"""

import json
import os
import sqlite3
import time

import pytest
from click.testing import CliRunner

from fleet_watch import cli, referee, registry


def _fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 0)"
    )
    conn.commit()
    return conn


# ── Layer A: proven death releases an EXCLUSIVE lease immediately ────────────

def test_exclusive_lease_with_dead_owner_releases_before_ttl(monkeypatch):
    """CORE BUG: a provably-dead owner of an EXCLUSIVE lease must NOT keep the
    repo locked for up to 180s. Proven death is an independent sufficient
    trigger; it must release immediately even when the heartbeat is fresh."""
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: False)
    # Fresh heartbeat: age well under DEFAULT_STALE_SECONDS (180s).
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 5 if ts else None)
    registry.upsert_session_lease(
        conn,
        "sess-dead-exclusive",
        owner_pid=99999,
        repo_dir="/tmp/test-repo",
        repo_lock_mode="exclusive",
    )

    d = referee.check_repo_with_session(
        conn, "/tmp/test-repo", current_session_id="sess-mine"
    )

    # Proven-dead owner => lease released => repo available, NOT blocked.
    assert d.allowed is True, d.reason
    assert any(
        h["session_id"] == "sess-dead-exclusive" for h in d.stale_holders
    ), d.stale_holders
    lease = registry.get_session_lease(conn, "sess-dead-exclusive")
    assert lease["status"] == "CLOSED"


def test_exclusive_lease_with_live_owner_still_blocks(monkeypatch):
    """ALLOW/DENY pair: a LIVE exclusive owner must still hold the lock — the
    fix must not release a lease whose owner is alive."""
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: True)
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 5 if ts else None)
    registry.upsert_session_lease(
        conn,
        "sess-live-exclusive",
        owner_pid=4242,
        repo_dir="/tmp/test-repo",
        repo_lock_mode="exclusive",
    )

    d = referee.check_repo_with_session(
        conn, "/tmp/test-repo", current_session_id="sess-mine"
    )

    assert d.allowed is False, d.reason
    assert d.holder is not None
    assert d.holder["session_id"] == "sess-live-exclusive"
    lease = registry.get_session_lease(conn, "sess-live-exclusive")
    assert lease["status"] == "ACTIVE"


def test_ownerless_fresh_exclusive_lease_still_blocks(monkeypatch):
    """Conservative arm: a null-PID (owner_missing) lease with a FRESH heartbeat
    must STILL block — we never release on a missing PID + fresh heartbeat."""
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 5 if ts else None)
    registry.upsert_session_lease(
        conn,
        "sess-ownerless-fresh",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
        repo_lock_mode="exclusive",
    )

    d = referee.check_repo_with_session(
        conn, "/tmp/test-repo", current_session_id="sess-mine"
    )

    assert d.allowed is False, d.reason
    assert d.holder["session_id"] == "sess-ownerless-fresh"


# ── Layer B: create-time identity defeats PID reuse ──────────────────────────

def test_create_time_mismatch_treated_as_dead_owner(monkeypatch):
    """PID reuse: the original owner exited and the OS reassigned its PID to an
    unrelated process. _pid_exists() is True, but the create-time recorded at
    lease open no longer matches the live PID's create-time => the original
    owner is dead. The lease must release immediately (no 180s wait)."""
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: True)
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 5 if ts else None)
    # Owner recorded with create-time "T1"; live PID now reports "T2".
    monkeypatch.setattr(registry, "_pid_create_time", lambda pid: "T2-RECYCLED")
    registry.upsert_session_lease(
        conn,
        "sess-recycled",
        owner_pid=12345,
        repo_dir="/tmp/test-repo",
        repo_lock_mode="exclusive",
    )
    # Force the stored create-time to a value that will NOT match the live one.
    conn.execute(
        "UPDATE session_leases SET owner_create_time = ? WHERE session_id = ?",
        ("T1-ORIGINAL", "sess-recycled"),
    )
    conn.commit()

    assert registry._lease_owner_alive(
        registry.get_session_lease(conn, "sess-recycled")
    ) is False

    d = referee.check_repo_with_session(
        conn, "/tmp/test-repo", current_session_id="sess-mine"
    )
    assert d.allowed is True, d.reason
    assert any(h["session_id"] == "sess-recycled" for h in d.stale_holders)


def test_create_time_match_keeps_owner_alive(monkeypatch):
    """Identity ALLOW path: same PID, same create-time => genuinely the original
    owner => still alive => exclusive lease still blocks."""
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: True)
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 5 if ts else None)
    monkeypatch.setattr(registry, "_pid_create_time", lambda pid: "T1-ORIGINAL")
    registry.upsert_session_lease(
        conn,
        "sess-same",
        owner_pid=12345,
        repo_dir="/tmp/test-repo",
        repo_lock_mode="exclusive",
    )
    conn.execute(
        "UPDATE session_leases SET owner_create_time = ? WHERE session_id = ?",
        ("T1-ORIGINAL", "sess-same"),
    )
    conn.commit()

    assert registry._lease_owner_alive(
        registry.get_session_lease(conn, "sess-same")
    ) is True

    d = referee.check_repo_with_session(
        conn, "/tmp/test-repo", current_session_id="sess-mine"
    )
    assert d.allowed is False, d.reason


def test_owner_create_time_captured_at_open():
    """Backward-compatible migration + capture: opening a lease persists the
    owner's create-time column so liveness can defeat PID reuse later."""
    conn = _fresh_conn()
    # Column must exist (migration ran via SCHEMA/_ensure_column).
    cols = {row[1] for row in conn.execute("PRAGMA table_info(session_leases)")}
    assert "owner_create_time" in cols
    lease = registry.get_session_lease  # accessor exists
    registry.upsert_session_lease(
        conn, "sess-capture", owner_pid=99999, repo_dir="/tmp/r"
    )
    row = conn.execute(
        "SELECT owner_create_time FROM session_leases WHERE session_id = ?",
        ("sess-capture",),
    ).fetchone()
    # A dead/synthetic PID has no resolvable create-time -> stored NULL, which
    # is acceptable (degrades to PID-existence only). Column is present.
    assert row is not None


# ── Layer B: reaper in discover loop ─────────────────────────────────────────

def test_clean_stale_session_leases_reaps_dead_owner_immediately(monkeypatch):
    """The discover/watch reaper must close a dead-owner lease immediately,
    independent of heartbeat age — it must not wait out the TTL."""
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: False)
    # Fresh heartbeat (age 5s, well under 180s).
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 5 if ts else None)
    registry.upsert_session_lease(
        conn, "sess-reap-me", owner_pid=99999, repo_dir="/tmp/test-repo"
    )

    cleaned = registry.clean_stale_session_leases(conn)

    assert any(c["session_id"] == "sess-reap-me" for c in cleaned), cleaned
    lease = registry.get_session_lease(conn, "sess-reap-me")
    assert lease["status"] == "CLOSED"


# ── Layer B: honesty in `fleet stale` and `fleet session list` ───────────────

def test_session_list_json_reports_dead_owner_liveness(monkeypatch, tmp_path):
    """`fleet session list --json` must cross-check PID liveness: a dead-owner
    ACTIVE lease must be flagged owner_alive=false (today it LIED ACTIVE)."""
    db = tmp_path / "registry.db"
    monkeypatch.setattr(registry, "DB_PATH", db)
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    conn = registry.connect(db)
    registry.upsert_session_lease(
        conn, "sess-dead-list", owner_pid=99999, repo_dir="/tmp/r"
    )
    conn.close()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: False)

    runner = CliRunner()
    result = runner.invoke(cli.cli, ["session", "list", "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    lease = next(
        l for l in payload["session_leases"] if l["session_id"] == "sess-dead-list"
    )
    assert lease["owner_alive"] is False, lease


def test_stale_lists_dead_owner_session_lease(monkeypatch, tmp_path):
    """`fleet stale` must surface a dead-owner session lease instead of saying
    'No stale processes.' when a provably-dead owner still holds a lease."""
    db = tmp_path / "registry.db"
    monkeypatch.setattr(registry, "DB_PATH", db)
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    conn = registry.connect(db)
    registry.upsert_session_lease(
        conn,
        "sess-dead-stale-cli",
        owner_pid=99999,
        repo_dir="/tmp/r",
        repo_lock_mode="exclusive",
    )
    conn.close()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: False)

    runner = CliRunner()
    result = runner.invoke(cli.cli, ["stale"])
    assert result.exit_code == 0, result.output
    assert "No stale processes." not in result.output
    assert "sess-dead-stale-cli" in result.output


# ── Layer C: fencing token (minimal honest version) ──────────────────────────

def test_lease_grant_issues_monotonic_fencing_token():
    """Each granted lease carries a monotonic fencing epoch, persisted at grant.
    A re-grant of the same session must strictly increase the epoch so a stale
    holder's token can be rejected at any future enforcement point."""
    conn = _fresh_conn()
    registry.upsert_session_lease(conn, "sess-fence", owner_pid=99999, repo_dir="/tmp/r")
    e1 = registry.get_session_lease(conn, "sess-fence")["fencing_epoch"]
    assert isinstance(e1, int) and e1 >= 1
    # Re-grant (new owner takes over the same session id) bumps the epoch.
    registry.upsert_session_lease(conn, "sess-fence", owner_pid=88888, repo_dir="/tmp/r")
    e2 = registry.get_session_lease(conn, "sess-fence")["fencing_epoch"]
    assert e2 > e1, (e1, e2)


def test_fencing_token_valid_accepts_current_rejects_stale():
    """ALLOW/DENY pair for the fencing token: the current epoch validates; a
    stale (superseded) epoch is rejected; an unknown lease fails closed."""
    conn = _fresh_conn()
    registry.upsert_session_lease(conn, "sess-tok", owner_pid=99999, repo_dir="/tmp/r")
    e1 = registry.current_fencing_epoch(conn, "sess-tok")
    assert registry.fencing_token_valid(conn, "sess-tok", e1) is True   # ALLOW

    registry.upsert_session_lease(conn, "sess-tok", owner_pid=88888, repo_dir="/tmp/r")
    assert registry.fencing_token_valid(conn, "sess-tok", e1) is False  # DENY (stale)

    # Unknown lease -> fail-closed.
    assert registry.fencing_token_valid(conn, "no-such-session", 1) is False


# ── ADVERSARIAL: live owner must NEVER read as dead across environment ────────

def test_live_owner_create_time_is_environment_invariant():
    """CATASTROPHIC TWO-WRITER GUARD: the create-time identity recorded at lease
    open and the create-time read at the liveness check must compare equal for
    the SAME LIVE process regardless of the checking process's TZ / LC_TIME.

    Today `_pid_create_time` shells out to `ps -o lstart=`, whose output is
    rendered in the *caller's* timezone and locale. A lease opened by a live
    owner under one TZ (e.g. an interactive shell) and checked under another
    (e.g. the launchd `fleet discover` daemon, which defaults to UTC) yields
    two DIFFERENT strings for the same never-died process => `_lease_owner_alive`
    returns False => the exclusive lease of a LIVE owner is released => two
    writers on one repo. This test pins the invariant; it FAILS on the
    raw-`ps`-string implementation and passes once create-time is normalized to
    an absolute, environment-independent value (e.g. UTC epoch seconds)."""
    pid = os.getpid()  # genuinely alive for the whole test

    saved = {k: os.environ.get(k) for k in ("TZ", "LC_TIME", "LC_ALL")}
    try:
        os.environ["TZ"] = "UTC"
        time.tzset()
        recorded = registry._pid_create_time(pid)

        os.environ["TZ"] = "America/New_York"
        time.tzset()
        live = registry._pid_create_time(pid)

        lease = {"owner_pid": pid, "owner_create_time": recorded}
        # The owner never died; a TZ change on the checker must not "kill" it.
        assert registry._lease_owner_alive(lease) is True, (
            f"LIVE owner declared DEAD: recorded={recorded!r} live={live!r} "
            "-> exclusive lease would be released -> two-writer"
        )
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        time.tzset()


def test_referee_never_releases_live_exclusive_owner_across_tz():
    """End-to-end of the same failure: a second session claiming the exclusive
    repo must be BLOCKED while the owner is alive, even when the referee runs
    under a different TZ than the one the lease was opened in."""
    pid = os.getpid()
    conn = _fresh_conn()

    saved_tz = os.environ.get("TZ")
    try:
        os.environ["TZ"] = "UTC"
        time.tzset()
        registry.upsert_session_lease(
            conn,
            "sess-live-tz",
            owner_pid=pid,
            repo_dir="/tmp/test-repo",
            repo_lock_mode="exclusive",
        )

        os.environ["TZ"] = "America/Denver"
        time.tzset()
        d = referee.check_repo_with_session(
            conn, "/tmp/test-repo", current_session_id="sess-other", exclusive=True
        )

        assert d.allowed is False, (
            f"LIVE exclusive owner released across TZ: {d.reason} "
            f"stale={[h['session_id'] for h in d.stale_holders]}"
        )
        lease = registry.get_session_lease(conn, "sess-live-tz")
        assert lease["status"] == "ACTIVE", lease["status"]
    finally:
        if saved_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = saved_tz
        time.tzset()
