"""Tests for session_coupling — single-writer awareness over live leases.

Pure: liveness (PID-alive + heartbeat-age), repo resolution (cwd), and the lease
set are all injected, so no live process or registry is touched. Identity
``root_resolver`` is used so repos compare by path without needing real ``.git``.
"""
from __future__ import annotations

from fleet_watch import session_coupling as sc

IDENT = lambda p: p  # noqa: E731 — identity git-root for pure path comparison


def lease(session_id, pid, repo=None, status="ACTIVE", hb="fresh", shutdown=None,
          mode="cooperative"):
    return {"session_id": session_id, "owner_pid": pid, "repo_dir": repo,
            "status": status, "last_heartbeat_at": hb, "shutdown_at": shutdown,
            "repo_lock_mode": mode}


# heartbeat-age + pid-alive injectors
AGE = {"fresh": 10.0, "stale": 999.0, None: None}
age_of = lambda hb: AGE.get(hb, 10.0)  # noqa: E731
alive = lambda pids: (lambda pid: pid in pids)  # noqa: E731


def _check(repo, me, leases, pids, **kw):
    return sc.single_writer_check(
        repo, me, leases, age_of=age_of, pid_alive=alive(pids),
        root_resolver=IDENT, cwd_resolver=lambda pid: None, **kw)


# ── the core collision case ─────────────────────────────────────────────────
def test_conflict_other_live_session_same_repo():
    leases = [lease("A", 100, repo="/r/x"), lease("B", 200, repo="/r/x")]
    v = _check("/r/x", "A", leases, pids={100, 200})
    assert v.decision == "CONFLICT" and v.exit_code == 3
    assert [c["session_id"] for c in v.conflicts] == ["B"]


def test_only_self_on_repo_allows():
    leases = [lease("A", 100, repo="/r/x")]
    v = _check("/r/x", "A", leases, pids={100})
    assert v.decision == "ALLOW" and v.exit_code == 0


def test_live_on_different_repo_allows():
    leases = [lease("A", 100, repo="/r/x"), lease("B", 200, repo="/r/y")]
    v = _check("/r/x", "A", leases, pids={100, 200})
    assert v.decision == "ALLOW"


# ── liveness gates (stale + dead PID must NOT count as conflicts) ───────────
def test_stale_heartbeat_excluded():
    leases = [lease("B", 200, repo="/r/x", hb="stale")]
    v = _check("/r/x", "A", leases, pids={200})
    assert v.decision == "ALLOW"  # B is ACTIVE + alive but heartbeat is stale


def test_dead_pid_excluded():
    leases = [lease("B", 200, repo="/r/x", hb="fresh")]
    v = _check("/r/x", "A", leases, pids=set())  # PID 200 not alive
    assert v.decision == "ALLOW"


def test_shutdown_lease_excluded():
    leases = [lease("B", 200, repo="/r/x", shutdown="2026-06-17T00:00:00Z")]
    v = _check("/r/x", "A", leases, pids={200})
    assert v.decision == "ALLOW"


# ── fail-closed negatives ───────────────────────────────────────────────────
def test_unknown_when_leases_unavailable():
    v = _check("/r/x", "A", None, pids={100})  # registry unreachable
    assert v.decision == "UNKNOWN" and v.exit_code == 4


def test_unknown_when_repo_unresolvable():
    v = _check("", "A", [], pids=set())
    assert v.decision == "UNKNOWN" and v.exit_code == 4


# ── the gap-closer: repo derived from PID cwd when repo_dir is null ─────────
def test_lease_repo_derived_from_pid_cwd_when_repo_dir_null():
    repo = sc.lease_repo(lease("B", 200, repo=None),
                         cwd_resolver=lambda pid: "/r/x", root_resolver=IDENT)
    assert repo == sc.canonical_repo("/r/x")


def test_conflict_surfaces_even_with_null_repo_dir():
    """The whole point: a live session with repo_dir=null still conflicts once
    its repo is derived from cwd."""
    leases = [lease("B", 200, repo=None)]
    v = sc.single_writer_check(
        "/r/x", "A", leases, age_of=age_of, pid_alive=alive({200}),
        root_resolver=IDENT, cwd_resolver=lambda pid: "/r/x")
    assert v.decision == "CONFLICT" and [c["session_id"] for c in v.conflicts] == ["B"]


# ── canonicalization + exit-code contract ───────────────────────────────────
def test_trailing_slash_repo_joins():
    leases = [lease("B", 200, repo="/r/x/")]
    v = _check("/r/x", "A", leases, pids={200})
    assert v.decision == "CONFLICT"


def test_pid_alive_rejects_zero_and_none():
    assert sc._pid_alive(0) is False
    assert sc._pid_alive(None) is False


def test_verdict_exit_codes():
    assert sc.Verdict("ALLOW", "/r", "ok").exit_code == 0
    assert sc.Verdict("CONFLICT", "/r", "x").exit_code == 3
    assert sc.Verdict("UNKNOWN", "/r", "x").exit_code == 4
