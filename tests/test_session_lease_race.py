"""Exclusive session-lease grant must be mutually exclusive under contention."""

from __future__ import annotations

import inspect
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

from fleet_watch import registry

N_CONCURRENT = 12
N_TRIALS = 3


def _patch_registry(monkeypatch, tmp_path: Path) -> Path:
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")
    return tmp_path / "registry.db"


def test_exclusive_repo_unique_index_created(tmp_path, monkeypatch):
    db = _patch_registry(monkeypatch, tmp_path)
    conn = registry.connect(db)
    names = [row[1] for row in conn.execute("PRAGMA index_list(session_leases)")]
    conn.close()
    assert registry.EXCLUSIVE_REPO_INDEX in names
    source = inspect.getsource(registry._ensure_exclusive_repo_index)
    assert "EXCLUSIVE_REPO_INDEX" in source


def test_second_transactional_grant_refused_when_live_holder_exists(
    tmp_path, monkeypatch
):
    db = _patch_registry(monkeypatch, tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    conn = registry.connect(db)
    meta = registry.collect_owner_metadata(os.getpid())
    first = registry.grant_exclusive_lease(
        conn, "s1", owner_metadata=meta, repo_dir=str(repo)
    )
    assert first["allowed"] is True
    second = registry.grant_exclusive_lease(
        conn, "s2", owner_metadata=meta, repo_dir=str(repo)
    )
    assert second["allowed"] is False
    rows = conn.execute(
        "SELECT session_id FROM session_leases "
        "WHERE status = 'ACTIVE' AND repo_lock_mode = 'exclusive'"
    ).fetchall()
    conn.close()
    assert [row[0] for row in rows] == ["s1"]


def test_grant_exclusive_overlap_denies_nested_and_parent(tmp_path, monkeypatch):
    db = _patch_registry(monkeypatch, tmp_path)
    root = tmp_path / "tree"
    sub = root / "sub"
    sub.mkdir(parents=True)
    conn = registry.connect(db)
    meta = registry.collect_owner_metadata(os.getpid())
    first = registry.grant_exclusive_lease(
        conn, "root", owner_metadata=meta, repo_dir=str(root)
    )
    nested = registry.grant_exclusive_lease(
        conn, "nested", owner_metadata=meta, repo_dir=str(sub)
    )
    conn.close()
    assert first["allowed"] is True
    assert nested["allowed"] is False

    db2 = tmp_path / "db2"
    db2.mkdir()
    monkeypatch.setattr(registry, "FLEET_DIR", db2)
    monkeypatch.setattr(registry, "DB_PATH", db2 / "registry.db")
    conn = registry.connect(db2 / "registry.db")
    first = registry.grant_exclusive_lease(
        conn, "nested", owner_metadata=meta, repo_dir=str(sub)
    )
    parent = registry.grant_exclusive_lease(
        conn, "root", owner_metadata=meta, repo_dir=str(root)
    )
    conn.close()
    assert first["allowed"] is True
    assert parent["allowed"] is False


def test_cooperative_holder_does_not_block_nested_exclusive_grant(
    tmp_path, monkeypatch
):
    db = _patch_registry(monkeypatch, tmp_path)
    root = tmp_path / "tree"
    sub = root / "sub"
    sub.mkdir(parents=True)
    conn = registry.connect(db)
    meta = registry.collect_owner_metadata(os.getpid())
    registry.upsert_session_lease(
        conn,
        "coop",
        owner_pid=os.getpid(),
        repo_dir=str(root),
        repo_lock_mode="cooperative",
        owner_metadata=meta,
    )
    grant = registry.grant_exclusive_lease(
        conn, "ex", owner_metadata=meta, repo_dir=str(sub)
    )
    rows = conn.execute(
        "SELECT session_id, repo_lock_mode FROM session_leases WHERE status = 'ACTIVE' "
        "ORDER BY session_id"
    ).fetchall()
    conn.close()
    assert grant["allowed"] is True
    assert rows == [("coop", "cooperative"), ("ex", "exclusive")]


def test_grant_stores_exact_resolved_path_not_git_toplevel(tmp_path, monkeypatch):
    db = _patch_registry(monkeypatch, tmp_path)
    repo = tmp_path / "gitrepo"
    pkg = repo / "pkg"
    pkg.mkdir(parents=True)
    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
    conn = registry.connect(db)
    meta = registry.collect_owner_metadata(os.getpid())
    grant = registry.grant_exclusive_lease(
        conn, "s-pkg", owner_metadata=meta, repo_dir=str(pkg)
    )
    lease = registry.get_session_lease(conn, "s-pkg")
    conn.close()
    assert grant["allowed"] is True
    assert lease is not None
    assert lease["repo_dir"] == str(pkg.resolve())
    assert lease["repo_dir"] != str(repo.resolve())


def test_grant_does_not_inspect_process_inside_transaction(tmp_path, monkeypatch):
    db = _patch_registry(monkeypatch, tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    conn = registry.connect(db)
    meta = registry.collect_owner_metadata(os.getpid())
    calls = {"n": 0}
    real = registry._inspect_process

    def wrapped(pid):
        calls["n"] += 1
        return real(pid)

    monkeypatch.setattr(registry, "_inspect_process", wrapped)
    result = registry.grant_exclusive_lease(
        conn, "s-tx", owner_metadata=meta, repo_dir=str(repo)
    )
    conn.close()
    assert result["allowed"] is True
    assert calls["n"] == 0


def test_duplicate_exclusive_rows_leave_index_absent_and_warn(tmp_path, monkeypatch):
    db = _patch_registry(monkeypatch, tmp_path)
    conn = sqlite3.connect(str(db))
    conn.executescript(registry.SCHEMA)
    now = "2026-09-05T00:00:00+00:00"
    for sid in ("a", "b"):
        conn.execute(
            """INSERT INTO session_leases (
                session_id, owner_pid, repo_dir, repo_lock_mode,
                started_at, last_heartbeat_at, status
            ) VALUES (?, NULL, ?, 'exclusive', ?, ?, 'ACTIVE')""",
            (sid, "/tmp/dup-repo", now, now),
        )
    conn.commit()
    conn.close()
    conn = registry.connect(db)
    names = [row[1] for row in conn.execute("PRAGMA index_list(session_leases)")]
    conn.close()
    assert registry.EXCLUSIVE_REPO_INDEX not in names
    warnings = registry.registry_warnings_for(db)
    assert any(warning["code"] == "exclusive_repo_index_absent" for warning in warnings)


def test_grant_exclusive_uses_prior_repo_when_repo_dir_omitted(tmp_path, monkeypatch):
    db = _patch_registry(monkeypatch, tmp_path)
    root = tmp_path / "tree"
    sub = root / "sub"
    sub.mkdir(parents=True)
    conn = registry.connect(db)
    meta = registry.collect_owner_metadata(os.getpid())
    first = registry.grant_exclusive_lease(
        conn, "root", owner_metadata=meta, repo_dir=str(root)
    )
    registry.upsert_session_lease(
        conn,
        "nested",
        owner_pid=os.getpid(),
        repo_dir=str(sub),
        repo_lock_mode="cooperative",
        owner_metadata=meta,
    )
    promoted = registry.grant_exclusive_lease(
        conn, "nested", owner_metadata=meta, repo_dir=None
    )
    conn.close()
    assert first["allowed"] is True
    assert promoted["allowed"] is False


def test_busy_lock_fails_closed(tmp_path, monkeypatch):
    db = _patch_registry(monkeypatch, tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    conn = registry.connect(db)
    conn.execute("PRAGMA busy_timeout = 0")
    blocker = sqlite3.connect(str(db), timeout=0)
    blocker.execute("PRAGMA busy_timeout = 0")
    blocker.execute("BEGIN IMMEDIATE")
    meta = registry.collect_owner_metadata(os.getpid())
    try:
        result = registry.grant_exclusive_lease(
            conn, "s-lock", owner_metadata=meta, repo_dir=str(repo)
        )
    finally:
        blocker.rollback()
        blocker.close()
        conn.close()
    assert result["allowed"] is False
    assert "registry locked" in result["reason"]


def _count_active_exclusive(db: Path, repo: Path) -> int:
    conn = sqlite3.connect(str(db))
    try:
        rows = conn.execute(
            "SELECT session_id FROM session_leases "
            "WHERE status = 'ACTIVE' AND repo_lock_mode = 'exclusive' "
            "AND repo_dir = ?",
            (str(repo.resolve()),),
        ).fetchall()
    finally:
        conn.close()
    return len(rows)


def _run_concurrent_exclusive_starts(home: Path, repo: Path, n: int) -> int:
    env = os.environ.copy()
    env["HOME"] = str(home)
    repo_root = str(Path(__file__).resolve().parents[1])
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        repo_root if not existing else repo_root + os.pathsep + existing
    )
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    # Each starter names a DISTINCT owner that stays alive for the whole trial.
    # If the owner died as soon as the CLI returned (a `sh -c exec` wrapper), a
    # later starter would legitimately reap the dead holder and be granted the
    # lease in sequence -- that is the documented liveness rule, not a race --
    # and the exit-0 count would measure liveness, not atomicity.
    owners: list[subprocess.Popen[bytes]] = [
        subprocess.Popen(["sleep", "120"]) for _ in range(n)
    ]
    procs: list[subprocess.Popen[str]] = []
    exit0 = 0
    errors: list[str] = []
    try:
        for i, owner in enumerate(owners):
            procs.append(
                subprocess.Popen(
                    [
                        sys.executable, "-m", "fleet_watch.cli", "session", "start",
                        "--session-id", f"t{i}", "--repo", str(repo),
                        "--owner-pid", str(owner.pid), "--exclusive-repo-lock",
                    ],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            )
        for proc in procs:
            proc.wait(timeout=60)
            if proc.returncode == 0:
                exit0 += 1
            else:
                err = (proc.stderr.read() if proc.stderr else "") or ""
                out = (proc.stdout.read() if proc.stdout else "") or ""
                errors.append(f"rc={proc.returncode} stderr={err!r} stdout={out!r}")
    finally:
        for owner in owners:
            owner.kill()
            owner.wait(timeout=10)
    if exit0 == 0:
        raise AssertionError("no exclusive grant succeeded:\n" + "\n".join(errors[:4]))
    return exit0


def test_concurrent_exclusive_session_start_one_winner(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    for trial in range(N_TRIALS):
        home = tmp_path / f"home-{trial}"
        if home.exists():
            shutil.rmtree(home)
        home.mkdir()
        exit0 = _run_concurrent_exclusive_starts(home, repo, N_CONCURRENT)
        db = home / ".fleet-watch" / "registry.db"
        active = _count_active_exclusive(db, repo)
        assert exit0 == 1, f"trial {trial}: exit0={exit0}/{N_CONCURRENT}"
        assert active == 1, f"trial {trial}: ACTIVE exclusive rows={active}"
