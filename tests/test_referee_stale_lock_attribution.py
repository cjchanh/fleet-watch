"""Regression tests for git-lock holder attribution."""

import subprocess

from fleet_watch import cli, referee, registry


def _repo_with_lock(tmp_path):
    repo = tmp_path / "repo"
    git_dir = repo / ".git"
    git_dir.mkdir(parents=True)
    (git_dir / "index.lock").write_text("")
    return repo


def test_lsof_parser_preserves_any_writable_descriptor(tmp_path, monkeypatch):
    lock = tmp_path / "index.lock"
    lock.write_text("")
    output = "p62514\nf10\nar\nf11\naw\nf12\nar\n"
    monkeypatch.setattr(
        referee.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, output, ""),
    )

    assert referee._open_file_holders(lock) == {62514: "w"}


def test_read_only_holder_of_stale_lock_is_not_a_live_claim(tmp_path, monkeypatch):
    """A live process merely reading stale debris must not block the repo."""
    repo = _repo_with_lock(tmp_path)
    monkeypatch.setattr(referee, "_open_file_holders", lambda _path: {62514: "r"})
    monkeypatch.setattr(referee, "_lsof_can_attribute_open_files", lambda: True)
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid == 62514)

    probe = referee.probe_repo_writers(str(repo))

    assert probe.status == referee.REPO_WRITER_STALE
    assert probe.pids == ()
    assert "read-only" in probe.detail


def test_live_writable_holder_remains_a_genuine_claim(tmp_path, monkeypatch):
    """The access-mode filter must preserve DENY for a real git writer."""
    repo = _repo_with_lock(tmp_path)
    monkeypatch.setattr(referee, "_open_file_holders", lambda _path: {1259: "w"})
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid == 1259)

    probe = referee.probe_repo_writers(str(repo))

    assert probe.status == referee.REPO_WRITER_HELD
    assert probe.pids == (1259,)
    assert "writable" in probe.detail


def test_guard_repo_evidence_explains_read_only_stale_lock(tmp_path, monkeypatch):
    """Machine output must expose why the read-only holder was not charged."""
    repo = _repo_with_lock(tmp_path)
    monkeypatch.setattr(referee, "_open_file_holders", lambda _path: {62514: "r"})
    monkeypatch.setattr(referee, "_lsof_can_attribute_open_files", lambda: True)
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid == 62514)
    monkeypatch.setattr(cli.ollama_runners, "discover_ollama_runners", lambda: [])
    conn = registry.connect(tmp_path / "registry.db")

    payload = cli._build_guard_payload(conn, repo_dir=str(repo))

    assert payload["allowed"] is True
    evidence = payload["checks"]["repo"]["evidence"]
    assert evidence["status"] == referee.REPO_WRITER_STALE
    assert "read-only" in evidence["detail"]
