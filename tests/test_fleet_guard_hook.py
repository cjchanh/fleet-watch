"""Executable tests for the fleet_guard_hook.py PreToolUse hook.

Each test invokes the hook as a subprocess with JSON on stdin,
exactly as Claude Code does. Asserts exit codes and output.

Tests that need repo locks use the real fleet CLI to register/release
external resources, making these true end-to-end integration tests.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

HOOK = str(Path("~/.claude/hooks/fleet_guard_hook.py").expanduser())


def _invoke_hook(command: str, env_override: dict | None = None) -> subprocess.CompletedProcess:
    """Run the hook with a Bash tool_input payload, return result."""
    payload = json.dumps({
        "tool_name": "Bash",
        "tool_input": {"command": command},
    })
    env = {**os.environ, **(env_override or {})}
    return subprocess.run(
        [sys.executable, HOOK],
        input=payload,
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
    )


def _fleet_claim(uuid: str, session_id: str, repo_dir: str) -> None:
    """Register an external resource in the real fleet-watch registry."""
    subprocess.run(
        [
            "fleet", "thunder", "claim",
            "--uuid", uuid,
            "--session-id", session_id,
            "--repo", repo_dir,
            "--workstream", "test",
            "--name", f"test-{uuid}",
        ],
        check=True,
        capture_output=True,
    )


def _fleet_release(uuid: str) -> None:
    """Remove a test resource from the real fleet-watch registry."""
    subprocess.run(
        ["fleet", "thunder", "release", "--uuid", uuid],
        capture_output=True,
    )


# --- Safe commands pass through ---

@pytest.mark.parametrize("cmd", [
    "ls -la",
    "git status",
    "git log --oneline -5",
    "git diff HEAD~1",
    "git -C /tmp diff",
    "fleet status --json",
    "cargo build --release",
    "python3 -m pytest tests -q",
    "pip install requests",
    "tnr status --json",
    "curl http://127.0.0.1:4242/v1/health",
])
def test_safe_commands_pass(cmd):
    result = _invoke_hook(cmd)
    assert result.returncode == 0, f"Safe command blocked: {cmd}\n{result.stdout}"


# --- Port conflict blocks ---

def test_port_conflict_blocks():
    """Server startup on a claimed port is blocked."""
    result = _invoke_hook("python3 -m mlx_lm server --model foo --port 4242")
    assert result.returncode == 1
    assert "FLEET GUARD BLOCKED" in result.stdout
    assert "4242" in result.stdout


def test_free_port_allows():
    """Server startup on an unclaimed port is allowed."""
    result = _invoke_hook("uvicorn app:main --port 9999")
    assert result.returncode == 0


# --- Repo mutation: git push with -C blocks locked repo ---

TEST_UUID = "hook-test-e2e"


def test_git_push_with_C_blocked(tmp_path):
    """git push -C <locked_repo> is blocked when another session holds it."""
    repo = str(tmp_path.resolve())
    try:
        _fleet_claim(TEST_UUID, "sess-other", repo)
        result = _invoke_hook(f"git -C {repo} push origin main")
        assert result.returncode == 1, f"Expected block, got:\n{result.stdout}"
        assert "FLEET GUARD BLOCKED" in result.stdout
    finally:
        _fleet_release(TEST_UUID)


def test_git_push_with_C_allowed_unlocked(tmp_path):
    """git push -C <unlocked_repo> is allowed."""
    result = _invoke_hook(f"git -C {tmp_path} push origin main")
    assert result.returncode == 0


# --- Repo mutation: git push with -C (double-quoted path) ---

def test_git_push_double_quoted_path_blocked(tmp_path):
    """git push -C "<path with spaces>" is blocked when locked."""
    spaced = tmp_path / "my repo"
    spaced.mkdir()
    repo = str(spaced.resolve())
    try:
        _fleet_claim(f"{TEST_UUID}-dq", "sess-other", repo)
        result = _invoke_hook(f'git -C "{repo}" push origin main')
        assert result.returncode == 1, f"Expected block, got:\n{result.stdout}"
        assert "FLEET GUARD BLOCKED" in result.stdout
    finally:
        _fleet_release(f"{TEST_UUID}-dq")


# --- Repo mutation: git push with -C (single-quoted path) ---

def test_git_push_single_quoted_path_blocked(tmp_path):
    """git push -C '<path>' is blocked when locked."""
    spaced = tmp_path / "another repo"
    spaced.mkdir()
    repo = str(spaced.resolve())
    try:
        _fleet_claim(f"{TEST_UUID}-sq", "sess-other", repo)
        result = _invoke_hook(f"git -C '{repo}' push origin main")
        assert result.returncode == 1, f"Expected block, got:\n{result.stdout}"
        assert "FLEET GUARD BLOCKED" in result.stdout
    finally:
        _fleet_release(f"{TEST_UUID}-sq")


# --- Repo mutation: git commit with -C ---

def test_git_commit_with_C_blocked(tmp_path):
    """git commit -C <locked_repo> is blocked."""
    repo = str(tmp_path.resolve())
    try:
        _fleet_claim(f"{TEST_UUID}-commit", "sess-other", repo)
        result = _invoke_hook(f"git -C {repo} commit -m 'test'")
        assert result.returncode == 1, f"Expected block, got:\n{result.stdout}"
        assert "FLEET GUARD BLOCKED" in result.stdout
    finally:
        _fleet_release(f"{TEST_UUID}-commit")


# --- Repo mutation: bare git push falls back to cwd / git root ---

def test_git_push_bare_blocked_when_cwd_repo_locked():
    """Bare git push resolves cwd to git root; blocks if that repo is locked."""
    # The hook subprocess inherits our cwd — fleet-watch repo root.
    repo_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True,
    ).stdout.strip()
    assert repo_root, "test must run from inside a git repo"

    try:
        _fleet_claim(f"{TEST_UUID}-bare", "sess-other", repo_root)
        result = _invoke_hook("git push origin main")
        assert result.returncode == 1, f"Expected block, got:\n{result.stdout}"
        assert "FLEET GUARD BLOCKED" in result.stdout
    finally:
        _fleet_release(f"{TEST_UUID}-bare")


def test_git_push_bare_allowed_when_cwd_repo_unlocked():
    """Bare git push from an unlocked cwd repo is allowed."""
    result = _invoke_hook("git push origin main")
    assert result.returncode == 0


def test_git_push_bare_fails_closed_outside_git_dir(tmp_path):
    """Bare git push from outside any git repo fails closed."""
    result = subprocess.run(
        [sys.executable, HOOK],
        input=json.dumps({
            "tool_name": "Bash",
            "tool_input": {"command": "git push origin main"},
        }),
        capture_output=True,
        text=True,
        timeout=10,
        cwd=str(tmp_path),  # tmp_path is not a git repo
    )
    assert result.returncode == 1
    assert "cannot determine repo" in result.stdout


# --- Same-session bypass: owned repo allows ---

def test_git_push_same_session_allowed(tmp_path):
    """git push to repo owned by current session is allowed via session bypass."""
    repo = str(tmp_path.resolve())
    try:
        _fleet_claim(f"{TEST_UUID}-same", "test-session-42", repo)
        result = _invoke_hook(
            f"git -C {repo} push origin main",
            env_override={"FLEET_SESSION_ID": "test-session-42"},
        )
        assert result.returncode == 0, f"Same-session should allow:\n{result.stdout}"
    finally:
        _fleet_release(f"{TEST_UUID}-same")


# --- Fail-closed: fleet unavailable blocks resource commands ---

def test_fail_closed_when_fleet_missing():
    """Resource commands are blocked when fleet CLI and state.json are both missing."""
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": "/tmp/fleet-guard-test-empty",
    }
    result = subprocess.run(
        [sys.executable, HOOK],
        input=json.dumps({
            "tool_name": "Bash",
            "tool_input": {"command": "python3 -m mlx_lm server --port 8899"},
        }),
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
    )
    assert result.returncode == 1
    assert "not reachable" in result.stdout


# --- Non-Bash tool passes through ---

def test_non_bash_tool_passes():
    payload = json.dumps({"tool_name": "Write", "tool_input": {"file_path": "/tmp/x"}})
    result = subprocess.run(
        [sys.executable, HOOK],
        input=payload,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0


# --- git read commands with -C pass through ---

@pytest.mark.parametrize("cmd", [
    "git -C /tmp status",
    "git -C /tmp log --oneline",
    "git -C /tmp diff HEAD",
    "git -C /tmp branch -a",
])
def test_git_read_with_C_passes(cmd):
    result = _invoke_hook(cmd)
    assert result.returncode == 0, f"Read command blocked: {cmd}\n{result.stdout}"
