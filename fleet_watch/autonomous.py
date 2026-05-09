"""Bounded Fleet Watch autonomous reconciler.

The reconciler is intentionally narrow: it can close implemented queue specs and
promote scoped implementation patches after local proofs. It never pushes,
merges, releases, installs packages, or kills processes.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


IMPLEMENTED_STATUSES = {"STAGE1_IMPLEMENTED_TESTED"}
IMPLEMENTED_CLAIMS = {"implemented+tested"}
SAFE_TOOL_PROOFS = {
    "tools/validate-spec-frontmatter.py",
    "tools/cost-aware-autopilot/routing.py",
    "tools/handoff-validator/validate.py",
}
SAFE_TEST_ROOTS = ("tests", "tools/handoff-validator", "tools/cost-aware-autopilot")
UNITTEST_VALUE_OPTIONS = {
    "-s",
    "--start-directory",
    "-p",
    "--pattern",
    "-t",
    "--top-level-directory",
}
UNITTEST_PATH_OPTIONS = {"-s", "--start-directory", "-t", "--top-level-directory"}
FORBIDDEN_SCOPE_TOKENS = (
    "private-corpus",
    "private_corpus",
    "secrets",
    ".env",
    "release",
    "merge",
)


@dataclass
class CandidateResult:
    spec: Path | None
    errors: list[dict[str, Any]]


def run_once(repo: Path, policy_path: Path | None = None) -> dict[str, Any]:
    repo = repo.expanduser().resolve()
    policy = load_policy(policy_path)
    if not policy.get("enabled", True):
        return {
            "allowed": False,
            "verdict": "BLOCKED_POLICY_DISABLED",
            "repo": str(repo),
            "policy_path": str(policy_path) if policy_path else None,
        }
    if not (repo / ".git").exists():
        return {"allowed": False, "verdict": "BLOCKED_NOT_GIT_REPO", "repo": str(repo)}

    dirty = git_dirty_paths(repo)
    if not dirty:
        skipped_debts: list[dict[str, Any]] = []
        skipped_paths: set[Path] = set()
        while True:
            debt = find_lifecycle_debt(repo, skipped_paths=skipped_paths)
            if debt is None:
                break
            policy_error = validate_lifecycle_policy(debt, policy)
            if policy_error:
                receipt = write_receipt(
                    repo,
                    "reconcile",
                    {
                        "verdict": "LIFECYCLE_DEBT_SKIPPED",
                        "spec": debt.name,
                        "reason": policy_error,
                    },
                    policy,
                )
                skipped_debts.append({"spec": debt.name, "reason": policy_error, "receipt": str(receipt)})
                skipped_paths.add(debt.resolve())
                continue
            result = close_lifecycle_debt(repo, debt, policy)
            if skipped_debts:
                result["skipped_lifecycle_debt"] = skipped_debts
            return result
        return launch_stage(repo, policy, skipped_debts)

    if git_staged_paths(repo):
        return {
            "allowed": False,
            "verdict": "BLOCKED_PREEXISTING_STAGED_CHANGES",
            "repo": str(repo),
            "dirty_paths": dirty,
            "staged_paths": git_staged_paths(repo),
        }

    candidate = find_dirty_scope_candidate(repo, dirty, policy)
    if candidate.spec is None:
        return {
            "allowed": False,
            "verdict": "BLOCKED_SCOPE_VIOLATION",
            "repo": str(repo),
            "dirty_paths": dirty,
            "reason": "dirty paths do not exactly fit one queue spec allowed_write_paths",
            "candidate_errors": candidate.errors,
        }
    return promote_scoped_implementation(repo, candidate.spec, dirty, policy)


def load_policy(path: Path | None) -> dict[str, Any]:
    policy = {
        "enabled": True,
        "allowed_spec_classes": ["deterministic_parser", "meta_tooling", "governance_writes"],
        "allowed_lifecycle_closeout_classes": ["deterministic_parser", "meta_tooling", "governance_writes"],
        "allowed_executors": ["opencode", "local"],
        "max_scope_files": 3,
        "require_post_session": True,
        "launch_enabled": True,
        "launch_gpu_mb": 8192,
        "baseline_command": None,
        "receipt_dir": "data/fleet-watch-autonomous",
        "commit_policy": {"implementation_commit": True, "lifecycle_closeout_commit": True},
    }
    if path is None or not path.exists():
        return policy
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        loaded = json.loads(text)
        if isinstance(loaded, dict):
            policy = merge_policy(policy, loaded)
        return policy
    for raw in text.splitlines():
        line = raw.strip()
        if line == "enabled: false":
            policy["enabled"] = False
        elif line == "implementation_commit: false":
            policy["commit_policy"]["implementation_commit"] = False
        elif line == "lifecycle_closeout_commit: false":
            policy["commit_policy"]["lifecycle_closeout_commit"] = False
    return policy


def merge_policy(base: dict[str, Any], loaded: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in loaded.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            nested = dict(merged[key])
            nested.update(value)
            merged[key] = nested
        else:
            merged[key] = value
    return merged


def run(cmd: list[str], repo: Path, *, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    return subprocess.run(cmd, cwd=str(repo), capture_output=True, text=True, timeout=timeout, env=env)


def run_env(
    cmd: list[str],
    repo: Path,
    *,
    timeout: int = 60,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", **(extra_env or {})}
    return subprocess.run(cmd, cwd=str(repo), capture_output=True, text=True, timeout=timeout, env=env)


def git_dirty_paths(repo: Path) -> list[str]:
    proc = run(["git", "status", "--porcelain"], repo, timeout=15)
    if proc.returncode != 0:
        return ["<git-status-failed>"]
    paths: list[str] = []
    for line in proc.stdout.splitlines():
        if not line:
            continue
        path = line[3:].strip()
        if " -> " in path:
            paths.extend(part.strip() for part in path.split(" -> ", 1))
        else:
            paths.append(path)
    return sorted(set(paths))


def git_staged_paths(repo: Path) -> list[str]:
    proc = run(["git", "diff", "--cached", "--name-only"], repo, timeout=15)
    if proc.returncode != 0:
        return ["<git-staged-failed>"]
    return sorted(line.strip() for line in proc.stdout.splitlines() if line.strip())


def git_staged_touched_paths(repo: Path) -> list[str]:
    proc = run(["git", "diff", "--cached", "--name-status"], repo, timeout=15)
    if proc.returncode != 0:
        return ["<git-staged-status-failed>"]
    touched: list[str] = []
    for line in proc.stdout.splitlines():
        parts = line.split("\t")
        if not parts:
            continue
        status = parts[0]
        if status.startswith(("R", "C")) and len(parts) >= 3:
            touched.extend([parts[1], parts[2]])
        elif len(parts) >= 2:
            touched.append(parts[1])
    return sorted(set(touched))


def parse_frontmatter(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}
    end = None
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            end = index
            break
    if end is None:
        return {}
    data: dict[str, Any] = {}
    current_list: str | None = None
    for raw in lines[1:end]:
        if not raw.strip():
            continue
        if raw.startswith("  - ") and current_list:
            data.setdefault(current_list, []).append(raw[4:].strip().strip('"'))
            continue
        current_list = None
        if ":" not in raw:
            continue
        key, value = raw.split(":", 1)
        key = key.strip()
        value = value.strip().strip('"')
        if value == "":
            data[key] = []
            current_list = key
        elif value.lower() == "true":
            data[key] = True
        elif value.lower() == "false":
            data[key] = False
        else:
            data[key] = value
    return data


def find_lifecycle_debt(repo: Path, skipped_paths: set[Path] | None = None) -> Path | None:
    queue_dir = repo / "specs" / "queue"
    if not queue_dir.is_dir():
        return None
    skipped = skipped_paths or set()
    for spec in sorted(queue_dir.glob("*.md")):
        if spec.resolve() in skipped:
            continue
        fm = parse_frontmatter(spec)
        if (
            str(fm.get("status")) in IMPLEMENTED_STATUSES
            and str(fm.get("claim_state")) in IMPLEMENTED_CLAIMS
        ):
            return spec
    return None


def write_receipt(repo: Path, kind: str, payload: dict[str, Any], policy: dict[str, Any]) -> Path:
    root = Path(str(policy.get("receipt_dir") or "data/fleet-watch-autonomous")).expanduser()
    if not root.is_absolute():
        root = repo / root
    root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    subject = str(payload.get("spec") or payload.get("spec_id") or payload.get("verdict") or "").strip()
    safe_subject = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in subject)[:80].strip("-")
    suffix = f"-{safe_subject}" if safe_subject else ""
    path = root / f"{stamp}-{kind}{suffix}.json"
    counter = 1
    while path.exists():
        path = root / f"{stamp}-{kind}{suffix}-{counter}.json"
        counter += 1
    body = {
        "schema_version": "fleet-watch-autonomous-v1",
        "kind": kind,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "repo": str(repo),
        **payload,
    }
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def find_dirty_scope_candidate(repo: Path, dirty_paths: list[str], policy: dict[str, Any]) -> CandidateResult:
    queue_dir = repo / "specs" / "queue"
    if not queue_dir.is_dir() or not dirty_paths:
        return CandidateResult(None, [])
    dirty = set(dirty_paths)
    errors: list[dict[str, Any]] = []
    for spec in sorted(queue_dir.glob("*.md")):
        fm = parse_frontmatter(spec)
        allowed = set(str(path) for path in fm.get("allowed_write_paths", []) if path)
        if not allowed:
            continue
        if dirty.issubset(allowed):
            spec_errors = validate_dirty_candidate(repo, spec, fm, policy)
            if spec_errors:
                errors.append({"spec": spec.name, "errors": spec_errors})
                continue
            return CandidateResult(spec, errors)
    return CandidateResult(None, errors)


def validate_dirty_candidate(repo: Path, spec: Path, fm: dict[str, Any], policy: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if fm.get("autopilot_eligible") is not True:
        errors.append("autopilot_eligible_not_true")
    if str(fm.get("status")) not in IMPLEMENTED_STATUSES:
        errors.append("status_not_implemented_tested")
    if str(fm.get("claim_state")) not in IMPLEMENTED_CLAIMS:
        errors.append("claim_state_not_implemented_tested")
    spec_class = str(fm.get("class") or "").strip()
    if spec_class not in set(policy.get("allowed_spec_classes", [])):
        errors.append(f"class_not_allowed:{spec_class or '<missing>'}")
    route = str(fm.get("route") or fm.get("worker") or "").strip()
    if route not in set(policy.get("allowed_executors", [])):
        errors.append(f"executor_not_allowed:{route or '<missing>'}")
    max_scope = int(policy.get("max_scope_files", 3))
    if count_scope_files(fm) > max_scope:
        errors.append(f"scope_files_count_gt_{max_scope}")
    forbidden = forbidden_scope_token(fm)
    if forbidden:
        errors.append(f"forbidden_scope_token:{forbidden}")
    frontmatter_error = validate_frontmatter_tool(repo, spec)
    if frontmatter_error:
        errors.append(frontmatter_error)
    routing_error = validate_routing_tool(repo, spec, policy)
    if routing_error:
        errors.append(routing_error)
    return errors


def count_scope_files(fm: dict[str, Any]) -> int:
    total = 0
    for key in ("scope_files", "new_files"):
        value = fm.get(key)
        if isinstance(value, list):
            total += len(value)
    return total


def forbidden_scope_token(fm: dict[str, Any]) -> str | None:
    haystacks: list[str] = []
    for key in ("scope_files", "new_files", "allowed_write_paths"):
        value = fm.get(key)
        if isinstance(value, list):
            haystacks.extend(str(item).lower() for item in value)
    for token in FORBIDDEN_SCOPE_TOKENS:
        if any(token in haystack for haystack in haystacks):
            return token
    return None


def validate_frontmatter_tool(repo: Path, spec: Path) -> str | None:
    tool = repo / "tools" / "validate-spec-frontmatter.py"
    if not tool.is_file():
        return "frontmatter_validator_missing"
    proc = run([sys.executable, str(tool), "--file", str(spec), "--json"], repo, timeout=60)
    if proc.returncode != 0:
        return "frontmatter_validator_failed"
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return "frontmatter_validator_invalid_json"
    summary = payload.get("summary", {})
    if summary.get("blocked", 0) != 0:
        return "frontmatter_blocked"
    if summary.get("warn", 0) != 0:
        return "frontmatter_warned"
    return None


def validate_routing_tool(repo: Path, spec: Path, policy: dict[str, Any]) -> str | None:
    tool = repo / "tools" / "cost-aware-autopilot" / "routing.py"
    if not tool.is_file():
        return "routing_tool_missing"
    proc = run([sys.executable, str(tool), "--spec", str(spec)], repo, timeout=60)
    if proc.returncode != 0:
        return "routing_tool_failed"
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return "routing_tool_invalid_json"
    failures = payload.get("gate_failures") or []
    if failures:
        return "routing_gate_failures"
    executor = payload.get("executor")
    if executor not in set(policy.get("allowed_executors", [])):
        return f"routing_executor_not_allowed:{executor}"
    metadata = payload.get("metadata") or {}
    max_scope = int(policy.get("max_scope_files", 3))
    if int(metadata.get("scope_files_count", 0)) > max_scope:
        return f"routing_scope_files_count_gt_{max_scope}"
    spec_class = metadata.get("spec_class") or payload.get("spec_class")
    if spec_class not in set(policy.get("allowed_spec_classes", [])):
        return f"routing_class_not_allowed:{spec_class}"
    return None


def close_lifecycle_debt(repo: Path, spec: Path, policy: dict[str, Any]) -> dict[str, Any]:
    if not policy.get("commit_policy", {}).get("lifecycle_closeout_commit", False):
        return {
            "allowed": False,
            "verdict": "BLOCKED_POLICY",
            "spec": spec.name,
            "reason": "lifecycle_closeout_commit_disabled",
        }
    policy_error = validate_lifecycle_policy(spec, policy)
    if policy_error:
        return {"allowed": False, "verdict": "BLOCKED_POLICY", "spec": spec.name, "reason": policy_error}
    resolver = repo / "tools" / "lifecycle-resolver" / "resolver.py"
    if not resolver.is_file():
        return {
            "allowed": False,
            "verdict": "BLOCKED_LIFECYCLE_RESOLVER_MISSING",
            "spec": spec.name,
        }

    head_before = git_head(repo)
    receipt: Path | None = None
    done_path = repo / "specs" / "done" / spec.name
    proc = run(
        [
            sys.executable,
            str(resolver),
            "done",
            "--spec",
            str(spec),
            "--done-dir",
            str(repo / "specs" / "done"),
            "--evidence-dir",
            str(repo / "evidence" / "lifecycle-resolver"),
            "--ai-root",
            str(repo),
        ],
        repo,
    )
    if proc.returncode != 0:
        rollback_lifecycle_mutation(repo, spec, done_path, receipt, head_before)
        return {
            "allowed": False,
            "verdict": "BLOCKED_RECEIPT_FAILURE",
            "spec": spec.name,
            "stdout": proc.stdout[-1000:],
            "stderr": proc.stderr[-1000:],
        }
    try:
        payload = json.loads(proc.stdout)
        receipt = Path(payload["receipt"])
    except (json.JSONDecodeError, KeyError):
        rollback_lifecycle_mutation(repo, spec, done_path, receipt, head_before)
        return {
            "allowed": False,
            "verdict": "BLOCKED_RECEIPT_FAILURE",
            "spec": spec.name,
            "reason": "resolver stdout did not include receipt",
        }
    receipt_error = validate_lifecycle_receipt(receipt, spec.stem)
    if receipt_error:
        rollback_lifecycle_mutation(repo, spec, done_path, receipt, head_before)
        return {"allowed": False, "verdict": "BLOCKED_RECEIPT_FAILURE", "reason": receipt_error}

    done_frontmatter_error = validate_frontmatter_tool(repo, done_path)
    if done_frontmatter_error:
        rollback_lifecycle_mutation(repo, spec, done_path, receipt, head_before)
        return {"allowed": False, "verdict": "BLOCKED_RECEIPT_FAILURE", "reason": done_frontmatter_error}

    add_proc = run(["git", "add", "-A", "--", "specs/queue", "specs/done"], repo)
    if add_proc.returncode != 0:
        rollback_lifecycle_mutation(repo, spec, done_path, receipt, head_before)
        return {"allowed": False, "verdict": "BLOCKED_COMMIT_FAILURE", "stderr": add_proc.stderr[-1000:]}
    receipt_add = run(["git", "add", "-f", "--", str(receipt.relative_to(repo))], repo)
    if receipt_add.returncode != 0:
        rollback_lifecycle_mutation(repo, spec, done_path, receipt, head_before)
        return {"allowed": False, "verdict": "BLOCKED_COMMIT_FAILURE", "stderr": receipt_add.stderr[-1000:]}
    expected_staged = sorted([
        str(spec.relative_to(repo)),
        str(done_path.relative_to(repo)),
        str(receipt.relative_to(repo)),
    ])
    staged = git_staged_touched_paths(repo)
    if staged != expected_staged:
        rollback_lifecycle_mutation(repo, spec, done_path, receipt, head_before)
        return {
            "allowed": False,
            "verdict": "BLOCKED_COMMIT_FAILURE",
            "reason": "staged set mismatch",
            "expected_staged": expected_staged,
            "staged_paths": staged,
        }
    check = run(["git", "diff", "--cached", "--check"], repo)
    if check.returncode != 0:
        rollback_lifecycle_mutation(repo, spec, done_path, receipt, head_before)
        return {"allowed": False, "verdict": "BLOCKED_PROOF_FAILURE", "stderr": check.stderr[-1000:]}
    number = spec.name.split("-", 1)[0]
    commit = run(["git", "commit", "-m", f"chore(specs): close {number} lifecycle"], repo)
    if commit.returncode != 0:
        rollback_lifecycle_mutation(repo, spec, done_path, receipt, head_before)
        return {"allowed": False, "verdict": "BLOCKED_COMMIT_FAILURE", "stderr": commit.stderr[-1000:]}
    post_session = run_post_session(repo, policy)
    if post_session:
        return {
            "allowed": False,
            "verdict": "BLOCKED_POST_SESSION_FAILURE",
            "spec": spec.name,
            "commit": git_head(repo),
            "post_session": post_session,
        }
    return {
        "allowed": True,
        "verdict": "LIFECYCLE_CLOSED",
        "action": "scoped_lifecycle_closeout_commit",
        "spec": spec.name,
        "receipt": str(receipt),
        "commit": git_head(repo),
    }


def validate_lifecycle_policy(spec: Path, policy: dict[str, Any]) -> str | None:
    fm = parse_frontmatter(spec)
    if fm.get("autopilot_eligible") is not True:
        return "autopilot_eligible_not_true"
    spec_class = str(fm.get("class") or "").strip()
    if spec_class not in set(policy.get("allowed_lifecycle_closeout_classes", [])):
        return f"class_not_allowed:{spec_class or '<missing>'}"
    return None


def validate_lifecycle_receipt(receipt: Path, expected_spec_id: str) -> str | None:
    if not receipt.is_file():
        return f"receipt missing: {receipt}"
    try:
        payload = json.loads(receipt.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return f"receipt invalid json: {exc}"
    required = {"schema_version", "transition", "spec_id", "moved_from", "moved_to", "committed_sha"}
    missing = sorted(required - set(payload))
    if missing:
        return f"receipt missing keys: {missing}"
    if payload.get("schema_version") != "lifecycle-resolver-v1":
        return "receipt schema_version mismatch"
    if payload.get("transition") != "done":
        return "receipt transition mismatch"
    if payload.get("spec_id") != expected_spec_id:
        return "receipt spec_id mismatch"
    receipt_root = receipt.parent.name
    if receipt_root != "lifecycle-resolver":
        return "receipt outside lifecycle-resolver evidence dir"
    moved_from = str(payload.get("moved_from", ""))
    moved_to = str(payload.get("moved_to", ""))
    if "/specs/queue/" not in moved_from:
        return "receipt moved_from is not queue path"
    if "/specs/done/" not in moved_to:
        return "receipt moved_to is not done path"
    return None


def promote_scoped_implementation(
    repo: Path,
    spec: Path,
    dirty_paths: list[str],
    policy: dict[str, Any],
) -> dict[str, Any]:
    if not policy.get("commit_policy", {}).get("implementation_commit", False):
        return {
            "allowed": False,
            "verdict": "BLOCKED_POLICY",
            "spec": spec.name,
            "reason": "implementation_commit_disabled",
        }
    proofs = extract_verification_commands(spec, repo)
    if not proofs:
        return {"allowed": False, "verdict": "BLOCKED_PROOF_FAILURE", "reason": "no safe proof commands"}
    proof_results = []
    dirty_before = git_dirty_paths(repo)
    for cwd, command in proofs:
        proc = run(shlex.split(command), cwd, timeout=180)
        proof_results.append({"cwd": str(cwd), "command": command, "returncode": proc.returncode})
        if proc.returncode != 0:
            return {
                "allowed": False,
                "verdict": "BLOCKED_PROOF_FAILURE",
                "spec": spec.name,
                "proofs": proof_results,
                "stdout": proc.stdout[-1000:],
                "stderr": proc.stderr[-1000:],
            }
    dirty_after = git_dirty_paths(repo)
    if dirty_after != dirty_before:
        return {
            "allowed": False,
            "verdict": "BLOCKED_PROOF_SIDE_EFFECT",
            "spec": spec.name,
            "dirty_before": dirty_before,
            "dirty_after": dirty_after,
            "proofs": proof_results,
        }
    add_proc = run(["git", "add", "--", *dirty_paths], repo)
    if add_proc.returncode != 0:
        return {"allowed": False, "verdict": "BLOCKED_COMMIT_FAILURE", "stderr": add_proc.stderr[-1000:]}
    staged = git_staged_paths(repo)
    if staged != sorted(dirty_paths):
        return {
            "allowed": False,
            "verdict": "BLOCKED_COMMIT_FAILURE",
            "reason": "staged set mismatch",
            "expected_staged": sorted(dirty_paths),
            "staged_paths": staged,
        }
    check = run(["git", "diff", "--cached", "--check"], repo)
    if check.returncode != 0:
        return {"allowed": False, "verdict": "BLOCKED_PROOF_FAILURE", "stderr": check.stderr[-1000:]}
    spec_id = parse_frontmatter(spec).get("spec_id") or spec.stem
    commit = run(["git", "commit", "-m", f"{spec_id}: autonomous implementation commit"], repo)
    if commit.returncode != 0:
        return {"allowed": False, "verdict": "BLOCKED_COMMIT_FAILURE", "stderr": commit.stderr[-1000:]}
    post_session = run_post_session(repo, policy)
    if post_session:
        return {
            "allowed": False,
            "verdict": "BLOCKED_POST_SESSION_FAILURE",
            "spec": spec.name,
            "commit": git_head(repo),
            "post_session": post_session,
        }
    return {
        "allowed": True,
        "verdict": "IMPLEMENTATION_COMMITTED",
        "action": "scoped_implementation_commit",
        "spec": spec.name,
        "dirty_paths": dirty_paths,
        "proofs": proof_results,
        "commit": git_head(repo),
    }


def extract_verification_commands(spec: Path, repo: Path) -> list[tuple[Path, str]]:
    text = spec.read_text(encoding="utf-8")
    commands: list[tuple[Path, str]] = []
    in_block = False
    cwd = repo.resolve()
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("```"):
            in_block = not in_block
            cwd = repo.resolve()
            continue
        if not in_block:
            continue
        if stripped.startswith("cd "):
            target = Path(shlex.split(stripped, comments=False)[1]).expanduser()
            if not target.is_absolute():
                target = cwd / target
            try:
                resolved = target.resolve()
                resolved.relative_to(repo.resolve())
            except (IndexError, ValueError, OSError):
                return []
            cwd = resolved
            continue
        if is_safe_proof_command(stripped):
            commands.append((cwd, stripped))
    return commands


def is_safe_proof_command(command: str) -> bool:
    try:
        parts = shlex.split(command, comments=False)
    except ValueError:
        return False
    if parts in (["git", "diff", "--check"], ["git", "diff", "--cached", "--check"]):
        return True
    if len(parts) < 3 or parts[0] != "python3":
        return False
    if parts[1:3] == ["-m", "unittest"]:
        return len(parts) >= 4 and parts[3] == "discover" and command_targets_tests(parts[4:])
    if parts[1:3] == ["-m", "pytest"]:
        return command_targets_tests(parts[3:])
    tool = parts[1] if len(parts) > 1 else ""
    if tool in SAFE_TOOL_PROOFS:
        return True
    return False


def command_targets_tests(args: list[str]) -> bool:
    saw_test_target = False
    skip_next = False
    for index, arg in enumerate(args):
        if skip_next:
            skip_next = False
            continue
        if arg in UNITTEST_VALUE_OPTIONS:
            if index + 1 >= len(args):
                return False
            value = args[index + 1]
            if arg in UNITTEST_PATH_OPTIONS and not is_safe_test_root(value):
                return False
            if arg in {"-s", "--start-directory"}:
                saw_test_target = True
            skip_next = True
            continue
        if arg.startswith("-"):
            continue
        target = arg.split("::", 1)[0]
        if is_safe_test_root(target):
            saw_test_target = True
            continue
        return False
    return saw_test_target


def is_safe_test_root(target: str) -> bool:
    if ".." in Path(target).parts:
        return False
    return any(target == root or target.startswith(f"{root}/") for root in SAFE_TEST_ROOTS)


def run_post_session(repo: Path, policy: dict[str, Any]) -> dict[str, Any] | None:
    if not policy.get("require_post_session", True):
        return None
    validator = Path.home() / ".codex" / "validators" / "post_session.py"
    if not validator.is_file():
        return {"reason": "post_session_validator_missing"}
    proc = run([sys.executable, str(validator), str(repo)], repo, timeout=180)
    if proc.returncode == 0:
        return None
    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout[-1000:],
        "stderr": proc.stderr[-1000:],
    }


def rollback_lifecycle_mutation(
    repo: Path,
    spec: Path,
    done_path: Path,
    receipt: Path | None,
    head_before: str | None,
) -> None:
    if head_before:
        run(["git", "reset", "--hard", head_before], repo, timeout=30)
    if done_path.exists():
        try:
            done_path.unlink()
        except OSError:
            pass
    if receipt is not None and receipt.exists():
        try:
            receipt.relative_to(repo / "evidence" / "lifecycle-resolver")
        except ValueError:
            return
        try:
            receipt.unlink()
        except OSError:
            pass


def launch_stage(repo: Path, policy: dict[str, Any], skipped_debts: list[dict[str, Any]]) -> dict[str, Any]:
    """Run the bounded launch stage after reconciliation finds no eligible closeout."""
    if not policy.get("launch_enabled", True):
        receipt = write_receipt(
            repo,
            "closeout",
            {"verdict": "NO_ACTION", "reason": "launch_disabled", "skipped_lifecycle_debt": skipped_debts},
            policy,
        )
        return {
            "allowed": True,
            "verdict": "NO_ACTION",
            "repo": str(repo),
            "skipped_lifecycle_debt": skipped_debts,
            "receipts": [str(receipt)],
        }

    dirty = git_dirty_paths(repo)
    if dirty:
        receipt = write_receipt(
            repo,
            "closeout",
            {"verdict": "BLOCKED_DIRTY_REPO", "dirty_paths": dirty, "skipped_lifecycle_debt": skipped_debts},
            policy,
        )
        return {
            "allowed": False,
            "verdict": "BLOCKED_DIRTY_REPO",
            "dirty_paths": dirty,
            "skipped_lifecycle_debt": skipped_debts,
            "receipts": [str(receipt)],
        }

    dry_run = run_fire_next_dry_run(repo)
    if dry_run["returncode"] != 0:
        receipt = write_receipt(
            repo,
            "closeout",
            {
                "verdict": "NO_DISPATCHABLE_CANDIDATE",
                "fire_next": dry_run,
                "skipped_lifecycle_debt": skipped_debts,
            },
            policy,
        )
        return {
            "allowed": True,
            "verdict": "NO_DISPATCHABLE_CANDIDATE",
            "fire_next": dry_run,
            "skipped_lifecycle_debt": skipped_debts,
            "receipts": [str(receipt)],
        }

    spec_id = str(dry_run["payload"].get("spec_id") or "").strip()
    if dry_run["payload"].get("verdict") != "DRY_RUN" or not spec_id:
        receipt = write_receipt(
            repo,
            "closeout",
            {
                "verdict": "BLOCKED_FIRE_NEXT_INVALID",
                "fire_next": dry_run,
                "skipped_lifecycle_debt": skipped_debts,
            },
            policy,
        )
        return {
            "allowed": False,
            "verdict": "BLOCKED_FIRE_NEXT_INVALID",
            "fire_next": dry_run,
            "skipped_lifecycle_debt": skipped_debts,
            "receipts": [str(receipt)],
        }

    baseline = run_baseline(repo, policy)
    if baseline["returncode"] != 0:
        receipt = write_receipt(
            repo,
            "closeout",
            {
                "verdict": "BLOCKED_BASELINE_REGRESSION",
                "spec_id": spec_id,
                "baseline": baseline,
                "skipped_lifecycle_debt": skipped_debts,
            },
            policy,
        )
        return {
            "allowed": False,
            "verdict": "BLOCKED_BASELINE_REGRESSION",
            "spec_id": spec_id,
            "baseline": baseline,
            "skipped_lifecycle_debt": skipped_debts,
            "receipts": [str(receipt)],
        }

    pressure = run_fleet_launch_guard(repo, policy)
    if pressure["returncode"] != 0 or pressure["payload"].get("allowed") is not True:
        pressure_receipt = write_receipt(
            repo,
            "pressure",
            {
                "verdict": "BLOCKED_FLEET_PRESSURE",
                "spec_id": spec_id,
                "fleet_guard": pressure,
                "skipped_lifecycle_debt": skipped_debts,
            },
            policy,
        )
        closeout_receipt = write_receipt(
            repo,
            "closeout",
            {
                "verdict": "BLOCKED_FLEET_PRESSURE",
                "spec_id": spec_id,
                "pressure_receipt": str(pressure_receipt),
                "skipped_lifecycle_debt": skipped_debts,
            },
            policy,
        )
        return {
            "allowed": False,
            "verdict": "BLOCKED_FLEET_PRESSURE",
            "spec_id": spec_id,
            "fleet_guard": pressure,
            "baseline": baseline,
            "skipped_lifecycle_debt": skipped_debts,
            "receipts": [str(pressure_receipt), str(closeout_receipt)],
        }

    monitored_path_error = validate_monitored_dispatch_path(repo)
    if monitored_path_error:
        closeout_receipt = write_receipt(
            repo,
            "closeout",
            {
                "verdict": "BLOCKED_MONITORED_PATH_MISSING",
                "spec_id": spec_id,
                "reason": monitored_path_error,
                "skipped_lifecycle_debt": skipped_debts,
            },
            policy,
        )
        return {
            "allowed": False,
            "verdict": "BLOCKED_MONITORED_PATH_MISSING",
            "spec_id": spec_id,
            "reason": monitored_path_error,
            "receipts": [str(closeout_receipt)],
        }

    launch = run_monitored_local_launch(repo, spec_id, policy)
    launch_receipt = write_receipt(
        repo,
        "launch",
        {
            "verdict": "LAUNCH_ATTEMPTED",
            "spec_id": spec_id,
            "launch": launch,
            "baseline": baseline,
            "fleet_guard": pressure,
            "skipped_lifecycle_debt": skipped_debts,
        },
        policy,
    )
    closeout_receipt = write_receipt(
        repo,
        "closeout",
        {
            "verdict": "LAUNCH_COMPLETE" if launch["returncode"] == 0 else "LAUNCH_FAILED",
            "spec_id": spec_id,
            "launch_receipt": str(launch_receipt),
            "skipped_lifecycle_debt": skipped_debts,
        },
        policy,
    )
    return {
        "allowed": launch["returncode"] == 0,
        "verdict": "LAUNCH_COMPLETE" if launch["returncode"] == 0 else "LAUNCH_FAILED",
        "spec_id": spec_id,
        "fire_next": dry_run,
        "baseline": baseline,
        "fleet_guard": pressure,
        "launch": launch,
        "skipped_lifecycle_debt": skipped_debts,
        "receipts": [str(launch_receipt), str(closeout_receipt)],
    }


def run_fire_next_dry_run(repo: Path) -> dict[str, Any]:
    cli = repo / "scripts" / "autopilot_cli.py"
    if not cli.is_file():
        return {"returncode": 2, "payload": {"verdict": "BLOCKED", "reason": "autopilot_cli_missing"}}
    proc = run([sys.executable, str(cli), "fire-next", "--dry-run"], repo, timeout=60)
    return completed_json(proc)


def run_baseline(repo: Path, policy: dict[str, Any]) -> dict[str, Any]:
    command = policy.get("baseline_command")
    if isinstance(command, str):
        cmd = shlex.split(command)
    elif isinstance(command, list) and all(isinstance(item, str) for item in command):
        cmd = command
    else:
        cmd = default_baseline_command(repo)
    if not cmd:
        return {"returncode": 0, "command": [], "stdout_tail": "", "stderr_tail": "", "skipped": True}
    proc = run(cmd, repo, timeout=int(policy.get("baseline_timeout_seconds", 300)))
    payload = completed_json(proc)
    payload["command"] = cmd
    return payload


def default_baseline_command(repo: Path) -> list[str]:
    if (repo / "tests").is_dir():
        return [sys.executable, "-m", "unittest", "discover", "-s", "tests"]
    return []


def run_fleet_launch_guard(repo: Path, policy: dict[str, Any]) -> dict[str, Any]:
    configured = policy.get("fleet_guard_command")
    if isinstance(configured, list) and all(isinstance(item, str) for item in configured):
        cmd = configured
    else:
        cmd = [
            "fleet",
            "guard",
            "--json",
            "--repo",
            str(repo),
            "--gpu",
            str(int(policy.get("launch_gpu_mb", 8192))),
        ]
    try:
        proc = run(cmd, repo, timeout=30)
    except FileNotFoundError:
        return {"returncode": 2, "payload": {"allowed": False, "reason": "fleet_cli_missing"}}
    return completed_json(proc)


def validate_monitored_dispatch_path(repo: Path) -> str | None:
    required = [
        repo / "tools" / "cost-aware-autopilot" / "cycle.py",
        repo / "tools" / "cost-aware-autopilot" / "dispatch_opencode.py",
        repo / "scripts" / "offline_coder_subprocess_monitor.py",
    ]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        return "missing:" + ",".join(missing)
    try:
        text = (repo / "tools" / "cost-aware-autopilot" / "dispatch_opencode.py").read_text(encoding="utf-8")
    except OSError as exc:
        return f"dispatch_opencode_unreadable:{exc}"
    if "offline_coder_subprocess_monitor.py" not in text:
        return "dispatch_opencode_not_monitor_wrapped"
    return None


def run_monitored_local_launch(repo: Path, spec_id: str, policy: dict[str, Any]) -> dict[str, Any]:
    cycle = repo / "tools" / "cost-aware-autopilot" / "cycle.py"
    command = [
        sys.executable,
        str(cycle),
        "--ai-root",
        str(repo),
        "--mode",
        "local-only",
        "--target-spec",
        spec_id,
    ]
    env = {
        "COST_AWARE_MODE": "local-only",
        "COST_AWARE_AUTO_COMMIT": "1" if policy.get("commit_policy", {}).get("implementation_commit", False) else "0",
        "COST_AWARE_AUTO_COMMIT_CRAFT_SKIP_REASON": "deterministic_micro_skip",
    }
    proc = run_env(command, repo, timeout=int(policy.get("launch_timeout_seconds", 1200)), extra_env=env)
    payload = completed_json(proc)
    payload["command"] = command
    return payload


def completed_json(proc: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    try:
        payload = json.loads(proc.stdout)
    except (TypeError, json.JSONDecodeError):
        payload = {
            "stdout_tail": (proc.stdout or "")[-1000:],
            "stderr_tail": (proc.stderr or "")[-1000:],
        }
    return {
        "returncode": proc.returncode,
        "payload": payload,
        "stdout_tail": (proc.stdout or "")[-1000:],
        "stderr_tail": (proc.stderr or "")[-1000:],
    }


def git_head(repo: Path) -> str | None:
    proc = run(["git", "rev-parse", "HEAD"], repo)
    return proc.stdout.strip() if proc.returncode == 0 else None
