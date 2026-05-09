import json
import subprocess
import sys
from pathlib import Path

from fleet_watch import autonomous


def run_git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=False)


def init_repo(repo: Path) -> None:
    repo.mkdir()
    run_git(repo, "init")
    run_git(repo, "config", "user.email", "tests@example.invalid")
    run_git(repo, "config", "user.name", "Fleet Watch Tests")
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    run_git(repo, "add", "README.md")
    run_git(repo, "commit", "-m", "base")


def write_policy(repo: Path) -> Path:
    policy = repo / "policy.json"
    policy.write_text(json.dumps({"require_post_session": False}) + "\n", encoding="utf-8")
    return policy


def write_policy_payload(repo: Path, payload: dict) -> Path:
    policy = repo / "policy.json"
    policy.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    return policy


def write_fake_frontmatter_validator(repo: Path) -> None:
    target = repo / "tools" / "validate-spec-frontmatter.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        """#!/usr/bin/env python3
import json
print(json.dumps({
  "schema_version": "validate-spec-frontmatter/1",
  "results": [{"spec": "test.md", "findings": [], "grandfathered": []}],
  "summary": {"specs": 1, "blocked": 0, "warn": 0, "grandfathered": 0}
}))
""",
        encoding="utf-8",
    )


def write_fake_routing(repo: Path) -> None:
    target = repo / "tools" / "cost-aware-autopilot" / "routing.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        """#!/usr/bin/env python3
import json
print(json.dumps({
  "executor": "opencode",
  "gate_failures": [],
  "metadata": {"scope_files_count": 1, "spec_class": "meta_tooling"},
  "spec_class": "meta_tooling"
}))
""",
        encoding="utf-8",
    )


def write_fake_lifecycle_resolver(repo: Path) -> None:
    target = repo / "tools" / "lifecycle-resolver" / "resolver.py"
    target.parent.mkdir(parents=True)
    target.write_text(
        """#!/usr/bin/env python3
import argparse, json, shutil
from pathlib import Path

parser = argparse.ArgumentParser()
sub = parser.add_subparsers(dest="cmd")
done = sub.add_parser("done")
done.add_argument("--spec", required=True)
done.add_argument("--done-dir", required=True)
done.add_argument("--evidence-dir", required=True)
done.add_argument("--ai-root", required=True)
args = parser.parse_args()
spec = Path(args.spec)
done_dir = Path(args.done_dir); done_dir.mkdir(parents=True, exist_ok=True)
target = done_dir / spec.name
shutil.move(str(spec), str(target))
receipt_dir = Path(args.evidence_dir); receipt_dir.mkdir(parents=True, exist_ok=True)
number = spec.name.split("-", 1)[0]
receipt = receipt_dir / f"20260508T000000Z-{number}-done.json"
payload = {
  "schema_version": "lifecycle-resolver-v1",
  "transition": "done",
  "spec_id": target.stem,
  "spec_number": number,
  "moved_from": str(spec),
  "moved_to": str(target),
  "committed_sha": "base",
  "generated_at_utc": "2026-05-08T00:00:00Z"
}
receipt.write_text(json.dumps(payload) + "\\n", encoding="utf-8")
print(json.dumps({"receipt": str(receipt), **payload}))
""",
        encoding="utf-8",
    )


def test_autonomous_once_closes_lifecycle_debt(tmp_path):
    repo = tmp_path / "repo"
    init_repo(repo)
    policy = write_policy(repo)
    write_fake_frontmatter_validator(repo)
    write_fake_lifecycle_resolver(repo)
    queue = repo / "specs" / "queue"
    queue.mkdir(parents=True)
    (repo / "specs" / "done").mkdir(parents=True)
    spec = queue / "315-local-cloud-handoff-schema-slice-h.md"
    spec.write_text(
        """---
spec_id: 315-local-cloud-handoff-schema-slice-h
route: opencode
worker: opencode
class: deterministic_parser
autopilot_eligible: true
status: STAGE1_IMPLEMENTED_TESTED
claim_state: implemented+tested
allowed_write_paths:
  - tools/handoff-validator/validate.py
---
""",
        encoding="utf-8",
    )
    run_git(repo, "add", ".")
    run_git(repo, "commit", "-m", "seed lifecycle debt")

    result = autonomous.run_once(repo, policy)

    assert result["verdict"] == "LIFECYCLE_CLOSED"
    assert result["commit"]
    assert not spec.exists()
    assert (repo / "specs" / "done" / spec.name).is_file()
    assert run_git(repo, "status", "--porcelain").stdout == ""


def test_autonomous_respects_disabled_lifecycle_closeout_policy(tmp_path):
    repo = tmp_path / "repo"
    init_repo(repo)
    policy = write_policy_payload(
        repo,
        {
            "require_post_session": False,
            "commit_policy": {"lifecycle_closeout_commit": False},
        },
    )
    write_fake_frontmatter_validator(repo)
    write_fake_lifecycle_resolver(repo)
    queue = repo / "specs" / "queue"
    queue.mkdir(parents=True)
    (repo / "specs" / "done").mkdir(parents=True)
    spec = queue / "315-local-cloud-handoff-schema-slice-h.md"
    spec.write_text(
        """---
spec_id: 315-local-cloud-handoff-schema-slice-h
route: opencode
worker: opencode
class: deterministic_parser
autopilot_eligible: true
status: STAGE1_IMPLEMENTED_TESTED
claim_state: implemented+tested
allowed_write_paths:
  - tools/handoff-validator/validate.py
---
""",
        encoding="utf-8",
    )
    run_git(repo, "add", ".")
    run_git(repo, "commit", "-m", "seed lifecycle debt")

    result = autonomous.run_once(repo, policy)

    assert result["verdict"] == "BLOCKED_POLICY"
    assert result["reason"] == "lifecycle_closeout_commit_disabled"
    assert spec.is_file()
    assert not (repo / "specs" / "done" / spec.name).exists()
    assert run_git(repo, "status", "--porcelain").stdout == ""


def test_autonomous_promotes_scoped_dirty_implementation(tmp_path):
    repo = tmp_path / "repo"
    init_repo(repo)
    policy = write_policy(repo)
    write_fake_frontmatter_validator(repo)
    write_fake_routing(repo)
    queue = repo / "specs" / "queue"
    queue.mkdir(parents=True)
    tool = repo / "tools" / "demo.py"
    tool.parent.mkdir(exist_ok=True)
    tool.write_text("VALUE = 1\n", encoding="utf-8")
    spec = queue / "316-demo.md"
    spec.write_text(
        f"""---
spec_id: 316-demo
route: opencode
worker: opencode
class: meta_tooling
autopilot_eligible: true
status: STAGE1_IMPLEMENTED_TESTED
claim_state: implemented+tested
scope_files:
  - tools/demo.py
allowed_write_paths:
  - tools/demo.py
---

## Verification

```bash
{sys.executable} -m unittest discover -s tests -v
git diff --check
```
""".replace(str(sys.executable), "python3"),
        encoding="utf-8",
    )
    tests = repo / "tests"
    tests.mkdir()
    (tests / "test_demo.py").write_text(
        "import unittest\n\nclass DemoTests(unittest.TestCase):\n    def test_true(self):\n        self.assertTrue(True)\n",
        encoding="utf-8",
    )
    run_git(repo, "add", ".")
    run_git(repo, "commit", "-m", "seed implementation spec")
    tool.write_text("VALUE = 2\n", encoding="utf-8")

    result = autonomous.run_once(repo, policy)

    assert result["verdict"] == "IMPLEMENTATION_COMMITTED"
    assert result["dirty_paths"] == ["tools/demo.py"]
    assert run_git(repo, "status", "--porcelain").stdout == ""


def test_autonomous_respects_disabled_implementation_commit_policy(tmp_path):
    repo = tmp_path / "repo"
    init_repo(repo)
    policy = write_policy_payload(
        repo,
        {
            "require_post_session": False,
            "commit_policy": {"implementation_commit": False},
        },
    )
    write_fake_frontmatter_validator(repo)
    write_fake_routing(repo)
    queue = repo / "specs" / "queue"
    queue.mkdir(parents=True)
    tool = repo / "tools" / "demo.py"
    tool.parent.mkdir(exist_ok=True)
    tool.write_text("VALUE = 1\n", encoding="utf-8")
    spec = queue / "316-demo.md"
    spec.write_text(
        """---
spec_id: 316-demo
route: opencode
worker: opencode
class: meta_tooling
autopilot_eligible: true
status: STAGE1_IMPLEMENTED_TESTED
claim_state: implemented+tested
scope_files:
  - tools/demo.py
allowed_write_paths:
  - tools/demo.py
---

## Verification

```bash
python3 -m unittest discover -s tests -v
git diff --check
```
""",
        encoding="utf-8",
    )
    tests = repo / "tests"
    tests.mkdir()
    (tests / "test_demo.py").write_text(
        "import unittest\n\nclass DemoTests(unittest.TestCase):\n    def test_true(self):\n        self.assertTrue(True)\n",
        encoding="utf-8",
    )
    run_git(repo, "add", ".")
    run_git(repo, "commit", "-m", "seed implementation spec")
    tool.write_text("VALUE = 2\n", encoding="utf-8")

    result = autonomous.run_once(repo, policy)

    assert result["verdict"] == "BLOCKED_POLICY"
    assert result["reason"] == "implementation_commit_disabled"
    assert run_git(repo, "status", "--porcelain").stdout == " M tools/demo.py\n"


def test_autonomous_rejects_unrouted_dirty_candidate(tmp_path):
    repo = tmp_path / "repo"
    init_repo(repo)
    policy = write_policy(repo)
    write_fake_frontmatter_validator(repo)
    queue = repo / "specs" / "queue"
    queue.mkdir(parents=True)
    tool = repo / "tools" / "demo.py"
    tool.parent.mkdir(exist_ok=True)
    tool.write_text("VALUE = 1\n", encoding="utf-8")
    (queue / "316-demo.md").write_text(
        """---
spec_id: 316-demo
route: opencode
worker: opencode
class: meta_tooling
autopilot_eligible: true
status: STAGE1_IMPLEMENTED_TESTED
claim_state: implemented+tested
scope_files:
  - tools/demo.py
allowed_write_paths:
  - tools/demo.py
---

## Verification

```bash
python3 -m unittest discover -s tests -v
```
""",
        encoding="utf-8",
    )
    run_git(repo, "add", ".")
    run_git(repo, "commit", "-m", "seed")
    tool.write_text("VALUE = 2\n", encoding="utf-8")

    result = autonomous.run_once(repo, policy)

    assert result["verdict"] == "BLOCKED_SCOPE_VIOLATION"
    assert result["candidate_errors"][0]["errors"] == ["routing_tool_missing"]


def test_autonomous_blocks_proof_side_effect(tmp_path):
    repo = tmp_path / "repo"
    init_repo(repo)
    policy = write_policy(repo)
    write_fake_frontmatter_validator(repo)
    write_fake_routing(repo)
    queue = repo / "specs" / "queue"
    queue.mkdir(parents=True)
    tool = repo / "tools" / "demo.py"
    tool.parent.mkdir(exist_ok=True)
    tool.write_text("VALUE = 1\n", encoding="utf-8")
    spec = queue / "316-demo.md"
    spec.write_text(
        """---
spec_id: 316-demo
route: opencode
worker: opencode
class: meta_tooling
autopilot_eligible: true
status: STAGE1_IMPLEMENTED_TESTED
claim_state: implemented+tested
scope_files:
  - tools/demo.py
allowed_write_paths:
  - tools/demo.py
---

## Verification

```bash
python3 -m unittest discover -s tests -v
```
""",
        encoding="utf-8",
    )
    tests = repo / "tests"
    tests.mkdir()
    (tests / "test_side_effect.py").write_text(
        "import pathlib, unittest\n\n"
        "class SideEffectTests(unittest.TestCase):\n"
        "    def test_writes_file(self):\n"
        "        pathlib.Path('side_effect.txt').write_text('bad\\n')\n",
        encoding="utf-8",
    )
    run_git(repo, "add", ".")
    run_git(repo, "commit", "-m", "seed")
    tool.write_text("VALUE = 2\n", encoding="utf-8")

    result = autonomous.run_once(repo, policy)

    assert result["verdict"] == "BLOCKED_PROOF_SIDE_EFFECT"
    assert "side_effect.txt" in result["dirty_after"]


def test_safe_proof_accepts_handoff_validator_unittest_discover_pattern():
    assert autonomous.is_safe_proof_command(
        "python3 -m unittest discover -s tools/handoff-validator -p 'test_*.py' -v"
    )


def test_safe_proof_rejects_unittest_discover_outside_test_roots():
    assert not autonomous.is_safe_proof_command(
        "python3 -m unittest discover -s /tmp -p 'test_*.py' -v"
    )
