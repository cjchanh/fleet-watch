# Upgrade report — Fleet Watch

Date: 2026-08-15. Checkout: a local clone on `main`. No push.

## What changed

This checkout was a local process-governance daemon (`fleet guard`, `discover`, `census`) whose README said nothing about GitHub. The missing purpose was a **read-only GitHub fleet sitrep**.

Added `fleet sitrep`:

- Module: `fleet_watch/github_sitrep.py`
- CLI: `fleet sitrep [--json] [--owner LOGIN] [--limit N] [--no-receipt] [--receipt-dir PATH]`
- Tests: `tests/test_github_sitrep.py` plus `test_sitrep_help_is_wired` in `tests/test_cli.py`

Behavior that is now real:

- One `gh api graphql` read query. Argv that is not `gh api graphql`, or that includes `-t` / `--token`, `clone`, `auth token`, or `mutation`, is refused without running `gh`.
- No `git clone` / `gh repo clone`. No SHA is taken from a local working tree.
- This code does not read `GITHUB_TOKEN` / `GH_TOKEN`, does not run `gh auth token`, and does not write credentials into a receipt. Auth stays in the operator's `gh` store (and whatever the operator already put in the process environment).
- `sha` is only a 40- or 64-char hex object id from GitHub `defaultBranchRef.target.oid`. Empty repos, missing oids, and abbreviated values become `sha: null` plus `sha_absent_reason`. Validation rejects a slipped-in short SHA.
- Missing `gh`, a failed query, GraphQL `errors`, or an unknown `--owner` is a `REFUSAL` (exit 1). It does not invent a fleet. A viewer who owns zero repos is a valid empty sitrep (`repo_count: 0`), which is not the same as an unknown owner.
- Receipts go to `~/.governance/receipts/fleet-github-sitrep/` (`fleet-github-sitrep/v1`) unless `--no-receipt`.

Docs that were lying by omission were updated: README lead, Commands, limitations; CHANGELOG; `pyproject.toml` / `__init__.py` description; CLAUDE.md invariant 4 (`fleet sitrep` is the only GitHub-talking path).

## What is still false / incomplete

- Most of this repo is still local process governance. GitHub sitrep is one command on top of that product, not a rewrite of `guard` / `discover` / `census`.
- `fleet sitrep` was **not** run against live GitHub in this upgrade. Tests mock `run_gh`. There is no live SHA in this change.
- Default listing is viewer-**owned** repos, at most `--limit` (default 30, max 100). Collaborator repos are omitted unless `--owner` is used. `truncated: true` means GitHub had another page; this upgrade does not paginate.
- If the operator's environment already contains `GH_TOKEN` / `GITHUB_TOKEN`, `gh` may use it. This process does not inspect those values and does not strip them.
- `AGENTS.md` still claims to be a byte-identical CLAUDE.md and is not (pre-existing).
- Package version is still `0.2.0`; sitrep is Unreleased.
- Full `pytest tests` inside the Cursor sandbox failed on socket bind (`PermissionError: Operation not permitted`) and on `test_autonomous` seeing a dirty worktree. That is the sandbox/worktree, not a sitrep assertion failure.

## How a reviewer should check

Offline (this is what this writer ran):

```bash
cd /Users/cj/Workspace/active/fleet-watch
.venv/bin/python -m pytest tests/test_github_sitrep.py tests/test_cli.py::test_sitrep_help_is_wired tests/test_no_external_egress.py tests/test_census.py tests/test_gpu_estimator.py tests/test_events.py tests/test_boot_map.py -q
```

Expected here: `207 passed in 1.00s`.

Wiring:

```bash
.venv/bin/python -m fleet_watch.cli sitrep --help
```

Must mention clone / invented SHA, and must not call GitHub.

Live GitHub (optional, operator machine, unsandboxed; this writer did not):

```bash
.venv/bin/python -m fleet_watch.cli sitrep --json --no-receipt
```

Check: every `repos[].sha` is 40/64 hex or `null` with `sha_absent_reason`; `clone` is `false`; `tokens_used` is `false`; refusal if `gh` is missing. Compare one `sha` to GitHub's UI for that default branch — do not compare to a local checkout and "fix" it.

Full suite, unsandboxed, after the tree is committed (so autonomous tests are not seeing staged files):

```bash
.venv/bin/python -m pytest tests -q
```
