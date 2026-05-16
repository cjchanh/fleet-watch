# CLAUDE.md — fleet-watch

## Folder Purpose
Process governance daemon that prevents port, GPU, and repo collisions across
AI workloads on a single machine by maintaining a shared SQLite registry and
exposing a fail-closed `fleet guard --json` decision interface.

## Global Rules Inherited
All rules from ~/.claude/CLAUDE.md apply. This file refines and extends them
for this folder only. On conflict, ~/.claude/CLAUDE.md is authoritative.

## Module Boundaries
- **May import from**: stdlib, `click>=8.0` (only declared dependency)
- **May NOT import from**: any CDS sibling repo (archivist, sovereign-stack,
  hermes-kernel, etc.); no cloud SDKs; no LLM clients
- **Exported API**: `fleet` CLI entry point (`fleet_watch.cli:main`);
  `fleet guard --json` is the canonical machine interface;
  `fleet_watch.registry` and `fleet_watch.referee` are the two internal
  surfaces other repos may read (read-only, no writes from outside)

## Local Invariants
1. `fleet guard --json` must return `{"allowed": false}` whenever Fleet Watch
   is unreachable, the DB is locked, or any required input is unresolvable —
   never silently allow on error (fail-closed; verified by test_adversarial.py
   and test_contracts.py).
2. The `events` table in `registry.db` is append-only: no UPDATE or DELETE
   against it (verified by grepping cli.py and registry.py for
   `DELETE FROM events` or `UPDATE events` — must return zero matches).
3. Session lease expiry must be deterministic: a lease with `status = 'ACTIVE'`
   and `last_heartbeat_at` older than `DEFAULT_STALE_SECONDS` (180s) is treated
   as stale by the referee; no lease may extend its own TTL without a heartbeat
   write (verified by test_referee.py stale-lease cases).
4. No network calls anywhere in `fleet_watch/` (local-only product invariant;
   verified by `grep -r "urllib\|httpx\|requests\|aiohttp" fleet_watch/` —
   must return zero matches).
5. Boot-persistence plist `com.cj.fleet-watch-sync.plist` must reload without
   kernel state corruption: the launchd agent runs `fleet discover` every 60s
   and must not bind ports or mutate OS state (read + DB write only).

## Security Surface
`false`. Fleet Watch holds no key material, performs no cryptographic
operations, and crosses no auth boundary. The SQLite registry stores PIDs,
port numbers, and repo paths — no secrets, tokens, or credentials.
Note: the `SECURITY.md` file in-repo covers responsible disclosure for the
daemon itself; that is a process boundary, not a Rule 2 surface.

## Voice Register
N/A

## Tuesday-Bar Applicability
`true`. Fleet Watch is the enforcement layer for every other CDS repo. If
`fleet guard --json` returns stale, wrong, or fail-open output on a Tuesday,
every downstream agent is ungoverned. Tuesday Bar here means: `fleet guard
--json` produces a deterministic ALLOW/DENY on first invocation with no retry,
and `python3 -m pytest tests -q` passes clean.

## Craft-Gate Dimensions
Primary: `error_paths`, `fail_closed` (guard decision path must be
exhaustive), `edge_cases` (stale leases, DB lock, missing registry).
Secondary: `abstraction_boundaries` (referee vs. cli boundary),
`readability_to_a_stranger` (decision output must be self-explanatory).

`craft-gate-required: true`
`tuesday-bar-required: true`

## Local Fail-Closed Rules
- FAIL-CLOSED: if `fleet guard --json` can raise an unhandled exception (vs.
  returning `{"allowed": false, "reason": "..."}`) on any error path, block
  commit — guard must never propagate exceptions to caller.
- FAIL-CLOSED: if a new guard dimension (port, GPU, repo, write-scope) is
  added to the CLI without a corresponding negative test (DENY path), block
  commit.
- ADVISORY: `DEFAULT_STALE_SECONDS` is 180; any change to this constant
  requires updating test_referee.py stale-threshold fixtures and the
  fleet-watch-ops.md ops doc.

## Test Convention
- Framework: pytest
- Test location: `tests/` at repo root
- Naming: `test_*.py`
- Minimum coverage expectation: every guard decision path (ALLOW + DENY) has
  at least one test; every new CLI subcommand has at least one smoke test in
  test_cli.py; adversarial/contract tests (test_adversarial.py,
  test_contracts.py) must pass before any release tag.

## Naming and Style
Follows ~/.claude/CLAUDE.md Engineering Standards (Python 3.10+, type hints,
pathlib, Black 88). No deviations.

## Cross-Folder Dependencies
- Depends on: `~/.claude/hooks/fleet_guard_hook.py` calling `fleet guard
  --json` — if the JSON output schema changes (key names, nesting), the hook
  breaks silently. Schema changes require coordinated update to the hook.
- Depends on: `~/.fleet-watch/registry.db` schema stability — registry.py
  `SCHEMA` constant is the single source of truth; migrations must be
  backward-compatible or all CDS repos using session leases will break.
