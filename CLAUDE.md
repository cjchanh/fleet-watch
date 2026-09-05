# CLAUDE.md — fleet-watch

> This file and `AGENTS.md` are byte-identical mirrors; `tests/test_agents_mirror.py` enforces it. Edit CLAUDE.md, then copy it over AGENTS.md.

## Folder Purpose
Process governance daemon that prevents port, GPU, and repo collisions across
AI workloads on a single machine by maintaining a shared SQLite registry and
exposing a fail-closed `fleet guard --json` decision interface. Also exposes
`fleet sitrep`, a read-only GitHub fleet view via the local `gh` CLI: no clone,
no tokens in this repo, no invented SHA.

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
3. Session lease expiry must be deterministic. A lease is stale when
   **(proven owner death) OR (heartbeat TTL expiry)** — two INDEPENDENT
   sufficient triggers, never ANDed:
   - **Proven owner death** releases the lease IMMEDIATELY, independent of
     heartbeat age. Death is proven when the owner PID no longer exists OR its
     kernel create-time no longer matches the create-time recorded at lease
     open (defeating PID reuse — `registry._lease_owner_alive`). A
     provably-dead owner must never hold a repo for up to the TTL.
   - **Heartbeat TTL expiry**: an ownerless (`owner_pid IS NULL`) `ACTIVE`
     lease whose `last_heartbeat_at` is older than `DEFAULT_STALE_SECONDS`
     (180s) is stale. A null-PID lease with a FRESH heartbeat keeps blocking
     (conservative, fail-closed).
   No lease may extend its own TTL without a heartbeat write. Release happens
   only on PROVEN death or DEFINITE TTL timeout — never on a guess; any error
   in liveness inspection degrades to "still alive / keep blocking", never
   fail-open (verified by test_referee.py stale-lease cases and
   test_path_c_lease_liveness.py).
4. No NON-LOOPBACK network calls anywhere in `fleet_watch/` except one
   command-scoped path: `fleet sitrep` (`fleet_watch/github_sitrep.py`) talks to
   GitHub only by subprocessing `gh api graphql`. It does not clone, does not
   read or inject tokens, and does not use `requests`/`httpx`/`urllib`. Guard,
   discover, census, and health stay zero-egress. Loopback probes to local
   runtimes (`127.0.0.1`/`localhost`/`::1`, e.g. the Ollama orphan-runner check
   in `discover.py` / `discovery/orphan_detector.py`) are permitted. Enforced by
   `tests/test_no_external_egress.py` plus `tests/test_github_sitrep.py`.
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
