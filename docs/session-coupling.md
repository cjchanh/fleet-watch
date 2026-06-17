# Session Coupling — live single-writer awareness

**Status:** built + tested (2026-06-17). **Module:** `fleet_watch/session_coupling.py`.
**CLI:** `fleet session check --repo <dir> [--me <session_id>] [--json]`.

## Problem
Session leases carry `owner_pid` + `last_heartbeat_at` (liveness) but `repo_dir`
is frequently **null** in practice (sessions don't bind it). So
`get_active_session_leases_by_repo` finds no conflicts, and the single-writer
rule — *only one session may mutate a repo at a time* — is **unenforceable**.
This is the gap behind the recurring multi-writer collisions (two agents
co-mutating the same repo; cross-session mutation landing mid-verify).

The two facts needed to answer *"is another live session about to write my
repo?"* live in two places that never joined:
- **Fleet** knows which PIDs are alive (heartbeat) — but `repo_dir` is null.
- The session's actual repo is recoverable from its **PID's cwd**.

## What this adds (boundary-clean: stdlib + `fleet_watch.registry` only)
A read-only preflight that makes a lease's repo resolvable even when `repo_dir`
is null, and answers single-writer safety:

- `lease_repo(lease)` — declared `repo_dir`, else derived from `owner_pid` cwd → git root.
- `is_live(lease)` — `status==ACTIVE` **and** PID alive **and** heartbeat fresher
  than `DEFAULT_STALE_SECONDS` (180). Heartbeat freshness also defeats PID
  recycling — a recycled PID never heartbeats Fleet.
- `who_is_live(repo)` — live sessions whose resolved repo matches.
- `single_writer_check(repo, me, leases)` → `Verdict`:
  - **ALLOW** (exit 0) — no other live session on the repo.
  - **CONFLICT** (exit 3) — another live session holds it (lists session_id, PID, lock mode).
  - **UNKNOWN** (exit 4) — leases unobtainable or repo unresolvable → **fail-closed**.

## Invariants (Tuesday Bar)
1. **Fail-closed.** Unobtainable leases or unresolvable repo → `UNKNOWN`, never `ALLOW`.
2. **Read-only.** Never opens, refreshes, closes, or mutates a lease.
3. **Liveness is coupled to a live process**, not a self-reported flag — a stale
   or dead lease never produces a false `CONFLICT` (so the check stays low-noise)
   and never hides a real one.
4. **Boundary.** No cross-repo imports, no network (fleet-watch product invariant).

## Out of scope (deliberately, per fleet-watch module boundary)
Joining each conflicting session to *what it's doing* (the cds-session-memory
route/summary) belongs in a consumer **outside** fleet-watch — fleet-watch may
not import a sibling repo. The conflict output already gives PID + repo + lock
mode, which is actionable on its own. The session-memory enrichment is the
optional next layer.

## Verify
```
python3 -m pytest tests/test_session_coupling.py tests/test_cli.py -q
fleet session check --repo "$PWD"      # 0 ALLOW / 3 CONFLICT / 4 UNKNOWN
```

## Wiring it as a pre-write gate (operator-paced, not auto-installed)
A session can gate its own writes by calling `fleet session check --repo <repo>
--me "$FLEET_SESSION_ID"` and refusing to proceed on exit 3. Installing that as
a hook is an operator decision, not done here.
