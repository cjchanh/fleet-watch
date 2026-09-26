# Fleet session identity repair — result report

Mission: correct session census identity without relaxing authorization.
Base: `e36fea8efd81f539ab2fa5fc56a379c8bcfc2845` · tree: `/Users/cj/Workspace/active/fleet-watch`
Incident: `fleet-session-recovery-20260926` (the 15:10 Devin dispatch was sandbox-refused
over 13,306 hardlinked files; this run executed on the ox lane against the admitted primary
tree, same write scope, same brief).

## Final behavior

1. **Census labels by executable identity, not by argument mentions.**
   Roster patterns carry `match_scope: "identity"` and are matched against a
   *census identity window*: `argv[0]` + flag **names** (inline values stripped at `=`)
   + the interpreter's script token (`node /opt/homebrew/bin/codex …` stays a Codex
   session). Argument values never enter the window, so a `/codex-…` path inside a
   Devin prompt-file argument, a backup tool's `--exclude` value, or a shell snapshot
   can no longer relabel the process. Custom config patterns are untouched: they keep
   whole-command semantics (that API intentionally matches arbitrary commands).

2. **Devin is in the roster** (`kind: devin`, `binary: devin`), which is strictly
   stronger for authorization: `_agent_runtime_in_ancestry` now recognizes Devin runs,
   so the operator-seat arm can no longer admit one. The census side labels Devin `devin`.

3. **The family walk is bounded.** A PPID cycle among matched rows previously spun
   `while ppid in by_pid` forever (found live while authoring the regressions with a
   self-parented row); the walk now carries a seen-set and canonicalizes cyclic families
   to the smallest pid, so the census always terminates.

## Evidence (literal)

| Gate | Base (before) | Candidate (after) |
|---|---|---|
| `tests/test_session_identity_regression.py` | 8 failed, 10 passed, 1 hang (cycle test) | **19 passed in 0.06s** |
| focused (`test_syshealth` + `test_session_close_operator_seat` + `test_session_close_authority` + regressions) | — | **133 passed in 13.59s** |
| full suite (`python -m pytest tests -q`) | — | **2 failed, 864 passed in 216.91s** |

Base-vs-candidate A/B for the 2 full-suite failures
(`test_cli.py::test_status_json_degrades_when_ollama_scan_hangs`,
`test_cli.py::test_status_json_degrades_when_orphan_probe_hangs`): with the candidate
`syshealth.py` replaced byte-for-byte by the base file (sha `9ffff51c…` = the
`fleet-before.json` pre-mission hash), both tests fail **identically** —
"status hung for 3.14s past its bound" (2.0s wall-clock bounds on a host running
~1,670 processes). They are pre-existing and unrelated; per the brief no unrelated
code was altered to make the baseline green.

Load-bearing authorization regression (fails on base, passes on candidate):

```
base:      allowed=True   "requester is the operator seat (same uid, no agent runtime in its ancestry)"
candidate: allowed=False  "requester is an agent runtime and is not the session owner, a descendant, or an ancestor"
```

A Devin run with **clean** arguments (no codex paths anywhere) was closing a peer's
session lease with operator authority. The roster repair closes it.

## Security audit (Rule 2, five items)

1. **Threat surface** — census identity + the shared `DEFAULT_SESSION_PATTERNS` roster;
   argument ambiguity relabeling processes; roster coupling to registry
   agent/operator classification. Vectors: prompt paths, scratch paths, flag values.
2. **Invariant impact** — Fail-Closed, Constraints Over Plasticity, Tuesday Bar,
   operator/agent authority separation. Public status schema stays compatible (additive
   `match_scope` key only); the shared roster stays the single source.
3. **Key material** — none involved.
4. **Failure modes** — unreadable/unknown ancestry stays DENY (unchanged); malformed
   patterns stay safe (bad regex skipped); custom API unchanged; PPID cycle now
   terminates (was: unbounded walk); operator-seat arm strictly narrower. No fail-open
   path introduced.
5. **Verification** — the tables above; planted-bad fixtures preserved in the regression
   file (if one ever passes as the wrong family, the batch is void); positive controls
   green on both sides; fixtures are sanitized real shapes from live `ps` rows
   (Devin prompt paths, restic excludes, zsh snapshots, node-launched codex).

## Limits / what this is NOT

- **Tested source edits only.** The live consumer (launchd `com.cds.fleet-watch` →
  pipx `fleet` 0.4.0 in site-packages) is a separate copy and still runs the old
  census. No install, no service restart, no commit, push or merge — landing is
  root/operator territory.
- No live close/reap/teardown/kill paths were called. The authorization tests inject
  every process fact; nothing here read the live registry.
- The held worktree `fleet-session-recovery-20260926` was not used (pre_session
  `HALT_MISSING_STATE`), as required.

## Files

- `work/session-recovery/SESSION_RECOVERY_RESULT.json` — machine-readable result
- `work/session-recovery/SESSION_RECOVERY.patch` — minimal diff (syshealth.py + new test file)
- `work/session-recovery/evidence/` — literal test outputs incl. the base A/B
