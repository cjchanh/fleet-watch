# Fleet Watch Validation Stack

Status: designed/unverified for Tier A closure.
Plan anchor: `/Users/cj/tmp/bridge-plan-fleet-watch-validation-20260606-A1/engine_plan.json`
Standard anchor: `/Users/cj/CDS_VALIDATION_ENGINE_STANDARD.md`
Target repo: `/Users/cj/Workspace/active/fleet-watch`
Target file: `docs/engineering/validation_stack.md`

## Governing Rule

Public analogs, synthetic fixtures, historical examples, and proxy runs improve
calibration confidence only. They never close the Fleet Watch real validation
gate. Only real Fleet Watch production telemetry with metadata, source
manifests, guard requests, state snapshots, registry/history receipts,
comparator receipts, calibrated thresholds, and uncertainty analysis can close
Tier A.

Verification language:

> Dataset existence, metadata, licensing, and high-level relevance are verified
> only when source manifests, hashes, and local parsing receipts exist.
> Guard-level and telemetry-level claims become verified only after local
> capture, hash logging, document parsing, comparator execution, and receipt
> extraction.

## Current Gate State

| Gate | State | Evidence | Required next action |
|---|---|---|---|
| Target manifest | PASS | Engine plan `ep-8f14aa06c6ccd362`; TargetManifest `tm-f3d196d3ec0b9860` | Keep writes inside `docs/engineering/validation_stack.md` |
| Work order | PASS | WorkOrder `wo-f6bffd1d7789a3c1`; dispatch disabled by plan | Use a separate operator boundary before dispatch, fanout, promotion, or commit |
| Repo preflight | PASS | `fleet guard --json --repo /Users/cj/Workspace/active/fleet-watch`; pre-session validator PASS | Preserve unrelated state |
| Real validation gate | BLOCKED | No approved production telemetry drop is promoted by this file | Create and validate a real telemetry drop |
| Promotion | BLOCKED | No Tier A receipt, comparator receipt, or uncertainty-analysis receipt exists for this lane | Promote only after real telemetry plus receipts pass |

This document is the stack contract. It does not close Fleet Watch validation.

## Domain-Appropriate Five-Tier Stack

### Tier 1: Guard Contract Integrity

Purpose: prove the canonical `fleet guard --json` interface returns
deterministic ALLOW/DENY decisions and fails closed when inputs, registry state,
or required resources are unresolved.

Required evidence:

- Guard request and response fixtures for port, repo, write-scope, GPU,
  framework, model, and combined checks.
- Negative tests for DB lock, stale sessions, malformed input, missing repo,
  unavailable registry, and resource overcommit.
- Contract receipts showing `allowed`, `reason`, `holder`, `holders`,
  `suggested_ports`, memory pressure, GPU budget, and repo holder fields.
- Grep or static checks proving no forbidden network client imports in
  `fleet_watch/`.

Fail-closed rule: a guard path that can raise an uncaught exception or silently
allow on uncertainty cannot support a product claim.

### Tier 2: Telemetry and Registry Integrity

Purpose: prove Fleet Watch captures the local operating state with enough
traceability to audit a guard decision after the fact.

Required evidence:

- Hashes for `~/.fleet-watch/state.json`, registry exports, and generated
  reports used in validation.
- Registry event receipts proving the events table remains append-only.
- Discovery receipts for ports, sessions, GPU budget, memory pressure, Ollama
  runners, orphan processes, stale leases, and runaway process candidates.
- Launchd or scheduler receipts showing discovery runs without binding ports or
  mutating OS state beyond the local registry.

Fail-closed rule: telemetry without hashes, timestamps, or source paths is an
observation, not validation evidence.

### Tier 3: Arbitration and Comparator Validation

Purpose: prove Fleet Watch decisions match expected outcomes across controlled
and real captured scenarios.

Required evidence:

- Expected-vs-observed comparator receipts for representative guard scenarios:
  available port, occupied port, clean repo, held repo, safe write-scope,
  blocked write-scope, GPU under budget, GPU over budget, stale holder, and DB
  error.
- Test receipts from `python3 -m pytest tests -q` meeting or exceeding the
  portfolio baseline.
- Coverage notes for adversarial and contract tests, especially
  `test_adversarial.py`, `test_contracts.py`, and stale-lease referee cases.
- Drift receipts when decision thresholds change, including memory-pressure and
  GPU-working-set thresholds.

Fail-closed rule: if a decision cannot be reproduced from input telemetry and
expected comparator logic, it remains designed/unverified.

### Tier 4: Production Fleet Telemetry

Purpose: prove the daemon works under real CDS machine conditions, not just
fixtures or local unit tests.

Required evidence:

- Real production telemetry drops containing guard calls, state snapshots,
  registry event extracts, health snapshots, and report outputs.
- Metadata with capture window, machine class, data source, process count,
  ports, repo locks, GPU budget, memory pressure, and privacy constraints.
- Comparator receipts matching real observed decisions to expected decisions.
- Post-calibration thresholds for swap pressure, memory pressure, stale leases,
  runaway detection, and GPU working set estimates.
- Uncertainty analysis documenting missing telemetry, stale state, sampling
  gaps, false positives, false negatives, and operator override cases.

Fail-closed rule: README examples, synthetic tests, and public scheduler
analogs can exercise mechanics but cannot close the production telemetry gate.

### Tier 5: Governance and Commercial Readiness

Purpose: prove Fleet Watch can be relied on as a CDS control-plane product and
described externally without overstating validation.

Required evidence:

- Capability map entries tied to receipt IDs, not prose-only status.
- Dispatch log entries naming real closer, current blocker, exact next action,
  and metric.
- Release gates that run from a clean checkout and include `fleet guard --json`
  smoke checks.
- Customer-facing claims limited to implemented+tested behavior.

Fail-closed rule: commercial language must downgrade to designed/unverified
until Tier A production telemetry receipts exist.

## Four-Tier Evidence Ladder

| Tier | Fleet Watch meaning | What it can prove | Gate effect |
|---|---|---|---|
| A | Real Fleet Watch production telemetry with manifests, hashes, guard calls, state snapshots, registry/history receipts, comparator receipts, calibrated thresholds, and uncertainty analysis | Real control-plane behavior on the target machine class | Only Tier A can close the real validation gate |
| B | Primary public analogs or benchmark scheduler/resource-manager data with local download, hashes, and parser receipts | Calibration confidence and engineering comparison | Advisory only |
| C | Synthetic fixtures, unit tests, README examples, old state snapshots, or partial local runs | Local mechanics and regression coverage | Advisory only |
| D | Plans, docs, proxy estimates, or unparsed references | Designed intent | No validation closure |

## Quant and Engine Roles

The bridge plan treats Fleet Watch validation as target-agnostic and composes
four engine families:

| Engine | Side effect class | Role |
|---|---|---|
| `validation_engine` | read_only | Apply the CDS Validation Engine Standard to Fleet Watch and emit gate status |
| `archivist_engine` | read_only | Produce evidence-grounded claims from manifests, receipts, tests, and telemetry drops |
| `quant_score_engine` | read_only | Score coverage, drift, missingness, false positive/negative risk, and comparator deltas |
| `dispatch_engine` | local_write | Write scoped artifacts only after TargetManifest and WorkOrder gates pass |

Quant outputs are advisory until connected to Tier A evidence. Scores must cite
inputs, formulas, thresholds, and receipts. A score without a receipt is a
planning signal, not validation.

## Real Telemetry Drop Contract

Fleet Watch should use a product-specific real validation drop, for example:

```text
real_validation_drop/
  README.md
  source_manifest.json
  metadata.json
  raw/
    guard_calls.jsonl
    state_snapshots.jsonl
    health_snapshots.jsonl
    registry_events.jsonl
  normalized/
  receipts/
  comparator/
  ua/
```

Minimum metadata fields:

- `schema_version`
- `data_source` with value `production_telemetry`
- `source_owner`
- `capture_window`
- `machine_class`
- `process_count`
- `port_count`
- `repo_lock_count`
- `gpu_budget_snapshot`
- `memory_pressure_snapshot`
- `raw_hash_manifest`
- `normalization_receipt`
- `privacy_constraints`
- `allowed_claim_scope`

Validator requirements:

- Refuse missing metadata, missing hashes, missing source manifest, or
  mismatched record counts.
- Refuse unknown data source classes.
- Verify that raw guard calls can be matched to state snapshots and comparator
  expectations.
- Emit a receipt with `schema_version`, `decision`, `triggered_rules`,
  `evidence_summary`, `required_next_action`, and `params`.
- Mark gate `BLOCKED` until the drop is promoted through comparator and
  uncertainty-analysis receipts.

## Promotion Rules

A real Fleet Watch validation package can promote only when all of these are
true:

1. Tier A production telemetry exists locally with source manifest, metadata,
   hashes, and parser receipts.
2. Guard calls, state snapshots, registry events, and health snapshots are
   normalized with count and hash receipts.
3. Comparator receipts show observed guard decisions against expected outcomes.
4. Post-calibration thresholds and uncertainty analysis are recorded.
5. `python3 -m pytest tests -q` passes at or above the portfolio baseline.
6. Dispatch, capability map, commercial readiness, and corpus sitrep are
   updated with the same receipt IDs.

Promotion remains forbidden without a separate operator authorization.

## Canonical Query Gate

Each Fleet Watch validation drop should answer seven queries:

1. What telemetry exists, and what is verified locally?
2. What guard claims are supported, contradicted, stale, or missing?
3. What resource classes were exercised: port, repo, write-scope, GPU, memory,
   model/framework, stale lease, runaway process, and launchd discovery?
4. What does Quant measure, and what are its confidence limits?
5. What comparator receipts cite expected vs. observed decisions?
6. What gate remains blocked, and what exact evidence would unblock it?
7. What external or commercial claim is safe to make today?

Each query must return a grounded answer, an abstention, or a BLOCKED receipt.

## Dispatch Boundary

The bridge plan for this lane is dry-run by default:

- `dispatch_allowed`: `false`
- `worker_spawn_allowed`: `false`
- `commit_allowed`: `false`
- `promotion_allowed`: `false`

Forbidden without a separate operator boundary:

- dispatch
- fanout or worker spawn
- process signal, restart, or live clear
- commit, push, deploy, send, submit, delete, or promote
- scope escape outside `docs/engineering/validation_stack.md`

## Next Implementation Targets

1. Create `real_validation_drop/` structure and validator for Fleet Watch
   production telemetry.
2. Define comparator scenarios for guard ALLOW/DENY decisions across port,
   repo, write-scope, GPU, memory, stale lease, DB error, and runaway cases.
3. Define the seven-query validation pack and expected receipt shape.
4. Add CAP-FLEET validation entries that reference receipt IDs instead of
   prose-only status.
5. Connect quant scoring to receipt-backed coverage, drift, missingness,
   false-positive risk, false-negative risk, and comparator metrics.
6. Re-run product truth gates and post-session validation before any promotion.

## Status Summary

Decision: designed/unverified.
Real validation gate: BLOCKED.
Allowed current artifact: this stack document only.
Required next action: create and validate a real production telemetry drop, then
run comparator and uncertainty-analysis receipts before any promotion.
