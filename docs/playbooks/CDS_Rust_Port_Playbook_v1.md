# CDS Rust Port Playbook v1

## Provenance

Distilled from the fleet-kernel port (2026-06-13): a live Python engine (`fleet_watch.referee`) ported to Rust byte-faithful, parity-proven against production data (16/16 match), with mock-injection for dangerous syscalls (the kill path). This is the reusable method — not the fleet-kernel itself.

**Honest payoff**: the fleet-kernel port produced **no measured latency win** — the Rust output is byte-identical to Python and was never benchmarked as faster on the guard hot path. Its real value was (a) proving the de-risked migration method below and (b) kernel-ness as a future product surface. Do not cite "faster in Rust" as a port justification without a benchmark (see *Measure the trigger*).

**Status**: proven once. Generalize on the second real port (Archivist Verify).

---

## What Gets Ported

The **decision core only** — the deterministic, pure-or-deterministic logic. Never the I/O glue, CLI entry points, or runtime orchestration.

| Port this | Don't port this |
|-----------|-----------------|
| Decision functions (allow/deny, overlap checks, hash-chain, reconciler) | CLI argument parsing |
| Pure helpers (path normalization, lexical collapse) | Subprocess management |
| Deterministic state transitions (ledger append, lease GC) | File watchers, signal handlers |
| Schema-constrained reads (registry queries) | Network servers, HTTP |

**Test**: if you can pin all non-determinism (time, PID liveness, filesystem) and get byte-identical output from the Python and Rust on the same inputs, it's port-eligible. If you can't, it's glue.

---

## The Phases (7 forward + 1 abort)

Phases 0–7 are the forward method; Phase 8 is the fail-closed abort layer for when parity breaks.

### Phase 0 — Security Audit (HARD GATE: Rule 2)

**Before any Rust is written.** Threat-surface analysis of the Python core.

Outputs:
- `PHASE0_SECURITY_AUDIT.md` containing:
  - **Invariants** — what must never be violated (e.g., single-writer exclusion, fail-closed Decision, hash-chain integrity)
  - **Threat surface** — every external input (registry rows, timestamps, PID lists, port numbers, repo paths)
  - **Key material** — any secrets, hashes, or integrity anchors
  - **Fail-closed modes** — what happens on DB error, malformed input, missing row, parse failure
  - **Mutating surfaces** — functions that write, delete, or signal (these need injection)
  - **Syscall surfaces** — `os.kill`, filesystem access, clock reads (these need injection)

Gate: Phase 0 audit must be reviewed and signed off before any Rust code exists. This is Rule 2 (security-first).

### Phase 1 — Ground & Scope

Name the exact Python module + the exact decision functions. Produce a scope manifest:

```json
{
  "source_module": "fleet_watch.referee",
  "target_crate": "fleet-kernel",
  "functions": [
    {"name": "check_port", "deterministic_after": ["clock_pinned", "db_snapshot"]},
    {"name": "check_gpu_budget", "deterministic_after": ["db_snapshot"]},
    {"name": "check_repo_with_session", "deterministic_after": ["clock_pinned", "pid_map_injected", "db_snapshot"]}
  ],
  "excluded": ["fleet guard CLI", "subprocess management", "file watchers"]
}
```

### Phase 2 — Golden-Vector Parity

Generate expected outputs from the live Python with **all non-determinism pinned**:

1. **Fixed timestamp** — monkeypatch `_now_iso` to a constant
2. **Mocked syscalls** — `os.kill` → no-op mock, `_pid_exists` → scripted alive/dead map
3. **Deterministic filesystem** — use paths that resolve consistently (no `/tmp` symlink on macOS)

Run every decision function against a matrix of inputs (empty state, normal, adversarial edge cases). Capture:
- Input parameters
- Mock configuration (alive_map, fixed_ts)
- Expected output (`Decision` struct: allowed, reason, holders, overlap_paths, stale_holders)
- Expected side effects (events appended, leases GC'd, processes released)

Output: `golden_vectors.json` — the parity oracle. This file is the contract: Rust must match it byte-for-byte.

**Tool**: `gen_vectors.py` (one per module). See `rust/fleet-kernel/tests/gen_reconciler_vectors.py` for the canonical pattern.

### Phase 3 — Inject the Dangerous Bits

Before the Rust kernel touches any real system:

1. **Signaller trait** — for `os.kill` / PID liveness. Production impl uses libc; test impl is a mock that issues no real signal.
2. **Clock injection** — accept a timestamp string rather than calling `now()`.
3. **DB snapshot discipline** — mutating functions take a throwaway snapshot; read-only functions open the DB read-only.

This makes the kernel **testable with zero real side effects** and **deterministic** for parity comparison.

Pattern (from fleet-kernel):

```rust
pub trait Signaller {
    fn pid_exists(&self, pid: i64) -> bool;
    fn send_signal(&self, pid: i64, sig: i32) -> bool;
}

pub struct RealSignaller;
impl Signaller for RealSignaller { /* libc::kill */ }

pub struct MockSignaller { pub alive: HashMap<i64, bool> }
impl Signaller for MockSignaller { /* map lookup, no real signal */ }
```

### Phase 4 — Builder/Auditor (HARD GATE: single-writer)

**Worktree-isolated builder** writes the Rust implementation. **Isolated craft-reviewer** audits it. They must not share context — the reviewer gets only the diff, the golden vectors, and the Phase 0 audit.

Convergence loop:
1. Builder writes → runs golden vector tests → fixes until all pass
2. Auditor reviews → returns S1/S2 findings
3. Builder fixes findings → reruns golden vectors → resubmits
4. Repeat until **CLEAN** (zero S1, ≤5 S2)

Gate: single-writer lens enforced. No same-thread review. No builder self-audit as final word.

### Phase 5 — Shadow on Live Data

**Read-only probe** against the live production registry. The Rust kernel runs alongside Python on real data, but:

- **Read-only checks** (`check_port`, `check_gpu_budget`): open the live DB read-only — safe.
- **Mutating checks** (`check_repo`): each side gets its **own throwaway snapshot copy** of the live DB. The live DB is never written.

Both sides use the same fixed timestamp for fair comparison.

Output: `shadow_parity_report.json` — every probe, both results, match/mismatch.

**Tool**: `shadow_parity.py`. See `rust/fleet-kernel/scripts/shadow_parity.py` for the canonical pattern.

Exit 0 iff every probe matches. Exit 1 on any disagreement.

### Phase 6 — Canary Accumulation (HARD GATE: N clean cycles)

Run shadow-parity on a schedule (launchd cron, or per-session hook). Each run:

- **Clean** → increment `clean_streak`
- **Dirty** (any mismatch) → reset `clean_streak` to 0

Gate: `clean_streak >= N` (fleet-kernel uses N=144). This proves parity holds over time, across varying live data, not just a one-shot snapshot.

**Tool**: `canary_run.py`. See `rust/fleet-kernel/scripts/canary_run.py` for the canonical pattern.

### Phase 7 — Gated Flip (HARD GATE: OPERATOR COMMIT)

Only after N clean canary cycles:

1. **Operator issues an explicit commit** — not automated. This is the "throw the switch" moment.
2. **Old implementation kept as instant rollback** — the Python path remains importable; the flip is a config toggle or feature flag, not a deletion.
3. **Rollback procedure documented** — one command to revert to Python, no data migration needed (they share the same registry schema).

### Phase 8 — Abort Paths (fail-closed, not green-path)

The seven phases are parity-forward. These are the off-ramps for when parity breaks — without them the method is green-path theater.

- **Canary goes dirty mid-accumulation** → streak resets to 0 (automatic). Do not promote. Capture the disagreeing probe, diff the two Decisions, fix the Rust to match Python, regenerate vectors, restart the canary. A dirty canary is the system working, not a setback.
- **Divergence is Python's bug, not Rust's** → the Rust still loses. Parity means matching the live Python *as it behaves today*, even where that behavior is arguably wrong (the fleet-kernel preempt review rejected a "better" Rust behavior precisely because Python didn't do it). Fix Python first, ship it, regenerate vectors, then let Rust match the corrected truth. Never let the port silently change behavior under cover of "improvement."
- **Post-cutover divergence in production** → the retained Python path is the instant rollback. Flip the toggle back to Python (no data migration — shared schema), file the divergence as a parity bug, return to shadow. Cutover is reversible by construction or it isn't cutover.
- **Cannot regenerate vectors** (generator wasn't preserved) → treat the oracle as untrusted and rebuild the generator from the Python source before trusting any further parity claim.

---

## The Three Reusable Tools

These are the mechanical, deterministic pieces that can be generalized across ports:

### 1. `golden-vector-gen`

**Input**: Python module path, function list, mock config (fixed_ts, alive_map, db schema)
**Output**: `golden_vectors.json` — the parity oracle
**Contract**: exit 0 on success, exit 3 on any non-determinism detected, emit receipt
**Pattern**: `rust/fleet-kernel/tests/gen_reconciler_vectors.py`

### 2. `shadow-parity`

**Input**: Rust binary path, Python module path, live DB path, probe list, fixed_ts
**Output**: `shadow_parity_report.json` — every probe diffed
**Contract**: exit 0 iff all match, exit 1 on any mismatch, exit 2 on config error, exit 3 on tool error. Never writes the live DB.
**Pattern**: `rust/fleet-kernel/scripts/shadow_parity.py`

### 3. `canary-gate`

**Input**: shadow-parity script path, canary directory, target streak N
**Output**: `tally.json` (streak accumulator) + per-run receipts
**Contract**: exit 0 on clean, exit 1 on dirty, exit 2 on config error. Read-only (invokes shadow-parity which is read-only).
**Pattern**: `rust/fleet-kernel/scripts/canary_run.py`

---

## The Orchestration Skill (`/rust-port`)

Build this **only when driving the second real port** (Archivist Verify). Building it from one example encodes fleet-kernel's accidents.

When built, it should be a model-invocable command (sibling of `/craft-gate`) that:

1. Walks the 7 phases
2. Invokes the three deterministic tools
3. Launches Builder/Auditor agents with context isolation
4. Enforces the three hard gates (Phase 0 audit, single-writer review, OPERATOR-COMMIT flip)
5. Emits receipts at each phase boundary

---

## When to Port (Decision Matrix)

| Trigger | Port? | Reason |
|---------|:-----:|--------|
| "It would be cool in Rust" | **NO** | Optionality tax with no payoff |
| Kernel-ness is the product (sellable/embeddable) | **YES** | Rust binary = moat (Archivist Verify) |
| Measured hot-path latency bite (>50ms per call, called per session) | **YES** | Cold-start win compounds (fleet guard) |
| Dormant/internal tooling | **NO** | Python works fine; port when it earns a trigger |
| SBIR/contract vehicle without live contract | **NO** | Premature; kernel earns existence only with live need |

### Measure the trigger (don't believe "it'll be faster")

The latency row above is a **measured** trigger, not a vibe. Before committing to a port on performance grounds:

1. Benchmark the Python hot path as-is: wrap the decision function, call it N=1000× against a representative DB, record p50/p99 wall-clock.
2. If it isn't actually a bottleneck (sub-millisecond, or called once per session), the latency trigger does **not** fire — port only if kernel-ness is the product.
3. After the port, benchmark the Rust path the same way. If it isn't measurably faster, record that honestly (fleet-kernel wasn't) — the payoff was method/product, not speed.

"Self-reported health is not evidence" applies to performance too: "Rust is faster" is a claim; the benchmark is the proof.

### Sizing the job

Count the port-eligible decision functions. Mutating + syscall surfaces (kill, lease GC, ledger writes) are the expensive ones — each needs an injected abstraction (Phase 3) **and** its own parity vectors + committed generator. Fleet-kernel was ~7 modules / 8 patchsets / one long session. A pure-read kernel (no kill, no mutate) is a fraction of that.

---

## Anti-Patterns (from the fleet-kernel marathon)

1. **Porting glue** — CLI entry points, subprocess management, file watchers. These are not the decision core and create two implementations to maintain with zero benefit.
2. **Porting before Phase 0 audit** — you'll discover the kill-path injection need halfway through and rewrite.
3. **Skipping golden vectors** — "it looks right" is not parity. Byte-for-byte or it's not proven.
4. **Shadow on live DB without snapshots for mutating fns** — `check_repo` GCs leases. Running it on the live DB is a write, not a shadow.
5. **Auto-flip without operator commit** — the cutover is a governance decision, not a CI pipeline.
6. **Building the orchestration skill from one example** — it'll encode fleet-kernel's specific function list, DB schema, and mock shape. Wait for the second port.
7. **Not preserving the vector generator** — fleet-kernel committed generators for only 2 of 7 vector files; the other 5 oracles cannot be regenerated when the Python core moves. Every `*_vectors.json` needs a committed `gen_*.py` beside it, or parity rots silently on the next Python change.

---

## Fleet-Kernel Reference Implementation

All patterns referenced above live at:

```
fleet-watch/rust/fleet-kernel/
├── src/lib.rs                  # Decision contract + pure helpers
├── src/checks.rs               # check_port, check_gpu_budget
├── src/reconciler.rs           # check_repo_with_session (single-writer core)
├── src/events.rs               # Hash-chain ledger
├── src/ledger.rs               # Write path (log_event, claim_port)
├── src/preempt.rs              # Kill authority + Signaller trait
├── src/registry.rs             # Read-only rusqlite layer
├── src/bin/kernel_shadow.rs    # Read-only shadow probe binary
├── tests/                      # PER-MODULE parity: one *_vectors.json + one *.rs runner each
│   ├── normalize.rs    normalize_vectors.json
│   ├── checks.rs       checks_vectors.json       gen_checks_vectors.py
│   ├── events.rs       events_vectors.json
│   ├── ledger.rs       ledger_vectors.json
│   ├── reconciler.rs   reconciler_vectors.json   gen_reconciler_vectors.py
│   ├── preempt_vectors.json    # consumed by inline tests in src/preempt.rs
│   └── golden.rs       golden_vectors.json       # cross-module end-to-end oracle
├── scripts/shadow_parity.py    # Live-data diff harness
├── scripts/canary_run.py       # Streak accumulator + gate
└── deploy/com.cj.fleet-kernel-canary.plist  # launchd schedule
```

> **Reference-impl debt (deliberately recorded, not hidden):** only `gen_checks_vectors.py` and `gen_reconciler_vectors.py` were committed — the `events`, `ledger`, `normalize`, and `preempt` oracles were generated ad hoc and their generators were not kept. That is exactly the anti-pattern #7 the second port must avoid.
