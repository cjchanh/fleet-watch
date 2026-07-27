# Changelog

## Unreleased

### Added

- **`fleet census`** — deterministic, read-only census of every boot and runtime surface on the machine: user LaunchAgents (cross-referenced against `launchctl list` and `launchctl print-disabled`, with both-direction orphan detection), `/Library` daemons and agents, live processes clustered by what they actually run, TCP listeners attributed to their owning process, crontab, login items, brew services, and the Fleet Watch registry itself.
- **Deterministic verdict engine** — every item gets `status`, `verdict` (`keep`/`investigate`/`close`/`remove`), evidence, and the named `rule` that produced it, so a receipt testifies which heuristic fired rather than just its conclusion.
- **`fleet-census/v1` receipt contract** — dated receipts plus an atomically swapped `latest.json` at `~/.governance/receipts/fleet-census/`. The payload is validated before any write and re-validated from disk before the pointer swap; a receipt that fails validation never replaces a good one. Contract: `docs/fleet-census-receipt-contract-v1.md`.
- **Drift detection** — each census diffs itself against the previous receipt and reports new, disappeared and verdict-changed boot entries. The volatile `processes` domain is excluded from the diff and named in `drift.excluded_domains`.
- **Staged daily launchd job** — `contrib/launchd/io.fleet-watch.census.plist`, emitted with the machine's resolved `fleet` path by `fleet census --emit-launchd-plist`. Staged only; Fleet Watch never bootstraps a launchd job.

### Notes

- Zero items across all domains is a refusal, not a receipt: nothing is written and the CLI exits 1. An unparseable plist is surfaced as `unknown`/`investigate`, never dropped.
- `close_command` is advisory text. `fleet census` is read-only and kills nothing.

## 0.2.0

### Added

- **GPU working set estimator** — pre-flight estimation of total working set (weights + KV cache + activations + framework overhead) against physical RAM. Catches memory overcommit before it turns into swap thrash.
- **Framework-aware overhead multipliers** — Candle 2.0x, MLX 1.3x, Ollama 1.1x, vLLM 1.4x. Configurable via `gpu_estimator.framework_overhead` in config.
- **`fleet guard --framework --model`** — new flags for working set estimation on the canonical guard path.
- **Grounded enforcement** — working set denials only fire when framework and model are explicitly provided, preventing false denials from guessed defaults.
- **Runtime GPU memory monitoring** — pageout rate tracking via `vm_stat` deltas, per-process footprint polling via macOS `footprint`, swap thrash detection. Wired into the 60s discovery cycle.
- **`GPU_MEMORY_PRESSURE` and `GPU_WORKING_SET_DENY` events** — auditable trail for memory guard decisions.
- **`fleet health` GPU memory watch section** — surfaces pageout rate, workload footprints, and active alerts.
- **Runaway process detection** — sustained high-CPU process scanning with auto-kill for Fleet-owned processes. `fleet runaway` CLI command and daemon integration.
- **`fleet session ensure`** — idempotent session management with automatic retry on transient SQLite failures. Fail-open on final failure.
- **`fleet reap-sessions`** — kill detached hot sessions (dry-run by default).
- **Reserve clamp** — `resolve_effective_reserve_mb` prevents impossible reserves on small machines (e.g., 16 GB reserve on 8 GB RAM).

### Changed

- Guard deny responses now include `working_set` breakdown, `detail` field, and `framework`/`model` in the request object.
- `fleet health --json` now includes `gpu_memory_monitor` with pageout rate and footprint data.
- `MemoryState` now tracks `pageouts` and `swapins` counters from `vm_stat`.
- `state.json` now includes `gpu_memory_monitor` snapshot from the discovery cycle.
- Discovery `sync()` now runs the GPU memory monitor on every cycle.

## 0.1.0

Initial release.

- Process discovery and registration via `lsof`/`ps` pattern matching.
- Port, repo, and GPU budget claim arbitration.
- Session leases with heartbeat and ownership tracking.
- Thunder instance coordination.
- Hash-chained event audit log.
- `STATE_REPORT.md` and `state.json` generation.
- launchd integration for always-on discovery.
- macOS notification for resource conflicts.
