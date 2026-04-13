# Changelog

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
