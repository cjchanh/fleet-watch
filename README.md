# Fleet Watch

Process governance for AI workloads on a single machine.

## The Problem

You're running MLX, Ollama, vLLM, Candle/Cake, experiment runners, and AI coding agents on the same machine. They don't know about each other. Port 8899 gets stolen by a canary model. A 7B model quietly allocates 11 GB of Metal buffers on an 8 GB machine, swapping to SSD and running 65x slower than expected. Two Codex sessions write to the same repo. Health endpoints say "ok" while GPU memory is exhausted.

Fleet Watch prevents these collisions by maintaining a shared registry of what's running, what resources are claimed, and what's available — including pre-flight working set estimation that catches memory overcommit before it becomes a six-hour debug session.

## Install

```bash
pipx install fleet-watch
```

Or from source:

```bash
pipx install ~/path/to/fleet-watch/
```

## How It Works

Fleet Watch auto-discovers running AI processes (MLX servers, Ollama, vLLM, Candle/Cake, etc.) by scanning `lsof` and `ps`. It registers them in a local SQLite database with their port claims, GPU memory estimates, and repo locks. Any tool — human or AI — can call `fleet guard --json` before taking resource actions.

**You don't register anything manually.** Run `fleet discover` or let the launchd agent do it every 60 seconds.

**GPU memory guard** estimates total working set (weights + KV cache + activations + framework overhead) and compares it against physical RAM. A Candle-based 7B model with Q4_K_M quantization needs ~10 GB of working set due to buffer pool overhead — Fleet Watch catches that on an 8 GB machine before you start the process.

## Quick Start

```bash
# See what's running
fleet status

# Auto-discover and register all AI processes
fleet discover

# Pre-flight: will this model fit?
fleet guard --gpu 4096 --framework candle --model "qwen2.5-7B-Q4_K_M" --json

# Check port and repo availability
fleet guard --port 8899 --repo ~/projects/my-app --json

# System health: memory pressure, sessions, GPU memory watch
fleet health

# See the audit trail
fleet history

# Generate state report
fleet report
```

### Example: 7B model on 8 GB Apple Silicon

```
$ fleet guard --gpu 4096 --framework candle --model "qwen2.5-7B-Q4_K_M"
DENY
GPU 4096MB: working set 7049MB exceeds physical RAM (8192MB) minus reserve (6144MB available)
  Breakdown: weights 3337MB + kv_cache 1792MB + activations 64MB x 2.0x (candle)
  Physical RAM available after reserve: 6144MB
  Suggestion: Use q2_k quantization (~1668 MB weights, ~5380 MB working set)
GPU budget available: 113664MB (0MB allocated)
```

The same model on a 128 GB machine:

```
$ fleet guard --gpu 4096 --framework candle --model "qwen2.5-7B-Q4_K_M"
ALLOW
GPU 4096MB: available (113664MB free)
  Working set: 7049MB (weights 3337 + kv 1792 + act 64) x 2.0x
  Physical RAM available after reserve: 114688MB
GPU budget available: 113664MB (0MB allocated)
```

## Always-On Mode (macOS)

```bash
fleet install-launchd
```

Fleet Watch will auto-discover processes and monitor GPU memory pressure every 60 seconds.

## AI Session Integration

Add this to your AI tool's system prompt or config:

> Before binding a port, starting a model server, or writing to a repo: run `fleet guard --json` with the relevant `--port`, `--repo`, `--gpu`, `--framework`, and `--model` flags. If `"allowed": false`, do not proceed. Use `~/.fleet-watch/state.json` only as a fallback artifact when the CLI is unavailable.

## JSON Contract

### `fleet guard --json`

Top-level keys:

- `allowed` — boolean allow/deny decision
- `request` — what the caller asked to use
- `checks` — per-resource decision objects
- `state` — current machine summary

`request` contains:

- `port` — requested port or `null`
- `repo_dir` — absolute repo path or `null`
- `gpu_mb` — requested GPU claim or `null`
- `framework` — inference framework hint or `null`
- `model` — model name/path hint or `null`

`checks.port` contains:

- `allowed`, `reason`, `holder`, `suggested_ports`

`checks.repo` contains:

- `allowed`, `reason`, `holder`

`checks.gpu` contains:

- `allowed` — boolean
- `reason` — human-readable or `"working_set_exceeds_physical_ram"`
- `detail` — expanded explanation when denied
- `requested_mb` — requested GPU claim
- `available_mb` — currently available budget
- `suggested_max_mb` — maximum claim that fits
- `working_set` — (present when framework/model provided) object with:
  - `weights_mb`, `kv_cache_mb`, `activations_mb` — component breakdown
  - `overhead_multiplier` — framework-specific pool overhead (e.g. 2.0x for Candle)
  - `total_mb` — estimated total working set
  - `framework`, `model_size`, `quantization` — detected parameters
  - `physical_ram_mb`, `available_after_reserve_mb` — machine context
  - `fits` — boolean: does the working set fit in available RAM?
  - `grounded` — boolean: were framework and model size detected from real input?
  - `source` — `"explicit"`, `"command"`, `"fallback_default"`, or `"insufficient_input"`
  - `suggestion` — (when `fits` is false) actionable alternative

`state` contains:

- `process_count`, `occupied_ports`, `safe_ports`, `locked_repos`
- `gpu_budget` — `total_mb`, `reserve_mb`, `allocated_mb`, `available_mb`
- `external_resources`

### `fleet health --json`

- `memory` — RAM snapshot: `total_mb`, `pressure_pct`, `pageouts`, `swapins`, etc.
- `sessions` — discovered CLI sessions with RSS, CPU, classification
- `idle` — workload processes at near-zero CPU
- `gpu_memory_monitor` — runtime pressure data: `pageout_rate`, `gpu_process_footprints`, `alerts`

### `~/.fleet-watch/state.json`

Top-level keys:

- `agent_interface`, `generated_utc`
- `processes`, `external_resources`, `process_count`
- `gpu_budget`, `ports_claimed`, `preferred_ports`, `safe_ports`, `repos_locked`
- `session_leases`, `process_classifications`, `stale_processes`, `recent_events`
- `conflicts_prevented_24h`, `system_memory`, `sessions`, `idle_processes`
- `gpu_memory_monitor` — latest discovery-cycle snapshot of pageout rate, per-process footprints, and active alerts

## Commands

### Core

| Command | What It Does |
|---------|-------------|
| `fleet status` | Show active processes, GPU budget, claimed ports |
| `fleet status --json` | Machine-readable process and budget state |
| `fleet guard --json` | Canonical pre-flight contract for agents |
| `fleet guard --gpu MB --framework FW --model MODEL` | Working set estimation + allow/deny |
| `fleet check --port N --repo PATH --gpu MB` | Honest availability probe (exit 0/1) |
| `fleet discover` | Scan and register running AI processes |
| `fleet report` | Write STATE_REPORT.md + state.json |

### Observability

| Command | What It Does |
|---------|-------------|
| `fleet health` | RAM pressure, sessions, idle processes, GPU memory watch |
| `fleet health --json` | Machine-readable health and GPU monitor snapshot |
| `fleet changelog` | Rolling state changelog |
| `fleet changelog --json` | Raw changelog entries |
| `fleet history` | Hash-chained event audit trail |
| `fleet stale` | List heartbeat-stale processes with evidence |
| `fleet reconcile` | Non-destructive ownership diagnosis |
| `fleet reconcile --json` | Machine-readable ownership diagnosis |

### Session Lifecycle

| Command | What It Does |
|---------|-------------|
| `fleet session start` | Open or refresh a session lease |
| `fleet session heartbeat` | Refresh session lease heartbeat |
| `fleet session ensure` | Idempotent session management with retry |
| `fleet session close` | Close a session lease |

### Process Management

| Command | What It Does |
|---------|-------------|
| `fleet register` | Manually register a process |
| `fleet heartbeat --pid N` | Refresh heartbeat for a registered process |
| `fleet release --pid N` | Release all claims for a PID |
| `fleet reap` | Dry-run: show orphan-confirmed processes |
| `fleet reap --confirm` | Kill and release orphan-confirmed processes |
| `fleet reap-sessions` | Kill detached hot sessions (dry-run by default) |
| `fleet runaway` | Detect runaway high-CPU processes |
| `fleet runaway --kill` | Kill flagged runaway processes |
| `fleet preempt` | Take a port from a lower-priority holder |
| `fleet clean` | Remove entries for dead PIDs |
| `fleet install-launchd` | Install/update a launchd agent |
| `fleet watch` | Continuous discovery loop (foreground) |

### Thunder (Remote GPU)

| Command | What It Does |
|---------|-------------|
| `fleet thunder sync` | Ingest Thunder instances into Fleet Watch |
| `fleet thunder claim` | Attach ownership to a Thunder instance |
| `fleet thunder heartbeat` | Refresh Thunder resource heartbeat |
| `fleet thunder release` | Remove a Thunder instance |

## GPU Working Set Estimation

Fleet Watch estimates total GPU working set per framework:

| Framework | Overhead Multiplier | Why |
|-----------|-------------------|-----|
| Candle/Cake | 2.0x | Retains intermediate buffers until command buffer completion |
| vLLM | 1.4x | Paged attention overhead |
| MLX | 1.3x | Aggressive buffer reuse |
| Ollama/llama.cpp | 1.1x | Tight memory management |

Working set = weights + (KV cache + activations) x overhead multiplier

Override multipliers via `~/.fleet-watch/config.json`:

```json
{
  "gpu_estimator": {
    "framework_overhead": {
      "candle": 2.5
    }
  }
}
```

## Configuration

Fleet Watch writes a default config on first run at `~/.fleet-watch/config.json`.

```json
{
  "gpu_total_mb": 131072,
  "gpu_reserve_mb": 16384,
  "preferred_ports": [8000, 8001, 8080, 8100, 8888, 8899, 11434],
  "patterns": [
    {
      "name_template": "My Server",
      "process_match": "my_server.*serve",
      "workstream": "my-project",
      "priority": 3,
      "gpu_mb_default": 4096
    }
  ],
  "session_patterns": [
    {"name": "Claude Code", "kind": "claude-code", "process_match": "/claude\\b.*--"},
    {"name": "Codex", "kind": "codex", "process_match": "/codex\\b"}
  ],
  "idle_patterns": ["reranker", "socat.*TCP-LISTEN", "mlx_lm.*server"],
  "idle_cpu_threshold": 1.0,
  "pressure_thresholds": {"elevated": 70, "critical": 85}
}
```

## Event Audit Trail

Every registration, release, conflict, and cleanup is logged with a SHA-256 hash chain. Verify integrity:

```python
from fleet_watch import events, registry
conn = registry.connect()
valid, count = events.verify_chain(conn)
print(f"Chain valid: {valid}, events: {count}")
```

## Ownership Model

Fleet Watch uses **session leases** to track who owns what. Process classification requires three independent signals before marking a process as safe to reap:

1. **Heartbeat expired** — not seen by discovery in >180 seconds
2. **Session lease missing or closed** — no active owner
3. **Parent chain detached** — parent PID is dead or PID 1

All three must be true for `orphan_confirmed`. Use `fleet reconcile` to inspect, `fleet reap --confirm` to act.

States: `live` > `disconnected` > `stale_candidate` > `orphan_confirmed` > `exited`

## Design Principles

1. **Advisory, not mandatory.** If Fleet Watch crashes, all processes continue normally.
2. **One honest verb per action.** `guard` decides, `check` probes, `discover` observes.
3. **Single machine.** No distributed consensus. SQLite is sufficient.
4. **Observe first.** Default to alerting, not killing.

## Limitations

- Discovery is heuristic and pattern-based. Unknown workloads are invisible until registered.
- GPU working set estimation uses architecture tables and framework multipliers, not kernel-level Metal accounting.
- Auto-discovery uses macOS `lsof`. On Linux, use manual registration via `fleet register`.
- Fleet Watch is advisory for human use. For AI agent sessions, a PreToolUse hook can make it fail-closed.
- Single-machine by design. No distributed coordination.

## License

MIT
