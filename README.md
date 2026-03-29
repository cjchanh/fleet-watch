# Fleet Watch

Process governance for AI workloads on a single machine.

One developer. One machine. Six AI workstreams. Fleet Watch is the referee.

## The Problem

You're running MLX, Ollama, vLLM, experiment runners, and AI coding agents on the same machine. They don't know about each other. Port 8899 gets stolen by a canary model. The 122B MLX worker evicts your SIEM model from GPU. Two Codex sessions write to the same repo. Health endpoints say "ok" while GPU memory is exhausted.

Fleet Watch prevents these collisions by maintaining a shared registry of what's running, what resources are claimed, and what's available.

## Install

```bash
pipx install fleet-watch
```

Or from source:

```bash
pipx install ~/path/to/fleet-watch/
```

## How It Works

Fleet Watch auto-discovers running AI processes (MLX servers, Ollama, vLLM, etc.) by scanning `lsof` and `ps`. It registers them in a local SQLite database with their port claims, GPU memory estimates, and repo locks. Any tool — human or AI — can call `fleet guard --json` before taking resource actions. `~/.fleet-watch/state.json` is the fallback artifact when the CLI is unavailable.

**You don't register anything manually.** Run `fleet discover` or let the launchd agent do it every 60 seconds.

## Quick Start

```bash
# See what's running
fleet status

# Auto-discover and register all AI processes
fleet discover

# Canonical agent/operator pre-flight
fleet guard --port 8899 --gpu 8192 --json

# Human shorthand availability check
fleet check --repo ~/Workspace/active/archivist

# See the audit trail
fleet history

# Generate state report
fleet report
cat ~/.fleet-watch/STATE_REPORT.md
```

## Always-On Mode (macOS)

Install the launchd agent with the real `fleet` executable path:

```bash
fleet install-launchd
```

Fleet Watch will keep `~/.fleet-watch/state.json` current without any manual intervention.

## AI Session Integration

Add this to your AI tool's system prompt or config:

> Before binding a port, starting a model server, or writing to a repo: run `fleet guard --json` with the relevant `--port`, `--repo`, and `--gpu` flags. If `"allowed": false`, do not proceed. Use `~/.fleet-watch/state.json` only as fallback when the CLI is unavailable.

For Claude Code, add a Fleet Watch block to `~/.claude/CLAUDE.md`. For Codex, add the same rule to `~/.codex/AGENTS.md`. The canonical machine contract is `fleet guard --json`; `state.json` is the fallback artifact.

## Commands

| Command | What It Does |
|---------|-------------|
| `fleet status` | Show active processes, GPU budget, claimed ports |
| `fleet status --json` | Machine-readable output |
| `fleet guard --json` | Canonical pre-flight contract for agents |
| `fleet guard --port 8899 --repo PATH --gpu 8192` | Allow/deny decision plus holder and suggestions |
| `fleet check --port N` | Honest availability probe (exit 0=free, 1=taken) |
| `fleet check --repo PATH` | Honest repo lock probe |
| `fleet check --gpu MB` | Honest GPU budget probe |
| `fleet discover` | Scan and register running AI processes |
| `fleet watch` | Continuous discovery loop (foreground) |
| `fleet install-launchd` | Install/update a launchd agent with the real `fleet` path |
| `fleet register` | Manually register a process |
| `fleet claim --port N` | Deprecated alias for `fleet check --port N` |
| `fleet release --pid N` | Release all claims for a PID |
| `fleet preempt --port N --priority 5 --reason "..."` | Take a port from lower-priority holder |
| `fleet report` | Write STATE_REPORT.md + state.json |
| `fleet history` | Show hash-chained event audit trail |
| `fleet clean` | Remove entries for dead PIDs |
| `fleet stale` | List processes with stale heartbeats |

## GPU Memory Budget

Fleet Watch tracks GPU memory claims against your machine's total. On a 128GB Apple Silicon Mac:

- Total: 128 GB
- System reserve: 16 GB
- Allocatable: 112 GB

Each discovered process gets a GPU estimate based on model size in the name (7B=4GB, 14B=8GB, 32B=18GB, 70B=40GB, 122B=54GB). Override via `~/.fleet-watch/config.json`.

## Configuration

Fleet Watch writes a default config on first run at `~/.fleet-watch/config.json`. Add your own discovery patterns:

```json
{
  "patterns": [
    {
      "name_template": "My Server",
      "process_match": "my_server.*serve",
      "workstream": "my-project",
      "priority": 3,
      "gpu_mb_default": 4096
    }
  ]
}
```

`preferred_ports` controls the ports Fleet Watch suggests when the requested one is occupied.

## Event Audit Trail

Every registration, release, conflict, and cleanup is logged with a SHA-256 hash chain. Each event's hash includes the previous event's hash, creating a tamper-evident audit log. Verify integrity:

```python
from fleet_watch import events, registry
conn = registry.connect()
valid, count = events.verify_chain(conn)
print(f"Chain valid: {valid}, events: {count}")
```

## Design Principles

1. **Advisory, not mandatory.** If Fleet Watch crashes, all processes continue normally.
2. **One honest verb per action.** `guard` decides, `check` probes, `discover` observes.
3. **Single machine.** No distributed consensus. SQLite is sufficient.
4. **Observe first.** Default to alerting, not killing. Preemption requires explicit priority override.

## What Fleet Watch Is Not

- Not a container orchestrator (Kubernetes, Docker Compose)
- Not a process supervisor (systemd, launchd) — it works alongside them
- Not a cloud service — everything is local, no telemetry, no accounts
- Not a security tool — it's a coordination tool for one developer's workloads

## License

MIT

## Author

CJ Chanhnourack — [Centennial Defense Systems](https://centennialdefense.com)
