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

Fleet Watch auto-discovers running AI processes (MLX servers, Ollama, vLLM, etc.) by scanning `lsof` and `ps`. It registers them in a local SQLite database with their port claims, GPU memory estimates, and repo locks. Any tool — human or AI — can check `~/.fleet-watch/state.json` before taking resource actions.

**You don't register anything manually.** Run `fleet discover` or let the launchd agent do it every 60 seconds.

## Quick Start

```bash
# See what's running
fleet status

# Auto-discover and register all AI processes
fleet discover

# Check if a port is available before using it
fleet claim --port 8899

# Check if a repo is free for writing
fleet claim --repo ~/Workspace/active/archivist

# See the audit trail
fleet history

# Generate state report
fleet report
cat ~/.fleet-watch/STATE_REPORT.md
```

## Always-On Mode (macOS)

Install the launchd agent to run discovery every 60 seconds:

```bash
cp com.cds.fleet-watch.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.cds.fleet-watch.plist
```

Fleet Watch will keep `~/.fleet-watch/state.json` current without any manual intervention.

## AI Session Integration

Add this to your AI tool's system prompt or config:

> Before binding a port, starting a model server, or writing to a repo: read `~/.fleet-watch/state.json` and check for conflicts. If `fleet` CLI is available, run `fleet claim --port <N>` before binding. Exit code 1 means the resource is taken — do not proceed.

For Claude Code, add a Fleet Watch block to `~/.claude/CLAUDE.md`. For Codex, add it to the task prompt. The integration point is always `state.json`.

## Commands

| Command | What It Does |
|---------|-------------|
| `fleet status` | Show active processes, GPU budget, claimed ports |
| `fleet status --json` | Machine-readable output |
| `fleet discover` | Scan and register running AI processes |
| `fleet watch` | Continuous discovery loop (foreground) |
| `fleet register` | Manually register a process |
| `fleet claim --port N` | Check port availability (exit 0=free, 1=taken) |
| `fleet claim --repo PATH` | Check repo lock (exit 0=free, 1=locked) |
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
2. **Fail-closed on claims.** If a port is taken, `fleet claim` returns exit 1. Don't proceed.
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
