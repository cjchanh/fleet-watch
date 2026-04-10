"""Auto-discovery engine — scans running processes and registers them."""

from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fleet_watch import events, referee, registry

DEFAULT_CONFIG: dict[str, Any] = {
    "gpu_total_mb": registry.DEFAULT_GPU_TOTAL_MB,
    "gpu_reserve_mb": registry.DEFAULT_GPU_RESERVE_MB,
    "preferred_ports": [8000, 8001, 8080, 8100, 8888, 8899, 11434],
    "patterns": [
        {
            "name_template": "{model_short} MLX",
            "process_match": "mlx_lm.*server|mlx_worker",
            "workstream": "inference",
            "priority": 3,
            "restart_policy": "RESTART_ON_FAILURE",
            "gpu_mb_default": 8192,
            "gpu_mb_models": {
                "122B": 55296,
                "70B": 40960,
                "35B": 20480,
                "32B": 18432,
                "14B": 8192,
                "9B": 5120,
                "7B": 4096,
                "3B": 2048,
            },
        },
        {
            "name_template": "Ollama",
            "process_match": "ollama serve",
            "workstream": "inference",
            "priority": 2,
            "restart_policy": "RESTART_ALWAYS",
            "gpu_mb_default": 1024,
            "port_default": 11434,
        },
        {
            "name_template": "Router",
            "process_match": "uvicorn.*router|fastapi.*router",
            "workstream": "routing",
            "priority": 3,
            "restart_policy": "RESTART_ON_FAILURE",
            "gpu_mb_default": 0,
        },
        {
            "name_template": "vLLM {model_short}",
            "process_match": "vllm.*serve|vllm\\.entrypoints",
            "workstream": "inference",
            "priority": 3,
            "restart_policy": "RESTART_ON_FAILURE",
            "gpu_mb_default": 20480,
        },
    ],
}


@dataclass
class DiscoveredProcess:
    pid: int
    port: int | None
    listener_owned: bool
    name: str
    workstream: str
    model: str | None
    gpu_mb: int
    priority: int
    restart_policy: str
    command: str


def config_path() -> Path:
    return registry.FLEET_DIR / "config.json"


def load_config() -> dict[str, Any]:
    path = config_path()
    if path.exists():
        with open(path) as f:
            user = json.load(f)
        # Merge: user overrides defaults
        merged = {**DEFAULT_CONFIG, **user}
        if "patterns" not in user:
            merged["patterns"] = DEFAULT_CONFIG["patterns"]
        if "preferred_ports" not in user:
            merged["preferred_ports"] = DEFAULT_CONFIG["preferred_ports"]
        return merged

    save_default_config()
    return dict(DEFAULT_CONFIG)


def save_default_config() -> Path:
    registry.ensure_dir()
    path = config_path()
    if not path.exists():
        path.write_text(json.dumps(DEFAULT_CONFIG, indent=2) + "\n")
    return path


def preferred_ports(config: dict[str, Any] | None = None) -> list[int]:
    loaded = config or load_config()
    ports = loaded.get("preferred_ports", DEFAULT_CONFIG["preferred_ports"])
    return [int(port) for port in ports]


def _get_listeners() -> dict[int, int]:
    """Return {pid: port} for all TCP listeners."""
    result: dict[int, int] = {}
    try:
        out = subprocess.run(
            ["lsof", "-iTCP", "-sTCP:LISTEN", "-P", "-n", "-F", "pn"],
            capture_output=True, text=True, timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return result

    current_pid = None
    for line in out.stdout.splitlines():
        if line.startswith("p"):
            current_pid = int(line[1:])
        elif line.startswith("n") and current_pid is not None:
            # Parse "n127.0.0.1:8100" or "n*:8100"
            match = re.search(r":(\d+)$", line)
            if match:
                port = int(match.group(1))
                if current_pid not in result:
                    result[current_pid] = port
    return result


def _get_process_commands() -> dict[int, str]:
    """Return {pid: command} for all running processes."""
    try:
        out = subprocess.run(
            ["ps", "-eo", "pid,command"],
            capture_output=True, text=True, timeout=5,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return {}

    result: dict[int, str] = {}
    for line in out.stdout.splitlines()[1:]:
        line = line.strip()
        parts = line.split(None, 1)
        if len(parts) == 2:
            try:
                result[int(parts[0])] = parts[1]
            except ValueError:
                pass
    return result


def _extract_model(command: str) -> str | None:
    """Extract model name from a command line."""
    # --model <value>
    match = re.search(r"--model\s+(\S+)", command)
    if match:
        return match.group(1)
    return None


def _model_short(model: str | None) -> str:
    """Extract a short model identifier like '122B' or '14B'."""
    if not model:
        return "unknown"
    # Match common size patterns
    match = re.search(r"(\d+)[Bb]", model)
    if match:
        return f"{match.group(1)}B"
    # Last path component
    parts = model.rstrip("/").split("/")
    return parts[-1][:30]


def _query_ollama_vram(port: int = 11434) -> int:
    """Query Ollama /api/ps for actual loaded model VRAM in MB.

    Returns real GPU residency instead of hardcoded estimates.
    Falls back to 0 if Ollama is unreachable.
    """
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/ps",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read())
            total_vram = sum(
                m.get("size_vram", 0) for m in data.get("models", [])
            )
            return total_vram // (1024 * 1024)  # bytes → MB
    except Exception:
        return 0


def _estimate_gpu(pattern: dict[str, Any], model: str | None) -> int:
    """Estimate GPU memory from pattern config and model size."""
    # Special case: Ollama — query real VRAM if available
    if pattern.get("name_template") == "Ollama":
        real_vram = _query_ollama_vram(pattern.get("port_default", 11434))
        if real_vram > 0:
            return real_vram

    if "gpu_mb_models" in pattern and model:
        short = _model_short(model)
        for key, mb in pattern["gpu_mb_models"].items():
            if key.lower() in short.lower():
                return mb
    return pattern.get("gpu_mb_default", 0)


def parse_tnr_instances_output(stdout: str, stderr: str) -> list[dict[str, Any]] | None:
    """Parse ``tnr status --json`` output.

    Only *stdout* is searched for JSON — stderr is checked solely for the
    "no instances found" sentinel so that error text containing ``[`` can
    never be mistaken for an instance list.

    Returns:
        A list of instance dicts (possibly empty) when the output is
        parseable, or ``None`` when the output is unavailable/malformed
        and should not mutate registry state.
    """
    raw = stdout.strip()
    if raw:
        bracket = raw.find("[")
        if bracket >= 0:
            try:
                instances = json.loads(raw[bracket:])
            except (json.JSONDecodeError, ValueError):
                pass
            else:
                if isinstance(instances, list):
                    return instances

    combined = f"{stdout}\n{stderr}".lower()
    if "no instances found" in combined:
        return []
    return None


def _sync_thunder(conn: sqlite3.Connection) -> int:
    """Sync Thunder instances from tnr CLI into external_resources.

    Returns the number of instances synced, or 0 if tnr is unavailable.
    Advisory — never raises on failure.
    """
    try:
        result = subprocess.run(
            ["tnr", "status", "--json"],
            capture_output=True, text=True, timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return 0

    if result.returncode != 0:
        return 0

    instances = parse_tnr_instances_output(result.stdout, result.stderr)
    if instances is None:
        return 0

    mapped: list[dict[str, Any]] = []
    for item in instances:
        external_id = str(item.get("uuid") or item.get("name") or item.get("id"))
        mapped.append({
            "resource_type": "instance",
            "external_id": external_id,
            "name": f"Thunder {external_id}",
            "status": str(item.get("status") or "UNKNOWN"),
            "metadata": item,
            "cleanup_cmd": f"tnr delete {item.get('id')} --yes",
            "safe_to_delete": False,
            "endpoint": None,
            "gpu_mb": 0,
        })

    registry.replace_provider_resources(conn, provider="thunder", resources=mapped)
    return len(mapped)


def _prefer_listener_owned_processes(found: list[DiscoveredProcess]) -> list[DiscoveredProcess]:
    """Prefer the real listener PID over parent/helper processes sharing a default port."""
    selected_by_key: dict[tuple[str, str, int], DiscoveredProcess] = {}
    selected_without_port: list[DiscoveredProcess] = []

    for proc in found:
        if proc.port is None:
            selected_without_port.append(proc)
            continue

        key = (proc.workstream, proc.name, proc.port)
        prior = selected_by_key.get(key)
        if prior is None:
            selected_by_key[key] = proc
            continue
        if proc.listener_owned and not prior.listener_owned:
            selected_by_key[key] = proc
            continue
        if proc.listener_owned == prior.listener_owned and proc.pid < prior.pid:
            selected_by_key[key] = proc

    return sorted(
        [*selected_without_port, *selected_by_key.values()],
        key=lambda proc: proc.pid,
    )


def discover(config: dict[str, Any] | None = None) -> list[DiscoveredProcess]:
    """Scan the system for processes matching known patterns."""
    loaded = config or load_config()
    listeners = _get_listeners()
    commands = _get_process_commands()
    found: list[DiscoveredProcess] = []

    for pid, cmd in commands.items():
        for pattern in loaded["patterns"]:
            regex = pattern["process_match"]
            if re.search(regex, cmd):
                listener_port = listeners.get(pid)
                port = listener_port
                # Some patterns have a default port (e.g., ollama always on 11434)
                if port is None:
                    port = pattern.get("port_default")

                model = _extract_model(cmd)
                gpu_mb = _estimate_gpu(pattern, model)
                short = _model_short(model)

                name = pattern["name_template"].format(
                    model_short=short,
                    model=model or "unknown",
                )

                found.append(
                    DiscoveredProcess(
                        pid=pid,
                        port=port,
                        listener_owned=listener_port is not None,
                        name=name,
                        workstream=pattern["workstream"],
                        model=model,
                        gpu_mb=gpu_mb,
                        priority=pattern["priority"],
                        restart_policy=pattern["restart_policy"],
                        command=cmd,
                    )
                )
                break  # First match wins

    return _prefer_listener_owned_processes(found)


def sync(conn=None) -> dict[str, list[dict[str, Any]]]:
    """Discover processes and sync with registry. Returns summary."""
    close_conn = False
    if conn is None:
        conn = registry.connect()
        close_conn = True

    config = load_config()
    discovered = discover(config=config)
    discovered_pids = {proc.pid for proc in discovered}
    registered = registry.get_all_processes(conn)
    replacement_keys = {
        (proc.name, proc.workstream, proc.port): proc.pid
        for proc in discovered
        if proc.port is not None
    }

    added: list[dict[str, Any]] = []
    cleaned: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    # Replace stale parent/helper registrations with the preferred live listener PID.
    for existing in registered:
        key = (existing["name"], existing["workstream"], existing["port"])
        replacement_pid = replacement_keys.get(key)
        if (
            replacement_pid is None
            or replacement_pid == existing["pid"]
            or existing["pid"] in discovered_pids
        ):
            continue
        released = registry.release_process(conn, existing["pid"])
        if released:
            events.log_event(
                conn,
                "CLEAN",
                pid=existing["pid"],
                workstream=existing["workstream"],
                detail={
                    "reason": "listener_reassign",
                    "name": existing["name"],
                    "source": "discover",
                    "replacement_pid": replacement_pid,
                    "port": existing["port"],
                },
            )
            cleaned.append(released)

    registered_pids = {p["pid"] for p in registry.get_all_processes(conn)}

    # Register new discoveries
    for proc in discovered:
        if proc.pid not in registered_pids:
            failures = referee.preflight_register(
                conn,
                port=proc.port,
                gpu_mb=proc.gpu_mb,
                repo_dir=None,
            )
            if failures:
                reason = "; ".join(f.reason for f in failures)
                event_type = (
                    "GPU_BUDGET_DENY"
                    if any("GPU budget exceeded" in f.reason for f in failures)
                    else "CONFLICT"
                )
                events.log_event(
                    conn,
                    event_type,
                    pid=proc.pid,
                    workstream=proc.workstream,
                    detail={
                        "name": proc.name,
                        "source": "discover",
                        "port": proc.port,
                        "gpu_mb": proc.gpu_mb,
                        "reason": reason,
                    },
                )
                skipped.append({"pid": proc.pid, "name": proc.name, "reason": reason})
                continue

            try:
                registry.register_process(
                    conn,
                    pid=proc.pid,
                    name=proc.name,
                    workstream=proc.workstream,
                    port=proc.port,
                    gpu_mb=proc.gpu_mb,
                    model=proc.model,
                    priority=proc.priority,
                    restart_policy=proc.restart_policy,
                    start_cmd=proc.command,
                    manage_session_lease=False,
                )
            except sqlite3.IntegrityError as exc:
                reason = str(exc)
                events.log_event(
                    conn,
                    "CONFLICT",
                    pid=proc.pid,
                    workstream=proc.workstream,
                    detail={"name": proc.name, "source": "discover", "reason": reason},
                )
                skipped.append({"pid": proc.pid, "name": proc.name, "reason": reason})
                continue

            events.log_event(
                conn,
                "REGISTER",
                pid=proc.pid,
                workstream=proc.workstream,
                detail={
                    "name": proc.name,
                    "source": "discover",
                    "port": proc.port,
                    "gpu_mb": proc.gpu_mb,
                },
            )
            added.append({"pid": proc.pid, "name": proc.name})
        else:
            # Update heartbeat for already-registered processes
            registry.heartbeat(conn, proc.pid)

    # Clean dead PIDs (registered but no longer running)
    dead = registry.clean_dead_pids(conn)
    for d in dead:
        events.log_event(conn, "CLEAN", pid=d["pid"], workstream=d["workstream"],
                         detail={"name": d["name"], "source": "discover"})
        cleaned.append(d)

    # Sync Thunder instances if tnr CLI is available
    thunder_synced = _sync_thunder(conn)

    if close_conn:
        conn.close()

    return {"added": added, "cleaned": cleaned, "skipped": skipped, "thunder_synced": thunder_synced}
