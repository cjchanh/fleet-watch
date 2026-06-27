"""boot_coverage — Cross-check Fleet Watch registry against launchd-loaded services.

Emits per-process persistence verdicts and suggested plist templates for any
process that will die on reboot without a launchd agent.

Schema version: boot_coverage_receipt_v1
"""

from __future__ import annotations

import json
import os
import plistlib
import subprocess
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "boot_coverage_receipt_v1"
LAUNCH_AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"
RECEIPT_DIR = Path.home() / ".governance" / "receipts" / "boot-coverage"


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def list_launchd_agents() -> dict[str, dict[str, Any]]:
    """Return {label: {plist_path, loaded, pid}} for all user launchd agents."""
    agents: dict[str, dict[str, Any]] = {}

    if LAUNCH_AGENTS_DIR.is_dir():
        for plist_path in sorted(LAUNCH_AGENTS_DIR.glob("*.plist")):
            try:
                with open(plist_path, "rb") as f:
                    plist = plistlib.load(f)
            except Exception:
                continue
            label = plist.get("Label", "")
            if not label:
                continue
            agents[label] = {
                "plist_path": str(plist_path),
                "loaded": False,
                "pid": None,
            }

    result = subprocess.run(
        ["launchctl", "list"],
        capture_output=True, text=True, timeout=5,
    )
    for line in result.stdout.strip().split("\n"):
        parts = line.split()
        if len(parts) >= 2:
            pid_str, label = parts[0], parts[-1]
            if label in agents:
                agents[label]["loaded"] = True
                try:
                    agents[label]["pid"] = int(pid_str) if pid_str != "-" else None
                except ValueError:
                    agents[label]["pid"] = None

    return agents


def _tokenize(s: str) -> set[str]:
    import re
    parts = re.split(r"[.\-\s_]+", s.lower())
    return {p for p in parts if len(p) >= 2}


def _find_launchd_for_process(
    proc: dict[str, Any],
    agents: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Match a Fleet Watch process to a launchd plist using port and name heuristics."""
    proc_name = (proc.get("name") or "").lower()
    proc_port = proc.get("port")
    proc_tokens = _tokenize(proc_name)

    for label, agent in agents.items():
        label_lower = label.lower()
        # Direct substring match
        if proc_name in label_lower or label_lower in proc_name:
            return agent
        # Token overlap match
        label_tokens = _tokenize(label)
        if proc_tokens & label_tokens:
            return agent

    if proc_port is not None:
        for label, agent in agents.items():
            try:
                with open(agent["plist_path"], "rb") as f:
                    plist = plistlib.load(f)
            except Exception:
                continue
            env = plist.get("EnvironmentVariables", {})
            for _key, val in env.items():
                if str(proc_port) in str(val):
                    return agent

    return None


def _plist_template(name: str, command: str, label: str) -> str:
    log_path = Path.home() / "Library" / "Logs" / f"{label}.log"
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" '
        '"http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
        '<plist version="1.0">\n'
        "<dict>\n"
        f"    <key>Label</key>\n"
        f"    <string>{label}</string>\n"
        f"    <key>ProgramArguments</key>\n"
        f"    <array>\n"
        f"        <string>{command}</string>\n"
        f"    </array>\n"
        f"    <key>RunAtLoad</key>\n"
        f"    <true/>\n"
        f"    <key>KeepAlive</key>\n"
        f"    <true/>\n"
        f"    <key>EnvironmentVariables</key>\n"
        f"    <dict>\n"
        f"        <key>PATH</key>\n"
        f"        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>\n"
        f"    </dict>\n"
        f"    <key>StandardOutPath</key>\n"
        f"    <string>{log_path}</string>\n"
        f"    <key>StandardErrorPath</key>\n"
        f"    <string>{log_path}</string>\n"
        f"</dict>\n"
        f"</plist>\n"
    )


def assess(processes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    agents = list_launchd_agents()
    results: list[dict[str, Any]] = []

    for proc in processes:
        matched = _find_launchd_for_process(proc, agents)
        if matched is None:
            label = f"com.cds.{proc['name'].lower().replace(' ', '-')}"
            verdict = "NO_PERSISTENCE_WILL_DIE_ON_REBOOT"
            results.append({
                "pid": proc["pid"],
                "name": proc["name"],
                "port": proc.get("port"),
                "verdict": verdict,
                "launchd_label": None,
                "plist_path": None,
                "loaded": False,
                "suggested_plist": _plist_template(
                    proc["name"],
                    proc.get("start_cmd") or f"/path/to/{proc['name'].lower()}",
                    label,
                ),
                "suggested_label": label,
            })
        elif matched["loaded"]:
            results.append({
                "pid": proc["pid"],
                "name": proc["name"],
                "port": proc.get("port"),
                "verdict": "HAS_PERSISTENCE",
                "launchd_label": Path(matched["plist_path"]).stem if matched.get("plist_path") else None,
                "plist_path": matched.get("plist_path"),
                "loaded": True,
                "suggested_plist": None,
                "suggested_label": None,
            })
        else:
            results.append({
                "pid": proc["pid"],
                "name": proc["name"],
                "port": proc.get("port"),
                "verdict": "PLIST_PRESENT_BUT_UNLOADED",
                "launchd_label": Path(matched["plist_path"]).stem if matched.get("plist_path") else None,
                "plist_path": matched.get("plist_path"),
                "loaded": False,
                "suggested_plist": None,
                "suggested_label": None,
            })

    return results


def run(processes: list[dict[str, Any]], as_json: bool = False) -> dict[str, Any]:
    results = assess(processes)
    RECEIPT_DIR.mkdir(parents=True, exist_ok=True)
    ts = _now_iso().replace(":", "").replace("-", "").replace("T", "T").replace("Z", "Z")
    safe_ts = ts.replace(":", "")
    receipt_path = RECEIPT_DIR / f"{safe_ts}.json"

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_utc": _now_iso(),
        "processes_assessed": len(results),
        "by_verdict": {},
        "results": results,
    }

    for r in results:
        v = r["verdict"]
        payload["by_verdict"][v] = payload["by_verdict"].get(v, 0) + 1

    receipt_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    payload["receipt_path"] = str(receipt_path)

    return payload
