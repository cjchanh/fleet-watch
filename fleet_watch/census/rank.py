"""Rank ``investigate`` census items by operator cost.

The census often surfaces dozens of ambers. This module collapses them into a
deterministic top-N so the CLI and ORDER boot pane lead with the things that
actually move RAM, CPU, or failure — not flat alphabetical noise.

Pure functions, no I/O, no network. Score is derived only from fields already
on the item (``resource``, ``reason``, ``evidence``, ``status``, ``rule``).
"""

from __future__ import annotations

import re
from typing import Any

DEFAULT_TOP_N = 10

# status → base attention weight (failing jobs outrank quiet dead plists)
_STATUS_WEIGHT: dict[str, float] = {
    "failing": 4000.0,
    "orphan": 2500.0,
    "stale": 1500.0,
    "dead": 1200.0,
    "running": 800.0,
    "unknown": 500.0,
    "idle-loaded": 200.0,
}

# resource string shapes the process domain already emits
_RE_CLUSTER = re.compile(
    r"(?P<procs>\d+)\s+procs?\s*/\s*(?P<rss>\d+)\s*MB\s*RSS\s*/\s*(?P<cpu>\d+(?:\.\d+)?)%\s*CPU",
    re.IGNORECASE,
)
_RE_CPU_ONLY = re.compile(r"(?P<cpu>\d+(?:\.\d+)?)%\s*CPU", re.IGNORECASE)
_RE_RSS_MB = re.compile(r"(?P<rss>\d+)\s*MB(?:\s+resident|\s+RSS)?", re.IGNORECASE)
_RE_PROC_COUNT = re.compile(r"(?P<procs>\d+)\s+process(?:es)?", re.IGNORECASE)


def parse_cost_signals(item: dict[str, Any]) -> dict[str, float | int | None]:
    """Extract rss_mb / cpu_pct / proc_count from an item's free-text fields."""
    resource = item.get("resource") if isinstance(item.get("resource"), str) else ""
    reason = item.get("reason") if isinstance(item.get("reason"), str) else ""
    evidence = item.get("evidence") if isinstance(item.get("evidence"), str) else ""
    blob = " | ".join(part for part in (resource, reason, evidence) if part)

    rss_mb: int | None = None
    cpu_pct: float | None = None
    proc_count: int | None = None

    cluster = _RE_CLUSTER.search(resource) or _RE_CLUSTER.search(blob)
    if cluster:
        proc_count = int(cluster.group("procs"))
        rss_mb = int(cluster.group("rss"))
        cpu_pct = float(cluster.group("cpu"))
    else:
        cpu_match = _RE_CPU_ONLY.search(resource) or _RE_CPU_ONLY.search(blob)
        if cpu_match:
            cpu_pct = float(cpu_match.group("cpu"))
        rss_match = _RE_RSS_MB.search(resource) or _RE_RSS_MB.search(blob)
        if rss_match:
            rss_mb = int(rss_match.group("rss"))
        proc_match = _RE_PROC_COUNT.search(blob)
        if proc_match:
            proc_count = int(proc_match.group("procs"))

    return {"rss_mb": rss_mb, "cpu_pct": cpu_pct, "proc_count": proc_count}


def score_investigate(item: dict[str, Any]) -> tuple[float, list[str]]:
    """Return (score, cost_drivers). Higher score = look first."""
    drivers: list[str] = []
    signals = parse_cost_signals(item)
    rss_mb = signals["rss_mb"]
    cpu_pct = signals["cpu_pct"]
    proc_count = signals["proc_count"]

    # RAM dominates (MB is the unit of real waste on this machine).
    score = 0.0
    if isinstance(rss_mb, int) and rss_mb > 0:
        score += float(rss_mb)
        drivers.append(f"rss_mb={rss_mb}")
    # CPU is scaled so 100% ≈ 1 GB of attention weight.
    if isinstance(cpu_pct, (int, float)) and cpu_pct > 0:
        score += float(cpu_pct) * 10.0
        drivers.append(f"cpu_pct={cpu_pct:g}")
    if isinstance(proc_count, int) and proc_count > 1:
        # Large fan-out without parsed RSS still matters (MCP clusters etc.).
        score += float(proc_count) * 5.0
        drivers.append(f"procs={proc_count}")

    status = item.get("status") if isinstance(item.get("status"), str) else ""
    weight = _STATUS_WEIGHT.get(status, 100.0)
    score += weight
    if status:
        drivers.append(f"status={status}")

    rule = item.get("rule") if isinstance(item.get("rule"), str) else ""
    if rule.startswith("process/large-cluster") or "large-cluster" in rule:
        score += 500.0
        drivers.append("large-cluster")
    if rule.endswith("/high-cpu") or "high-cpu" in rule:
        score += 300.0
        drivers.append("high-cpu-rule")
    if status == "failing":
        drivers.append("failing-job")

    if item.get("close_command"):
        score += 250.0
        drivers.append("has-close-command")

    # Stable floor so zero-signal investigates still sort deterministically
    # below anything with real cost, but above nothing.
    if score < 1.0:
        score = 1.0
        drivers.append("no-cost-signal")

    return score, drivers


def rank_investigate(
    domains: list[dict[str, Any]],
    *,
    top_n: int = DEFAULT_TOP_N,
) -> list[dict[str, Any]]:
    """Flatten investigate items across domains and return the top-N by score.

    Each entry is a plain dict suitable for the receipt field
    ``ranked_investigate``. Ties break on higher RSS, then higher CPU, then
    label ascending (stable, deterministic).
    """
    if top_n < 1:
        return []

    candidates: list[tuple[float, int, float, str, dict[str, Any]]] = []
    for domain in domains:
        if not isinstance(domain, dict):
            continue
        domain_id = domain.get("domain_id") if isinstance(domain.get("domain_id"), str) else ""
        domain_name = domain.get("domain") if isinstance(domain.get("domain"), str) else domain_id
        items = domain.get("items")
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("verdict") != "investigate":
                continue
            label = item.get("label") if isinstance(item.get("label"), str) else "?"
            score, drivers = score_investigate(item)
            signals = parse_cost_signals(item)
            rss = signals["rss_mb"] if isinstance(signals["rss_mb"], int) else 0
            cpu = signals["cpu_pct"] if isinstance(signals["cpu_pct"], (int, float)) else 0.0
            entry = {
                "label": label,
                "domain_id": domain_id,
                "domain": domain_name,
                "status": item.get("status") if isinstance(item.get("status"), str) else "unknown",
                "verdict": "investigate",
                "score": round(score, 2),
                "reason": item.get("reason") if isinstance(item.get("reason"), str) else "",
                "rule": item.get("rule") if isinstance(item.get("rule"), str) else "",
                "cost_drivers": drivers,
            }
            if isinstance(signals["rss_mb"], int):
                entry["rss_mb"] = signals["rss_mb"]
            if isinstance(signals["cpu_pct"], (int, float)):
                entry["cpu_pct"] = float(signals["cpu_pct"])
            if isinstance(signals["proc_count"], int):
                entry["proc_count"] = signals["proc_count"]
            resource = item.get("resource")
            if isinstance(resource, str) and resource:
                entry["resource"] = resource
            close = item.get("close_command")
            if isinstance(close, str) and close:
                entry["close_command"] = close
            # sort key parts carried alongside for stable ordering
            candidates.append((score, rss, float(cpu), label, entry))

    candidates.sort(key=lambda row: (-row[0], -row[1], -row[2], row[3]))
    ranked: list[dict[str, Any]] = []
    for index, (_score, _rss, _cpu, _label, entry) in enumerate(candidates[:top_n], start=1):
        out = dict(entry)
        out["rank"] = index
        ranked.append(out)
    return ranked


def format_rank_line(entry: dict[str, Any]) -> str:
    """One-line human summary for CLI / pane evidence."""
    parts: list[str] = []
    if "rss_mb" in entry:
        parts.append(f"{entry['rss_mb']} MB")
    if "cpu_pct" in entry:
        parts.append(f"{entry['cpu_pct']:g}% CPU")
    if "proc_count" in entry and int(entry["proc_count"]) > 1:
        parts.append(f"{entry['proc_count']} procs")
    if not parts and entry.get("status"):
        parts.append(str(entry["status"]))
    cost = ", ".join(parts) if parts else "no cost signal"
    reason = entry.get("reason") or ""
    if len(reason) > 90:
        reason = reason[:87] + "..."
    return f"#{entry.get('rank', '?')} {entry.get('label', '?')} [{cost}] — {reason}"
