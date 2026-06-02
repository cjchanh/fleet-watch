"""Swap-pressure budget gate for Fleet Watch guard decisions.

Three-tier threshold system (config-overridable via ~/.fleet-watch/policy.json):
  - swap > 50%: MEMORY_PRESSURE_RISING warning event (swap-% alone)
  - swap > 80% AND memory pressured: refuse new GPU workloads
  - swap > 95% AND memory pressured: refuse ALL new workloads

"memory pressured" = available RAM below ``swap_refusal_min_avail_mb`` OR memory
pressure at/above ``swap_refusal_min_pressure_pct``. swap-% alone is a thrashing
PROXY that over-fires on big-RAM hosts: a small dynamic swapfile fills to a high
percentage without real pressure (e.g. 81% of a 4GB swap on a 128GB machine is
3.3GB swapped while 78GB RAM is free). Requiring real-memory corroboration before
refusing GPU/ALL workloads removes that false-block while preserving the gate.
Fail-closed: if memory telemetry is unavailable, memory is treated as pressured
so the conservative swap-only behavior is preserved.

All gates run in audit-only mode for the first 10 cycles post-deployment.
Threshold crossings emit structured events to state_changelog.jsonl.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fleet_watch import syshealth
from fleet_watch.registry import FLEET_DIR

DEFAULT_THRESHOLDS = {
    "swap_warning_pct": 50,
    "swap_gpu_refusal_pct": 80,
    "swap_all_refusal_pct": 95,
    # Memory corroboration (hardware-aware). GPU/ALL refusal additionally
    # require memory to be genuinely pressured: available RAM below the floor
    # OR pressure at/above the ceiling. Warning stays swap-% only (advisory).
    # The pressure ceiling MUST match syshealth.DEFAULT_LAUNCH_GUARD_THRESHOLDS
    # ["max_pressure_pct"] so the two guard surfaces agree on "memory pressured".
    "swap_refusal_min_avail_mb": 8192,
    "swap_refusal_min_pressure_pct": 80,
}

POLICY_PATH = FLEET_DIR / "policy.json"


def load_thresholds() -> dict[str, int]:
    """Load mergeable thresholds from policy.json, overriding defaults."""
    thresholds = dict(DEFAULT_THRESHOLDS)
    if POLICY_PATH.exists():
        try:
            policy = json.loads(POLICY_PATH.read_text())
        except (OSError, json.JSONDecodeError, ValueError):
            return thresholds
        if isinstance(policy, dict):
            mp = policy.get("memory_pressure", {})
            if isinstance(mp, dict):
                for key in DEFAULT_THRESHOLDS:
                    if key in mp and isinstance(mp[key], (int, float)):
                        thresholds[key] = int(mp[key])
    return thresholds


@dataclass
class SwapPressureVerdict:
    """Result of a swap-pressure gate check."""

    swap_used_pct: float
    swap_total_mb: int
    swap_used_mb: int
    swap_free_mb: int
    mem_available_mb: int = 0
    mem_pressure_pct: int = -1
    mem_pressured: bool = True
    warning: bool = False
    gpu_blocked: bool = False
    all_blocked: bool = False
    thresholds_used: dict[str, int] = field(default_factory=dict)
    threshold_crossings: list[str] = field(default_factory=list)

    @property
    def blocked(self) -> bool:
        return self.gpu_blocked or self.all_blocked

    def to_dict(self) -> dict[str, Any]:
        return {
            "swap_used_pct": self.swap_used_pct,
            "swap_total_mb": self.swap_total_mb,
            "swap_used_mb": self.swap_used_mb,
            "swap_free_mb": self.swap_free_mb,
            "mem_available_mb": self.mem_available_mb,
            "mem_pressure_pct": self.mem_pressure_pct,
            "mem_pressured": self.mem_pressured,
            "warning": self.warning,
            "gpu_blocked": self.gpu_blocked,
            "all_blocked": self.all_blocked,
            "thresholds_used": self.thresholds_used,
            "threshold_crossings": self.threshold_crossings,
        }


def check_swap_pressure(
    thresholds: dict[str, int] | None = None,
    swap_state: "syshealth.SwapState | None" = None,
    mem_state: "syshealth.MemoryState | None" = None,
) -> SwapPressureVerdict:
    """Evaluate swap pressure against configured thresholds.

    GPU and ALL refusal require BOTH high swap-% AND corroborating real-memory
    pressure (available RAM below the floor, or pressure at/above the ceiling).
    Fail-closed: if memory telemetry is unavailable, memory is treated as
    pressured, preserving the conservative swap-only refusal behavior.

    ``swap_state``/``mem_state`` may be injected for testing; by default they are
    read live from syshealth.
    """
    # Precedence: built-in defaults < operator policy.json < caller overrides.
    # Merging defaults underneath guarantees every key (incl. the corroboration
    # keys) is present even for a partial caller dict, without dropping policy.
    t = {**DEFAULT_THRESHOLDS, **load_thresholds(), **(thresholds or {})}
    swap = swap_state if swap_state is not None else syshealth.get_swap_state()
    mem = mem_state if mem_state is not None else syshealth.get_memory_state()

    used_pct = float(swap.used_pct) if swap.is_available else 0.0
    total = swap.total_mb if swap.is_available else 0
    used = swap.used_mb if swap.is_available else 0
    free = swap.free_mb if swap.is_available else 0

    mem_available = mem.available_mb if mem.is_available else 0
    mem_pressure = mem.pressure_pct if mem.is_available else -1
    # Fail-closed: missing OR invalid (negative/unknown) memory telemetry =>
    # assume pressured so swap-% retains its conservative refusal behavior.
    mem_pressured = (
        not mem.is_available
        or mem_pressure < 0
        or mem_available < t["swap_refusal_min_avail_mb"]
        or mem_pressure >= t["swap_refusal_min_pressure_pct"]
    )

    verdict = SwapPressureVerdict(
        swap_used_pct=used_pct,
        swap_total_mb=total,
        swap_used_mb=used,
        swap_free_mb=free,
        mem_available_mb=mem_available,
        mem_pressure_pct=mem_pressure,
        mem_pressured=mem_pressured,
        thresholds_used=t,
    )

    if not swap.is_available:
        return verdict

    if used_pct > t["swap_warning_pct"]:
        verdict.warning = True
        verdict.threshold_crossings.append("warning")

    # GPU/ALL refusal are conditioned on real-memory corroboration so swap-%
    # alone cannot false-block GPU on big-RAM hosts (see module docstring).
    if used_pct > t["swap_gpu_refusal_pct"] and mem_pressured:
        verdict.gpu_blocked = True
        verdict.threshold_crossings.append("gpu_refusal")

    if used_pct > t["swap_all_refusal_pct"] and mem_pressured:
        verdict.all_blocked = True
        verdict.threshold_crossings.append("all_refusal")

    return verdict


def guard_decision(
    verdict: SwapPressureVerdict,
    gpu_requested: bool = False,
    audit_cycles: int = 0,
    audit_floor: int = 10,
) -> dict[str, Any]:
    """Translate a swap pressure verdict into a fleet guard decision.

    In audit mode (< audit_floor cycles), gates never block —
    they emit advisory events only.
    """
    in_audit = audit_cycles < audit_floor

    result: dict[str, Any] = {
        "allowed": True,
        "audit_mode": in_audit,
        "verdict": verdict.to_dict(),
    }

    if verdict.all_blocked:
        if in_audit:
            result["audit_note"] = (
                f"would BLOCK all workloads (swap {verdict.swap_used_pct:.0f}% > "
                f"{verdict.thresholds_used['swap_all_refusal_pct']}% + memory "
                f"pressured: avail {verdict.mem_available_mb}MB, "
                f"pressure {verdict.mem_pressure_pct}%)"
            )
        else:
            result["allowed"] = False
            result["reason"] = (
                f"swap pressure critical: {verdict.swap_used_pct:.0f}% used + "
                f"memory pressured (avail {verdict.mem_available_mb}MB, "
                f"pressure {verdict.mem_pressure_pct}%), all new workloads refused"
            )
        return result

    if verdict.gpu_blocked and gpu_requested:
        if in_audit:
            result["audit_note"] = (
                f"would BLOCK GPU workload (swap {verdict.swap_used_pct:.0f}% > "
                f"{verdict.thresholds_used['swap_gpu_refusal_pct']}% + memory "
                f"pressured: avail {verdict.mem_available_mb}MB, "
                f"pressure {verdict.mem_pressure_pct}%)"
            )
        else:
            result["allowed"] = False
            result["reason"] = (
                f"swap pressure high: {verdict.swap_used_pct:.0f}% used + "
                f"memory pressured (avail {verdict.mem_available_mb}MB, "
                f"pressure {verdict.mem_pressure_pct}%), GPU workloads refused above "
                f"{verdict.thresholds_used['swap_gpu_refusal_pct']}%"
            )
        return result

    if verdict.warning:
        result["warning"] = (
            f"memory pressure rising: swap {verdict.swap_used_pct:.0f}% used"
        )

    return result
