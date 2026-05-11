"""Swap-pressure budget gate for Fleet Watch guard decisions.

Three-tier threshold system (config-overridable via ~/.fleet-watch/policy.json):
  - swap > 50%: MEMORY_PRESSURE_RISING warning event
  - swap > 80%: refuse new GPU workloads
  - swap > 95%: refuse ALL new workloads

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
            "warning": self.warning,
            "gpu_blocked": self.gpu_blocked,
            "all_blocked": self.all_blocked,
            "thresholds_used": self.thresholds_used,
            "threshold_crossings": self.threshold_crossings,
        }


def check_swap_pressure(
    thresholds: dict[str, int] | None = None,
) -> SwapPressureVerdict:
    """Evaluate swap pressure against configured thresholds."""
    t = thresholds or load_thresholds()
    swap = syshealth.get_swap_state()

    used_pct = float(swap.used_pct) if swap.is_available else 0.0
    total = swap.total_mb if swap.is_available else 0
    used = swap.used_mb if swap.is_available else 0
    free = swap.free_mb if swap.is_available else 0

    verdict = SwapPressureVerdict(
        swap_used_pct=used_pct,
        swap_total_mb=total,
        swap_used_mb=used,
        swap_free_mb=free,
        thresholds_used=t,
    )

    if not swap.is_available:
        return verdict

    if used_pct > t["swap_warning_pct"]:
        verdict.warning = True
        verdict.threshold_crossings.append("warning")

    if used_pct > t["swap_gpu_refusal_pct"]:
        verdict.gpu_blocked = True
        verdict.threshold_crossings.append("gpu_refusal")

    if used_pct > t["swap_all_refusal_pct"]:
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
                f"{verdict.thresholds_used['swap_all_refusal_pct']}%)"
            )
        else:
            result["allowed"] = False
            result["reason"] = (
                f"swap pressure critical: {verdict.swap_used_pct:.0f}% used, "
                f"all new workloads refused"
            )
        return result

    if verdict.gpu_blocked and gpu_requested:
        if in_audit:
            result["audit_note"] = (
                f"would BLOCK GPU workload (swap {verdict.swap_used_pct:.0f}% > "
                f"{verdict.thresholds_used['swap_gpu_refusal_pct']}%)"
            )
        else:
            result["allowed"] = False
            result["reason"] = (
                f"swap pressure high: {verdict.swap_used_pct:.0f}% used, "
                f"GPU workloads refused above {verdict.thresholds_used['swap_gpu_refusal_pct']}%"
            )
        return result

    if verdict.warning:
        result["warning"] = (
            f"memory pressure rising: swap {verdict.swap_used_pct:.0f}% used"
        )

    return result
