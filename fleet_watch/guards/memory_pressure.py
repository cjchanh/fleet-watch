"""Memory-pressure budget gate for Fleet Watch guard decisions.

Spec 2615908 — the GATE is macOS's own authoritative pressure signal
(``syshealth.get_vm_pressure_level`` -> ``kern.memorystatus_vm_pressure_level``:
1 normal / 2 warning / 4 critical, the level the kernel itself uses to decide
app jetsam-kills), NOT swap-%. swap-% and the computed pressure-% are PROXIES
that over-fire on big-RAM hosts: a small dynamic swapfile fills to a high
percentage without real pressure (swap 97% on a 128GB Mac while
``vm_pressure_level`` = 1 NORMAL and ~58GB is available). swap-% is therefore
demoted to an advisory WARNING.

Tiers (thresholds config-overridable via ~/.fleet-watch/policy.json):
  - swap > swap_warning_pct: advisory MEMORY_PRESSURE_RISING warning only.
  - GPU refusal: high swap (> swap_gpu_refusal_pct) as the TRIGGER, corroborated
    by the authoritative signal (level >= warning OR available below the floor).
  - ALL refusal: DECOUPLED from swap-% — fires when level >= critical OR
    available RAM is below an hw.memsize-scaled floor (max(min_avail_mb,
    total * avail_floor_pct%)). High swap on a healthy big-RAM box no longer
    halts every workload.

FAIL-CLOSED: an unreadable pressure level (``None``) OR unavailable memory
telemetry is treated as pressured/critical, so a blind guard refuses rather than
admit a workload onto a possibly-starved host (the dangerous direction here is
under-refusal letting a real OOM through).

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
    # Spec 2615908 — authoritative refusal. The GATE is macOS's own pressure
    # level (syshealth.get_vm_pressure_level: 1 normal / 2 warn / 4 critical),
    # NOT swap-%. swap-% is demoted to the advisory WARNING only. ALL-refusal
    # fires at the critical level OR when true-available RAM drops below the
    # hw.memsize-scaled floor; GPU-refusal keeps high swap as its trigger but
    # corroborates on the authoritative level/floor (never swap-% alone, which
    # over-reads on big-RAM hosts with small dynamic swapfiles).
    "pressure_gpu_level": 2,        # >= warning corroborates a GPU refusal
    "pressure_critical_level": 4,   # >= critical refuses ALL workloads
    # Available-RAM floor, scaled to the host: max(min_avail_mb, total * pct%).
    # The scaled term tracks hw.memsize (a FIXED floor is wrong across a 16GB
    # laptop and a 128GB Mac — 8192MB is most of an 8GB box but a sliver of a
    # 128GB one); the min is a universal hard OOM backstop, not the floor itself.
    "swap_refusal_min_avail_mb": 1024,
    "avail_floor_pct": 6,
    # Retained for back-compat / telemetry only — the over-reading pressure-%
    # corroboration arm it drove is REPLACED by the authoritative level above.
    "swap_refusal_min_pressure_pct": 80,
}

POLICY_PATH = FLEET_DIR / "policy.json"

# Sentinel so a caller can inject pressure_level=None ("unreadable" -> fail-closed)
# DISTINCTLY from "not provided" (read the live signal from syshealth).
_UNSET: Any = object()


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
    pressure_level: int | None = None
    scaled_avail_floor_mb: int = 0
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
            "pressure_level": self.pressure_level,
            "scaled_avail_floor_mb": self.scaled_avail_floor_mb,
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
    pressure_level: "int | None | Any" = _UNSET,
    total_mem_mb: "int | Any" = _UNSET,
) -> SwapPressureVerdict:
    """Evaluate memory pressure against macOS's authoritative signal (spec 2615908).

    The GATE is ``kern.memorystatus_vm_pressure_level`` (1 normal / 2 warn /
    4 critical) plus an hw.memsize-scaled available-RAM floor — NOT swap-%.
    Swap-% is demoted to an advisory WARNING. ALL-refusal fires at the critical
    level OR when true-available RAM is below the scaled floor; GPU-refusal keeps
    high swap as its TRIGGER but corroborates on the authoritative level/floor,
    so swap-% alone can no longer false-block on big-RAM hosts.

    FAIL-CLOSED: an unreadable pressure level (``None``) OR unavailable memory
    telemetry is treated as pressured/critical — both GPU and ALL refuse — so a
    blind guard never admits a workload onto a possibly-starved host.

    ``swap_state`` / ``mem_state`` / ``pressure_level`` / ``total_mem_mb`` may be
    injected for testing; by default they are read live from syshealth.
    """
    # Precedence: built-in defaults < operator policy.json < caller overrides.
    t = {**DEFAULT_THRESHOLDS, **load_thresholds(), **(thresholds or {})}
    swap = swap_state if swap_state is not None else syshealth.get_swap_state()
    mem = mem_state if mem_state is not None else syshealth.get_memory_state()
    level = (
        syshealth.get_vm_pressure_level()
        if pressure_level is _UNSET
        else pressure_level
    )
    # Total RAM for the scaled floor: explicit override > the MemoryState's own
    # total (already read — keeps the floor consistent with the injected/mocked
    # mem) > a direct syshealth read as a last resort.
    if total_mem_mb is not _UNSET:
        total_mb = total_mem_mb
    else:
        total_mb = int(getattr(mem, "total_mb", 0) or 0) or syshealth.get_total_memory_mb()

    used_pct = float(swap.used_pct) if swap.is_available else 0.0
    total = swap.total_mb if swap.is_available else 0
    used = swap.used_mb if swap.is_available else 0
    free = swap.free_mb if swap.is_available else 0

    mem_available = mem.available_mb if mem.is_available else 0
    mem_pressure = mem.pressure_pct if mem.is_available else -1

    # Available-RAM floor scaled to the host (a fixed floor is wrong across a
    # 16GB laptop and a 128GB Mac); the absolute min keeps a sane lower bound.
    scaled_floor = max(
        int(t["swap_refusal_min_avail_mb"]),
        int(total_mb) * int(t["avail_floor_pct"]) // 100,
    )

    # FAIL-CLOSED corroboration on the AUTHORITATIVE signal (never swap-% alone):
    #   - pressure level unreadable, OR memory telemetry unavailable -> pressured
    #   - true-available RAM below the scaled floor -> pressured
    level_unreadable = level is None
    avail_below_floor = (not mem.is_available) or mem_available < scaled_floor

    all_critical = (
        level_unreadable
        or (level is not None and level >= t["pressure_critical_level"])
        or avail_below_floor
    )
    gpu_pressured = (
        level_unreadable
        or (level is not None and level >= t["pressure_gpu_level"])
        or avail_below_floor
    )

    verdict = SwapPressureVerdict(
        swap_used_pct=used_pct,
        swap_total_mb=total,
        swap_used_mb=used,
        swap_free_mb=free,
        mem_available_mb=mem_available,
        mem_pressure_pct=mem_pressure,
        mem_pressured=gpu_pressured,
        pressure_level=level,
        scaled_avail_floor_mb=scaled_floor,
        thresholds_used=t,
    )

    if used_pct > t["swap_warning_pct"]:
        verdict.warning = True
        verdict.threshold_crossings.append("warning")

    # ALL-refusal is DECOUPLED from swap-% (spec 2615908 AC#2): it fires purely
    # on the authoritative critical level / scaled floor / blind-signal, so high
    # swap on a healthy big-RAM host no longer halts every workload.
    if all_critical:
        verdict.all_blocked = True
        verdict.threshold_crossings.append("all_refusal")

    # GPU-refusal keeps high swap as its TRIGGER (swap thrash hurts heavy
    # workloads first) but only when the authoritative signal corroborates real
    # pressure — swap-% alone can no longer false-block GPU.
    if used_pct > t["swap_gpu_refusal_pct"] and gpu_pressured:
        verdict.gpu_blocked = True
        verdict.threshold_crossings.append("gpu_refusal")

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
                f"would BLOCK all workloads (vm_pressure_level="
                f"{verdict.pressure_level}, avail {verdict.mem_available_mb}MB "
                f"vs floor {verdict.scaled_avail_floor_mb}MB)"
            )
        else:
            result["allowed"] = False
            result["reason"] = (
                f"memory pressure critical (vm_pressure_level="
                f"{verdict.pressure_level}, avail {verdict.mem_available_mb}MB "
                f"vs floor {verdict.scaled_avail_floor_mb}MB), all new workloads refused"
            )
        return result

    if verdict.gpu_blocked and gpu_requested:
        if in_audit:
            result["audit_note"] = (
                f"would BLOCK GPU workload (swap {verdict.swap_used_pct:.0f}% > "
                f"{verdict.thresholds_used['swap_gpu_refusal_pct']}% corroborated by "
                f"vm_pressure_level={verdict.pressure_level}, avail "
                f"{verdict.mem_available_mb}MB vs floor {verdict.scaled_avail_floor_mb}MB)"
            )
        else:
            result["allowed"] = False
            result["reason"] = (
                f"memory pressure high: swap {verdict.swap_used_pct:.0f}% + "
                f"vm_pressure_level={verdict.pressure_level} (avail "
                f"{verdict.mem_available_mb}MB vs floor {verdict.scaled_avail_floor_mb}MB), "
                f"GPU workloads refused"
            )
        return result

    if verdict.warning:
        result["warning"] = (
            f"memory pressure rising: swap {verdict.swap_used_pct:.0f}% used"
        )

    return result
