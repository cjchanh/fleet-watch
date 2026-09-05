"""Memory-pressure budget gate for Fleet Watch guard decisions.

Spec 2615908 — the GATE is macOS's own authoritative pressure signal
(``syshealth.get_vm_pressure_level`` -> ``kern.memorystatus_vm_pressure_level``:
1 normal / 2 warning / 4 critical, the level the kernel itself uses to decide
app jetsam-kills), NOT swap-%. swap-% and the computed pressure-% are PROXIES
that over-fire on big-RAM hosts: a small dynamic swapfile fills to a high
percentage without real pressure (swap 97% on a 128GB Mac while
``vm_pressure_level`` = 1 NORMAL and ~58GB is available). swap-% is therefore
demoted to an advisory WARNING. Linux uses PSI ``some avg10`` mapped by
``syshealth`` to the same 1/2/4 scale; unreadable PSI remains fail-closed.

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

Spec 2624307 — the syshealth failure provenance (``MemoryState.failure_reason``,
``ProbeResult.failure_reason``) is carried through the verdict, and a blind
refusal's reason names the unavailable telemetry instead of asserting a physical
"avail 0MB" that was never measured. A genuine zero is still reported as 0MB.

All gates run in audit-only mode for the first 10 cycles post-deployment.
Threshold crossings emit structured events to state_changelog.jsonl.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
STATE_FALLBACK_MAX_AGE_SECONDS = 120

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


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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
    # Spec 2624307 — deterministic failure provenance from syshealth, set only
    # when the corresponding probe was unavailable (None/unavailable state).
    # Distinguishes a blind refusal from a genuine physical 0 MB reading.
    pressure_failure_reason: str | None = None
    mem_failure_reason: str | None = None
    scaled_avail_floor_mb: int = 0
    warning: bool = False
    gpu_blocked: bool = False
    all_blocked: bool = False
    thresholds_used: dict[str, int] = field(default_factory=dict)
    threshold_crossings: list[str] = field(default_factory=list)
    telemetry_source: str = "live"

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
            "pressure_failure_reason": self.pressure_failure_reason,
            "mem_failure_reason": self.mem_failure_reason,
            "scaled_avail_floor_mb": self.scaled_avail_floor_mb,
            "warning": self.warning,
            "gpu_blocked": self.gpu_blocked,
            "all_blocked": self.all_blocked,
            "thresholds_used": self.thresholds_used,
            "threshold_crossings": self.threshold_crossings,
            "telemetry_source": self.telemetry_source,
        }


def _fresh_state_verdict(
    thresholds: dict[str, int],
    *,
    now: datetime | None = None,
) -> SwapPressureVerdict | None:
    """Use the daemon-written state snapshot when sandboxed probes are blind."""
    path = FLEET_DIR / "state.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    generated = _parse_utc(payload.get("generated_utc"))
    if generated is None:
        return None
    reference = now or datetime.now(timezone.utc)
    age_seconds = (reference - generated).total_seconds()
    if age_seconds < -30 or age_seconds > STATE_FALLBACK_MAX_AGE_SECONDS:
        return None
    raw = payload.get("swap_pressure")
    if not isinstance(raw, dict):
        return None
    pressure_level = raw.get("pressure_level")
    mem_available = raw.get("mem_available_mb")
    if not isinstance(pressure_level, int) or not isinstance(mem_available, int):
        return None
    if mem_available <= 0:
        return None
    threshold_values = raw.get("thresholds_used")
    if not isinstance(threshold_values, dict):
        threshold_values = thresholds
    crossings = raw.get("threshold_crossings")
    try:
        return SwapPressureVerdict(
            swap_used_pct=float(raw.get("swap_used_pct", 0.0)),
            swap_total_mb=int(raw.get("swap_total_mb", 0)),
            swap_used_mb=int(raw.get("swap_used_mb", 0)),
            swap_free_mb=int(raw.get("swap_free_mb", 0)),
            mem_available_mb=mem_available,
            mem_pressure_pct=int(raw.get("mem_pressure_pct", -1)),
            mem_pressured=bool(raw.get("mem_pressured", False)),
            pressure_level=pressure_level,
            scaled_avail_floor_mb=int(raw.get("scaled_avail_floor_mb", 0)),
            warning=bool(raw.get("warning", False)),
            gpu_blocked=bool(raw.get("gpu_blocked", False)),
            all_blocked=bool(raw.get("all_blocked", False)),
            thresholds_used={str(k): int(v) for k, v in threshold_values.items()},
            threshold_crossings=[
                str(item) for item in crossings if isinstance(item, str)
            ] if isinstance(crossings, list) else [],
            telemetry_source="state_fallback",
        )
    except (TypeError, ValueError):
        return None


def check_swap_pressure(
    thresholds: dict[str, int] | None = None,
    swap_state: "syshealth.SwapState | None" = None,
    mem_state: "syshealth.MemoryState | None" = None,
    pressure_level: "int | None | Any" = _UNSET,
    total_mem_mb: "int | Any" = _UNSET,
) -> SwapPressureVerdict:
    """Evaluate macOS kernel pressure or Linux PSI on the shared 1/2/4 scale.

    The GATE is macOS ``kern.memorystatus_vm_pressure_level`` or mapped Linux
    PSI (1 normal / 2 warn / 4 critical), plus a RAM-scaled available-RAM floor.
    Swap-% is demoted to an advisory WARNING. ALL-refusal fires at the critical
    level OR when true-available RAM is below the scaled floor; GPU-refusal keeps
    high swap as its TRIGGER but corroborates on the authoritative level/floor,
    so swap-% alone can no longer false-block on big-RAM hosts.

    FAIL-CLOSED: an unreadable pressure level (``None``) OR unavailable memory
    telemetry is treated as pressured/critical — both GPU and ALL refuse — so a
    blind guard never admits a workload onto a possibly-starved host. Blind
    probes also record syshealth's failure provenance on the verdict
    (``pressure_failure_reason`` / ``mem_failure_reason``, spec 2624307) so a
    blind refusal is distinguishable from a genuine physical 0 MB reading.

    ``swap_state`` / ``mem_state`` / ``pressure_level`` / ``total_mem_mb`` may be
    injected for testing; by default they are read live from syshealth.
    """
    # Precedence: built-in defaults < operator policy.json < caller overrides.
    t = {**DEFAULT_THRESHOLDS, **load_thresholds(), **(thresholds or {})}
    swap = swap_state if swap_state is not None else syshealth.get_swap_state()
    mem = mem_state if mem_state is not None else syshealth.get_memory_state()
    # Spec 2624307 — carry syshealth's deterministic failure provenance. The
    # governing level read stays ``get_vm_pressure_level``; provenance is only
    # resolved when that read is blind, so a readable level costs nothing and
    # patched-over readers keep their exact prior behavior.
    pressure_failure: str | None = None
    if pressure_level is _UNSET:
        level = syshealth.get_vm_pressure_level()
        if level is None:
            probe = syshealth.get_vm_pressure_probe()
            pressure_failure = probe.failure_reason or "unavailable"
    else:
        level = pressure_level
        if level is None:
            pressure_failure = "unavailable"
    live_probe = (
        swap_state is None
        and mem_state is None
        and pressure_level is _UNSET
        and total_mem_mb is _UNSET
    )
    if live_probe and (
        not swap.is_available or not mem.is_available or level is None
    ):
        try:
            fallback = _fresh_state_verdict(t)
        except Exception:  # noqa: BLE001 — fallback miss keeps the live fail-closed path
            fallback = None
        if fallback is not None:
            return fallback
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
    # Provenance for a blind memory probe: syshealth's recorded failure reason
    # (e.g. ``vm_stat_timeout``) or a neutral marker when the caller injected a
    # state without one. Never set when telemetry was readable.
    mem_failure = (
        (getattr(mem, "failure_reason", None) or "unavailable")
        if not mem.is_available
        else None
    )

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
        pressure_failure_reason=pressure_failure,
        mem_failure_reason=mem_failure,
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


def _blind_provenance_parts(verdict: SwapPressureVerdict) -> list[str]:
    """Human-readable provenance for unavailable telemetry (spec 2624307).

    Empty when every probe was readable, so callers keep the physical reading
    (which may honestly be 0 MB). Non-empty only for a blind probe — the
    refusal must then name the unavailable telemetry, never assert a physical
    "avail 0MB" that was never measured.
    """
    parts: list[str] = []
    if verdict.pressure_failure_reason:
        parts.append(
            f"vm_pressure_level unreadable ({verdict.pressure_failure_reason})"
        )
    if verdict.mem_failure_reason:
        parts.append(f"memory probe unavailable ({verdict.mem_failure_reason})")
    return parts


def guard_decision(
    verdict: SwapPressureVerdict,
    gpu_requested: bool = False,
    audit_cycles: int = 0,
    audit_floor: int = 10,
) -> dict[str, Any]:
    """Translate a swap pressure verdict into a fleet guard decision.

    In audit mode (< audit_floor cycles), gates never block —
    they emit advisory events only.

    When telemetry was blind (spec 2624307 provenance carried on the verdict),
    the refusal/audit note names the unavailable probes instead of asserting a
    physical "avail 0MB" reading that never happened.
    """
    in_audit = audit_cycles < audit_floor
    blind_parts = _blind_provenance_parts(verdict)

    result: dict[str, Any] = {
        "allowed": True,
        "audit_mode": in_audit,
        "verdict": verdict.to_dict(),
    }

    if verdict.all_blocked:
        if blind_parts:
            provenance = "; ".join(blind_parts)
            if in_audit:
                result["audit_note"] = (
                    f"would BLOCK all workloads (memory telemetry unavailable: "
                    f"{provenance})"
                )
            else:
                result["allowed"] = False
                result["reason"] = (
                    f"memory telemetry unavailable ({provenance}), "
                    "fail-closed: all new workloads refused"
                )
            return result
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
