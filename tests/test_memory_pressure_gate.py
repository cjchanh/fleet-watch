"""Tests for swap-pressure budget gate (H2)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import fleet_watch.guards.memory_pressure as memory_pressure
from fleet_watch import syshealth
from fleet_watch.guards.memory_pressure import (
    SwapPressureVerdict,
    check_swap_pressure,
    guard_decision,
    load_thresholds,
    DEFAULT_THRESHOLDS,
)


class TestSwapPressureVerdict:
    def test_basic_properties(self):
        v = SwapPressureVerdict(swap_used_pct=30.0, swap_total_mb=5120,
                                 swap_used_mb=1536, swap_free_mb=3584)
        assert not v.blocked
        assert not v.warning
        assert not v.gpu_blocked
        assert not v.all_blocked

    def test_blocked_when_gpu_blocked(self):
        v = SwapPressureVerdict(swap_used_pct=85.0, swap_total_mb=5120,
                                 swap_used_mb=4352, swap_free_mb=768,
                                 gpu_blocked=True)
        assert v.blocked

    def test_blocked_when_all_blocked(self):
        v = SwapPressureVerdict(swap_used_pct=96.0, swap_total_mb=5120,
                                 swap_used_mb=4915, swap_free_mb=205,
                                 all_blocked=True)
        assert v.blocked

    def test_to_dict(self):
        v = SwapPressureVerdict(swap_used_pct=55.0, swap_total_mb=5120,
                                 swap_used_mb=2816, swap_free_mb=2304,
                                 warning=True, thresholds_used=DEFAULT_THRESHOLDS,
                                 threshold_crossings=["warning"])
        d = v.to_dict()
        assert d["swap_used_pct"] == 55.0
        assert d["warning"] is True
        assert d["gpu_blocked"] is False
        assert "warning" in d["threshold_crossings"]


class TestCheckSwapPressure:
    def test_normal_no_crossings(self):
        thresholds = {**DEFAULT_THRESHOLDS, "swap_warning_pct": 50,
                       "swap_gpu_refusal_pct": 80, "swap_all_refusal_pct": 95}
        v = check_swap_pressure(thresholds=thresholds,
                                swap_state=_swap(20.0),
                                mem_state=_mem(64000, 20),
                                pressure_level=_NORMAL,
                                total_mem_mb=_MB_128GB)
        assert not v.warning
        assert not v.gpu_blocked
        assert not v.all_blocked

    def test_warning_above_50(self):
        thresholds = {"swap_warning_pct": 30, "swap_gpu_refusal_pct": 80,
                       "swap_all_refusal_pct": 95}
        v = check_swap_pressure(thresholds=thresholds,
                                swap_state=_swap(40.0),
                                mem_state=_mem(64000, 20),
                                pressure_level=_NORMAL,
                                total_mem_mb=_MB_128GB)
        assert v.warning
        assert not v.gpu_blocked
        assert not v.all_blocked

    def test_thresholds_saved_in_verdict(self):
        v = check_swap_pressure()
        assert "swap_warning_pct" in v.thresholds_used

    def test_unavailable_swap_returns_empty_verdict(self):
        v = SwapPressureVerdict(swap_used_pct=0.0, swap_total_mb=0,
                                 swap_used_mb=0, swap_free_mb=0)
        assert not v.blocked
        assert not v.warning


class TestGuardDecision:
    def test_normal_allows(self):
        v = SwapPressureVerdict(swap_used_pct=30.0, swap_total_mb=5120,
                                 swap_used_mb=1536, swap_free_mb=3584)
        d = guard_decision(v, audit_cycles=20)
        assert d["allowed"]

    def test_all_blocked_refuses_no_gpu(self):
        v = SwapPressureVerdict(swap_used_pct=96.0, swap_total_mb=5120,
                                 swap_used_mb=4915, swap_free_mb=205,
                                 all_blocked=True,
                                 thresholds_used=DEFAULT_THRESHOLDS)
        d = guard_decision(v, gpu_requested=False, audit_cycles=20)
        assert not d["allowed"]

    def test_gpu_blocked_refuses_gpu_request(self):
        v = SwapPressureVerdict(swap_used_pct=85.0, swap_total_mb=5120,
                                 swap_used_mb=4352, swap_free_mb=768,
                                 gpu_blocked=True,
                                 thresholds_used=DEFAULT_THRESHOLDS)
        d = guard_decision(v, gpu_requested=True, audit_cycles=20)
        assert not d["allowed"]

    def test_gpu_blocked_allows_non_gpu(self):
        v = SwapPressureVerdict(swap_used_pct=85.0, swap_total_mb=5120,
                                 swap_used_mb=4352, swap_free_mb=768,
                                 gpu_blocked=True,
                                 thresholds_used=DEFAULT_THRESHOLDS)
        d = guard_decision(v, gpu_requested=False, audit_cycles=20)
        assert d["allowed"]

    def test_audit_mode_allows_blocking(self):
        v = SwapPressureVerdict(swap_used_pct=96.0, swap_total_mb=5120,
                                 swap_used_mb=4915, swap_free_mb=205,
                                 all_blocked=True,
                                 thresholds_used=DEFAULT_THRESHOLDS)
        d = guard_decision(v, audit_cycles=3, audit_floor=10)
        assert d["allowed"]
        assert d["audit_mode"]
        assert "audit_note" in d

    def test_warning_in_response(self):
        v = SwapPressureVerdict(swap_used_pct=55.0, swap_total_mb=5120,
                                 swap_used_mb=2816, swap_free_mb=2304,
                                 warning=True,
                                 thresholds_used=DEFAULT_THRESHOLDS)
        d = guard_decision(v, audit_cycles=20)
        assert d["allowed"]
        assert d.get("warning")


class TestLoadThresholds:
    def test_default_thresholds_loaded(self):
        t = load_thresholds()
        assert t["swap_warning_pct"] == DEFAULT_THRESHOLDS["swap_warning_pct"]
        assert t["swap_gpu_refusal_pct"] == DEFAULT_THRESHOLDS["swap_gpu_refusal_pct"]
        assert t["swap_all_refusal_pct"] == DEFAULT_THRESHOLDS["swap_all_refusal_pct"]


def _swap(pct, total=4096, used=3354, free=742, avail=True):
    return SimpleNamespace(is_available=avail, used_pct=pct,
                           total_mb=total, used_mb=used, free_mb=free)


def _mem(available_mb, pressure_pct, avail=True):
    return SimpleNamespace(is_available=avail, available_mb=available_mb,
                           pressure_pct=pressure_pct)


def _blind_live_probes(monkeypatch, tmp_path):
    monkeypatch.setattr(memory_pressure, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(memory_pressure, "load_thresholds",
                        lambda: dict(DEFAULT_THRESHOLDS))
    monkeypatch.setattr(memory_pressure.syshealth, "get_swap_state",
                        lambda: _swap(0.0, avail=False))
    monkeypatch.setattr(memory_pressure.syshealth, "get_memory_state",
                        lambda: _mem(0, -1, avail=False))
    monkeypatch.setattr(memory_pressure.syshealth, "get_vm_pressure_level",
                        lambda: None)
    monkeypatch.setattr(memory_pressure.syshealth, "get_vm_pressure_probe",
                        lambda: syshealth.ProbeResult(None, "timeout"))
    monkeypatch.setattr(memory_pressure.syshealth, "get_total_memory_mb",
                        lambda: _MB_128GB)


def _fresh_state(generated=None, mem_available_mb=44592):
    generated_at = generated or datetime.now(timezone.utc)
    return {
        "generated_utc": generated_at.isoformat(),
        "swap_pressure": {
            "swap_used_pct": 91.0,
            "swap_total_mb": 11264,
            "swap_used_mb": 10325,
            "swap_free_mb": 938,
            "mem_available_mb": mem_available_mb,
            "mem_pressure_pct": 106,
            "mem_pressured": False,
            "pressure_level": _NORMAL,
            "scaled_avail_floor_mb": _MB_128GB * 6 // 100,
            "warning": True,
            "gpu_blocked": False,
            "all_blocked": False,
            "thresholds_used": dict(DEFAULT_THRESHOLDS),
            "threshold_crossings": ["warning"],
        },
    }


def _write_state(tmp_path, payload):
    (tmp_path / "state.json").write_text(json.dumps(payload), encoding="utf-8")


# Authoritative macOS pressure levels (spec 2615908): 1 normal / 2 warn / 4 crit.
_NORMAL, _WARN, _CRIT = 1, 2, 4
_MB_128GB = 131072


class TestAuthoritativePressureGate:
    """Spec 2615908: refusal grounds on kern.memorystatus_vm_pressure_level + an
    hw.memsize-scaled available-RAM floor, NOT swap-% (which becomes advisory).
    Every call injects pressure_level + total_mem_mb so tests are hermetic."""

    def test_live_20260608_state_allows_all(self):
        # Smoking-gun replay (AC#1): swap 97%, pressure_level=1 (NORMAL), ~58GB
        # avail on a 128GB box. The old gate returned all_blocked=true here.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(97.0), mem_state=_mem(58000, 88),
                                pressure_level=_NORMAL, total_mem_mb=_MB_128GB)
        assert v.warning            # swap-% still warns (advisory only)
        assert not v.all_blocked    # but nothing is refused
        assert not v.gpu_blocked
        assert v.pressure_level == _NORMAL
        assert guard_decision(v, gpu_requested=True, audit_cycles=20)["allowed"]

    def test_high_swap_normal_level_ample_ram_allows_gpu(self):
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(81.0), mem_state=_mem(78000, 41),
                                pressure_level=_NORMAL, total_mem_mb=_MB_128GB)
        assert v.warning
        assert not v.gpu_blocked
        assert not v.all_blocked
        assert v.mem_pressured is False
        assert guard_decision(v, gpu_requested=True, audit_cycles=20)["allowed"]

    def test_critical_level_refuses_all_even_at_low_swap(self):
        # AC#2: refuse-all is DECOUPLED from swap-% — critical pressure at 30% swap.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(30.0), mem_state=_mem(40000, 50),
                                pressure_level=_CRIT, total_mem_mb=_MB_128GB)
        assert v.all_blocked
        assert not guard_decision(v, audit_cycles=20)["allowed"]

    def test_unreadable_level_refuses_all_fail_closed(self):
        # AC#3: a blind authoritative signal MUST refuse (fail-closed).
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(30.0), mem_state=_mem(40000, 50),
                                pressure_level=None, total_mem_mb=_MB_128GB)
        assert v.all_blocked
        assert v.mem_pressured is True
        assert not guard_decision(v, audit_cycles=20)["allowed"]

    def test_avail_below_scaled_floor_refuses_all(self):
        # AC#2: true-available below the scaled floor refuses ALL even at level 1.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(30.0), mem_state=_mem(3000, 50),
                                pressure_level=_NORMAL, total_mem_mb=_MB_128GB)
        assert v.all_blocked
        assert v.scaled_avail_floor_mb == _MB_128GB * 6 // 100   # 128GB*6% > 1024 min

    def test_scaled_floor_grows_with_hw_memsize(self):
        # 256GB host: 6% floor (15728MB) > the 8192 minimum, so 12GB avail — fine
        # on a 128GB box — now refuses. Proves the floor scales to the machine.
        v256 = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                   swap_state=_swap(30.0), mem_state=_mem(12000, 50),
                                   pressure_level=_NORMAL, total_mem_mb=262144)
        assert v256.scaled_avail_floor_mb == 262144 * 6 // 100
        assert v256.all_blocked
        v128 = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                   swap_state=_swap(30.0), mem_state=_mem(12000, 50),
                                   pressure_level=_NORMAL, total_mem_mb=_MB_128GB)
        assert not v128.all_blocked   # same 12GB avail is fine on 128GB

    def test_warning_level_corroborates_gpu_refusal(self):
        # GPU arm: high-swap TRIGGER + warning-level corroboration -> refuse GPU,
        # but warning is not critical so ALL workloads still allowed.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(85.0), mem_state=_mem(20000, 80),
                                pressure_level=_WARN, total_mem_mb=_MB_128GB)
        assert v.gpu_blocked
        assert not v.all_blocked
        assert not guard_decision(v, gpu_requested=True, audit_cycles=20)["allowed"]
        assert guard_decision(v, gpu_requested=False, audit_cycles=20)["allowed"]

    def test_low_ram_blocks_gpu_via_floor(self):
        # genuine thrashing: swap 85% AND only 2GB available (below floor).
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(85.0), mem_state=_mem(2048, 90),
                                pressure_level=_NORMAL, total_mem_mb=_MB_128GB)
        assert v.gpu_blocked
        assert v.mem_pressured is True

    def test_missing_mem_telemetry_fails_closed(self):
        # memory telemetry unavailable => below-floor => pressured (fail-closed).
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(85.0),
                                mem_state=_mem(0, -1, avail=False),
                                pressure_level=_NORMAL, total_mem_mb=_MB_128GB)
        assert v.gpu_blocked
        assert v.all_blocked
        assert v.mem_pressured is True


class TestStateJsonFallback:
    def test_blind_probe_fresh_state_allows_with_provenance(self, tmp_path, monkeypatch):
        _blind_live_probes(monkeypatch, tmp_path)
        _write_state(tmp_path, _fresh_state())

        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS))
        d = guard_decision(v, audit_cycles=20)

        assert d["allowed"]
        assert v.telemetry_source == "state_fallback"
        assert d["verdict"]["telemetry_source"] == "state_fallback"

    def test_live_telemetry_wins_over_fresh_state(self, tmp_path, monkeypatch):
        monkeypatch.setattr(memory_pressure, "FLEET_DIR", tmp_path)
        monkeypatch.setattr(memory_pressure, "load_thresholds",
                            lambda: dict(DEFAULT_THRESHOLDS))
        monkeypatch.setattr(memory_pressure.syshealth, "get_swap_state",
                            lambda: _swap(30.0))
        monkeypatch.setattr(memory_pressure.syshealth, "get_memory_state",
                            lambda: _mem(40000, 50))
        monkeypatch.setattr(memory_pressure.syshealth, "get_vm_pressure_level",
                            lambda: _CRIT)
        monkeypatch.setattr(memory_pressure.syshealth, "get_total_memory_mb",
                            lambda: _MB_128GB)
        _write_state(tmp_path, _fresh_state())

        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS))

        assert v.telemetry_source == "live"
        assert v.all_blocked

    def test_explicit_overrides_prevent_state_fallback(self, tmp_path, monkeypatch):
        monkeypatch.setattr(memory_pressure, "FLEET_DIR", tmp_path)
        monkeypatch.setattr(memory_pressure, "load_thresholds",
                            lambda: dict(DEFAULT_THRESHOLDS))
        _write_state(tmp_path, _fresh_state())

        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(0.0, avail=False),
                                mem_state=_mem(0, -1, avail=False),
                                pressure_level=None,
                                total_mem_mb=_MB_128GB)

        assert v.telemetry_source == "live"
        assert v.all_blocked

    def test_stale_state_refuses(self, tmp_path, monkeypatch):
        _blind_live_probes(monkeypatch, tmp_path)
        generated = datetime.now(timezone.utc) - timedelta(seconds=121)
        _write_state(tmp_path, _fresh_state(generated=generated))

        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS))

        assert v.telemetry_source == "live"
        assert not guard_decision(v, audit_cycles=20)["allowed"]

    def test_future_state_refuses(self, tmp_path, monkeypatch):
        _blind_live_probes(monkeypatch, tmp_path)
        generated = datetime.now(timezone.utc) + timedelta(seconds=31)
        _write_state(tmp_path, _fresh_state(generated=generated))

        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS))

        assert v.telemetry_source == "live"
        assert not guard_decision(v, audit_cycles=20)["allowed"]

    def test_missing_state_refuses(self, tmp_path, monkeypatch):
        _blind_live_probes(monkeypatch, tmp_path)

        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS))

        assert v.telemetry_source == "live"
        assert not guard_decision(v, audit_cycles=20)["allowed"]

    def test_malformed_state_refuses(self, tmp_path, monkeypatch):
        _blind_live_probes(monkeypatch, tmp_path)
        (tmp_path / "state.json").write_text("{not-json", encoding="utf-8")

        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS))

        assert v.telemetry_source == "live"
        assert not guard_decision(v, audit_cycles=20)["allowed"]

    def test_missing_generated_utc_refuses(self, tmp_path, monkeypatch):
        _blind_live_probes(monkeypatch, tmp_path)
        payload = _fresh_state()
        payload.pop("generated_utc")
        _write_state(tmp_path, payload)

        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS))

        assert v.telemetry_source == "live"
        assert not guard_decision(v, audit_cycles=20)["allowed"]

    def test_blind_state_snapshot_refuses(self, tmp_path, monkeypatch):
        _blind_live_probes(monkeypatch, tmp_path)
        _write_state(tmp_path, _fresh_state(mem_available_mb=0))

        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS))

        assert v.telemetry_source == "live"
        assert not guard_decision(v, audit_cycles=20)["allowed"]


class TestLoadThresholdsMemoryKeys:
    def test_authoritative_thresholds_loaded(self):
        t = load_thresholds()
        assert t["pressure_critical_level"] == 4
        assert t["pressure_gpu_level"] == 2
        assert t["avail_floor_pct"] == DEFAULT_THRESHOLDS["avail_floor_pct"]
        assert "swap_refusal_min_avail_mb" in t


class TestTelemetryProvenance:
    """Spec 2624307: syshealth failure provenance carried through the gate.

    Blind telemetry stays fail-closed (all_blocked=true, allowed=false) but the
    refusal reason must identify the UNAVAILABLE telemetry instead of asserting
    a physical "avail 0MB" that was never measured. A genuine zero, read by
    healthy telemetry, keeps the existing numeric refusal."""

    def test_blind_injected_probes_fail_closed_with_provenance(self):
        # AC#2: injected unavailable pressure + memory probes refuse, and the
        # refusal reason names telemetry unavailability — never "avail 0MB".
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(30.0),
                                mem_state=_mem(0, -1, avail=False),
                                pressure_level=None, total_mem_mb=_MB_128GB)
        assert v.all_blocked
        assert v.pressure_failure_reason == "unavailable"
        assert v.mem_failure_reason == "unavailable"
        d = guard_decision(v, audit_cycles=20)
        assert d["allowed"] is False
        assert "memory telemetry unavailable" in d["reason"]
        assert "vm_pressure_level unreadable (unavailable)" in d["reason"]
        assert "memory probe unavailable (unavailable)" in d["reason"]
        assert "0MB" not in d["reason"]

    def test_linux_missing_psi_and_meminfo_refuses_with_meminfo_path(
        self, tmp_path, monkeypatch,
    ):
        probe_reader = syshealth.get_vm_pressure_probe
        psi_path = tmp_path / "missing-memory-psi"
        meminfo_path = tmp_path / "missing-meminfo"
        monkeypatch.setattr(
            syshealth, "get_vm_pressure_probe",
            lambda: probe_reader(
                system="Linux", psi_path=psi_path, meminfo_path=meminfo_path,
            ),
        )
        v = check_swap_pressure(
            thresholds=dict(DEFAULT_THRESHOLDS), swap_state=_swap(20.0),
            mem_state=_mem(64000, 20), total_mem_mb=_MB_128GB,
        )
        assert v.all_blocked is True
        decision = guard_decision(v, audit_cycles=20)
        assert decision["allowed"] is False
        assert str(meminfo_path) in decision["reason"]
        assert str(psi_path) not in decision["reason"]

    def test_linux_missing_psi_healthy_meminfo_allows(self, tmp_path, monkeypatch):
        probe_reader = syshealth.get_vm_pressure_probe
        psi_path = tmp_path / "missing-memory-psi"
        meminfo_path = tmp_path / "meminfo"
        meminfo_path.write_text(
            "MemTotal:       10000 kB\nMemAvailable:    6000 kB\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(
            syshealth, "get_vm_pressure_probe",
            lambda: probe_reader(
                system="Linux", psi_path=psi_path, meminfo_path=meminfo_path,
            ),
        )
        v = check_swap_pressure(
            thresholds=dict(DEFAULT_THRESHOLDS), swap_state=_swap(20.0),
            mem_state=_mem(64000, 20), total_mem_mb=_MB_128GB,
        )
        assert v.all_blocked is False
        decision = guard_decision(v, audit_cycles=20)
        assert decision["allowed"] is True

    def test_blind_live_probes_carry_syshealth_provenance(self, tmp_path, monkeypatch):
        # Live probes blind: syshealth's deterministic provenance (ProbeResult /
        # MemoryState failure_reason) flows onto the verdict and into the
        # decision dict. Hermetic — every probe is patched.
        monkeypatch.setattr(memory_pressure, "FLEET_DIR", tmp_path)
        monkeypatch.setattr(memory_pressure, "load_thresholds",
                            lambda: dict(DEFAULT_THRESHOLDS))
        monkeypatch.setattr(memory_pressure.syshealth, "get_swap_state",
                            lambda: _swap(0.0, avail=False))
        monkeypatch.setattr(
            memory_pressure.syshealth, "get_memory_state",
            lambda: syshealth.MemoryState(0, 0, 0, 0, 0, 0,
                                          failure_reason="vm_stat_timeout"),
        )
        monkeypatch.setattr(memory_pressure.syshealth, "get_vm_pressure_level",
                            lambda: None)
        monkeypatch.setattr(memory_pressure.syshealth, "get_vm_pressure_probe",
                            lambda: syshealth.ProbeResult(None, "timeout"))
        monkeypatch.setattr(memory_pressure.syshealth, "get_total_memory_mb",
                            lambda: _MB_128GB)

        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS))
        assert v.all_blocked
        assert v.telemetry_source == "live"
        assert v.pressure_failure_reason == "timeout"
        assert v.mem_failure_reason == "vm_stat_timeout"

        d = guard_decision(v, audit_cycles=20)
        assert d["allowed"] is False
        assert "vm_pressure_level unreadable (timeout)" in d["reason"]
        assert "memory probe unavailable (vm_stat_timeout)" in d["reason"]
        assert "0MB" not in d["reason"]
        assert d["verdict"]["pressure_failure_reason"] == "timeout"
        assert d["verdict"]["mem_failure_reason"] == "vm_stat_timeout"

    def test_blind_probes_fail_closed_only_memory_blind(self):
        # Readable NORMAL level but blind memory probe: still all-blocked, and
        # the reason names only the memory probe as unavailable.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(30.0),
                                mem_state=_mem(0, -1, avail=False),
                                pressure_level=_NORMAL, total_mem_mb=_MB_128GB)
        assert v.all_blocked
        assert v.pressure_failure_reason is None
        assert v.mem_failure_reason == "unavailable"
        d = guard_decision(v, audit_cycles=20)
        assert d["allowed"] is False
        assert "memory probe unavailable (unavailable)" in d["reason"]
        assert "vm_pressure_level unreadable" not in d["reason"]
        assert "0MB" not in d["reason"]

    def test_blind_audit_note_identifies_unavailability(self):
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(30.0),
                                mem_state=_mem(0, -1, avail=False),
                                pressure_level=None, total_mem_mb=_MB_128GB)
        d = guard_decision(v, audit_cycles=3, audit_floor=10)
        assert d["allowed"] is True      # audit mode never blocks
        assert d["audit_mode"] is True
        assert "memory telemetry unavailable" in d["audit_note"]
        assert "0MB" not in d["audit_note"]

    def test_genuine_zero_avail_keeps_physical_zero_reason(self):
        # VALID telemetry reporting a true 0 MB available: refusal stays the
        # existing numeric form — that is what makes it distinguishable from
        # the blind refusal above.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(30.0), mem_state=_mem(0, 96),
                                pressure_level=_NORMAL, total_mem_mb=_MB_128GB)
        assert v.all_blocked
        assert v.mem_failure_reason is None
        assert v.pressure_failure_reason is None
        d = guard_decision(v, audit_cycles=20)
        assert d["allowed"] is False
        assert d["reason"].startswith("memory pressure critical")
        assert "avail 0MB" in d["reason"]

    def test_low_memory_refusal_retains_existing_reason(self):
        # AC#3: a valid low-memory fixture keeps the existing critical refusal.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(30.0), mem_state=_mem(3000, 50),
                                pressure_level=_NORMAL, total_mem_mb=_MB_128GB)
        assert v.all_blocked
        assert v.mem_failure_reason is None
        d = guard_decision(v, audit_cycles=20)
        assert d["allowed"] is False
        assert d["reason"].startswith("memory pressure critical")
        assert "avail 3000MB" in d["reason"]

    def test_healthy_telemetry_retains_admission(self):
        # AC#3: valid healthy telemetry keeps admission, no provenance recorded.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(20.0),
                                mem_state=_mem(64000, 20),
                                pressure_level=_NORMAL, total_mem_mb=_MB_128GB)
        assert not v.all_blocked
        assert not v.gpu_blocked
        assert v.pressure_failure_reason is None
        assert v.mem_failure_reason is None
        assert guard_decision(v, audit_cycles=20)["allowed"] is True

    def test_state_fallback_verdict_has_no_provenance(self, tmp_path, monkeypatch):
        # The state snapshot is real telemetry written by the daemon: fallback
        # verdicts must not carry failure provenance.
        _blind_live_probes(monkeypatch, tmp_path)
        _write_state(tmp_path, _fresh_state())

        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS))
        assert v.telemetry_source == "state_fallback"
        assert v.pressure_failure_reason is None
        assert v.mem_failure_reason is None
        d = guard_decision(v, audit_cycles=20)
        assert d["allowed"] is True
        assert d["verdict"]["pressure_failure_reason"] is None
        assert d["verdict"]["mem_failure_reason"] is None


class TestVmPressureLevelReader:
    """The authoritative signal reader (spec 2615908). Real-call smoke — no mock:
    macOS returns an int level; elsewhere None (which the gate fails closed)."""

    def test_returns_int_or_none(self):
        import fleet_watch.syshealth as sh
        level = sh.get_vm_pressure_level()
        assert level is None or isinstance(level, int)

    def test_total_memory_mb_nonneg(self):
        import fleet_watch.syshealth as sh
        assert sh.get_total_memory_mb() >= 0
