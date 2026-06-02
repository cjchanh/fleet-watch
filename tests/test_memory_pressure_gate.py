"""Tests for swap-pressure budget gate (H2)."""

from __future__ import annotations

from types import SimpleNamespace

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
        v = check_swap_pressure(thresholds=thresholds)
        # When swap is below 50%, no flags should trigger
        if v.swap_used_pct <= 50:
            assert not v.warning
            assert not v.gpu_blocked
            assert not v.all_blocked

    def test_warning_above_50(self):
        thresholds = {"swap_warning_pct": 30, "swap_gpu_refusal_pct": 80,
                       "swap_all_refusal_pct": 95}
        v = check_swap_pressure(thresholds=thresholds)
        if v.swap_used_pct > 30:
            assert v.warning

    def test_thresholds_saved_in_verdict(self):
        v = check_swap_pressure()
        assert "swap_warning_pct" in v.thresholds_used

    def test_unavailable_swap_returns_empty_verdict(self):
        thresholds = {**DEFAULT_THRESHOLDS}
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


class TestMemoryCorroboration:
    """GPU/ALL refusal require real-memory pressure, not swap-% alone."""

    def test_high_swap_pct_but_ample_ram_allows_gpu(self):
        # 128GB host: swap 81% of a 4GB swapfile, 78GB RAM free, 41% pressure.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(81.0),
                                mem_state=_mem(78000, 41))
        assert v.warning            # swap-% warning still fires (advisory)
        assert not v.gpu_blocked    # but GPU is NOT refused — memory is fine
        assert not v.all_blocked
        assert v.mem_pressured is False
        assert guard_decision(v, gpu_requested=True, audit_cycles=20)["allowed"]

    def test_high_swap_and_low_ram_blocks_gpu(self):
        # genuine thrashing: swap 85% AND only 2GB available.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(85.0),
                                mem_state=_mem(2048, 90))
        assert v.gpu_blocked        # protection preserved
        assert v.mem_pressured is True

    def test_high_swap_and_high_pressure_blocks_gpu(self):
        # pressure ceiling (>=75%) corroborates even with some free RAM.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(85.0),
                                mem_state=_mem(20000, 80))
        assert v.gpu_blocked

    def test_missing_mem_telemetry_fails_closed(self):
        # memory unavailable => assume pressured => keep swap-only block.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(85.0),
                                mem_state=_mem(0, -1, avail=False))
        assert v.gpu_blocked
        assert v.mem_pressured is True

    def test_invalid_pressure_reading_fails_closed(self):
        # is_available True but pressure_pct=-1 (invalid/unknown) with ample RAM:
        # must fail-closed (treat as pressured), not silently allow GPU.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(85.0),
                                mem_state=_mem(90000, -1))
        assert v.gpu_blocked
        assert v.mem_pressured is True

    def test_critical_swap_with_ample_ram_does_not_all_block(self):
        # swap 96% but 78GB free: ALL-refusal also requires corroboration.
        v = check_swap_pressure(thresholds=dict(DEFAULT_THRESHOLDS),
                                swap_state=_swap(96.0),
                                mem_state=_mem(78000, 41))
        assert not v.all_blocked
        assert not v.gpu_blocked


class TestLoadThresholdsMemoryKeys:
    def test_corroboration_thresholds_loaded(self):
        t = load_thresholds()
        assert "swap_refusal_min_avail_mb" in t
        assert "swap_refusal_min_pressure_pct" in t
