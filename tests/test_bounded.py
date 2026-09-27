from __future__ import annotations

import threading
import time

from fleet_watch import cli_support


def test_bounded_result_distinguishes_error_from_success():
    def boom():
        raise RuntimeError("probe failed")

    value, reason = cli_support._run_bounded_result(
        boom, timeout_seconds=0.2, default="fallback"
    )
    assert value == "fallback"
    assert reason == "error"


def test_bounded_result_reuses_inflight_call_after_timeout():
    release = threading.Event()
    entered = threading.Event()
    calls = 0

    def slow():
        nonlocal calls
        calls += 1
        entered.set()
        release.wait(1.0)
        return "late"

    first, first_reason = cli_support._run_bounded_result(
        slow, timeout_seconds=0.03, default="unknown"
    )
    assert first == "unknown"
    assert first_reason == "timeout"
    assert entered.wait(0.5)
    second, second_reason = cli_support._run_bounded_result(
        slow, timeout_seconds=0.03, default="unknown"
    )
    assert second == "unknown"
    assert second_reason == "in_flight"
    assert calls == 1
    release.set()
    for _ in range(20):
        if not any(t.name.startswith("bounded-") and t.is_alive() for t in threading.enumerate()):
            break
        time.sleep(0.01)


def test_bounded_legacy_wrapper_remains_boolean_compatible():
    assert cli_support._run_bounded(lambda: 7, timeout_seconds=0.2, default=None) == (7, False)
    assert cli_support._run_bounded(
        lambda: (_ for _ in ()).throw(ValueError("x")),
        timeout_seconds=0.2,
        default=None,
    ) == (None, False)
