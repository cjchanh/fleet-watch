"""The lease CLAIM path must answer as soon as the lease is durable.

Regression for a live 2026-09-03 failure: an Edit was denied with
``single_writer_claim_failed: 'fleet session start' timed out after 5 seconds``
while the registry showed that very lease ACTIVE, one write scope, started
seconds earlier. The registry write had landed; only the acknowledgement was
late, because ``session start`` ran ``reporter.write_report`` — a `ps`/socket
fan-out measured at ~900ms quiet and documented at 8s under load — INLINE
before echoing. The command reported failure for a claim that had succeeded,
so a gate refused a write it had already been granted authority for.

These tests assert the ORDER of operations, not a wall clock. A timing-only
test would pass on a fast machine no matter how the code was arranged, which is
precisely the property that let the defect exist; the clock is used only to
prove the command does not WAIT for the slow half.
"""

import os
import sqlite3
import time

import pytest
from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import events, referee, registry, reporter


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    """Registry and report output both inside tmp_path — never ~/.fleet-watch."""
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")
    return tmp_path


def _lease(session_id: str) -> dict | None:
    conn = registry.connect()
    try:
        return registry.get_session_lease(conn, session_id)
    finally:
        conn.close()


def test_ack_lands_after_the_commit_and_before_the_report(isolated, monkeypatch):
    """upsert -> event -> ACK -> report. The ack may not wait on the report."""
    order: list[str] = []

    real_upsert = registry.upsert_session_lease
    real_log = events.log_event
    real_ack = cli_module._ack

    def traced_upsert(conn, session_id, **kwargs):
        real_upsert(conn, session_id, **kwargs)
        # Appended AFTER the call, so this records the commit, not the intent.
        order.append("commit")

    def traced_log(conn, event_type, **kwargs):
        order.append(f"event:{event_type}")
        return real_log(conn, event_type, **kwargs)

    def traced_ack(message):
        order.append("ack")
        real_ack(message)

    def traced_report(conn, output_dir=None):
        order.append("report")
        return (isolated / "STATE_REPORT.md", isolated / "state.json")

    monkeypatch.setattr(registry, "upsert_session_lease", traced_upsert)
    monkeypatch.setattr(events, "log_event", traced_log)
    monkeypatch.setattr(cli_module, "_ack", traced_ack)
    monkeypatch.setattr(reporter, "write_report", traced_report)

    result = CliRunner().invoke(
        cli_module.cli,
        ["session", "start", "--session-id", "order-test",
         "--owner-pid", str(os.getpid())],
    )

    assert result.exit_code == 0, result.output
    assert order == ["commit", "event:SESSION_START", "ack", "report"], order
    # _ack really does write the acknowledgement (the trace above wraps it, so
    # this is what proves the traced step is the user-visible one).
    assert "order-test" in result.output and "active" in result.output


def test_heartbeat_and_ensure_use_the_same_ordering(isolated, monkeypatch):
    """Heartbeats run every turn from several sessions — they generate the
    contention that made the claim path slow, so they carry the same rule."""
    order: list[str] = []
    real_ack = cli_module._ack

    def traced_ack(message):
        order.append("ack")
        real_ack(message)

    def traced_report(conn, output_dir=None):
        order.append("report")
        return (isolated / "STATE_REPORT.md", isolated / "state.json")

    monkeypatch.setattr(cli_module, "_ack", traced_ack)
    monkeypatch.setattr(reporter, "write_report", traced_report)
    # Coalescing is exercised separately; this test is about ORDER, so every
    # command must actually reach its report step.
    monkeypatch.setenv(cli_module.REPORT_MIN_INTERVAL_ENV, "0")

    runner = CliRunner()
    started = runner.invoke(
        cli_module.cli,
        ["session", "ensure", "--session-id", "hb-test",
         "--owner-pid", str(os.getpid()), "--retries", "1"],
    )
    assert started.exit_code == 0, started.output

    beat = runner.invoke(
        cli_module.cli,
        ["session", "heartbeat", "--session-id", "hb-test",
         "--owner-pid", str(os.getpid())],
    )
    assert beat.exit_code == 0, beat.output

    # ensure: ack then report. heartbeat: ack then report.
    assert order == ["ack", "report", "ack", "report"], order


def test_claim_returns_without_waiting_for_a_slow_report(isolated, monkeypatch):
    """The critical section is short BY CONSTRUCTION, not by luck.

    The report is made pathologically slow (10s) and the budget tiny. The
    command must acknowledge and exit anyway: the lease is durable, and an
    observability refresh may not decide whether a gate gets its answer.
    The 5s bound is generous on purpose — it is the caller's real timeout, and
    the assertion is 'nowhere near the 10s sleep', not 'fast'.
    """
    monkeypatch.setenv(cli_module.REPORT_BUDGET_ENV, "0.25")

    def glacial_report(conn, output_dir=None):
        time.sleep(10.0)
        raise AssertionError("unreachable: this thread must be abandoned")

    monkeypatch.setattr(reporter, "write_report", glacial_report)

    start = time.perf_counter()
    result = CliRunner().invoke(
        cli_module.cli,
        ["session", "start", "--session-id", "slow-report",
         "--owner-pid", str(os.getpid())],
    )
    elapsed = time.perf_counter() - start

    assert result.exit_code == 0, result.output
    assert "active" in result.output
    assert elapsed < 5.0, f"claim waited {elapsed:.2f}s on the report"
    # The overrun is disclosed, never silent.
    assert "report refresh exceeded" in result.stderr
    # And the thing the caller actually asked for is durable.
    lease = _lease("slow-report")
    assert lease is not None and lease["status"] == "ACTIVE"


def test_a_failing_report_does_not_fail_the_claim(isolated, monkeypatch):
    """The lease is committed before the report runs; a broken report is a
    stale dashboard, not a denied write."""

    def broken_report(conn, output_dir=None):
        raise OSError("no space left on device")

    monkeypatch.setattr(reporter, "write_report", broken_report)

    result = CliRunner().invoke(
        cli_module.cli,
        ["session", "start", "--session-id", "broken-report",
         "--owner-pid", str(os.getpid())],
    )

    assert result.exit_code == 0, result.output
    assert "report refresh failed" in result.stderr
    lease = _lease("broken-report")
    assert lease is not None and lease["status"] == "ACTIVE"


def test_restarting_the_same_session_id_is_idempotent(isolated, monkeypatch):
    """A caller that times out and retries must not create a second lease,
    transfer ownership, or lose its scopes."""
    monkeypatch.setattr(reporter, "write_report",
                        lambda conn, output_dir=None: (isolated / "a", isolated / "b"))
    repo = isolated / "repo"
    (repo / "src").mkdir(parents=True)
    scope = str(repo / "src" / "mod.py")

    argv = ["session", "start", "--session-id", "retry-same",
            "--owner-pid", str(os.getpid()), "--repo", str(repo),
            "--write-scope", scope]
    runner = CliRunner()
    first = runner.invoke(cli_module.cli, argv)
    assert first.exit_code == 0, first.output
    before = _lease("retry-same")

    second = runner.invoke(cli_module.cli, argv)
    assert second.exit_code == 0, second.output
    after = _lease("retry-same")

    conn = registry.connect()
    try:
        rows = conn.execute(
            "SELECT COUNT(*) FROM session_leases WHERE session_id = ?",
            ("retry-same",),
        ).fetchone()[0]
    finally:
        conn.close()

    assert rows == 1, "a retry created a second lease row"
    assert after["status"] == "ACTIVE" and after["shutdown_at"] is None
    assert after["started_at"] == before["started_at"], "a retry restarted the clock"
    assert after["owner_pid"] == before["owner_pid"]
    assert after["repo_dir"] == before["repo_dir"]
    assert after["write_scopes"] == before["write_scopes"]
    assert after["repo_lock_mode"] == before["repo_lock_mode"]
    # The one thing a retry DOES change, pinned by test rather than assumed:
    # every grant advances the fencing epoch. Its only reader is
    # registry.fencing_token_valid (advisory, no production caller), so a retry
    # is safe today — but a consumer that caches an epoch across a retry breaks.
    assert after["fencing_epoch"] == before["fencing_epoch"] + 1


def test_deny_path_is_unchanged_by_the_reordering(isolated, monkeypatch):
    """NEGATIVE CONTROL. Moving the report after the ack must not have moved
    the refusal: an overlapping scope still exits 1 and writes no lease."""
    monkeypatch.setattr(reporter, "write_report",
                        lambda conn, output_dir=None: (isolated / "a", isolated / "b"))
    repo = isolated / "shared"
    (repo / "src").mkdir(parents=True)
    scope = str(repo / "src" / "contested.py")

    conn = registry.connect()
    registry.upsert_session_lease(
        conn, "peer", owner_pid=os.getpid(), repo_dir=str(repo),
        repo_lock_mode="cooperative", write_scopes=(scope,),
    )
    conn.close()

    result = CliRunner().invoke(
        cli_module.cli,
        ["session", "start", "--session-id", "intruder",
         "--owner-pid", str(os.getpid()), "--repo", str(repo),
         "--write-scope", scope],
    )

    assert result.exit_code == 1
    assert "DENY:" in result.stderr
    assert _lease("intruder") is None, "a denied claim still wrote a lease"


def test_report_budget_is_env_overridable_and_rejects_nonsense(monkeypatch):
    monkeypatch.delenv(cli_module.REPORT_BUDGET_ENV, raising=False)
    assert cli_module._report_budget_seconds() == cli_module.DEFAULT_REPORT_BUDGET_S
    monkeypatch.setenv(cli_module.REPORT_BUDGET_ENV, "0.5")
    assert cli_module._report_budget_seconds() == 0.5
    for bad in ("not-a-number", "0", "-3"):
        monkeypatch.setenv(cli_module.REPORT_BUDGET_ENV, bad)
        assert cli_module._report_budget_seconds() == cli_module.DEFAULT_REPORT_BUDGET_S


def test_a_recent_report_coalesces_the_next_claims_refresh(isolated, monkeypatch):
    """6-7 sessions heartbeat every turn. Without coalescing each one rebuilds a
    report the previous one just published, so the fan-out cost multiplies by
    the number of live sessions — which is exactly the load that produced the
    timeout."""
    calls: list[str] = []

    def counting_report(conn, output_dir=None):
        calls.append("report")
        (isolated / "state.json").write_text("{}")
        return (isolated / "STATE_REPORT.md", isolated / "state.json")

    monkeypatch.setattr(reporter, "write_report", counting_report)
    monkeypatch.setenv(cli_module.REPORT_MIN_INTERVAL_ENV, "60")

    runner = CliRunner()
    for n in range(3):
        result = runner.invoke(
            cli_module.cli,
            ["session", "start", "--session-id", f"coalesce-{n}",
             "--owner-pid", str(os.getpid())],
        )
        assert result.exit_code == 0, result.output

    assert calls == ["report"], (
        "the report was rebuilt again inside its own freshness window"
    )
    # All three leases still landed — coalescing skips the REPORT, never the write.
    for n in range(3):
        assert _lease(f"coalesce-{n}") is not None


def test_coalescing_is_disabled_by_a_zero_window(isolated, monkeypatch):
    """NEGATIVE CONTROL for the test above: with the window off, every claim
    refreshes, so the skip is the window's doing and not a broken call path."""
    calls: list[str] = []

    def counting_report(conn, output_dir=None):
        calls.append("report")
        (isolated / "state.json").write_text("{}")
        return (isolated / "STATE_REPORT.md", isolated / "state.json")

    monkeypatch.setattr(reporter, "write_report", counting_report)
    monkeypatch.setenv(cli_module.REPORT_MIN_INTERVAL_ENV, "0")

    runner = CliRunner()
    for n in range(3):
        runner.invoke(
            cli_module.cli,
            ["session", "start", "--session-id", f"nocoalesce-{n}",
             "--owner-pid", str(os.getpid())],
        )

    assert len(calls) == 3, calls


def test_a_refresh_that_never_completes_still_coalesces(isolated, monkeypatch):
    """The case a success-only key cannot cover. On a host where the rebuild
    reliably exceeds the budget, `state.json` is never republished by this path
    — so keying coalescing on success alone would make every claim pay the full
    budget forever and buy nothing. `fleet discover` publishes it unbounded."""
    starts: list[str] = []

    def never_finishes(conn, output_dir=None):
        starts.append("attempt")
        time.sleep(10.0)

    monkeypatch.setattr(reporter, "write_report", never_finishes)
    monkeypatch.setenv(cli_module.REPORT_BUDGET_ENV, "0.15")
    monkeypatch.setenv(cli_module.REPORT_MIN_INTERVAL_ENV, "60")

    runner = CliRunner()
    elapsed = []
    for n in range(3):
        t0 = time.perf_counter()
        result = runner.invoke(
            cli_module.cli,
            ["session", "start", "--session-id", f"nofinish-{n}",
             "--owner-pid", str(os.getpid())],
        )
        elapsed.append(time.perf_counter() - t0)
        assert result.exit_code == 0, result.output

    assert len(starts) == 1, f"paid the budget {len(starts)} times for nothing"
    assert not (isolated / "state.json").exists()
    # The 2nd and 3rd claims skip the budget entirely.
    assert elapsed[1] < elapsed[0] and elapsed[2] < elapsed[0], elapsed


def test_a_stale_report_is_not_treated_as_fresh(isolated, monkeypatch):
    """The freshness check must key on age, not on mere existence."""
    state = isolated / "state.json"
    state.write_text("{}")
    old = time.time() - 3600
    os.utime(state, (old, old))
    monkeypatch.setenv(cli_module.REPORT_MIN_INTERVAL_ENV, "10")
    assert cli_module._report_is_fresh() is False

    state.write_text("{}")
    assert cli_module._report_is_fresh() is True

    # A missing report is never fresh.
    state.unlink()
    assert cli_module._report_is_fresh() is False

    # An aged-out attempt marker is not fresh either.
    cli_module._mark_report_attempt()
    assert cli_module._report_is_fresh() is True
    marker = isolated / cli_module.REPORT_ATTEMPT_MARKER
    os.utime(marker, (old, old))
    assert cli_module._report_is_fresh() is False


def test_close_acks_before_its_report(isolated, monkeypatch):
    """A close that appears to fail makes the caller retry a revocation it
    already completed."""
    order: list[str] = []
    real_ack = cli_module._ack

    def traced_ack(message):
        order.append("ack")
        real_ack(message)

    monkeypatch.setattr(cli_module, "_ack", traced_ack)
    monkeypatch.setattr(reporter, "write_report",
                        lambda conn, output_dir=None: order.append("report") or
                        (isolated / "a", isolated / "b"))
    monkeypatch.setenv(cli_module.REPORT_MIN_INTERVAL_ENV, "0")

    runner = CliRunner()
    started = runner.invoke(
        cli_module.cli,
        ["session", "start", "--session-id", "close-order",
         "--owner-pid", str(os.getpid())],
    )
    assert started.exit_code == 0, started.output
    order.clear()

    closed = runner.invoke(
        cli_module.cli, ["session", "close", "--session-id", "close-order"]
    )
    assert closed.exit_code == 0, closed.output
    assert order == ["ack", "report"], order
    assert _lease("close-order")["status"] == "CLOSED"


def test_start_docstring_carries_the_read_back_contract():
    """The contract a timing-out caller needs is on the command it called.

    The hook that timed out lives in another runtime and another owner's tree;
    the only place it can read the rule is here.
    """
    doc = cli_module.session_start.__doc__ or ""
    assert "fleet session list --json" in doc
    assert "fencing_epoch" in doc
    for field in ("status", "repo_lock_mode", "write_scopes", "last_heartbeat_at"):
        assert field in doc, f"read-back contract omits {field}"
