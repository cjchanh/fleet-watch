"""Reflex verify: verify-on-land re-check of every open receipt.

Three outcomes, one per open receipt: cleared (finding gone -> receipt
``verified=true``), persists (still open, inside the TTL -> hold), escalated
(still open past the TTL -> re-queue a new spec with an escalation note).

Hermetic: the probe and the clock are injected, queue dir and receipts dir are
temp dirs, and the default live probe is never reached from a test.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import reflex


class FakeClock:
    def __init__(self, start: float = 1_757_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class RecordingProbe:
    """Injectable re-check. Records every finding id it was asked about."""

    def __init__(self, present: dict[str, bool] | None = None) -> None:
        self.present = present or {}
        self.calls: list[str] = []

    def __call__(self, finding_id: str) -> reflex.Recheck:
        self.calls.append(finding_id)
        return reflex.Recheck(
            present=self.present.get(finding_id, False),
            detail={"source": "test", "checked": finding_id},
        )


def stale_finding(*, pid: int = 31337) -> dict:
    return {
        "kind": "heartbeat_stale",
        "finding_id": f"heartbeat_stale:pid={pid}",
        "summary": f"heartbeat stale: worker (pid {pid}) — 900s",
        "severity": "medium",
        "detail": {"pid": pid, "name": "worker", "stale_seconds": 900},
    }


@pytest.fixture()
def seeded(tmp_path):
    """One written emission receipt, ready to be re-checked."""
    queue = tmp_path / "queue"
    receipts = tmp_path / "receipts"
    clock = FakeClock()

    def _seed(*rows: dict):
        report = reflex.emit(
            list(rows) or [stale_finding()],
            queue_dir=queue,
            receipts_dir=receipts,
            write=True,
            clock=clock,
        )
        return report, clock

    _seed.queue = queue
    _seed.receipts = receipts
    _seed.clock = clock
    return _seed


def read_receipt(seeded) -> dict:
    paths = list(seeded.receipts.glob("*.json"))
    assert len(paths) == 1
    return json.loads(paths[0].read_text(encoding="utf-8"))


def specs(seeded) -> list[Path]:
    return sorted(seeded.queue.glob("*.md"))


# ── cleared ─────────────────────────────────────────────────────────────────


def test_verify_clears_when_the_finding_is_gone(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]
    probe = RecordingProbe({finding_id: False})

    result = reflex.verify_open(
        probe=probe,
        receipts_dir=seeded.receipts,
        queue_dir=seeded.queue,
        write=True,
        ttl_seconds=3600,
        clock=clock,
    )
    assert result.cleared == [finding_id]
    assert result.escalated == []
    assert len(specs(seeded)) == 1

    receipt = read_receipt(seeded)
    assert receipt["verified"] is True
    assert receipt["verified_at"] == reflex._iso(clock.now)
    assert receipt["rechecks"][-1]["outcome"] == "cleared"
    assert receipt["rechecks"][-1]["present"] is False


def test_verify_holds_when_the_finding_persists_inside_the_ttl(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]
    clock.advance(600)
    probe = RecordingProbe({finding_id: True})

    result = reflex.verify_open(
        probe=probe,
        receipts_dir=seeded.receipts,
        queue_dir=seeded.queue,
        write=True,
        ttl_seconds=3600,
        clock=clock,
    )
    assert result.persisting == [finding_id]
    assert result.cleared == []
    assert result.escalated == []
    assert len(specs(seeded)) == 1

    receipt = read_receipt(seeded)
    assert receipt["verified"] is False
    assert receipt["rechecks"][-1]["outcome"] == "persists"
    assert receipt["rechecks"][-1]["age_seconds"] == 600


# ── escalated ───────────────────────────────────────────────────────────────


def test_verify_requeues_with_an_escalation_note_past_the_ttl(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]
    first_spec_id = report.emitted[0]["spec_id"]
    clock.advance(7200)
    probe = RecordingProbe({finding_id: True})

    result = reflex.verify_open(
        probe=probe,
        receipts_dir=seeded.receipts,
        queue_dir=seeded.queue,
        write=True,
        ttl_seconds=3600,
        clock=clock,
    )
    assert len(result.escalated) == 1
    escalation = result.escalated[0]
    assert escalation["requeues"] == 1
    assert escalation["spec_id"] != first_spec_id
    assert f"past the 3600s verify TTL" in escalation["escalation_note"]

    landed = sorted(seeded.queue.glob("*.md"))
    assert len(landed) == 2, "the superseded spec must survive the re-queue"
    new_spec = Path(escalation["spec_path"])
    assert new_spec.is_file()
    text = new_spec.read_text(encoding="utf-8")
    assert "escalation_note:" in text
    assert "## Escalation" in text
    assert escalation["escalation_note"] in text

    receipt = read_receipt(seeded)
    assert receipt["spec_id"] == escalation["spec_id"]
    assert receipt["requeues"] == 1
    assert receipt["escalation_note"] == escalation["escalation_note"]
    assert receipt["verified"] is False
    assert receipt["rechecks"][-1]["outcome"] == "escalated"
    assert receipt["rechecks"][-1]["spec_id"] == escalation["spec_id"]


def test_second_escalation_gets_its_own_spec_and_note(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]
    probe = RecordingProbe({finding_id: True})
    ids = [report.emitted[0]["spec_id"]]

    for _ in range(2):
        clock.advance(7200)
        result = reflex.verify_open(
            probe=probe,
            receipts_dir=seeded.receipts,
            queue_dir=seeded.queue,
            write=True,
            ttl_seconds=3600,
            clock=clock,
        )
        ids.append(result.escalated[0]["spec_id"])

    assert len(set(ids)) == 3, "every re-queue gets a distinct spec id"
    assert len(specs(seeded)) == 3
    receipt = read_receipt(seeded)
    assert receipt["requeues"] == 2
    assert "after 2 re-queue(s)" in receipt["escalation_note"]


# ── receipts for every re-check ─────────────────────────────────────────────


def test_each_recheck_appends_a_receipt_entry(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]
    probe = RecordingProbe({finding_id: True})

    for _ in range(3):
        clock.advance(60)
        reflex.verify_open(
            probe=probe,
            receipts_dir=seeded.receipts,
            queue_dir=seeded.queue,
            write=True,
            ttl_seconds=99999,
            clock=clock,
        )

    receipt = read_receipt(seeded)
    assert len(receipt["rechecks"]) == 3
    stamps = [entry["at"] for entry in receipt["rechecks"]]
    assert stamps == sorted(stamps)
    for entry in receipt["rechecks"]:
        assert set(entry) >= {"at", "at_epoch", "outcome", "present", "age_seconds", "detail"}
    assert len(specs(seeded)) == 1, "inside the TTL no new spec is queued"


def test_verified_receipts_are_not_probed_again(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]
    probe = RecordingProbe({finding_id: False})
    reflex.verify_open(
        probe=probe,
        receipts_dir=seeded.receipts,
        queue_dir=seeded.queue,
        write=True,
        clock=clock,
    )
    assert probe.calls == [finding_id]

    second = reflex.verify_open(
        probe=probe,
        receipts_dir=seeded.receipts,
        queue_dir=seeded.queue,
        write=True,
        clock=clock,
    )
    assert second.checked == []
    assert probe.calls == [finding_id], "a verified receipt is closed, not re-tested"


# ── dry run is the default here too ─────────────────────────────────────────


def test_verify_is_dry_run_by_default_and_touches_nothing(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]
    clock.advance(7200)
    before = read_receipt(seeded)

    result = reflex.verify_open(
        probe=RecordingProbe({finding_id: True}),
        receipts_dir=seeded.receipts,
        queue_dir=seeded.queue,
        write=False,
        ttl_seconds=3600,
        clock=clock,
    )
    assert result.mode == "dry-run"
    assert len(result.escalated) == 1
    assert result.escalated[0]["spec_id"] not in [p.name for p in specs(seeded)]
    assert len(specs(seeded)) == 1
    assert read_receipt(seeded) == before


def test_probe_failure_is_reported_not_raised(seeded):
    report, clock = seeded()

    def exploding_probe(finding_id: str) -> reflex.Recheck:
        raise RuntimeError("registry unreachable")

    result = reflex.verify_open(
        probe=exploding_probe,
        receipts_dir=seeded.receipts,
        queue_dir=seeded.queue,
        write=True,
        clock=clock,
    )
    assert result.checked == []
    assert len(result.errors) == 1
    assert "registry unreachable" in result.errors[0]["reason"]
    assert read_receipt(seeded)["verified"] is False


# ── status surfaces the loop's state ────────────────────────────────────────


def test_status_reports_freshness_and_ttl(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]

    fresh = reflex.build_status(
        queue_dir=seeded.queue, receipts_dir=seeded.receipts, clock=clock, ttl_seconds=3600
    )
    assert fresh["counts"]["open_findings"] == 1
    assert fresh["open_findings"][0]["receipt_fresh"] is True
    assert fresh["open_findings"][0]["ttl_exceeded"] is False
    assert fresh["open_findings"][0]["spec_id"] == report.emitted[0]["spec_id"]

    clock.advance(7200)
    stale = reflex.build_status(
        queue_dir=seeded.queue, receipts_dir=seeded.receipts, clock=clock, ttl_seconds=3600
    )
    assert stale["open_findings"][0]["ttl_exceeded"] is True

    specs(seeded)[0].unlink()
    gone = reflex.build_status(
        queue_dir=seeded.queue, receipts_dir=seeded.receipts, clock=clock, ttl_seconds=3600
    )
    assert gone["open_findings"][0]["receipt_fresh"] is False
    assert "STALE" in reflex.status_text(gone)


def test_status_moves_a_cleared_receipt_out_of_the_open_list(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]
    reflex.verify_open(
        probe=RecordingProbe({finding_id: False}),
        receipts_dir=seeded.receipts,
        queue_dir=seeded.queue,
        write=True,
        clock=clock,
    )
    status = reflex.build_status(
        queue_dir=seeded.queue, receipts_dir=seeded.receipts, clock=clock
    )
    assert status["open_findings"] == []
    assert status["counts"]["verified_findings"] == 1


# ── CLI surface ─────────────────────────────────────────────────────────────


def run_cli(args: list[str]):
    return CliRunner().invoke(cli_module.cli, ["reflex", *args])


def test_cli_verify_clears_through_a_probe_file(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]
    probe_file = seeded.receipts.parent / "probe.json"
    probe_file.write_text(json.dumps({finding_id: {"present": False}}), encoding="utf-8")

    result = run_cli([
        "verify",
        "--write",
        "--probe-file", str(probe_file),
        "--receipts-dir", str(seeded.receipts),
        "--queue-dir", str(seeded.queue),
        "--json",
    ])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["cleared"] == [finding_id]
    assert read_receipt(seeded)["verified"] is True


def test_cli_verify_escalates_past_the_ttl(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]
    clock.advance(7200)
    probe_file = seeded.receipts.parent / "probe.json"
    probe_file.write_text(
        json.dumps([{"finding_id": finding_id, "present": True, "detail": {"source": "cli"}}]),
        encoding="utf-8",
    )

    result = run_cli([
        "verify",
        "--write",
        "--probe-file", str(probe_file),
        "--ttl", "3600",
        "--receipts-dir", str(seeded.receipts),
        "--queue-dir", str(seeded.queue),
        "--json",
    ])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["counts"]["escalated"] == 1
    assert payload["ttl_seconds"] == 3600
    assert len(specs(seeded)) == 2
    assert "escalation_note" in payload["escalated"][0]


def test_cli_verify_reports_an_unreadable_probe_file(seeded):
    result = run_cli([
        "verify",
        "--probe-file", str(seeded.receipts.parent / "missing.json"),
        "--receipts-dir", str(seeded.receipts),
        "--queue-dir", str(seeded.queue),
        "--json",
    ])
    assert result.exit_code == 0, result.output
    assert "probe_file_unreadable" in json.loads(result.output)["error"]


def test_cli_verify_text_mode(seeded):
    report, clock = seeded()
    finding_id = report.emitted[0]["finding_id"]
    probe_file = seeded.receipts.parent / "probe.json"
    probe_file.write_text(json.dumps({finding_id: False}), encoding="utf-8")
    result = run_cli([
        "verify",
        "--write",
        "--probe-file", str(probe_file),
        "--receipts-dir", str(seeded.receipts),
        "--queue-dir", str(seeded.queue),
    ])
    assert result.exit_code == 0, result.output
    assert "Reflex verify" in result.output
    assert "cleared" in result.output


def test_cli_verify_reports_no_open_receipts(tmp_path):
    probe_file = tmp_path / "probe.json"
    probe_file.write_text("{}", encoding="utf-8")
    result = run_cli([
        "verify",
        "--probe-file", str(probe_file),
        "--receipts-dir", str(tmp_path / "none"),
        "--queue-dir", str(tmp_path / "none-queue"),
    ])
    assert result.exit_code == 0, result.output
    assert "No open receipts." in result.output
