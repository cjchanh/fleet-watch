"""Reflex emit: findings -> queued spec + receipt, dry-run by default.

Failing-before anchor: on base (18d4b97) this module cannot be collected at
all — ``fleet_watch/reflex.py`` and ``fleet_watch/commands/reflex.py`` do not
exist, so ``from fleet_watch import reflex`` raises ImportError and pytest
reports a collection error for both test files. See
``test_base_revision_has_no_reflex_module`` for the in-test proof.

Everything here is hermetic: queue dir, receipts dir, the finding source and
the clock are all injected, and the suite never reads the live registry.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import reflex
from fleet_watch.commands import reflex as reflex_command

BASE_REV = "18d4b97"

# The planted bad. A supervised keeper (launchd KeepAlive / explicit keeper)
# presents to a naive parent-PID heuristic exactly like an orphan: its owner is
# gone and the row looks dead. It is not an orphan. The emitter MUST reject it
# — if a spec is ever queued from this row, the batch is void.
PLANTED_BAD_ROW = {
    "kind": "dead_session_orphan",
    "finding_id": "dead_session_orphan:session=keeper-a",
    "summary": "dead session owner: keeper-a (pid 4242) holds a lease",
    "severity": "high",
    "ticks": 9,
    "detail": {
        "session_id": "keeper-a",
        "owner_pid": 4242,
        "orphan_confirmed": True,
        "supervised": True,
        "supervisor_label": "com.cj.ollama-runner",
        "owner_alive": False,
    },
}


class FakeClock:
    """Injectable clock. Every timestamp in the module flows through this."""

    def __init__(self, start: float = 1_757_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def denial_finding(*, ticks: int = 3, pid: int = 5150) -> dict:
    return {
        "kind": "process_decision_denial",
        "finding_id": f"process_decision_denial:pid={pid}:action=terminate",
        "summary": f"process decision denied: pid {pid} terminate — policy denied",
        "severity": "high",
        "ticks": ticks,
        "detail": {
            "pid": pid,
            "action": "terminate",
            "reason": "policy denied",
            "ticks": ticks,
            "event_hash": "d" * 64,
        },
    }


def stale_finding(*, pid: int = 31337) -> dict:
    return {
        "kind": "heartbeat_stale",
        "finding_id": f"heartbeat_stale:pid={pid}",
        "summary": f"heartbeat stale: worker (pid {pid}) — 900s",
        "severity": "medium",
        "detail": {
            "pid": pid,
            "name": "worker",
            "classification": "stale",
            "stale_seconds": 900,
            "session_id": "sess-1",
            "evidence": ["no heartbeat for 900s"],
        },
    }


def runaway_finding(*, pid: int = 777) -> dict:
    return {
        "kind": "runaway",
        "finding_id": f"runaway:pid={pid}",
        "summary": f"runaway cpu: burner (pid {pid}) at 99.5%",
        "severity": "high",
        "detail": {"pid": pid, "name": "burner", "cpu_pct": 99.5, "runtime_seconds": 900},
    }


def write_rows(path: Path, rows: list[dict]) -> Path:
    path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    return path


# ── failing-before anchor ───────────────────────────────────────────────────


def test_base_revision_has_no_reflex_module():
    """The module did not exist on base, so the suite below could not run."""
    git_dir = Path(__file__).resolve().parents[1] / ".git"
    if not git_dir.exists():
        pytest.skip("not a git checkout; run the base-revision proof manually")
    for path in ("fleet_watch/reflex.py", "fleet_watch/commands/reflex.py"):
        proc = subprocess.run(
            ["git", "cat-file", "-e", f"{BASE_REV}:{path}"],
            cwd=git_dir.parent,
            capture_output=True,
        )
        assert proc.returncode != 0, f"{path} unexpectedly exists on {BASE_REV}"


# ── the loop holds no process-control authority ─────────────────────────────


def test_reflex_source_has_no_process_control_call_sites():
    """Grep-verified: the new code observes and emits, nothing more."""
    package = Path(reflex.__file__).parent
    targets = [package / "reflex.py", package / "commands" / "reflex.py"]
    forbidden = re.compile(
        r"os\.kill|os\.waitpid|\bkill\b|\bsignal\b|subprocess|\bexecv|\bexecve|"
        r"\bpopen\b|\bfork\b|os\.system|os\.fork|pty\.spawn",
    )
    for target in targets:
        text = target.read_text(encoding="utf-8")
        hits = [
            f"{target.name}:{i}: {line.strip()}"
            for i, line in enumerate(text.splitlines(), 1)
            if forbidden.search(line)
        ]
        assert hits == [], f"process-control call sites in {target}: {hits}"


# ── dry run is the default ──────────────────────────────────────────────────


def test_emit_is_dry_run_by_default_and_touches_nothing(tmp_path):
    queue = tmp_path / "queue"
    receipts = tmp_path / "receipts"
    report = reflex.emit(
        [denial_finding()], queue_dir=queue, receipts_dir=receipts, write=False
    )
    assert report.mode == "dry-run"
    assert len(report.emitted) == 1
    assert report.emitted[0]["wrote"] is False
    assert report.emitted[0]["spec_path"] == str(
        queue / f"{report.emitted[0]['spec_id']}.md"
    )
    assert not queue.exists()
    assert not receipts.exists()


def test_emit_write_queues_exactly_one_spec_and_one_receipt(tmp_path):
    """Acceptance 1: a finding surviving N ticks -> one spec, one receipt."""
    queue = tmp_path / "queue"
    receipts = tmp_path / "receipts"
    clock = FakeClock()
    report = reflex.emit(
        [denial_finding(ticks=reflex.DEFAULT_MIN_TICKS)],
        queue_dir=queue,
        receipts_dir=receipts,
        write=True,
        clock=clock,
    )
    assert report.rejected == []
    assert len(report.emitted) == 1
    assert len(list(queue.glob("*.md"))) == 1
    assert len(list(receipts.glob("*.json"))) == 1

    receipt = json.loads(next(receipts.glob("*.json")).read_text())
    assert receipt["schema_version"] == reflex.RECEIPT_SCHEMA_VERSION
    assert receipt["finding_id"] == denial_finding()["finding_id"]
    assert receipt["spec_id"] == report.emitted[0]["spec_id"]
    assert receipt["emitted_at"] == reflex._iso(clock.now)
    assert receipt["verified"] is False
    assert receipt["evidence_sha256"] == reflex.evidence_sha256(
        reflex.finding_from_row(denial_finding())
    )
    assert receipt["ticks_observed"] == reflex.DEFAULT_MIN_TICKS


def test_receipt_path_is_under_the_injected_receipts_dir(tmp_path):
    receipts = tmp_path / "nested" / "receipts"
    report = reflex.emit(
        [stale_finding()], queue_dir=tmp_path / "q", receipts_dir=receipts, write=True
    )
    written = Path(report.emitted[0]["receipt_path"])
    assert written.parent == receipts
    assert written.is_file()


# ── the spec template ───────────────────────────────────────────────────────


def test_spec_carries_the_tuesday_template(tmp_path):
    queue = tmp_path / "queue"
    report = reflex.emit(
        [stale_finding()], queue_dir=queue, receipts_dir=tmp_path / "receipts", write=True
    )
    spec_path = Path(report.emitted[0]["spec_path"])
    text = spec_path.read_text(encoding="utf-8")

    assert text.startswith("---\n")
    for key in (
        "spec_id:",
        "finding_id:",
        "title:",
        "created:",
        "evidence_sha256:",
        "tuesday_standard: true",
        f"template_version: {reflex.SPEC_TEMPLATE_VERSION}",
    ):
        assert key in text, key
    for section in (
        "## Objective",
        "## Acceptance Criteria",
        "## Tuesday Bar Principles",
        "## Evidence",
        "## Stop Conditions",
    ):
        assert section in text, section
    assert report.emitted[0]["spec_sha256"] == reflex._sha256(text)


def test_emit_is_idempotent_while_a_receipt_is_open(tmp_path):
    queue = tmp_path / "queue"
    receipts = tmp_path / "receipts"
    first = reflex.emit(
        [denial_finding()], queue_dir=queue, receipts_dir=receipts, write=True
    )
    second = reflex.emit(
        [denial_finding()], queue_dir=queue, receipts_dir=receipts, write=True
    )
    assert len(first.emitted) == 1
    assert second.emitted == []
    assert second.rejected[0]["reason"].startswith("already_open:")
    assert len(list(queue.glob("*.md"))) == 1
    assert len(list(receipts.glob("*.json"))) == 1


def test_reemission_after_verification_gets_its_own_spec_id(tmp_path):
    queue = tmp_path / "queue"
    receipts = tmp_path / "receipts"
    first = reflex.emit(
        [denial_finding()], queue_dir=queue, receipts_dir=receipts, write=True
    )
    path = next(receipts.glob("*.json"))
    receipt = json.loads(path.read_text())
    receipt["verified"] = True
    path.write_text(json.dumps(receipt), encoding="utf-8")

    second = reflex.emit(
        [denial_finding()], queue_dir=queue, receipts_dir=receipts, write=True
    )
    assert len(second.emitted) == 1
    assert second.emitted[0]["spec_id"] != first.emitted[0]["spec_id"]
    assert len(list(queue.glob("*.md"))) == 2


# ── the planted bad must never be emitted ───────────────────────────────────


def test_planted_bad_supervised_keeper_is_not_emitted(tmp_path):
    queue = tmp_path / "queue"
    receipts = tmp_path / "receipts"
    report = reflex.emit(
        [PLANTED_BAD_ROW], queue_dir=queue, receipts_dir=receipts, write=True
    )
    assert report.emitted == []
    assert len(report.rejected) == 1
    assert report.rejected[0]["reason"] == "supervised_keeper"
    assert not list(queue.glob("*.md"))
    assert not list(receipts.glob("*.json"))


@pytest.mark.parametrize(
    "key,value",
    [
        ("supervised", True),
        ("supervision", True),
        ("supervisor", "com.cj.ollama-runner"),
        ("supervisor_label", "com.cj.ollama-runner"),
        ("launchd_label", "com.cj.ollama-runner"),
        ("keepalive", True),
        ("keeper", True),
    ],
)
def test_every_supervision_key_rejects_the_keeper(tmp_path, key, value):
    row = dict(PLANTED_BAD_ROW)
    row["detail"] = {**PLANTED_BAD_ROW["detail"], "supervised": False, "keeper": False}
    row["detail"][key] = value
    report = reflex.emit(
        [row], queue_dir=tmp_path / "q", receipts_dir=tmp_path / "r", write=True
    )
    assert report.emitted == []
    assert report.rejected[0]["reason"] == "supervised_keeper"


def test_planted_bad_does_not_vacate_the_real_findings_in_the_batch(tmp_path):
    """A batch containing the keeper still emits the genuine finding."""
    queue = tmp_path / "queue"
    receipts = tmp_path / "receipts"
    report = reflex.emit(
        [PLANTED_BAD_ROW, stale_finding(), runaway_finding()],
        queue_dir=queue,
        receipts_dir=receipts,
        write=True,
    )
    assert {row["finding_id"] for row in report.emitted} == {
        stale_finding()["finding_id"],
        runaway_finding()["finding_id"],
    }
    assert [row["reason"] for row in report.rejected] == ["supervised_keeper"]
    assert len(list(queue.glob("*.md"))) == 2
    assert len(list(receipts.glob("*.json"))) == 2


def test_orphan_without_proven_identity_is_rejected(tmp_path):
    row = dict(PLANTED_BAD_ROW)
    row["detail"] = {**PLANTED_BAD_ROW["detail"]}
    row["detail"].pop("supervised")
    row["detail"].pop("supervisor_label")
    row["detail"]["orphan_confirmed"] = False
    report = reflex.emit(
        [row], queue_dir=tmp_path / "q", receipts_dir=tmp_path / "r", write=True
    )
    assert report.emitted == []
    assert report.rejected[0]["reason"] == "identity_unproven:orphan_not_confirmed"


# ── tick gating on the event-shaped finding ─────────────────────────────────


def test_denial_below_the_tick_threshold_is_rejected(tmp_path):
    report = reflex.emit(
        [denial_finding(ticks=2)],
        queue_dir=tmp_path / "q",
        receipts_dir=tmp_path / "r",
        write=True,
        min_ticks=3,
    )
    assert report.emitted == []
    assert report.rejected[0]["reason"] == "below_tick_threshold:2<3"


def test_tick_threshold_is_configurable(tmp_path):
    report = reflex.emit(
        [denial_finding(ticks=2)],
        queue_dir=tmp_path / "q",
        receipts_dir=tmp_path / "r",
        write=True,
        min_ticks=2,
    )
    assert len(report.emitted) == 1


def test_state_shaped_findings_are_not_tick_gated(tmp_path):
    """heartbeat_stale / runaway / orphan are current state, not events."""
    for kind in ("heartbeat_stale", "runaway"):
        row = {"kind": kind, "summary": f"{kind} row", "detail": {"pid": 1}, "ticks": 1}
        finding = reflex.finding_from_row(row)
        screening = reflex.screen_finding(finding, min_ticks=3)
        assert screening.emit is True, kind


# ── screening rejects the rest of the garbage ───────────────────────────────


@pytest.mark.parametrize(
    "row,reason",
    [
        ({"kind": "not_a_kind", "summary": "x", "detail": {"a": 1}}, "unknown_kind:not_a_kind"),
        ({"kind": "", "summary": "x", "detail": {"a": 1}}, "unknown_kind:missing"),
        ({"kind": "runaway", "summary": "x", "detail": {}}, "insufficient_evidence:empty_detail"),
    ],
)
def test_screen_rejects_malformed_rows(row, reason):
    screening = reflex.screen_finding(reflex.finding_from_row(row))
    assert screening.emit is False
    assert screening.reason == reason


def test_screen_rejects_a_finding_with_no_summary():
    finding = reflex.Finding(finding_id="x", kind="runaway", summary="", detail={"a": 1})
    assert reflex.screen_finding(finding).reason == "malformed:missing_summary"


def test_finding_from_row_is_total_on_garbage():
    for row in (None, "not a mapping", 42, []):
        finding = reflex.finding_from_row(row)
        assert isinstance(finding.finding_id, str)
        assert reflex.screen_finding(finding).emit is False


def test_finding_id_is_deterministic_across_parsing():
    first = reflex.finding_from_row(stale_finding())
    second = reflex.finding_from_row(stale_finding())
    assert first.finding_id == second.finding_id
    assert reflex.finding_key(first) == reflex.finding_key(second)


def test_finding_id_derives_from_identity_when_absent():
    finding = reflex.finding_from_row({"kind": "runaway", "pid": 9, "detail": {"cpu_pct": 99}})
    assert finding.finding_id == "runaway:pid=9"


# ── CLI surface ─────────────────────────────────────────────────────────────


def run_cli(args: list[str]):
    return CliRunner().invoke(cli_module.cli, ["reflex", *args])


def test_cli_emit_dry_run_writes_nothing(tmp_path):
    rows = write_rows(tmp_path / "rows.json", [denial_finding(), PLANTED_BAD_ROW])
    result = run_cli([
        "emit",
        "--findings-file", str(rows),
        "--queue-dir", str(tmp_path / "queue"),
        "--receipts-dir", str(tmp_path / "receipts"),
        "--json",
    ])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["mode"] == "dry-run"
    assert len(payload["emitted"]) == 1
    assert payload["rejected"][0]["reason"] == "supervised_keeper"
    assert not (tmp_path / "queue").exists()
    assert not (tmp_path / "receipts").exists()


def test_cli_emit_write_lands_the_spec(tmp_path):
    rows = write_rows(tmp_path / "rows.json", [stale_finding()])
    result = run_cli([
        "emit",
        "--write",
        "--findings-file", str(rows),
        "--queue-dir", str(tmp_path / "queue"),
        "--receipts-dir", str(tmp_path / "receipts"),
        "--json",
    ])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert len(list((tmp_path / "queue").glob("*.md"))) == 1
    assert len(list((tmp_path / "receipts").glob("*.json"))) == 1
    assert payload["counts"]["emitted"] == 1


def test_cli_emit_text_mode_reports_the_rejection(tmp_path):
    rows = write_rows(tmp_path / "rows.json", [PLANTED_BAD_ROW])
    result = run_cli([
        "emit",
        "--findings-file", str(rows),
        "--queue-dir", str(tmp_path / "queue"),
        "--receipts-dir", str(tmp_path / "receipts"),
    ])
    assert result.exit_code == 0, result.output
    assert "rejected" in result.output
    assert "supervised_keeper" in result.output


def test_cli_emit_reports_an_unreadable_source_without_crashing(tmp_path):
    result = run_cli([
        "emit",
        "--findings-file", str(tmp_path / "missing.json"),
        "--queue-dir", str(tmp_path / "queue"),
        "--receipts-dir", str(tmp_path / "receipts"),
        "--json",
    ])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["source_errors"]
    assert payload["counts"]["emitted"] == 0


def test_cli_status_json_is_schema_parseable(tmp_path):
    rows = write_rows(tmp_path / "rows.json", [denial_finding(), PLANTED_BAD_ROW])
    run_cli([
        "emit",
        "--write",
        "--findings-file", str(rows),
        "--queue-dir", str(tmp_path / "queue"),
        "--receipts-dir", str(tmp_path / "receipts"),
    ])
    result = run_cli([
        "status",
        "--json",
        "--findings-file", str(rows),
        "--queue-dir", str(tmp_path / "queue"),
        "--receipts-dir", str(tmp_path / "receipts"),
    ])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)

    assert payload["schema_version"] == reflex.STATUS_SCHEMA_VERSION
    assert set(payload) >= {
        "schema_version",
        "generated_at",
        "queue_dir",
        "receipts_dir",
        "ttl_seconds",
        "counts",
        "open_findings",
        "observed_not_queued",
    }
    assert payload["counts"]["open_findings"] == 1
    open_row = payload["open_findings"][0]
    assert set(open_row) >= {
        "finding_id",
        "kind",
        "spec_id",
        "spec_path",
        "receipt_fresh",
        "age_seconds",
        "ttl_exceeded",
    }
    assert open_row["finding_id"] == denial_finding()["finding_id"]
    assert open_row["receipt_fresh"] is True
    assert Path(open_row["spec_path"]).is_file()


def test_cli_status_json_parses_when_nothing_is_queued(tmp_path):
    result = run_cli([
        "status",
        "--json",
        "--findings-file", str(write_rows(tmp_path / "empty.json", [])),
        "--queue-dir", str(tmp_path / "queue"),
        "--receipts-dir", str(tmp_path / "receipts"),
    ])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["open_findings"] == []
    assert payload["counts"]["open_findings"] == 0


def test_cli_status_text_mode(tmp_path):
    result = run_cli([
        "status",
        "--findings-file", str(write_rows(tmp_path / "empty.json", [])),
        "--queue-dir", str(tmp_path / "queue"),
        "--receipts-dir", str(tmp_path / "receipts"),
    ])
    assert result.exit_code == 0, result.output
    assert "Reflex status" in result.output
    assert "No open findings." in result.output


def test_reflex_group_is_registered_on_the_root_cli():
    assert "reflex" in cli_module.cli.commands
    assert set(reflex_command.reflex_group.commands) == {"emit", "verify", "status"}
    assert reflex_command.reflex_group.name == "reflex"
