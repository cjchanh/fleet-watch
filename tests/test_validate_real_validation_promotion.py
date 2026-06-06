from __future__ import annotations

import json
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import validate_real_validation_promotion as validator


def _write_json(path: Path, data: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _valid_receipts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "receipts"
    intake = _write_json(
        root / "intake.json",
        {
            "schema_version": validator.INTAKE_RECEIPT_SCHEMA,
            "decision": "PASS",
            "params": {
                "promotion_gate_closed": False,
                "verified_raw_files": [
                    "raw/guard_calls.jsonl",
                    "raw/state_snapshots.jsonl",
                    "raw/health_snapshots.jsonl",
                    "raw/registry_events.jsonl",
                ],
            },
        },
    )
    comparator = _write_json(
        root / "comparator.json",
        {
            "schema_version": validator.COMPARATOR_SCHEMA,
            "decision": "PASS",
            "metrics": {
                "observation_count": 12,
                "mismatch_count": 0,
                "false_positive_count": 0,
                "false_negative_count": 0,
                "post_calibration_error_pct": 8.5,
            },
            "calibration": {
                "method": "fixture_threshold_backtest",
                "threshold_version": "v1",
            },
            "evidence": [
                {
                    "guard_call_id": "guard-1",
                    "path": "raw/guard_calls.jsonl",
                    "sha256": "0" * 64,
                }
            ],
        },
    )
    ua = _write_json(
        root / "ua.json",
        {
            "schema_version": validator.UA_SCHEMA,
            "decision": "PASS",
            "uncertainty": {
                "combined_uncertainty_pct": 9.0,
                "false_positive_risk_pct": 3.0,
                "false_negative_risk_pct": 4.0,
                "method": "fixture_bound_uncertainty",
            },
            "evidence": [
                {
                    "receipt_id": "ua-fixture",
                    "path": "ua/uncertainty_receipt.json",
                }
            ],
        },
    )
    return {"intake": intake, "comparator": comparator, "ua": ua}


def _build(paths: dict[str, Path]) -> dict:
    return validator.build_receipt(
        intake_path=paths["intake"],
        comparator_path=paths["comparator"],
        ua_path=paths["ua"],
        error_threshold_pct=15.0,
        ua_threshold_pct=15.0,
        risk_threshold_pct=15.0,
    )


def test_valid_receipts_mark_ready_without_applying_promotion(tmp_path: Path) -> None:
    receipt = _build(_valid_receipts(tmp_path))
    assert receipt["decision"] == "PASS"
    assert receipt["triggered_rules"] == []
    assert receipt["params"]["tier_a_promotion_ready"] is True
    assert receipt["params"]["promotion_applied"] is False


def test_intake_must_pass_and_keep_gate_open(tmp_path: Path) -> None:
    paths = _valid_receipts(tmp_path)
    intake = json.loads(paths["intake"].read_text(encoding="utf-8"))
    intake["decision"] = "BLOCKED"
    intake["params"]["promotion_gate_closed"] = True
    _write_json(paths["intake"], intake)
    receipt = _build(paths)
    assert receipt["decision"] == "BLOCKED"
    assert "intake_decision_not_pass" in receipt["triggered_rules"]
    assert "intake_promotion_gate_state_invalid" in receipt["triggered_rules"]


def test_missing_comparator_blocks(tmp_path: Path) -> None:
    paths = _valid_receipts(tmp_path)
    paths["comparator"].unlink()
    receipt = _build(paths)
    assert receipt["decision"] == "BLOCKED"
    assert "missing_json:comparator" in receipt["triggered_rules"]


def test_comparator_mismatch_blocks(tmp_path: Path) -> None:
    paths = _valid_receipts(tmp_path)
    comparator = json.loads(paths["comparator"].read_text(encoding="utf-8"))
    comparator["metrics"]["mismatch_count"] = 1
    _write_json(paths["comparator"], comparator)
    receipt = _build(paths)
    assert receipt["decision"] == "BLOCKED"
    assert "comparator_mismatch_count_nonzero" in receipt["triggered_rules"]


def test_comparator_error_threshold_blocks(tmp_path: Path) -> None:
    paths = _valid_receipts(tmp_path)
    comparator = json.loads(paths["comparator"].read_text(encoding="utf-8"))
    comparator["metrics"]["post_calibration_error_pct"] = 16.0
    _write_json(paths["comparator"], comparator)
    receipt = _build(paths)
    assert receipt["decision"] == "BLOCKED"
    assert "comparator_error_pct_over_threshold" in receipt["triggered_rules"]


def test_ua_thresholds_block(tmp_path: Path) -> None:
    paths = _valid_receipts(tmp_path)
    ua = json.loads(paths["ua"].read_text(encoding="utf-8"))
    ua["uncertainty"]["combined_uncertainty_pct"] = 16.0
    ua["uncertainty"]["false_negative_risk_pct"] = 16.0
    _write_json(paths["ua"], ua)
    receipt = _build(paths)
    assert receipt["decision"] == "BLOCKED"
    assert "ua_combined_uncertainty_pct_over_threshold" in receipt["triggered_rules"]
    assert "ua_false_negative_risk_pct_over_threshold" in receipt["triggered_rules"]


def test_unanchored_evidence_blocks(tmp_path: Path) -> None:
    paths = _valid_receipts(tmp_path)
    comparator = json.loads(paths["comparator"].read_text(encoding="utf-8"))
    comparator["evidence"] = [{}]
    _write_json(paths["comparator"], comparator)
    receipt = _build(paths)
    assert receipt["decision"] == "BLOCKED"
    assert "comparator_evidence_unanchored:0" in receipt["triggered_rules"]


def test_cli_writes_blocked_receipt_and_exit_code(tmp_path: Path, capsys) -> None:
    paths = _valid_receipts(tmp_path)
    ua = json.loads(paths["ua"].read_text(encoding="utf-8"))
    ua["decision"] = "BLOCKED"
    _write_json(paths["ua"], ua)
    out_path = tmp_path / "promotion_readiness_receipt.json"
    rc = validator.main(
        [
            "--intake-receipt",
            str(paths["intake"]),
            "--comparator",
            str(paths["comparator"]),
            "--ua",
            str(paths["ua"]),
            "--receipt-out",
            str(out_path),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    written = json.loads(out_path.read_text(encoding="utf-8"))
    assert rc == validator.EXIT_BLOCKED
    assert output == written
    assert "ua_decision_not_pass" in output["triggered_rules"]
