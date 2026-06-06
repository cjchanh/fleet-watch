#!/usr/bin/env python3
"""Validate Fleet Watch promotion readiness without applying promotion."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EXIT_PASS = 0
EXIT_BLOCKED = 3

RECEIPT_SCHEMA = "fleet-watch/real-validation-promotion-readiness-receipt/v1"
INTAKE_RECEIPT_SCHEMA = "fleet-watch/real-validation-drop-receipt/v1"
COMPARATOR_SCHEMA = "fleet-watch/real-validation-comparator/v1"
UA_SCHEMA = "fleet-watch/real-validation-uncertainty-analysis/v1"

DEFAULT_ERROR_THRESHOLD_PCT = 15.0
DEFAULT_UA_THRESHOLD_PCT = 15.0
DEFAULT_RISK_THRESHOLD_PCT = 15.0


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_json(path: Path, rules: list[str], label: str) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        rules.append(f"missing_json:{label}")
        return {}
    except json.JSONDecodeError:
        rules.append(f"invalid_json:{label}")
        return {}
    if not isinstance(data, dict):
        rules.append(f"json_not_object:{label}")
        return {}
    return data


def require_schema(receipt: dict[str, Any], *, expected: str, label: str, rules: list[str]) -> None:
    if receipt.get("schema_version") != expected:
        rules.append(f"{label}_schema_mismatch")


def require_pass(receipt: dict[str, Any], *, label: str, rules: list[str]) -> None:
    if receipt.get("decision") != "PASS":
        rules.append(f"{label}_decision_not_pass")


def as_float(value: object) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def as_int(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def validate_intake(intake: dict[str, Any], rules: list[str]) -> None:
    require_schema(intake, expected=INTAKE_RECEIPT_SCHEMA, label="intake", rules=rules)
    require_pass(intake, label="intake", rules=rules)
    params = intake.get("params")
    if not isinstance(params, dict):
        rules.append("intake_params_missing")
        return
    if params.get("promotion_gate_closed") is not False:
        rules.append("intake_promotion_gate_state_invalid")
    verified_raw_files = params.get("verified_raw_files")
    if not isinstance(verified_raw_files, list) or len(verified_raw_files) < 4:
        rules.append("intake_verified_raw_files_insufficient")


def validate_citations(receipt: dict[str, Any], rules: list[str], label: str) -> None:
    evidence = receipt.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        rules.append(f"{label}_evidence_missing")
        return
    for index, citation in enumerate(evidence):
        if not isinstance(citation, dict):
            rules.append(f"{label}_evidence_not_object:{index}")
            continue
        if not (
            citation.get("path")
            or citation.get("sha256")
            or citation.get("guard_call_id")
            or citation.get("receipt_id")
        ):
            rules.append(f"{label}_evidence_unanchored:{index}")


def validate_comparator(
    comparator: dict[str, Any],
    rules: list[str],
    *,
    error_threshold_pct: float,
) -> float | None:
    require_schema(comparator, expected=COMPARATOR_SCHEMA, label="comparator", rules=rules)
    require_pass(comparator, label="comparator", rules=rules)
    validate_citations(comparator, rules, "comparator")

    metrics = comparator.get("metrics")
    if not isinstance(metrics, dict):
        rules.append("comparator_metrics_missing")
        return None

    observation_count = as_int(metrics.get("observation_count"))
    if observation_count is None or observation_count < 1:
        rules.append("comparator_observation_count_invalid")

    mismatch_count = as_int(metrics.get("mismatch_count"))
    if mismatch_count is None:
        rules.append("comparator_mismatch_count_missing")
    elif mismatch_count != 0:
        rules.append("comparator_mismatch_count_nonzero")

    false_positive_count = as_int(metrics.get("false_positive_count"))
    false_negative_count = as_int(metrics.get("false_negative_count"))
    if false_positive_count is None or false_positive_count < 0:
        rules.append("comparator_false_positive_count_invalid")
    if false_negative_count is None or false_negative_count < 0:
        rules.append("comparator_false_negative_count_invalid")

    error_pct = as_float(metrics.get("post_calibration_error_pct"))
    if error_pct is None:
        rules.append("comparator_error_pct_missing")
    elif error_pct > error_threshold_pct:
        rules.append("comparator_error_pct_over_threshold")

    calibration = comparator.get("calibration")
    if not isinstance(calibration, dict) or not calibration.get("method"):
        rules.append("comparator_calibration_method_missing")

    return error_pct


def validate_ua(
    ua_receipt: dict[str, Any],
    rules: list[str],
    *,
    ua_threshold_pct: float,
    risk_threshold_pct: float,
) -> tuple[float | None, float | None, float | None]:
    require_schema(ua_receipt, expected=UA_SCHEMA, label="ua", rules=rules)
    require_pass(ua_receipt, label="ua", rules=rules)
    validate_citations(ua_receipt, rules, "ua")

    uncertainty = ua_receipt.get("uncertainty")
    if not isinstance(uncertainty, dict):
        rules.append("ua_uncertainty_missing")
        return None, None, None

    combined_pct = as_float(uncertainty.get("combined_uncertainty_pct"))
    if combined_pct is None:
        rules.append("ua_combined_uncertainty_pct_missing")
    elif combined_pct > ua_threshold_pct:
        rules.append("ua_combined_uncertainty_pct_over_threshold")

    false_positive_risk_pct = as_float(uncertainty.get("false_positive_risk_pct"))
    false_negative_risk_pct = as_float(uncertainty.get("false_negative_risk_pct"))
    if false_positive_risk_pct is None:
        rules.append("ua_false_positive_risk_pct_missing")
    elif false_positive_risk_pct > risk_threshold_pct:
        rules.append("ua_false_positive_risk_pct_over_threshold")
    if false_negative_risk_pct is None:
        rules.append("ua_false_negative_risk_pct_missing")
    elif false_negative_risk_pct > risk_threshold_pct:
        rules.append("ua_false_negative_risk_pct_over_threshold")

    if not uncertainty.get("method"):
        rules.append("ua_method_missing")

    return combined_pct, false_positive_risk_pct, false_negative_risk_pct


def build_receipt(
    *,
    intake_path: Path,
    comparator_path: Path,
    ua_path: Path,
    error_threshold_pct: float,
    ua_threshold_pct: float,
    risk_threshold_pct: float,
) -> dict[str, Any]:
    rules: list[str] = []

    intake = read_json(intake_path, rules, "intake")
    comparator = read_json(comparator_path, rules, "comparator")
    ua_receipt = read_json(ua_path, rules, "ua")

    if intake:
        validate_intake(intake, rules)
    error_pct = (
        validate_comparator(comparator, rules, error_threshold_pct=error_threshold_pct)
        if comparator
        else None
    )
    ua_pct, fp_risk_pct, fn_risk_pct = (
        validate_ua(
            ua_receipt,
            rules,
            ua_threshold_pct=ua_threshold_pct,
            risk_threshold_pct=risk_threshold_pct,
        )
        if ua_receipt
        else (None, None, None)
    )

    decision = "BLOCKED" if rules else "PASS"
    tier_a_ready = decision == "PASS"
    evidence_summary = [
        f"intake_receipt={intake_path}",
        f"comparator_error_pct={error_pct if error_pct is not None else 'missing'}",
        f"ua_combined_uncertainty_pct={ua_pct if ua_pct is not None else 'missing'}",
        f"false_positive_risk_pct={fp_risk_pct if fp_risk_pct is not None else 'missing'}",
        f"tier_a_promotion_ready={str(tier_a_ready).lower()}",
    ]

    return {
        "schema_version": RECEIPT_SCHEMA,
        "created_utc": utc_now(),
        "decision": decision,
        "triggered_rules": sorted(set(rules)),
        "evidence_summary": evidence_summary[:5],
        "required_next_action": (
            "fix triggered rules and rerun promotion-readiness validator"
            if rules
            else "operator may authorize a separate promotion application gate"
        ),
        "params": {
            "intake_receipt": str(intake_path),
            "comparator_receipt": str(comparator_path),
            "ua_receipt": str(ua_path),
            "error_threshold_pct": error_threshold_pct,
            "ua_threshold_pct": ua_threshold_pct,
            "risk_threshold_pct": risk_threshold_pct,
            "false_negative_risk_pct": fn_risk_pct,
            "tier_a_promotion_ready": tier_a_ready,
            "promotion_applied": False,
        },
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--intake-receipt", type=Path, required=True)
    parser.add_argument("--comparator", type=Path, required=True)
    parser.add_argument("--ua", type=Path, required=True)
    parser.add_argument(
        "--error-threshold-pct",
        type=float,
        default=DEFAULT_ERROR_THRESHOLD_PCT,
    )
    parser.add_argument(
        "--ua-threshold-pct",
        type=float,
        default=DEFAULT_UA_THRESHOLD_PCT,
    )
    parser.add_argument(
        "--risk-threshold-pct",
        type=float,
        default=DEFAULT_RISK_THRESHOLD_PCT,
    )
    parser.add_argument("--receipt-out", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    receipt = build_receipt(
        intake_path=args.intake_receipt.expanduser().resolve(),
        comparator_path=args.comparator.expanduser().resolve(),
        ua_path=args.ua.expanduser().resolve(),
        error_threshold_pct=args.error_threshold_pct,
        ua_threshold_pct=args.ua_threshold_pct,
        risk_threshold_pct=args.risk_threshold_pct,
    )
    text = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    if args.receipt_out:
        args.receipt_out.parent.mkdir(parents=True, exist_ok=True)
        args.receipt_out.write_text(text, encoding="utf-8")
    print(text, end="")
    return EXIT_PASS if receipt["decision"] == "PASS" else EXIT_BLOCKED


if __name__ == "__main__":
    raise SystemExit(main())
