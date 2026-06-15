from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import validate_real_validation_drop as validator


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(item, sort_keys=True) + "\n" for item in records),
        encoding="utf-8",
    )


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _make_valid_drop(tmp_path: Path) -> Path:
    drop = tmp_path / "real_validation_drop"
    for dirname in ("raw", "normalized", "receipts", "comparator", "ua"):
        (drop / dirname).mkdir(parents=True)

    _write_jsonl(
        drop / "raw" / "state_snapshots.jsonl",
        [
            {
                "state_snapshot_id": "state-1",
                "timestamp_utc": "2026-06-06T00:00:00Z",
                "state": {
                    "process_count": 1,
                    "occupied_ports": [4242],
                    "locked_repos": [],
                    "gpu_budget": {
                        "total_mb": 131072,
                        "reserve_mb": 16384,
                        "allocated_mb": 1376,
                        "available_mb": 113312,
                    },
                    "system_memory": {"pressure_pct": 40},
                },
            }
        ],
    )
    _write_jsonl(
        drop / "raw" / "guard_calls.jsonl",
        [
            {
                "guard_call_id": "guard-1",
                "timestamp_utc": "2026-06-06T00:00:01Z",
                "state_snapshot_id": "state-1",
                "request": {"repo_dir": "/tmp/repo", "write_scopes": ["/tmp/repo/docs"]},
                "response": {"allowed": True, "checks": {"repo": {"allowed": True}}},
            }
        ],
    )
    _write_jsonl(
        drop / "raw" / "health_snapshots.jsonl",
        [{"health_snapshot_id": "health-1", "timestamp_utc": "2026-06-06T00:00:02Z"}],
    )
    _write_jsonl(
        drop / "raw" / "registry_events.jsonl",
        [{"event_id": "event-1", "event_type": "GUARD_CHECK", "timestamp_utc": "2026-06-06T00:00:03Z"}],
    )
    _write_jsonl(
        drop / "comparator" / "expected_decisions.jsonl",
        [
            {
                "guard_call_id": "guard-1",
                "expected_allowed": True,
                "expectation_reason": "repo available",
            }
        ],
    )

    raw_files = [
        "raw/guard_calls.jsonl",
        "raw/state_snapshots.jsonl",
        "raw/health_snapshots.jsonl",
        "raw/registry_events.jsonl",
    ]
    _write_json(
        drop / "receipts" / "raw_hash_manifest.json",
        {
            "schema_version": validator.RAW_HASH_MANIFEST_SCHEMA,
            "files": [
                {
                    "path": rel,
                    "sha256": _sha(drop / rel),
                    "bytes": (drop / rel).stat().st_size,
                    "record_count": 1,
                }
                for rel in raw_files
            ],
        },
    )
    _write_json(
        drop / "receipts" / "normalization_receipt.json",
        {
            "schema_version": "fleet-watch/real-validation-drop/normalization-receipt/v1",
            "decision": "PASS",
        },
    )
    capture_window = {
        "start_utc": "2026-06-06T00:00:00Z",
        "end_utc": "2026-06-06T00:01:00Z",
    }
    _write_json(
        drop / "metadata.json",
        {
            "schema_version": validator.METADATA_SCHEMA,
            "data_source": "production_telemetry",
            "source_owner": "operator",
            "capture_window": capture_window,
            "machine_class": "local_mac_studio",
            "process_count": 1,
            "port_count": 1,
            "repo_lock_count": 0,
            "gpu_budget_snapshot": {"available_mb": 113312},
            "memory_pressure_snapshot": {"pressure_pct": 40},
            "raw_hash_manifest": "receipts/raw_hash_manifest.json",
            "normalization_receipt": "receipts/normalization_receipt.json",
            "comparator_expectations": "comparator/expected_decisions.jsonl",
            "privacy_constraints": "local_only_no_secret_values",
            "allowed_claim_scope": "validator_fixture_only",
        },
    )
    _write_json(
        drop / "source_manifest.json",
        {
            "schema_version": validator.SOURCE_MANIFEST_SCHEMA,
            "source_owner": "operator",
            "data_class": "production_telemetry",
            "capture_window": capture_window,
            "source_count": len(raw_files),
            "source_paths": raw_files,
            "privacy_constraints": "local_only_no_secret_values",
        },
    )
    return drop


def test_valid_drop_passes_but_promotion_stays_blocked(tmp_path: Path) -> None:
    drop = _make_valid_drop(tmp_path)
    receipt = validator.validate_drop(drop)
    assert receipt["decision"] == "PASS"
    assert receipt["triggered_rules"] == []
    assert receipt["params"]["promotion_gate_closed"] is False
    assert "promotion_status=BLOCKED_UNTIL_COMPARATOR_UA_PROMOTION" in receipt["evidence_summary"]


def test_missing_metadata_fails_closed(tmp_path: Path) -> None:
    drop = _make_valid_drop(tmp_path)
    (drop / "metadata.json").unlink()
    receipt = validator.validate_drop(drop)
    assert receipt["decision"] == "BLOCKED"
    assert "missing_json:metadata.json" in receipt["triggered_rules"]


def test_non_production_telemetry_fails_closed(tmp_path: Path) -> None:
    drop = _make_valid_drop(tmp_path)
    metadata = json.loads((drop / "metadata.json").read_text(encoding="utf-8"))
    metadata["data_source"] = "synthetic_proxy"
    _write_json(drop / "metadata.json", metadata)
    receipt = validator.validate_drop(drop)
    assert receipt["decision"] == "BLOCKED"
    assert "metadata_data_source_not_production_telemetry" in receipt["triggered_rules"]


def test_raw_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    drop = _make_valid_drop(tmp_path)
    (drop / "raw" / "guard_calls.jsonl").write_text("tampered\n", encoding="utf-8")
    receipt = validator.validate_drop(drop)
    assert receipt["decision"] == "BLOCKED"
    assert "raw_file_hash_mismatch:raw/guard_calls.jsonl" in receipt["triggered_rules"]


def test_missing_state_snapshot_for_guard_fails_closed(tmp_path: Path) -> None:
    drop = _make_valid_drop(tmp_path)
    guard_calls = [
        {
            "guard_call_id": "guard-1",
            "state_snapshot_id": "missing-state",
            "request": {},
            "response": {"allowed": True},
        }
    ]
    _write_jsonl(drop / "raw" / "guard_calls.jsonl", guard_calls)
    manifest = json.loads((drop / "receipts" / "raw_hash_manifest.json").read_text(encoding="utf-8"))
    for item in manifest["files"]:
        if item["path"] == "raw/guard_calls.jsonl":
            item["sha256"] = _sha(drop / "raw" / "guard_calls.jsonl")
            item["bytes"] = (drop / "raw" / "guard_calls.jsonl").stat().st_size
    _write_json(drop / "receipts" / "raw_hash_manifest.json", manifest)
    receipt = validator.validate_drop(drop)
    assert receipt["decision"] == "BLOCKED"
    assert "missing_state_snapshot_for_guard:guard-1" in receipt["triggered_rules"]


def test_comparator_mismatch_fails_closed(tmp_path: Path) -> None:
    drop = _make_valid_drop(tmp_path)
    _write_jsonl(
        drop / "comparator" / "expected_decisions.jsonl",
        [{"guard_call_id": "guard-1", "expected_allowed": False}],
    )
    receipt = validator.validate_drop(drop)
    assert receipt["decision"] == "BLOCKED"
    assert "comparator_decision_mismatch:guard-1" in receipt["triggered_rules"]


def test_metadata_state_count_mismatch_fails_closed(tmp_path: Path) -> None:
    drop = _make_valid_drop(tmp_path)
    metadata = json.loads((drop / "metadata.json").read_text(encoding="utf-8"))
    metadata["process_count"] = 2
    _write_json(drop / "metadata.json", metadata)
    receipt = validator.validate_drop(drop)
    assert receipt["decision"] == "BLOCKED"
    assert "metadata_process_count_mismatch" in receipt["triggered_rules"]


def test_cli_writes_receipt_and_uses_blocked_exit_code(tmp_path: Path, capsys) -> None:
    drop = _make_valid_drop(tmp_path)
    (drop / "metadata.json").unlink()
    receipt_out = tmp_path / "receipt.json"
    rc = validator.main([str(drop), "--receipt-out", str(receipt_out)])
    out = capsys.readouterr().out
    receipt = json.loads(out)
    written = json.loads(receipt_out.read_text(encoding="utf-8"))
    assert rc == validator.EXIT_BLOCKED
    assert receipt == written
    assert "missing_json:metadata.json" in receipt["triggered_rules"]
