#!/usr/bin/env python3
"""Validate a Fleet Watch real telemetry drop and emit a fail-closed receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EXIT_PASS = 0
EXIT_BLOCKED = 3

RECEIPT_SCHEMA = "fleet-watch/real-validation-drop-receipt/v1"
METADATA_SCHEMA = "fleet-watch/real-validation-drop/metadata/v1"
SOURCE_MANIFEST_SCHEMA = "fleet-watch/real-validation-drop/source-manifest/v1"
RAW_HASH_MANIFEST_SCHEMA = "fleet-watch/real-validation-drop/raw-hash-manifest/v1"

REAL_DATA_SOURCES = {"production_telemetry"}
REQUIRED_DIRS = ("raw", "normalized", "receipts", "comparator", "ua")
REQUIRED_RAW_FILES = (
    "raw/guard_calls.jsonl",
    "raw/state_snapshots.jsonl",
    "raw/health_snapshots.jsonl",
    "raw/registry_events.jsonl",
)
REQUIRED_METADATA_FIELDS = (
    "schema_version",
    "data_source",
    "source_owner",
    "capture_window",
    "machine_class",
    "process_count",
    "port_count",
    "repo_lock_count",
    "gpu_budget_snapshot",
    "memory_pressure_snapshot",
    "raw_hash_manifest",
    "normalization_receipt",
    "comparator_expectations",
    "privacy_constraints",
    "allowed_claim_scope",
)
REQUIRED_SOURCE_MANIFEST_FIELDS = (
    "schema_version",
    "source_owner",
    "data_class",
    "capture_window",
    "source_count",
    "source_paths",
    "privacy_constraints",
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_json(path: Path, rules: list[str]) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        rules.append(f"missing_json:{path.name}")
        return {}
    except json.JSONDecodeError:
        rules.append(f"invalid_json:{path.name}")
        return {}
    if not isinstance(data, dict):
        rules.append(f"json_not_object:{path.name}")
        return {}
    return data


def read_jsonl(path: Path, rules: list[str], label: str) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        rules.append(f"missing_jsonl:{label}")
        return []

    records: list[dict[str, Any]] = []
    for index, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            rules.append(f"invalid_jsonl:{label}:{index}")
            continue
        if not isinstance(item, dict):
            rules.append(f"jsonl_record_not_object:{label}:{index}")
            continue
        records.append(item)

    if not records:
        rules.append(f"jsonl_empty:{label}")
    return records


def rel_path(drop_dir: Path, value: object) -> Path | None:
    if not isinstance(value, str) or not value.strip():
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return drop_dir / path


def rel_string(drop_dir: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(drop_dir.resolve()))
    except ValueError:
        return str(path)


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_required_fields(
    obj: dict[str, Any],
    required: tuple[str, ...],
    *,
    label: str,
    rules: list[str],
) -> None:
    for field in required:
        if field not in obj:
            rules.append(f"missing_{label}_field:{field}")
        elif obj[field] in ("", None, [], {}):
            rules.append(f"empty_{label}_field:{field}")


def validate_capture_window(value: object, rules: list[str], label: str) -> None:
    if not isinstance(value, dict):
        rules.append(f"{label}_capture_window_not_object")
        return
    for field in ("start_utc", "end_utc"):
        if not isinstance(value.get(field), str) or not value[field]:
            rules.append(f"missing_{label}_capture_window:{field}")


def validate_source_paths(source_manifest: dict[str, Any], rules: list[str]) -> None:
    source_paths = source_manifest.get("source_paths")
    if not isinstance(source_paths, list) or not source_paths:
        rules.append("source_manifest_source_paths_missing")
        return
    for expected in REQUIRED_RAW_FILES:
        if expected not in source_paths:
            rules.append(f"source_manifest_missing_raw_path:{expected}")
    source_count = source_manifest.get("source_count")
    if isinstance(source_count, int) and source_count != len(source_paths):
        rules.append("source_manifest_count_mismatch")


def validate_raw_hash_manifest(
    drop_dir: Path,
    metadata: dict[str, Any],
    rules: list[str],
) -> tuple[dict[str, int], list[str]]:
    manifest_path = rel_path(drop_dir, metadata.get("raw_hash_manifest"))
    if manifest_path is None:
        rules.append("raw_hash_manifest_path_missing")
        return {}, []

    manifest = read_json(manifest_path, rules)
    if not manifest:
        return {}, []

    if manifest.get("schema_version") != RAW_HASH_MANIFEST_SCHEMA:
        rules.append("raw_hash_manifest_schema_mismatch")

    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        rules.append("raw_hash_manifest_files_missing")
        return {}, []

    by_path: dict[str, dict[str, Any]] = {}
    verified_paths: list[str] = []
    record_counts: dict[str, int] = {}
    for index, item in enumerate(files):
        if not isinstance(item, dict):
            rules.append(f"raw_hash_manifest_file_not_object:{index}")
            continue
        path = rel_path(drop_dir, item.get("path"))
        if path is None:
            rules.append(f"raw_hash_manifest_file_path_missing:{index}")
            continue
        rel = rel_string(drop_dir, path)
        by_path[rel] = item
        expected_hash = item.get("sha256")
        if not isinstance(expected_hash, str) or not SHA256_RE.match(expected_hash):
            rules.append(f"raw_hash_manifest_bad_sha256:{rel}")
        if not path.exists() or not path.is_file():
            rules.append(f"raw_file_missing:{rel}")
            continue
        if isinstance(item.get("bytes"), int) and path.stat().st_size != item["bytes"]:
            rules.append(f"raw_file_size_mismatch:{rel}")
        if isinstance(expected_hash, str) and SHA256_RE.match(expected_hash):
            actual_hash = file_sha256(path)
            if actual_hash != expected_hash:
                rules.append(f"raw_file_hash_mismatch:{rel}")
            else:
                verified_paths.append(rel)
        if isinstance(item.get("record_count"), int):
            record_counts[rel] = item["record_count"]

    for expected in REQUIRED_RAW_FILES:
        if expected not in by_path:
            rules.append(f"raw_hash_manifest_missing_required_file:{expected}")

    return record_counts, verified_paths


def state_process_count(state_snapshot: dict[str, Any]) -> int | None:
    state = state_snapshot.get("state")
    if isinstance(state, dict) and isinstance(state.get("process_count"), int):
        return state["process_count"]
    if isinstance(state_snapshot.get("process_count"), int):
        return state_snapshot["process_count"]
    return None


def state_port_count(state_snapshot: dict[str, Any]) -> int | None:
    state = state_snapshot.get("state")
    if not isinstance(state, dict):
        state = state_snapshot
    occupied_ports = state.get("occupied_ports")
    if isinstance(occupied_ports, list):
        return len(occupied_ports)
    ports_claimed = state.get("ports_claimed")
    if isinstance(ports_claimed, dict):
        return len(ports_claimed)
    return None


def state_repo_lock_count(state_snapshot: dict[str, Any]) -> int | None:
    state = state_snapshot.get("state")
    if not isinstance(state, dict):
        state = state_snapshot
    locked_repos = state.get("locked_repos")
    if isinstance(locked_repos, list):
        return len(locked_repos)
    repos_locked = state.get("repos_locked")
    if isinstance(repos_locked, dict):
        return len(repos_locked)
    return None


def validate_count_field(metadata: dict[str, Any], field: str, rules: list[str]) -> int:
    value = metadata.get(field)
    if not isinstance(value, int) or value < 0:
        rules.append(f"metadata_{field}_invalid")
        return 0
    return value


def validate_guard_links(
    drop_dir: Path,
    metadata: dict[str, Any],
    rules: list[str],
) -> tuple[int, int, int]:
    guard_calls = read_jsonl(drop_dir / "raw" / "guard_calls.jsonl", rules, "guard_calls")
    state_snapshots = read_jsonl(drop_dir / "raw" / "state_snapshots.jsonl", rules, "state_snapshots")
    health_snapshots = read_jsonl(drop_dir / "raw" / "health_snapshots.jsonl", rules, "health_snapshots")
    registry_events = read_jsonl(drop_dir / "raw" / "registry_events.jsonl", rules, "registry_events")

    state_by_id: dict[str, dict[str, Any]] = {}
    for index, snapshot in enumerate(state_snapshots, start=1):
        snapshot_id = snapshot.get("state_snapshot_id")
        if not isinstance(snapshot_id, str) or not snapshot_id:
            rules.append(f"state_snapshot_missing_id:{index}")
            continue
        state_by_id[snapshot_id] = snapshot

    comparator_path = rel_path(drop_dir, metadata.get("comparator_expectations"))
    if comparator_path is None:
        rules.append("comparator_expectations_path_missing")
        expectations: list[dict[str, Any]] = []
    else:
        expectations = read_jsonl(comparator_path, rules, "comparator_expectations")

    expected_by_call: dict[str, dict[str, Any]] = {}
    for index, expectation in enumerate(expectations, start=1):
        guard_call_id = expectation.get("guard_call_id")
        if not isinstance(guard_call_id, str) or not guard_call_id:
            rules.append(f"comparator_expectation_missing_guard_call_id:{index}")
            continue
        if not isinstance(expectation.get("expected_allowed"), bool):
            rules.append(f"comparator_expectation_missing_expected_allowed:{guard_call_id}")
        expected_by_call[guard_call_id] = expectation

    seen_guard_ids: set[str] = set()
    for index, call in enumerate(guard_calls, start=1):
        guard_call_id = call.get("guard_call_id")
        if not isinstance(guard_call_id, str) or not guard_call_id:
            rules.append(f"guard_call_missing_id:{index}")
            continue
        seen_guard_ids.add(guard_call_id)
        snapshot_id = call.get("state_snapshot_id")
        if not isinstance(snapshot_id, str) or snapshot_id not in state_by_id:
            rules.append(f"missing_state_snapshot_for_guard:{guard_call_id}")
        response = call.get("response")
        if not isinstance(response, dict) or not isinstance(response.get("allowed"), bool):
            rules.append(f"guard_call_missing_response_allowed:{guard_call_id}")
            continue
        expectation = expected_by_call.get(guard_call_id)
        if expectation is None:
            rules.append(f"missing_comparator_expectation:{guard_call_id}")
            continue
        if isinstance(expectation.get("expected_allowed"), bool) and expectation["expected_allowed"] != response["allowed"]:
            rules.append(f"comparator_decision_mismatch:{guard_call_id}")

    for guard_call_id in sorted(set(expected_by_call).difference(seen_guard_ids)):
        rules.append(f"comparator_orphan_expectation:{guard_call_id}")

    latest_state = state_snapshots[-1] if state_snapshots else {}
    process_count = state_process_count(latest_state)
    port_count = state_port_count(latest_state)
    repo_lock_count = state_repo_lock_count(latest_state)

    expected_process_count = validate_count_field(metadata, "process_count", rules)
    expected_port_count = validate_count_field(metadata, "port_count", rules)
    expected_repo_lock_count = validate_count_field(metadata, "repo_lock_count", rules)

    if process_count is None:
        rules.append("state_snapshot_missing_process_count")
    elif process_count != expected_process_count:
        rules.append("metadata_process_count_mismatch")
    if port_count is None:
        rules.append("state_snapshot_missing_port_count")
    elif port_count != expected_port_count:
        rules.append("metadata_port_count_mismatch")
    if repo_lock_count is None:
        rules.append("state_snapshot_missing_repo_lock_count")
    elif repo_lock_count != expected_repo_lock_count:
        rules.append("metadata_repo_lock_count_mismatch")

    return len(guard_calls), len(state_snapshots), len(health_snapshots) + len(registry_events)


def validate_normalization_receipt(drop_dir: Path, metadata: dict[str, Any], rules: list[str]) -> None:
    normalization_receipt_path = rel_path(drop_dir, metadata.get("normalization_receipt"))
    if normalization_receipt_path is None:
        rules.append("normalization_receipt_path_missing")
        return
    normalization_receipt = read_json(normalization_receipt_path, rules)
    if normalization_receipt and normalization_receipt.get("decision") != "PASS":
        rules.append("normalization_receipt_not_pass")


def validate_drop(drop_dir: Path) -> dict[str, Any]:
    rules: list[str] = []

    if not drop_dir.exists() or not drop_dir.is_dir():
        rules.append("drop_dir_missing")

    for dirname in REQUIRED_DIRS:
        path = drop_dir / dirname
        if not path.exists() or not path.is_dir():
            rules.append(f"missing_required_dir:{dirname}")

    metadata = read_json(drop_dir / "metadata.json", rules)
    source_manifest = read_json(drop_dir / "source_manifest.json", rules)

    validate_required_fields(metadata, REQUIRED_METADATA_FIELDS, label="metadata", rules=rules)
    validate_required_fields(
        source_manifest,
        REQUIRED_SOURCE_MANIFEST_FIELDS,
        label="source_manifest",
        rules=rules,
    )

    if metadata.get("schema_version") != METADATA_SCHEMA:
        rules.append("metadata_schema_mismatch")
    if source_manifest.get("schema_version") != SOURCE_MANIFEST_SCHEMA:
        rules.append("source_manifest_schema_mismatch")

    data_source = metadata.get("data_source")
    if data_source not in REAL_DATA_SOURCES:
        rules.append("metadata_data_source_not_production_telemetry")
    if source_manifest.get("data_class") != "production_telemetry":
        rules.append("source_manifest_data_class_not_production_telemetry")
    if metadata.get("source_owner") != source_manifest.get("source_owner"):
        rules.append("source_owner_mismatch")

    validate_capture_window(metadata.get("capture_window"), rules, "metadata")
    validate_capture_window(source_manifest.get("capture_window"), rules, "source_manifest")
    validate_source_paths(source_manifest, rules)

    for field in ("gpu_budget_snapshot", "memory_pressure_snapshot"):
        if field in metadata and not isinstance(metadata.get(field), dict):
            rules.append(f"metadata_{field}_not_object")

    raw_record_counts, verified_raw_paths = validate_raw_hash_manifest(drop_dir, metadata, rules)
    for expected in REQUIRED_RAW_FILES:
        expected_count = raw_record_counts.get(expected)
        actual_count = len(read_jsonl(drop_dir / expected, rules, expected.replace("/", "_")))
        if isinstance(expected_count, int) and expected_count != actual_count:
            rules.append(f"raw_record_count_mismatch:{expected}")

    guard_count, state_count, support_count = validate_guard_links(drop_dir, metadata, rules)
    validate_normalization_receipt(drop_dir, metadata, rules)

    decision = "BLOCKED" if rules else "PASS"
    evidence_summary = [
        f"drop_dir={drop_dir}",
        f"data_source={data_source or 'missing'}",
        f"guard_call_count={guard_count}",
        f"state_snapshot_count={state_count}",
        "promotion_status=BLOCKED_UNTIL_COMPARATOR_UA_PROMOTION",
    ]

    return {
        "schema_version": RECEIPT_SCHEMA,
        "created_utc": utc_now(),
        "decision": decision,
        "triggered_rules": sorted(set(rules)),
        "evidence_summary": evidence_summary[:5],
        "required_next_action": (
            "fix triggered rules and rerun Fleet Watch intake validator"
            if rules
            else "run comparator receipt, uncertainty analysis, and separate promotion gate"
        ),
        "params": {
            "drop_dir": str(drop_dir),
            "metadata_path": str(drop_dir / "metadata.json"),
            "source_manifest_path": str(drop_dir / "source_manifest.json"),
            "real_data_sources": sorted(REAL_DATA_SOURCES),
            "required_raw_files": list(REQUIRED_RAW_FILES),
            "verified_raw_files": sorted(verified_raw_paths),
            "support_record_count": support_count,
            "promotion_gate_closed": False,
        },
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("drop_dir", type=Path, help="Fleet Watch real telemetry drop directory.")
    parser.add_argument(
        "--receipt-out",
        type=Path,
        help="Optional path for the JSON validation receipt.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    receipt = validate_drop(args.drop_dir.expanduser().resolve())
    text = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    if args.receipt_out:
        args.receipt_out.parent.mkdir(parents=True, exist_ok=True)
        args.receipt_out.write_text(text, encoding="utf-8")
    print(text, end="")
    return EXIT_PASS if receipt["decision"] == "PASS" else EXIT_BLOCKED


if __name__ == "__main__":
    raise SystemExit(main())
