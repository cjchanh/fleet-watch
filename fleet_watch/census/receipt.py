"""census.receipt — schema validation, atomic latest-pointer swap, drift.

Contract: ``docs/fleet-census-receipt-contract-v1.md`` (SSOT in this repo).

Two rules carry the weight here:

* **Never clobber good with bad.** A payload is validated *before* anything is
  written, and the dated file is read back off disk and validated *again*
  before ``latest.json`` is swapped. A receipt that fails either check raises
  :class:`CensusRefusal` and leaves the previous ``latest.json`` untouched.
* **Degenerate output is a refusal, not a receipt.** Zero items or zero domains
  means the probes failed, not that the machine is empty.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fleet_watch.census.verdicts import STATUSES, VERDICTS

SCHEMA_VERSION = "fleet-census/v1"
RECEIPT_DIR = Path.home() / ".governance" / "receipts" / "fleet-census"
LATEST_NAME = "latest.json"

_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_REQUIRED_TOP = (
    "schema_version",
    "generated_at",
    "host",
    "machine",
    "totals",
    "domains",
    "drift",
)
_REQUIRED_ITEM = ("label", "path", "status", "evidence", "verdict", "reason", "rule")
_REQUIRED_DOMAIN = ("domain_id", "domain", "summary")
_TOTAL_KEYS = ("items", "keep", "investigate", "close", "remove")


class CensusRefusal(Exception):
    """Raised instead of writing a receipt that would not testify truthfully."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors))


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def receipt_filename(generated_at: str) -> str:
    """``2026-07-27T23:59:59Z`` -> ``census-20260727T235959Z.json``."""
    compact = generated_at.replace("-", "").replace(":", "")
    return f"census-{compact}.json"


def validate(payload: Any) -> list[str]:
    """Return a list of contract violations. Empty list means valid."""
    errors: list[str] = []

    if not isinstance(payload, dict):
        return [f"payload is {type(payload).__name__}, expected object"]

    for key in _REQUIRED_TOP:
        if key not in payload:
            errors.append(f"missing required key: {key}")
    if errors:
        return errors

    if payload["schema_version"] != SCHEMA_VERSION:
        errors.append(
            f"schema_version is {payload['schema_version']!r}, expected {SCHEMA_VERSION!r}"
        )
    if not isinstance(payload["generated_at"], str) or not _TIMESTAMP_RE.match(
        payload["generated_at"]
    ):
        errors.append("generated_at must be UTC 'YYYY-MM-DDTHH:MM:SSZ'")
    if not isinstance(payload["host"], str) or not payload["host"].strip():
        errors.append("host must be a non-empty string")
    if not isinstance(payload["machine"], dict):
        errors.append("machine must be an object")

    totals = payload["totals"]
    if not isinstance(totals, dict):
        errors.append("totals must be an object")
        totals = {}
    for key in _TOTAL_KEYS:
        if not isinstance(totals.get(key), int):
            errors.append(f"totals.{key} must be an integer")

    domains = payload["domains"]
    if not isinstance(domains, list) or not domains:
        errors.append("domains must be a non-empty array")
        domains = []

    counted = {key: 0 for key in _TOTAL_KEYS}
    for index, domain in enumerate(domains):
        where = f"domains[{index}]"
        if not isinstance(domain, dict):
            errors.append(f"{where} must be an object")
            continue
        for key in _REQUIRED_DOMAIN:
            value = domain.get(key)
            if not isinstance(value, str) or not value.strip():
                errors.append(f"{where}.{key} must be a non-empty string")
        domain_totals = domain.get("totals")
        if not isinstance(domain_totals, dict):
            errors.append(f"{where}.totals must be an object")
            domain_totals = {}
        domain_counted = {key: 0 for key in _TOTAL_KEYS}
        items = domain.get("items")
        if not isinstance(items, list):
            errors.append(f"{where}.items must be an array")
            continue
        for item_index, item in enumerate(items):
            item_where = f"{where}.items[{item_index}]"
            if not isinstance(item, dict):
                errors.append(f"{item_where} must be an object")
                continue
            for key in _REQUIRED_ITEM:
                value = item.get(key)
                if not isinstance(value, str) or not value.strip():
                    errors.append(f"{item_where}.{key} must be a non-empty string")
            status, verdict = item.get("status"), item.get("verdict")
            if status not in STATUSES:
                errors.append(f"{item_where}.status {status!r} not in {sorted(STATUSES)}")
            if verdict not in VERDICTS:
                errors.append(
                    f"{item_where}.verdict {verdict!r} not in {sorted(VERDICTS)}"
                )
            else:
                counted[verdict] += 1
                domain_counted[verdict] += 1
            counted["items"] += 1
            domain_counted["items"] += 1
            for optional in ("resource", "close_command"):
                if optional in item and not isinstance(item[optional], str):
                    errors.append(f"{item_where}.{optional} must be a string when present")

        # A domain's own roll-up must agree with its own items, not just the
        # grand total — otherwise a per-domain miscount hides inside a correct sum.
        for key in _TOTAL_KEYS:
            value = domain_totals.get(key)
            if not isinstance(value, int):
                errors.append(f"{where}.totals.{key} must be an integer")
            elif value != domain_counted[key]:
                errors.append(
                    f"{where}.totals.{key} is {value} but {domain_counted[key]} "
                    "were counted in that domain's items"
                )

    if counted["items"] == 0:
        errors.append(
            "degenerate receipt: 0 items across all domains — probes failed, "
            "the machine is not empty (REFUSAL)"
        )

    for key in _TOTAL_KEYS:
        if isinstance(totals.get(key), int) and totals[key] != counted[key]:
            errors.append(
                f"totals.{key} is {totals[key]} but {counted[key]} were counted in domains"
            )

    drift = payload["drift"]
    if not isinstance(drift, dict):
        errors.append("drift must be an object")
    else:
        prior = drift.get("prior_receipt")
        if prior is not None and not isinstance(prior, str):
            errors.append("drift.prior_receipt must be a string or null")
        for key in ("new_items", "disappeared", "verdict_changes"):
            if not isinstance(drift.get(key), list):
                errors.append(f"drift.{key} must be an array")

    return errors


# --------------------------------------------------------------------------
# drift
# --------------------------------------------------------------------------


#: Domains whose membership changes every minute by construction. Diffing them
#: would bury real drift under process churn, so they are excluded from the
#: drift section and named in ``drift.excluded_domains``.
VOLATILE_DOMAIN_IDS = frozenset({"processes"})


def _item_keys(payload: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Map ``<domain_id>::<label>`` -> {domain, label, verdict}."""
    keys: dict[str, dict[str, str]] = {}
    for domain in payload.get("domains", []):
        if not isinstance(domain, dict):
            continue
        domain_name = str(domain.get("domain", ""))
        domain_id = str(domain.get("domain_id") or domain_name)
        if domain_id in VOLATILE_DOMAIN_IDS:
            continue
        for item in domain.get("items", []):
            if not isinstance(item, dict):
                continue
            label = str(item.get("label", ""))
            if not label:
                continue
            key = f"{domain_id}::{label}"
            if key in keys:
                # Two items can legitimately share a label inside one domain
                # (two plists declaring the same Label). Disambiguate rather
                # than let the later one silently erase the earlier from drift.
                suffix = 2
                while f"{key}#{suffix}" in keys:
                    suffix += 1
                key = f"{key}#{suffix}"
            keys[key] = {
                "domain": domain_name,
                "label": label,
                "verdict": str(item.get("verdict", "")),
            }
    return keys


def compute_drift(
    current: dict[str, Any],
    prior: dict[str, Any] | None,
    prior_path: str | None,
    prior_status: str = "ok",
) -> dict[str, Any]:
    """Diff this census against the previous receipt.

    New / disappeared / verdict-changed entries are the testify events: the
    census is not interesting because it lists 178 things, it is interesting
    because three of them changed since yesterday.
    """
    drift: dict[str, Any] = {
        "prior_receipt": prior_path if prior is not None else None,
        "prior_status": prior_status,
        "excluded_domains": sorted(VOLATILE_DOMAIN_IDS),
        "new_items": [],
        "disappeared": [],
        "verdict_changes": [],
    }
    if prior is None:
        return drift

    now = _item_keys(current)
    before = _item_keys(prior)

    for key in sorted(set(now) - set(before)):
        drift["new_items"].append({"key": key, **now[key]})
    for key in sorted(set(before) - set(now)):
        drift["disappeared"].append({"key": key, **before[key]})
    for key in sorted(set(now) & set(before)):
        if now[key]["verdict"] != before[key]["verdict"]:
            drift["verdict_changes"].append(
                {
                    "key": key,
                    "domain": now[key]["domain"],
                    "label": now[key]["label"],
                    "from": before[key]["verdict"],
                    "to": now[key]["verdict"],
                }
            )
    return drift


# --------------------------------------------------------------------------
# read / write
# --------------------------------------------------------------------------


def load_latest(receipt_dir: Path = RECEIPT_DIR) -> tuple[dict[str, Any] | None, str | None, str]:
    """Load ``latest.json``.

    Returns ``(payload, path, status)`` where status is ``ok``, ``absent``, or
    ``invalid``. An invalid prior receipt is treated as absent for drift
    purposes and the reason is recorded — never silently rendered as "no change".
    """
    path = receipt_dir / LATEST_NAME
    try:
        if not path.exists():
            return None, None, "absent"
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        return None, str(path), f"invalid: unreadable ({exc})"

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, str(path), f"invalid: not JSON ({exc})"

    errors = validate(payload)
    if errors:
        return None, str(path), f"invalid: {errors[0]}"
    return payload, str(path), "ok"


def _atomic_write(path: Path, text: str) -> None:
    """Write via temp file in the same directory, then rename over the target."""
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    )
    tmp_path = Path(handle.name)
    try:
        with handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def write_receipt(
    payload: dict[str, Any], receipt_dir: Path = RECEIPT_DIR
) -> tuple[Path, Path]:
    """Validate, write the dated receipt, re-validate from disk, then swap latest.

    Returns ``(dated_path, latest_path)``. Raises :class:`CensusRefusal` before
    touching ``latest.json`` if the payload does not satisfy the contract.
    """
    errors = validate(payload)
    if errors:
        raise CensusRefusal(errors)

    receipt_dir.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"

    dated_path = receipt_dir / receipt_filename(payload["generated_at"])
    _atomic_write(dated_path, text)

    # Verify what actually landed on disk, not what we intended to write.
    try:
        written = json.loads(dated_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CensusRefusal([f"dated receipt unreadable after write: {exc}"]) from exc
    readback_errors = validate(written)
    if readback_errors:
        raise CensusRefusal(
            [f"dated receipt failed validation after write: {readback_errors[0]}"]
        )

    latest_path = receipt_dir / LATEST_NAME
    _atomic_write(latest_path, text)
    return dated_path, latest_path
