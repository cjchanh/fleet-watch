"""Optional, on-demand explanation sidecar contract.

FleetWatch's zero-egress monitor never imports or invokes a model.  This
module is a narrow, manual boundary: it redacts an incident, asks an explicitly
selected local runtime for a classification/explanation, validates the JSON,
and returns no executable capability.  A model result can never authorize a
process-control operation.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import threading
import time
from collections import OrderedDict
from typing import Any, Iterable


ALLOWED_CLASSIFICATIONS = frozenset(
    {"active", "stale_candidate", "orphan_confirmed", "protected", "unknown"}
)
_ALLOWED_INCIDENT_KEYS = frozenset(
    {
        "pid",
        "name",
        "workstream",
        "classification",
        "owner",
        "service_class",
        "cpu_pct",
        "rss_mb",
        "age_seconds",
        "reason",
        "observed_at",
    }
)
_FORBIDDEN_KEYS = frozenset(
    {"action", "signal", "tool", "shell", "command", "cwd", "environment", "env"}
)

# The sidecar is manual, but repeated operator requests should not repeatedly
# wake a local model. Keys are hashes of already-redacted data; no raw incident
# text is retained in the process-level cache.
_CACHE_LOCK = threading.Lock()
_CACHE: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()
_LAST_CALL: dict[str, float] = {}
_MAX_CACHE_ENTRIES = 32
_MIN_CALL_INTERVAL_SECONDS = 0.25
_DEDUP_TTL_SECONDS = 60.0


def _redact_value(value: Any) -> Any:
    if isinstance(value, str):
        if value.startswith("/") or "\\" in value:
            return "[redacted]"
        return value[:160]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_redact_value(item) for item in value[:32]]
    if isinstance(value, dict):
        return {
            str(key)[:64]: _redact_value(item)
            for key, item in list(value.items())[:32]
            if str(key).lower() not in _FORBIDDEN_KEYS
        }
    return "[redacted]"


def redact_incident(incident: dict[str, Any]) -> dict[str, Any]:
    """Return a bounded, data-only incident safe to hand to a local model."""
    clean: dict[str, Any] = {}
    for key, value in incident.items():
        key_text = str(key)
        if key_text.lower() in _FORBIDDEN_KEYS or key_text not in _ALLOWED_INCIDENT_KEYS:
            continue
        if key_text == "command":
            clean[key_text] = "[redacted]"
        else:
            clean[key_text] = _redact_value(value)
    clean.setdefault("observed_at", None)
    return clean


def build_explanation_prompt(incident: dict[str, Any]) -> str:
    """Build a minimal prompt with no command, tool, or process-control field."""
    return (
        "You are explaining one bounded machine observation. "
        "Return JSON only with keys classification, explanation, confidence. "
        "Classifications are active, stale_candidate, orphan_confirmed, "
        "protected, or unknown. Do not propose commands or tools. Observation:\n"
        + json.dumps(redact_incident(incident), sort_keys=True, separators=(",", ":"))
    )


def _extract_json(raw: str) -> dict[str, Any] | None:
    raw = raw.strip()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        start = min((i for i in (raw.find("{"), raw.find("[")) if i >= 0), default=-1)
        if start < 0:
            return None
        try:
            value = json.loads(raw[start:])
        except json.JSONDecodeError:
            return None
    return value if isinstance(value, dict) else None


def _contains_forbidden_key(value: Any) -> bool:
    if isinstance(value, dict):
        return any(
            str(key).lower() in _FORBIDDEN_KEYS or _contains_forbidden_key(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_forbidden_key(item) for item in value)
    return False


def validate_explanation(
    explanation: dict[str, Any], incident: dict[str, Any]
) -> dict[str, Any]:
    """Validate a model response without granting it any authority."""
    if not isinstance(explanation, dict):
        return {"valid": False, "reason": "response_not_object"}
    if _contains_forbidden_key(explanation):
        return {"valid": False, "reason": "forbidden executable field"}
    classification = explanation.get("classification")
    if classification not in ALLOWED_CLASSIFICATIONS:
        return {"valid": False, "reason": "classification_not_allowed"}
    text = explanation.get("explanation")
    confidence = explanation.get("confidence")
    if not isinstance(text, str) or not text.strip() or len(text) > 1200:
        return {"valid": False, "reason": "explanation_invalid"}
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        return {"valid": False, "reason": "confidence_invalid"}
    if not 0.0 <= float(confidence) <= 1.0:
        return {"valid": False, "reason": "confidence_out_of_range"}
    return {
        "valid": True,
        "classification": classification,
        "explanation": text.strip(),
        "confidence": float(confidence),
        "incident": redact_incident(incident),
    }


def _cache_key(incident: dict[str, Any]) -> str:
    encoded = json.dumps(incident, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _cached_or_rate_limited(
    incident: dict[str, Any],
    *,
    ttl_seconds: float,
    min_interval_seconds: float,
) -> dict[str, Any] | None:
    """Return a cached result or an UNKNOWN rate-limit result, if applicable."""
    key = _cache_key(incident)
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if cached is not None and now - cached[0] <= ttl_seconds:
            return {**cached[1], "cached": True}
        last = _LAST_CALL.get(key)
        if last is not None and now - last < min_interval_seconds:
            return {
                "status": "UNKNOWN",
                "reason": "rate_limited",
                "incident": incident,
            }
        _LAST_CALL[key] = now
        # Keep the recent-call index bounded along with successful responses.
        if len(_LAST_CALL) > _MAX_CACHE_ENTRIES:
            cutoff = now - max(ttl_seconds, min_interval_seconds)
            for old_key, old_time in list(_LAST_CALL.items()):
                if old_time < cutoff:
                    _LAST_CALL.pop(old_key, None)
    return None


def _remember_result(
    incident: dict[str, Any], result: dict[str, Any], *, ttl_seconds: float
) -> None:
    if result.get("status") != "OK":
        return
    key = _cache_key(incident)
    with _CACHE_LOCK:
        _CACHE[key] = (time.monotonic(), dict(result))
        _CACHE.move_to_end(key)
        while len(_CACHE) > _MAX_CACHE_ENTRIES:
            _CACHE.popitem(last=False)


def _reset_deduplication() -> None:
    """Test-only reset hook; no durable state is involved."""
    with _CACHE_LOCK:
        _CACHE.clear()
        _LAST_CALL.clear()


def explain_incident(
    incident: dict[str, Any],
    *,
    runtime: Iterable[str] = ("ollama", "run"),
    model: str = "local-model",
    timeout_seconds: float = 15.0,
    dedup_ttl_seconds: float = _DEDUP_TTL_SECONDS,
    min_call_interval_seconds: float = _MIN_CALL_INTERVAL_SECONDS,
) -> dict[str, Any]:
    """Run one bounded, redacted, deduplicated local-runtime request."""
    clean = redact_incident(incident)
    cached = _cached_or_rate_limited(
        clean,
        ttl_seconds=max(0.0, float(dedup_ttl_seconds)),
        min_interval_seconds=max(0.0, float(min_call_interval_seconds)),
    )
    if cached is not None:
        return cached
    argv = [*runtime, model]
    try:
        completed = subprocess.run(
            argv,
            input=build_explanation_prompt(clean),
            capture_output=True,
            text=True,
            timeout=max(0.1, float(timeout_seconds)),
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"status": "UNKNOWN", "reason": f"runtime_{type(exc).__name__}", "incident": clean}
    if completed.returncode != 0:
        return {
            "status": "UNKNOWN",
            "reason": f"runtime_exit_{completed.returncode}",
            "incident": clean,
        }
    parsed = _extract_json(completed.stdout)
    if parsed is None:
        return {"status": "UNKNOWN", "reason": "response_not_json", "incident": clean}
    validated = validate_explanation(parsed, clean)
    if not validated["valid"]:
        return {"status": "UNKNOWN", "reason": validated["reason"], "incident": clean}
    result = {"status": "OK", "incident": clean, "explanation": validated}
    _remember_result(clean, result, ttl_seconds=max(0.0, float(dedup_ttl_seconds)))
    return result
