from __future__ import annotations

import json

from fleet_watch.explanation import (
    build_explanation_prompt,
    explain_incident,
    redact_incident,
    validate_explanation,
)


def test_redaction_removes_command_paths_and_forbidden_action_fields():
    incident = {
        "pid": 123,
        "name": "fixture",
        "command": "/Users/cj/private/secret.py --token supersecret",
        "cwd": "/Users/cj/private",
        "environment": {"OPENAI_API_KEY": "do-not-send"},
        "action": "kill",
        "signal": 9,
    }
    clean = redact_incident(incident)
    encoded = json.dumps(clean)
    assert "supersecret" not in encoded
    assert "secret.py" not in encoded
    assert "action" not in clean
    assert "signal" not in clean
    assert clean["pid"] == 123


def test_prompt_and_validation_have_no_signal_authority():
    incident = redact_incident(
        {"pid": 123, "name": "runner", "command": "python runner.py"}
    )
    prompt = build_explanation_prompt(incident)
    assert "kill" not in prompt.lower()
    assert "signal" not in prompt.lower()
    explanation = {
        "classification": "stale_candidate",
        "explanation": "Owner evidence is incomplete; inspect before acting.",
        "confidence": 0.7,
    }
    result = validate_explanation(explanation, incident)
    assert result["valid"] is True
    assert result["classification"] == "stale_candidate"


def test_validator_rejects_tool_or_action_fields():
    incident = redact_incident({"pid": 123, "name": "runner"})
    result = validate_explanation(
        {
            "classification": "unknown",
            "explanation": "x",
            "confidence": 0.2,
            "tool": "kill",
            "action": "terminate",
        },
        incident,
    )
    assert result["valid"] is False
    assert "forbidden" in result["reason"]


def test_explain_incident_deduplicates_successful_local_calls(monkeypatch):
    import fleet_watch.explanation as explanation

    explanation._reset_deduplication()
    calls = []

    class Result:
        returncode = 0
        stdout = json.dumps(
            {
                "classification": "protected",
                "explanation": "Protected workload; no action.",
                "confidence": 0.95,
            }
        )
        stderr = ""

    monkeypatch.setattr(
        explanation.subprocess,
        "run",
        lambda argv, **kwargs: (calls.append(argv), Result())[1],
    )
    incident = {"pid": 987654, "name": "unique-sidecar-fixture"}
    first = explain_incident(incident, runtime=["fake-ollama"], timeout_seconds=0.2)
    second = explain_incident(incident, runtime=["fake-ollama"], timeout_seconds=0.2)
    assert first["status"] == "OK"
    assert second["status"] == "OK"
    assert second.get("cached") is True
    assert len(calls) == 1


def test_explain_incident_rate_limits_repeated_request_without_cache(monkeypatch):
    import fleet_watch.explanation as explanation

    explanation._reset_deduplication()
    calls = []

    class Result:
        returncode = 0
        stdout = json.dumps(
            {
                "classification": "unknown",
                "explanation": "Insufficient evidence.",
                "confidence": 0.2,
            }
        )
        stderr = ""

    monkeypatch.setattr(
        explanation.subprocess,
        "run",
        lambda argv, **kwargs: (calls.append(argv), Result())[1],
    )
    incident = {"pid": 987655, "name": "rate-limit-fixture"}
    first = explain_incident(incident, runtime=["fake-ollama"], timeout_seconds=0.2)
    second = explain_incident(
        incident,
        runtime=["fake-ollama"],
        timeout_seconds=0.2,
        dedup_ttl_seconds=0.0,
        min_call_interval_seconds=60.0,
    )
    assert first["status"] == "OK"
    assert second == {
        "status": "UNKNOWN",
        "reason": "rate_limited",
        "incident": first["incident"],
    }
    assert len(calls) == 1


    calls = []

    class Result:
        returncode = 0
        stdout = json.dumps(
            {
                "classification": "protected",
                "explanation": "Protected workload; no action.",
                "confidence": 0.95,
            }
        )
        stderr = ""

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return Result()

    monkeypatch.setattr("fleet_watch.explanation.subprocess.run", fake_run)
    result = explain_incident(
        {"pid": 7, "name": "WindowServer", "command": "WindowServer -daemon"},
        runtime=["fake-ollama"],
        model="local-model",
        timeout_seconds=0.2,
    )
    assert result["status"] == "OK"
    assert result["incident"]["name"] == "WindowServer"
    assert "command" not in result["incident"]
    assert result["explanation"]["classification"] == "protected"
    assert calls and calls[0][1]["timeout"] == 0.2
