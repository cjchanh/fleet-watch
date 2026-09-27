from __future__ import annotations

from fleet_watch.discovery import mcp_orphan_detector, orphan_detector


def test_orphan_probe_failure_is_unknown_not_clean(monkeypatch):
    monkeypatch.setattr(
        orphan_detector,
        "_get_known_models_with_status",
        lambda: ([], "known_models_unavailable"),
    )
    monkeypatch.setattr(
        orphan_detector,
        "_get_runner_processes_with_status",
        lambda: ([], "ps_unavailable"),
    )
    result = orphan_detector.detect_orphans()
    assert result.orphans_detected is False
    assert result.error == "known_models_unavailable;ps_unavailable"
    assert result.to_dict()["error"] == result.error


def test_orphan_known_model_failure_cannot_create_orphans(monkeypatch):
    monkeypatch.setattr(
        orphan_detector,
        "_get_known_models_with_status",
        lambda: ([], "known_models_unavailable"),
    )
    monkeypatch.setattr(
        orphan_detector,
        "_get_runner_processes_with_status",
        lambda: ([{"pid": 42, "rss_mb": 1, "cmdline": "ollama runner --model abc"}], None),
    )
    result = orphan_detector.detect_orphans()
    assert result.orphans_detected is False
    assert result.error == "known_models_unavailable"


def test_mcp_probe_failure_is_surfaced(monkeypatch):
    monkeypatch.setattr(
        mcp_orphan_detector,
        "_get_mcp_processes_with_status",
        lambda: ([], "ps_unavailable"),
    )
    result = mcp_orphan_detector.detect()
    assert result.error == "ps_unavailable"
    assert result.orphan_pids == []
    assert result.to_dict()["error"] == "ps_unavailable"
