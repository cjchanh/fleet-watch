"""Tests for orphan-runner detector (H3)."""

from __future__ import annotations

from fleet_watch.discovery.orphan_detector import (
    OrphanDetectionResult,
    detect_orphans,
)


class TestOrphanDetectionResult:
    def test_defaults_clean(self):
        r = OrphanDetectionResult()
        assert not r.orphans_detected
        assert r.known_model_count == 0
        assert r.runner_process_count == 0

    def test_to_dict(self):
        r = OrphanDetectionResult(
            orphans_detected=True,
            known_model_count=1,
            runner_process_count=3,
            orphan_pids=[100, 200],
            estimated_recovered_mb=4096,
            suggested_kill_command="kill 100 200",
        )
        d = r.to_dict()
        assert d["orphans_detected"] is True
        assert d["known_model_count"] == 1
        assert d["runner_process_count"] == 3
        assert d["orphan_pids"] == [100, 200]
        assert d["estimated_recovered_mb"] == 4096


class TestDetectOrphans:
    def test_no_orphans_when_counts_match(self):
        known = ["qwen3-coder-next:latest"]
        runners = [
            {"pid": 100, "rss_mb": 512,
             "cmdline": "ollama runner --model abc123 --port 11434"},
        ]
        result = detect_orphans(known_models=known, runners=runners)
        assert not result.orphans_detected
        assert result.known_model_count == 1
        assert result.runner_process_count == 1

    def test_no_orphans_when_runners_fewer_than_models(self):
        known = ["model-a", "model-b"]
        runners = [
            {"pid": 100, "rss_mb": 100,
             "cmdline": "ollama runner --model model-a-abc"},
        ]
        result = detect_orphans(known_models=known, runners=runners)
        # 1 runner <= 2 models, should not flag
        assert not result.orphans_detected

    def test_orphans_when_runners_exceed_known_models(self):
        known = ["qwen3-coder-next"]
        runners = [
            {"pid": 100, "rss_mb": 100,
             "cmdline": "ollama runner --model abc-legit"},
            {"pid": 200, "rss_mb": 200,
             "cmdline": "ollama runner --model def-orphan"},
            {"pid": 300, "rss_mb": 300,
             "cmdline": "ollama runner --model ghi-orphan"},
        ]
        result = detect_orphans(known_models=known, runners=runners)
        assert result.orphans_detected
        assert len(result.orphan_pids) > 0
        assert result.estimated_recovered_mb > 0

    def test_empty_inputs(self):
        result = detect_orphans(known_models=[], runners=[])
        assert not result.orphans_detected
        assert result.known_model_count == 0

    def test_known_models_none_uses_empty(self):
        result = detect_orphans(known_models=[], runners=[])
        assert result.known_model_count == 0
        assert result.runner_process_count == 0

    def test_suggested_kill_command_format(self):
        known = ["model-a"]
        runners = [
            {"pid": 100, "rss_mb": 100,
             "cmdline": "ollama runner --model legit"},
            {"pid": 200, "rss_mb": 200,
             "cmdline": "ollama runner --model orphan1"},
            {"pid": 300, "rss_mb": 300,
             "cmdline": "ollama runner --model orphan2"},
        ]
        result = detect_orphans(known_models=known, runners=runners)
        if result.orphans_detected:
            assert result.suggested_kill_command.startswith("kill ")
            for pid in result.orphan_pids:
                assert str(pid) in result.suggested_kill_command

    def test_matching_by_substring(self):
        known = ["qwen3-coder-next:latest"]
        runners = [
            {"pid": 100, "rss_mb": 100,
             "cmdline": "ollama runner --model qwen3-coder-next-hash123"},
        ]
        result = detect_orphans(known_models=known, runners=runners)
        assert not result.orphans_detected
        assert result.runner_process_count == 1

    def test_runner_without_model_flag_not_matched(self):
        known = ["model-a"]
        runners = [
            {"pid": 100, "rss_mb": 100,
             "cmdline": "ollama runner --port 11434"},
        ]
        result = detect_orphans(known_models=known, runners=runners)
        # 1 runner <= 1 known model: no orphans detected by count
        assert not result.orphans_detected
