"""Tests for ollama runner auto-discovery (H1)."""

from __future__ import annotations

from fleet_watch.discovery.ollama_runners import (
    OllamaRunner,
    OllamaRunnerReport,
    _extract_model_hash,
    _extract_port,
    _is_ollama_runner,
    runner_entries_for_status,
    total_actual_gpu_mb,
)


class TestIsOllamaRunner:
    def test_positive_match_underscore(self):
        assert _is_ollama_runner("/path/to/ollama_runner --model abc --port 11434")

    def test_positive_match_hyphen(self):
        assert _is_ollama_runner("/path/to/ollama-runner --model abc")

    def test_positive_match_space(self):
        assert _is_ollama_runner("ollama runner --model xyz")

    def test_negative_ollama_serve(self):
        assert not _is_ollama_runner("ollama serve")

    def test_negative_unrelated(self):
        assert not _is_ollama_runner("python train.py")


class TestExtractModelHash:
    def test_extracts_model(self):
        assert _extract_model_hash("ollama runner --model abc123def") == "abc123def"

    def test_unknown_when_missing(self):
        assert _extract_model_hash("ollama runner --port 11434") == "unknown"

    def test_multiple_flags_extracts_first(self):
        result = _extract_model_hash("ollama runner --model first --model second")
        assert result == "first"


class TestExtractPort:
    def test_extracts_port(self):
        assert _extract_port("ollama runner --port 11435") == 11435

    def test_none_when_missing(self):
        assert _extract_port("ollama runner --model abc") is None


class TestRunnerEntriesForStatus:
    def test_converts_runners_to_entries(self):
        reports = [
            OllamaRunnerReport(
                serve_pid=100,
                runners=[
                    OllamaRunner(
                        pid=200, parent_pid=100, rss_mb=512,
                        model_hash="abc123", port=11434,
                        cmdline="ollama runner --model abc123 --port 11434",
                    ),
                ],
                total_runner_rss_mb=512,
            )
        ]
        entries = runner_entries_for_status(reports)
        assert len(entries) == 1
        entry = entries[0]
        assert entry["pid"] == 200
        assert entry["synthetic"] is True
        assert entry["parent_pid"] == 100
        assert entry["rss_mb"] == 512
        assert entry["model_hash"] == "abc123"

    def test_multiple_runners_across_serves(self):
        reports = [
            OllamaRunnerReport(
                serve_pid=100,
                runners=[
                    OllamaRunner(pid=200, parent_pid=100, rss_mb=100,
                                 model_hash="a", port=11434, cmdline=""),
                ],
                total_runner_rss_mb=100,
            ),
            OllamaRunnerReport(
                serve_pid=300,
                runners=[
                    OllamaRunner(pid=400, parent_pid=300, rss_mb=200,
                                 model_hash="b", port=11434, cmdline=""),
                ],
                total_runner_rss_mb=200,
            ),
        ]
        entries = runner_entries_for_status(reports)
        assert len(entries) == 2

    def test_empty_reports_returns_empty(self):
        assert runner_entries_for_status([]) == []


class TestTotalActualGpuMb:
    def test_sums_all_serves(self):
        reports = [
            OllamaRunnerReport(serve_pid=1, total_runner_rss_mb=1000),
            OllamaRunnerReport(serve_pid=2, total_runner_rss_mb=2000),
        ]
        assert total_actual_gpu_mb(reports) == 3000

    def test_empty_reports_returns_zero(self):
        assert total_actual_gpu_mb([]) == 0


class TestOllamaRunnerReportToDict:
    def test_to_dict_basic(self):
        report = OllamaRunnerReport(serve_pid=100)
        d = report.to_dict()
        assert d["serve_pid"] == 100
        assert d["runner_count"] == 0
        assert d["total_runner_rss_mb"] == 0

    def test_to_dict_with_runners(self):
        runner = OllamaRunner(
            pid=200, parent_pid=100, rss_mb=512,
            model_hash="abc", port=11434, cmdline="ollama runner"
        )
        report = OllamaRunnerReport(
            serve_pid=100, runners=[runner], total_runner_rss_mb=512
        )
        d = report.to_dict()
        assert d["runner_count"] == 1
        assert d["runners"][0]["pid"] == 200
        assert d["runners"][0]["model_hash"] == "abc"
