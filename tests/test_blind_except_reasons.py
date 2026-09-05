"""Decision-path blind excepts must name the exception type in the reason."""

from __future__ import annotations

import os

from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import referee, registry, syshealth


def _boom(*_a, **_k):
    raise RuntimeError("boom")


def test_syshealth_numeric_probe_surfaces_exception_type(monkeypatch):
    monkeypatch.setattr(syshealth.subprocess, "run", _boom)
    result = syshealth._run_numeric_probe(["sysctl", "-n", "hw.memsize"])
    assert "RuntimeError" in (result.failure_reason or "")


def test_referee_repo_writer_probe_surfaces_exception_type(monkeypatch, tmp_path):
    git = tmp_path / ".git"
    git.mkdir()
    (git / "index.lock").write_text("held")
    monkeypatch.setattr(referee.subprocess, "run", _boom)
    probe = referee.probe_repo_writers(str(tmp_path))
    assert "RuntimeError" in probe.detail


def test_cli_register_surfaces_exception_type(monkeypatch, tmp_path):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")
    monkeypatch.setattr(cli_module.registry, "register_process", _boom)
    result = CliRunner().invoke(
        cli_module.cli,
        ["register", "--pid", str(os.getpid()), "--name", "t", "--workstream", "ws"],
    )
    assert "RuntimeError" in (result.output or "")
