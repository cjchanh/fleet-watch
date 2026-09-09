"""``fleet guard --json`` must never emit Click usage text (red-team F-4)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from fleet_watch import cli as cli_module

REPO = Path(__file__).resolve().parent.parent


def _guard(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "fleet_watch.cli", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )


def _assert_json_deny(proc: subprocess.CompletedProcess[str]) -> dict:
    combined = proc.stdout + proc.stderr
    assert "Usage:" not in combined, combined
    assert proc.stdout.strip().startswith("{"), combined
    payload = json.loads(proc.stdout)
    assert payload["allowed"] is False
    assert payload.get("reason")
    return payload


def test_guard_json_negative_gpu_returns_json_deny() -> None:
    payload = _assert_json_deny(_guard("guard", "--json", "--gpu", "-5"))
    assert "negative" in payload["reason"].lower()


def test_guard_json_non_numeric_gpu_returns_json_deny() -> None:
    payload = _assert_json_deny(_guard("guard", "--json", "--gpu", "abc"))
    assert "integer" in payload["reason"].lower() or "gpu" in payload["reason"].lower()


def test_guard_json_bad_port_returns_json_deny() -> None:
    payload = _assert_json_deny(_guard("guard", "--json", "--port", "notaport"))
    assert "integer" in payload["reason"].lower() or "port" in payload["reason"].lower()


def test_guard_json_usage_error_via_cli_runner() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["guard", "--json", "--gpu", "-5"])
    payload = json.loads(result.output)
    assert payload["allowed"] is False
    assert "negative" in payload["reason"].lower()
    assert result.exit_code == 1


def test_guard_negative_gpu_without_json_stays_usage_error() -> None:
    proc = _guard("guard", "--gpu", "-5")
    combined = proc.stdout + proc.stderr
    assert proc.returncode == 2
    assert "Usage:" in combined
    assert not proc.stdout.strip().startswith("{")


def _assert_click_usage_exit_2(proc: subprocess.CompletedProcess[str]) -> None:
    combined = proc.stdout + proc.stderr
    assert proc.returncode == 2, combined
    assert "Usage:" in combined
    assert not proc.stdout.strip().startswith("{"), combined


def test_pkill_guard_json_is_not_a_guard_verdict() -> None:
    _assert_click_usage_exit_2(_guard("pkill", "guard", "--json", "--bogus"))


def test_register_name_guard_json_is_not_a_guard_verdict() -> None:
    _assert_click_usage_exit_2(_guard("register", "--name", "guard", "--json"))


def test_status_json_guard_is_not_a_guard_verdict() -> None:
    _assert_click_usage_exit_2(_guard("status", "--json", "guard"))


def test_guard_json_ctx_exit_preserves_code() -> None:
    """standalone_mode=False must still honor ctx.exit(n) as process exit n."""

    @click.group(cls=cli_module.FleetGroup)
    def tiny() -> None:
        pass

    @tiny.command()
    @click.option("--json", "as_json", is_flag=True)
    @click.pass_context
    def guard(ctx: click.Context, as_json: bool) -> None:
        ctx.exit(7)

    with pytest.raises(SystemExit) as caught:
        tiny.main(args=["guard", "--json"], prog_name="tiny")
    assert caught.value.code == 7
