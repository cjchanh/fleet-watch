"""Live adversarial integration tests for Fleet Watch."""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import events, registry


LISTENER_CODE = """
import signal
import socket
import sys

port = int(sys.argv[1])
sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(("127.0.0.1", port))
sock.listen()
running = True

def stop(*_args):
    global running
    running = False

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

while running:
    sock.settimeout(0.2)
    try:
        conn, _addr = sock.accept()
        conn.close()
    except socket.timeout:
        pass

sock.close()
"""

SLEEP_CODE = """
import signal
import time

running = True

def stop(*_args):
    global running
    running = False

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

while running:
    time.sleep(0.2)
"""

STATE_KEYS = {
    "agent_interface",
    "generated_utc",
    "processes",
    "process_count",
    "gpu_budget",
    "ports_claimed",
    "preferred_ports",
    "safe_ports",
    "repos_locked",
    "stale_processes",
    "recent_events",
    "conflicts_prevented_24h",
}


def _patch_paths(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")


def _write_test_config(tmp_path: Path, preferred_ports: list[int] | None = None):
    config = {
        "gpu_total_mb": registry.DEFAULT_GPU_TOTAL_MB,
        "gpu_reserve_mb": registry.DEFAULT_GPU_RESERVE_MB,
        "preferred_ports": preferred_ports or [47001, 47002, 47003, 47004, 47005, 47006],
        "patterns": [
            {
                "name_template": "Adversarial HTTP",
                "process_match": "adversarial_fleet_test.*server",
                "workstream": "adversarial-test",
                "priority": 2,
                "restart_policy": "RESTART_NEVER",
                "gpu_mb_default": 0,
            }
        ],
    }
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "config.json").write_text(json.dumps(config, indent=2) + "\n")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_port(port: int, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise AssertionError(f"port {port} did not start listening within {timeout}s")


def _terminate(proc: subprocess.Popen[str]):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


@contextmanager
def _listener_process(port: int):
    proc = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            LISTENER_CODE,
            str(port),
            "adversarial_fleet_test",
            "server",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        _wait_for_port(port)
        yield proc
    finally:
        _terminate(proc)


@contextmanager
def _sleep_process():
    proc = subprocess.Popen(
        [sys.executable, "-u", "-c", SLEEP_CODE],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        deadline = time.time() + 5
        while proc.poll() is None and time.time() < deadline:
            returncode = proc.poll()
            if returncode is None:
                yield proc
                break
        else:
            raise AssertionError("sleep subprocess did not stay alive")
    finally:
        _terminate(proc)


def test_discover_finds_live_listener_and_cleans_dead_process(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    port = _free_port()
    _write_test_config(tmp_path, preferred_ports=[port, port + 1, port + 2, port + 3])
    runner = CliRunner()

    with _listener_process(port) as proc:
        result = runner.invoke(cli_module.cli, ["discover"])
        assert result.exit_code == 0
        assert f"+ PID {proc.pid} (Adversarial HTTP)" in result.output

        conn = registry.connect()
        discovered = registry.get_process(conn, proc.pid)
        conn.close()
        assert discovered is not None
        assert discovered["port"] == port

    result = runner.invoke(cli_module.cli, ["discover"])
    assert result.exit_code == 0
    assert f"- PID {proc.pid} (Adversarial HTTP) [dead]" in result.output

    conn = registry.connect()
    assert registry.get_process(conn, proc.pid) is None
    conn.close()


def test_discover_cleans_dead_listener_on_second_sync(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    port = _free_port()
    _write_test_config(tmp_path, preferred_ports=[port, port + 1, port + 2, port + 3])
    runner = CliRunner()

    with _listener_process(port) as proc:
        first = runner.invoke(cli_module.cli, ["discover"])
        assert first.exit_code == 0
        assert f"+ PID {proc.pid} (Adversarial HTTP)" in first.output

    second = runner.invoke(cli_module.cli, ["discover"])
    assert second.exit_code == 0
    assert f"- PID {proc.pid} (Adversarial HTTP) [dead]" in second.output

    state = json.loads((tmp_path / "state.json").read_text())
    assert all(item["pid"] != proc.pid for item in state["processes"])


def test_register_denies_second_process_on_same_port(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    runner = CliRunner()
    port = _free_port()

    with _sleep_process() as proc_one, _sleep_process() as proc_two:
        first = runner.invoke(
            cli_module.cli,
            [
                "register",
                "--pid",
                str(proc_one.pid),
                "--name",
                "first",
                "--workstream",
                "adversarial-test",
                "--port",
                str(port),
            ],
        )
        second = runner.invoke(
            cli_module.cli,
            [
                "register",
                "--pid",
                str(proc_two.pid),
                "--name",
                "second",
                "--workstream",
                "adversarial-test",
                "--port",
                str(port),
            ],
        )

        assert first.exit_code == 0
        assert second.exit_code == 1
        assert "DENY: port" in second.output


def test_register_denies_gpu_claim_exceeding_budget(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    runner = CliRunner()

    with _sleep_process() as proc:
        result = runner.invoke(
            cli_module.cli,
            [
                "register",
                "--pid",
                str(proc.pid),
                "--name",
                "gpu-hog",
                "--workstream",
                "adversarial-test",
                "--gpu",
                "200000",
            ],
        )

    assert result.exit_code == 1
    assert "GPU budget exceeded" in result.output


def test_guard_json_reports_holder_and_suggested_ports_for_taken_port(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    port = _free_port()
    preferred = [port, port + 1, port + 2, port + 3, port + 4, port + 5]
    _write_test_config(tmp_path, preferred_ports=preferred)
    runner = CliRunner()

    with _listener_process(port) as proc:
        discover_result = runner.invoke(cli_module.cli, ["discover"])
        assert discover_result.exit_code == 0

        guard_result = runner.invoke(
            cli_module.cli,
            ["guard", "--port", str(port), "--json"],
        )

        assert guard_result.exit_code == 1
        payload = json.loads(guard_result.output)
        assert payload["allowed"] is False
        assert payload["checks"]["port"]["holder"]["pid"] == proc.pid
        assert payload["checks"]["port"]["holder"]["name"] == "Adversarial HTTP"
        assert payload["checks"]["port"]["suggested_ports"] == preferred[1:]


def test_hash_chain_stays_valid_after_many_events(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    conn = registry.connect()

    for idx in range(25):
        events.log_event(
            conn,
            "HEARTBEAT",
            pid=5000 + idx,
            workstream="adversarial-test",
            detail={"sequence": idx},
        )

    valid, checked = events.verify_chain(conn)
    conn.close()

    assert valid is True
    assert checked == 25


def test_status_json_shows_live_process_then_clean_removes_it(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    runner = CliRunner()

    with _sleep_process() as proc:
        register_result = runner.invoke(
            cli_module.cli,
            [
                "register",
                "--pid",
                str(proc.pid),
                "--name",
                "status-proc",
                "--workstream",
                "adversarial-test",
            ],
        )
        assert register_result.exit_code == 0

        status_result = runner.invoke(cli_module.cli, ["status", "--json"])
        assert status_result.exit_code == 0
        payload = json.loads(status_result.output)
        assert any(item["pid"] == proc.pid for item in payload["processes"])

    clean_result = runner.invoke(cli_module.cli, ["clean"])
    assert clean_result.exit_code == 0
    assert f"Cleaned PID {proc.pid} (status-proc)" in clean_result.output

    status_result = runner.invoke(cli_module.cli, ["status", "--json"])
    assert status_result.exit_code == 0
    payload = json.loads(status_result.output)
    assert all(item["pid"] != proc.pid for item in payload["processes"])


def test_discover_writes_state_json_with_required_contract_fields(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    port = _free_port()
    preferred = [port, port + 1, port + 2, port + 3, port + 4, port + 5]
    _write_test_config(tmp_path, preferred_ports=preferred)
    runner = CliRunner()

    with _listener_process(port) as proc:
        result = runner.invoke(cli_module.cli, ["discover"])
        assert result.exit_code == 0

        state_path = tmp_path / "state.json"
        assert state_path.exists()

        state = json.loads(state_path.read_text())
        assert set(state.keys()) == STATE_KEYS
        assert state["agent_interface"] == "fleet guard --json"
        assert state["ports_claimed"][str(port)] == proc.pid
        assert any(item["pid"] == proc.pid for item in state["processes"])
        assert state["preferred_ports"] == preferred
        assert state["safe_ports"] == preferred[1:]
