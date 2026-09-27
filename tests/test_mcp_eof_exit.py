"""Characterization tests for CDS MCP server shutdown: EOF and BrokenPipe.

THE LOAD-BEARING INVARIANT
--------------------------
The "exit when the parent session dies" behavior does NOT live in the eight
CDS server scripts. Each one delegates its whole read loop to the `mcp` SDK
(`run_server(server)` -> `mcp_transport_harness.run_server` ->
`FastMCP.run(transport="stdio")` -> `anyio.run`, SDK 1.27.1). The shutdown-on-EOF
path is therefore an SDK property: `stdio_server()`'s task group ends when the
read stream ends, and lowlevel `Server.run` has a `finally: tg.cancel_scope.cancel()`.

That makes the invariant UNTESTED-BY-ACCIDENT: an SDK upgrade that regresses EOF
shutdown would turn every session's MCP child into a permanent orphan reparented
to launchd, and nothing would fail. These tests pin the observed contract for all
eight servers so that regression fails here instead of silently orphaning.

Hermetic: loopback only, ephemeral ports via bind(0), temp dirs, no network, and
no host state beyond the eight named scripts plus the one named gateway plist.

INTERPRETERS
------------
The servers are spawned with an interpreter that HAS the `mcp` module. The
fleet-watch venv does not (it has no mcp), so it runs pytest itself but never
the servers. `CDS_MCP_TRANSPORT=stdio` is pinned in the child env for the stdio
cases so an ambient export cannot silently turn a stdio case into an HTTP case.
"""

from __future__ import annotations

import json
import os
import plistlib
import queue
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

# ── The eight servers under characterization ────────────────────────────────
# Exactly the CDS stdio MCP servers guarded by mcp_transport_harness. The
# path list is the write-scope of the original EOF/orphan task.
SERVERS = [
    Path.home() / ".codex/scripts/mcp_sitrep_server.py",
    Path.home() / ".codex/scripts/mcp_engines_server.py",
    Path.home() / ".codex/scripts/mcp_compile_server.py",
    Path.home() / ".codex/scripts/mcp_frontdoor_server.py",
    Path.home() / ".codex/scripts/mcp_dispatch_server.py",
    Path.home() / ".codex/scripts/mcp_capability_index_server.py",
    Path.home() / ".codex/scripts/mcp_session_memory_server.py",
    Path.home() / "ai/scripts/mcp_graph_context_server.py",
]
SERVER_IDS = [p.stem for p in SERVERS]

# The one launchd plist whose real env defines the gateway (non-stdio) shape.
GATEWAY_PLIST = Path.home() / "Library/LaunchAgents/com.cds.mcp-gateway.plist"

# Shutdown budget. The SDK's EOF path is ~0.1s in practice; 2s is slack, not a
# wait-for-timeout design. Generous enough to survive a loaded host, tight
# enough that a real hang (the orphan symptom) still fails the test.
EXIT_TIMEOUT_S = 2.0
GATEWAY_ALIVE_S = 3.0

LOOPBACK = "127.0.0.1"

# A valid MCP `initialize` request. Sending it proves the server SERVED a
# message, which is what makes the following EOF an orphan-signal rather than
# a never-used process.
INITIALIZE = json.dumps(
    {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "fleet-watch-eof-test", "version": "0"},
        },
    }
) + "\n"

# A follow-up request that forces the server to WRITE a response. Used in the
# BrokenPipe case, after the parent's read end of stdout is closed.
TOOLS_LIST = json.dumps(
    {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
) + "\n"


def _server_ids() -> list[str]:
    return SERVER_IDS


# ── Interpreter discovery ───────────────────────────────────────────────────
# The servers need `mcp` importable. Probe candidates once and cache.
_SERVER_PYTHON: str | None = None


def server_python() -> str:
    """Return an interpreter that can import `mcp`, else skip the test.

    Fleet-watch's own venv is intentionally NOT assumed: it runs pytest, but
    it has no mcp module, so it cannot run the servers.
    """
    global _SERVER_PYTHON
    if _SERVER_PYTHON is not None:
        return _SERVER_PYTHON
    candidates = [
        "/opt/homebrew/bin/python3",
        sys.executable,
        "/usr/bin/python3",
    ]
    for cand in candidates:
        if not cand or not os.path.exists(cand):
            continue
        probe = subprocess.run(
            [cand, "-c", "import mcp"],
            capture_output=True,
            timeout=30,
        )
        if probe.returncode == 0:
            _SERVER_PYTHON = cand
            return cand
    pytest.skip("no interpreter with the mcp module is available to run the servers")


def free_loopback_port() -> int:
    """Bind(0) on loopback for an OS-assigned port (no fixed ports in tests)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind((LOOPBACK, 0))
        return int(probe.getsockname()[1])


def stdio_env() -> dict[str, str]:
    """Child env with the stdio transport pinned, immune to ambient exports."""
    env = dict(os.environ)
    env["CDS_MCP_TRANSPORT"] = "stdio"
    return env


def gateway_env() -> dict[str, str]:
    """Real gateway env from the named plist, moved onto a free loopback port.

    The plist's real port is already bound by the live KeepAlive job, so the
    port is re-allocated here; every other value (notably
    CDS_MCP_TRANSPORT=streamable-http) is taken verbatim from the plist.
    """
    if not GATEWAY_PLIST.exists():
        pytest.skip(f"gateway plist not present: {GATEWAY_PLIST}")
    with GATEWAY_PLIST.open("rb") as fh:
        plist = plistlib.load(fh)
    env = dict(os.environ)
    env.update(plist.get("EnvironmentVariables", {}))
    env["CDS_MCP_HOST"] = LOOPBACK
    env["CDS_MCP_PORT"] = str(free_loopback_port())
    env["CDS_MCP_TRANSPORT"] = "streamable-http"
    return env


def spawn(server: Path, *, env: dict[str, str], stdin, stdout, stderr) -> subprocess.Popen:
    return subprocess.Popen(
        [server_python(), str(server)],
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        env=env,
        text=True,
        bufsize=1,
        cwd="/",  # never inherit a repo cwd; the servers resolve their own paths
    )


def _drain(stream) -> list[str]:
    """Collect a child stream from a thread so the child can never block on a full pipe."""
    lines: list[str] = []

    def pump() -> None:
        try:
            for line in stream:
                lines.append(line)
        except (ValueError, OSError):
            pass

    thread = threading.Thread(target=pump, daemon=True)
    thread.start()
    return lines


def reap(proc: subprocess.Popen) -> None:
    """Terminate a child THIS test started. Never touches any other process."""
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def read_line_within(stream, timeout: float) -> str:
    """Read one line from a child pipe, bounded. Returns "" on timeout.

    A thread + queue is used rather than select() so this is correct regardless
    of the pipe's buffering mode.
    """
    got: queue.Queue = queue.Queue(maxsize=1)

    def pump() -> None:
        try:
            got.put(stream.readline())
        except (ValueError, OSError) as exc:  # closed under us
            got.put(f"<stream-error {exc}>")

    threading.Thread(target=pump, daemon=True).start()
    try:
        return got.get(timeout=timeout)
    except queue.Empty:
        return ""


# ── 1. The load-bearing invariant: EOF after a served message exits 0 ───────
# If the SDK ever regresses EOF shutdown, every case here fails instead of
# leaving a live reparented orphan behind.
@pytest.mark.parametrize("server_id", _server_ids())
def test_stdio_eof_after_served_message_exits_zero(server_id: str) -> None:
    server = SERVERS[SERVER_IDS.index(server_id)]
    assert server.exists(), f"server script missing: {server}"

    proc = spawn(
        server,
        env=stdio_env(),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stderr_lines = _drain(proc.stderr)
    try:
        proc.stdin.write(INITIALIZE)
        proc.stdin.flush()

        response = read_line_within(proc.stdout, timeout=5.0)
        assert response, f"{server_id}: no response to initialize (never served?)"
        assert '"result"' in response, f"{server_id}: initialize not answered: {response[:200]!r}"

        # The session dies: stdin closes. The server must shut itself down.
        proc.stdin.close()

        try:
            code = proc.wait(timeout=EXIT_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            pytest.fail(
                f"{server_id}: STILL ALIVE {EXIT_TIMEOUT_S}s after stdin EOF — "
                "this is the orphan regression (would be reparented to launchd)"
            )
        assert code == 0, f"{server_id}: exited {code} on EOF, expected 0"
    finally:
        reap(proc)

    noise = "".join(stderr_lines)
    assert "Traceback" not in noise, (
        f"{server_id}: clean EOF produced a traceback:\n{noise[:1500]}"
    )


# ── 2. Control: the launchd gateway shape must NOT exit on stdin ────────────
# The gateways run streamable-HTTP, so they never read stdin even though
# launchd hands them /dev/null. This is the case that would respawn-loop if a
# future "exit on EOF" guard were written without the transport gate.
@pytest.mark.parametrize("server_id", _server_ids())
def test_gateway_shape_with_devnull_stdin_stays_alive(server_id: str) -> None:
    server = SERVERS[SERVER_IDS.index(server_id)]
    assert server.exists(), f"server script missing: {server}"

    proc = spawn(
        server,
        env=gateway_env(),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + GATEWAY_ALIVE_S
        while time.monotonic() < deadline:
            code = proc.poll()
            assert code is None, (
                f"{server_id}: gateway shape exited rc={code} with stdin=/dev/null "
                f"after {GATEWAY_ALIVE_S}s — KeepAlive would respawn-loop it"
            )
            time.sleep(0.1)
        assert proc.poll() is None, f"{server_id}: gateway shape died"
    finally:
        reap(proc)


# ── 3. Control: BrokenPipe mid-serve must not leave a live process ─────────
# Before the `except* BrokenPipeError` guard landed, the server DID exit (so
# there was never an orphan here) but it exited 120 with a ~4KB ExceptionGroup
# traceback. This test used to assert that BrokenPipeError was present in
# stderr, pinning the noisy shutdown. The guard suppresses it, so the contract
# is now the stronger one: the process exits AND shutdowns quietly. Reinstating
# the traceback (SDK change, or a guard dropped in one file) fails here.
@pytest.mark.parametrize("server_id", _server_ids())
def test_brokenpipe_mid_serve_exits_and_does_not_orphan(server_id: str) -> None:
    server = SERVERS[SERVER_IDS.index(server_id)]
    assert server.exists(), f"server script missing: {server}"

    proc = spawn(
        server,
        env=stdio_env(),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stderr_lines = _drain(proc.stderr)
    try:
        proc.stdin.write(INITIALIZE)
        proc.stdin.flush()
        assert read_line_within(proc.stdout, timeout=5.0), (
            f"{server_id}: no response to initialize (never served?)"
        )

        # Mid-serve: the client's read end of stdout goes away.
        proc.stdout.close()
        try:
            proc.stdin.write(TOOLS_LIST)
            proc.stdin.flush()
        except (BrokenPipeError, ValueError, OSError):
            pass  # our own pipe may already be unusable; the child's write is the point
        time.sleep(0.4)
        try:
            proc.stdin.close()
        except (BrokenPipeError, ValueError, OSError):
            pass

        try:
            proc.wait(timeout=EXIT_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            pytest.fail(
                f"{server_id}: STILL ALIVE {EXIT_TIMEOUT_S}s after stdout broke "
                "mid-serve — a live process is the orphan symptom"
            )
    finally:
        reap(proc)

    noise = "".join(stderr_lines)
    assert "Traceback" not in noise, (
        f"{server_id}: broken client stdout produced a traceback — the "
        f"except* BrokenPipeError guard is missing or not matching:\n{noise[:1500]}"
    )


def test_brokenpipe_mid_serve_exits_zero_without_traceback() -> None:
    """Contract for a broken client pipe: exit 0, no traceback.

    Was a strict xfail until the `except* BrokenPipeError` guard landed in each
    server's main(); the marker came off when it passed. This is the receipt
    that the guard is present in all eight servers.
    """
    server = SERVERS[0]
    proc = spawn(
        server,
        env=stdio_env(),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stderr_lines = _drain(proc.stderr)
    try:
        proc.stdin.write(INITIALIZE)
        proc.stdin.flush()
        assert read_line_within(proc.stdout, timeout=5.0)
        proc.stdout.close()
        try:
            proc.stdin.write(TOOLS_LIST)
            proc.stdin.flush()
        except (BrokenPipeError, ValueError, OSError):
            pass
        time.sleep(0.4)
        try:
            proc.stdin.close()
        except (BrokenPipeError, ValueError, OSError):
            pass
        code = proc.wait(timeout=EXIT_TIMEOUT_S)
    finally:
        reap(proc)

    noise = "".join(stderr_lines)
    assert code == 0, f"expected clean exit 0 on broken stdout, got {code}"
    assert "Traceback" not in noise, f"broken stdout produced a traceback:\n{noise[:1500]}"
