"""The Rust fleet-kernel is behind Python by the whole OS-truth layer.

That is a deliberate choice, not a bug. The bug would be letting it read as
parity. `rust/fleet-kernel/src/checks.rs` once described itself as a "faithful
port" and quoted, verbatim, Python that had since been deleted; its vector
generator claimed to "mirror referee.check_port exactly" while re-implementing
the old logic inline, so it was structurally incapable of noticing the drift.

These tests live in the PYTHON suite on purpose. `pytest tests/ -q` is the
command the operator actually runs; a guard that only fires under `cargo test`
would not have caught the drift it exists to catch, because the canary plist is
not installed and nothing runs the Rust suite on this machine.
"""
from __future__ import annotations

import json
import socket
from pathlib import Path

import pytest

from fleet_watch import referee, registry

KERNEL = Path(__file__).resolve().parent.parent / "rust" / "fleet-kernel"
VECTORS = KERNEL / "tests" / "checks_vectors.json"
CHECKS_RS = KERNEL / "src" / "checks.rs"
GENERATOR = KERNEL / "tests" / "gen_checks_vectors.py"


def _require_kernel() -> None:
    if not KERNEL.is_dir():
        pytest.skip("rust/fleet-kernel not present in this checkout")


def _load_vectors() -> list[dict]:
    _require_kernel()
    if not VECTORS.is_file():
        pytest.skip("checks_vectors.json not present")
    return json.loads(VECTORS.read_text(encoding="utf-8"))


def _fixture_conn():
    """The same fixture the vectors were generated against."""
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        """INSERT INTO processes
           (pid, session_id, workstream, name, priority, port, gpu_mb,
            repo_dir, model, restart_policy, start_cmd, start_time, last_heartbeat)
           VALUES (999, 'sess-1', 'test', 'demo', 3, 4242, 0, NULL, NULL,
                   'ALERT_ONLY', NULL, '2026-01-01T00:00:00+00:00',
                   '2026-01-01T00:00:00+00:00')""",
    )
    conn.execute(
        "INSERT OR REPLACE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 40960)"
    )
    conn.commit()
    return conn


def _python_answer(conn, vector: dict) -> tuple[bool, str]:
    if vector["check"] == "check_port":
        d = referee.check_port(conn, vector["args"]["port"])
    else:
        d = referee.check_gpu_budget(
            conn,
            vector["args"]["gpu_mb"],
            residency=referee.GpuResidencyProbe(
                referee.GPU_TELEMETRY_MEASURED, 0, "test: idle device", ("fixture",)
            ),
        )
    return bool(d.allowed), d.reason


def _mirror_violations(vectors: list[dict]) -> list[str]:
    """Return a message per vector whose ``mirrors_python`` claim is false.

    Factored out so a positive control can prove this checker has teeth.
    """
    conn = _fixture_conn()
    problems: list[str] = []
    for vector in vectors:
        if not vector.get("mirrors_python"):
            continue
        if not vector.get("short_circuits_before_os"):
            problems.append(
                f"{vector['scenario']}: claims parity without short-circuiting "
                f"before the authority the Rust lacks — that is coincidence, "
                f"not parity"
            )
            continue
        py_allowed, py_reason = _python_answer(conn, vector)
        if py_allowed != vector["expected_allowed"] or py_reason != vector["expected_reason"]:
            problems.append(
                f"{vector['scenario']}: claims to mirror Python, but Python now "
                f"returns ({py_allowed}, {py_reason!r}) while the vector expects "
                f"({vector['expected_allowed']}, {vector['expected_reason']!r})"
            )
    conn.close()
    return problems


def test_vectors_claiming_parity_actually_mirror_python():
    """A vector may claim parity only if the live referee still agrees."""
    problems = _mirror_violations(_load_vectors())
    assert not problems, (
        "Rust kernel vectors assert a parity with fleet_watch.referee that no "
        "longer holds:\n  " + "\n  ".join(problems)
    )


def test_the_parity_checker_has_teeth():
    """POSITIVE CONTROL. A checker that never fails proves nothing.

    Feed it a vector that claims parity while stating an answer Python cannot
    produce; it must reject it.
    """
    forged = [
        {
            "scenario": "forged",
            "check": "check_port",
            "args": {"port": 4242},
            "expected_allowed": True,
            "expected_reason": "port available",
            "short_circuits_before_os": True,
            "mirrors_python": True,
        }
    ]
    assert _mirror_violations(forged), (
        "the parity checker accepted a vector that contradicts Python — it "
        "cannot detect the drift it exists to detect"
    )


def test_divergence_is_recorded_rather_than_hidden():
    """The OS-truth layer is not ported, so some vector MUST be marked diverged.

    Liveness guard: if every vector claims parity, either the layer was ported
    (update this test deliberately) or the generator stopped consulting the real
    referee — which is exactly how the drift went unnoticed the first time.
    """
    vectors = _load_vectors()
    diverged = [v for v in vectors if not v.get("mirrors_python")]
    assert diverged, (
        "no vector records a divergence, but the Rust kernel implements neither "
        "the OS socket-table authority nor GPU telemetry"
    )
    for vector in diverged:
        assert vector.get("divergence"), (
            f"{vector['scenario']} is marked diverged but carries no explanation"
        )


def test_vectors_record_the_real_python_answer():
    """Every vector must carry what Python actually said, so a reader can see
    the gap without re-deriving it."""
    for vector in _load_vectors():
        assert "python_allowed" in vector and "python_reason" in vector, (
            f"{vector['scenario']} does not record the real referee's answer"
        )


def test_checks_rs_does_not_claim_to_be_a_faithful_port():
    """The false claim itself was a defect of the same class as the fail-open."""
    _require_kernel()
    source = CHECKS_RS.read_text(encoding="utf-8")
    lowered = source.lower()
    assert "faithful port of" not in lowered, (
        "src/checks.rs still advertises itself as a faithful port of a Python "
        "function it no longer matches"
    )
    assert "diverge" in lowered, (
        "src/checks.rs must state its divergence where the next reader meets it"
    )


def test_generator_observes_the_real_referee():
    """A generator that re-implements referee inline cannot detect drift.

    The original one did exactly that, which is why the stale vectors survived
    the port fix. Require it to IMPORT the thing it claims to mirror.
    """
    _require_kernel()
    source = GENERATOR.read_text(encoding="utf-8")
    assert "from fleet_watch import" in source, (
        "gen_checks_vectors.py must import the real referee, not re-implement it"
    )
    assert "def check_port(" not in source, (
        "gen_checks_vectors.py re-implements check_port inline again — that copy "
        "is what made the drift invisible"
    )
    assert "def check_gpu_budget(" not in source, (
        "gen_checks_vectors.py re-implements check_gpu_budget inline again"
    )


def test_referee_still_denies_an_unregistered_live_listener():
    """The premise behind every port divergence record, re-proven here.

    Uses a port we bind ourselves rather than a hardcoded one, so this asserts a
    property of the code and never of whatever the host happens to be running.
    """
    conn = _fixture_conn()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        port = int(sock.getsockname()[1])
        decision = referee.check_port(conn, port)
    conn.close()
    assert decision.allowed is False, (
        "check_port allowed a port held by a live unregistered listener — the "
        "OS-truth layer regressed, and every divergence record above is now wrong"
    )
