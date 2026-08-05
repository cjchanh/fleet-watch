#!/usr/bin/env python3
"""Generate checks_vectors.json for the Rust parity test — and record, per
scenario, whether that vector still mirrors the real Python referee.

WHY THIS FILE WAS REWRITTEN. It used to re-implement `check_port` and
`check_gpu_budget` INLINE, with docstrings reading "Mirrors referee.check_port
exactly". It imported nothing from `fleet_watch`, so the copy could not drift
*detectably*: it was self-consistent with the Rust kernel and blind to the
Python. When `referee.check_port` grew an OS-socket-table authority, the
generator kept emitting `{"scenario": "port_free", "expected_allowed": true}`
and would have gone on emitting it forever. A generator that cannot observe the
thing it claims to mirror is not a generator, it is a second copy of the bug.

WHAT IT DOES NOW. It imports the REAL `fleet_watch.referee`, asks it, and
compares against the registry-only answer the Rust kernel produces. Vectors
still encode what the RUST must return (so `tests/checks.rs` keeps testing the
Rust), but each one now carries:

    mirrors_python  — does the real referee agree with this expectation?
    python_allowed  — what the real referee actually returned
    divergence      — why not, when it does not

Rust's serde ignores unknown fields, so the added keys do not break
`tests/checks.rs`.

The divergence is DELIBERATE: the OS-truth layer was not ported to Rust. This
file exists so that choice stays visible instead of decaying into a false parity
claim. `tests/test_rust_kernel_divergence.py` fails loudly if these records stop
matching reality.
"""
from __future__ import annotations

import json
import socket
import sqlite3
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from fleet_watch import referee, registry  # noqa: E402

# Fixture values — these MUST match the constants in tests/checks.rs.
FIXTURE_TOTAL_MB = 131072      # 128 GiB
FIXTURE_RESERVE_MB = 16384     # 16 GiB
FIXTURE_ALLOCATED_MB = 40960   # 40 GiB already allocated
FIXTURE_AVAILABLE_MB = FIXTURE_TOTAL_MB - FIXTURE_RESERVE_MB - FIXTURE_ALLOCATED_MB

FIXTURE_PID = 999
FIXTURE_NAME = "demo"
FIXTURE_PORT = 4242
FREE_PORT_SCENARIO_PORT = 5000


def setup_db(db_path: str) -> sqlite3.Connection:
    """Build the fixture DB from the REAL schema, not a hand-copied fragment."""
    conn = sqlite3.connect(db_path, timeout=10)
    conn.executescript(registry.SCHEMA)
    conn.execute(
        """INSERT INTO processes
           (pid, session_id, workstream, name, priority, port, gpu_mb,
            repo_dir, model, restart_policy, start_cmd, start_time, last_heartbeat)
           VALUES (?, 'sess-1', 'test', ?, 3, ?, 0, NULL, NULL, 'ALERT_ONLY',
                   NULL, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')""",
        (FIXTURE_PID, FIXTURE_NAME, FIXTURE_PORT),
    )
    conn.execute(
        "INSERT OR REPLACE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, ?, ?, ?)",
        (FIXTURE_TOTAL_MB, FIXTURE_RESERVE_MB, FIXTURE_ALLOCATED_MB),
    )
    conn.commit()
    return conn


# ── what the RUST kernel produces (src/checks.rs), stated explicitly ─────────
# Not derived from Python — that is the whole point. If someone changes the Rust,
# these must be updated by hand, which is a decision rather than a silent
# regeneration.
#
# ``short_circuits_before_os`` is the load-bearing field. A vector may only claim
# to mirror Python when the Python code path RETURNS BEFORE reaching the
# authority the Rust lacks — a registered holder (check_port) or a non-positive
# claim (check_gpu_budget). Otherwise the two agree only by coincidence of host
# state, and a coincidence must never be recorded as parity.
#
# This distinction is not academic. Generated on 2026-08-04, the `port_free`
# vector matched Python exactly — because nothing happened to be listening on
# port 5000 that minute. Bind 5000 and the same vector "diverges". Marking that
# as parity would re-create the original defect one level up: a green signal
# whose truth depends on something nobody is watching.
RUST_EXPECTATIONS: list[dict] = [
    {
        "scenario": "port_claimed",
        "check": "check_port",
        "args": {"port": FIXTURE_PORT},
        "expected_allowed": False,
        "expected_reason": f"port {FIXTURE_PORT} claimed by PID {FIXTURE_PID} ({FIXTURE_NAME})",
        # A registered holder returns before probe_port is ever called.
        "short_circuits_before_os": True,
    },
    {
        "scenario": "port_free",
        "check": "check_port",
        "args": {"port": FREE_PORT_SCENARIO_PORT},
        "expected_allowed": True,
        "expected_reason": "port available",
        # No registry holder => Python goes on to bind the port. The Rust cannot.
        "short_circuits_before_os": False,
    },
    {
        "scenario": "gpu_zero",
        "check": "check_gpu_budget",
        "args": {"gpu_mb": 0},
        "expected_allowed": True,
        "expected_reason": "no GPU claim",
        # gpu_mb <= 0 returns before any telemetry probe.
        "short_circuits_before_os": True,
    },
    {
        "scenario": "gpu_fits",
        "check": "check_gpu_budget",
        "args": {"gpu_mb": 8192},
        "expected_allowed": True,
        "expected_reason": f"8192MB fits in {FIXTURE_AVAILABLE_MB}MB available",
        "short_circuits_before_os": False,
    },
    {
        "scenario": "gpu_exceeds",
        "check": "check_gpu_budget",
        "args": {"gpu_mb": 100000},
        "expected_allowed": False,
        "expected_reason": (
            f"GPU budget exceeded: requesting 100000MB but only "
            f"{FIXTURE_AVAILABLE_MB}MB available "
            f"({FIXTURE_ALLOCATED_MB}MB allocated of "
            f"{FIXTURE_TOTAL_MB - FIXTURE_RESERVE_MB}MB allocatable)"
        ),
        "short_circuits_before_os": False,
    },
]


def python_answer(conn: sqlite3.Connection, vector: dict) -> tuple[bool, str]:
    """Ask the REAL referee the same question the Rust vector encodes.

    ``check_gpu_budget`` is given an explicit zero-residency probe so the
    comparison isolates the LOGIC difference rather than reporting whatever this
    host's GPU happens to be doing. Any remaining disagreement is structural.
    """
    if vector["check"] == "check_port":
        decision = referee.check_port(conn, vector["args"]["port"])
    elif vector["check"] == "check_gpu_budget":
        decision = referee.check_gpu_budget(
            conn,
            vector["args"]["gpu_mb"],
            residency=referee.GpuResidencyProbe(
                referee.GPU_TELEMETRY_MEASURED, 0, "generator: idle device", ("fixture",)
            ),
        )
    else:  # pragma: no cover - guarded by the vector table above
        raise ValueError(f"unknown check {vector['check']!r}")
    return bool(decision.allowed), decision.reason


def prove_port_os_authority(conn: sqlite3.Connection) -> str:
    """Demonstrate the OS layer exists, without depending on host port state.

    Binds an ephemeral port WE control, registers nothing, and asks the referee.
    A DENY proves check_port consults an authority beyond the registry — the
    evidence behind every ``mirrors_python: false`` on a port scenario. Probing
    a hardcoded port instead would make this claim depend on whatever the
    machine happens to be running.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        port = int(sock.getsockname()[1])
        decision = referee.check_port(conn, port)
        if decision.allowed:
            raise SystemExit(
                "FATAL: referee.check_port allowed a port held by a live "
                "unregistered listener. Either the OS-truth layer regressed, or "
                "this generator's premise is wrong. Refusing to emit vectors."
            )
        return (
            f"referee.check_port denied an unregistered live listener on an "
            f"ephemeral port: {decision.reason!r}"
        )


def main() -> None:
    out_path = Path(__file__).parent / "checks_vectors.json"

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tf:
        conn = setup_db(tf.name)
        os_authority_evidence = prove_port_os_authority(conn)

        vectors = []
        for expectation in RUST_EXPECTATIONS:
            vector = dict(expectation)
            py_allowed, py_reason = python_answer(conn, expectation)
            same_answer_here = (
                py_allowed == expectation["expected_allowed"]
                and py_reason == expectation["expected_reason"]
            )
            # Parity requires BOTH: the answers match, and the Python path
            # provably never reached the authority the Rust lacks. Matching
            # alone is a coincidence of host state.
            mirrors = same_answer_here and expectation["short_circuits_before_os"]
            vector["mirrors_python"] = mirrors
            vector["agrees_on_this_host"] = same_answer_here
            vector["python_allowed"] = py_allowed
            vector["python_reason"] = py_reason
            if not mirrors:
                if same_answer_here:
                    vector["divergence"] = (
                        "COINCIDENTAL AGREEMENT ONLY. The answers happen to match "
                        "on this host, but the Python path reaches an authority "
                        "the Rust kernel does not implement, so the match is a "
                        "property of current host state and not of the code. "
                        "Evidence: " + os_authority_evidence
                    )
                else:
                    vector["divergence"] = (
                        "The Rust kernel answers from the registry alone; the "
                        "Python referee now consults an additional authority (OS "
                        "socket table for ports, live VRAM residency for GPU). "
                        "The OS-truth layer was deliberately NOT ported. "
                        "Evidence: " + os_authority_evidence
                    )
            vectors.append(vector)

        conn.close()

    out_path.write_text(json.dumps(vectors, indent=2) + "\n")
    diverged = [v["scenario"] for v in vectors if not v["mirrors_python"]]
    print(f"Wrote {len(vectors)} vectors to {out_path}")
    for v in vectors:
        flag = "MIRRORS" if v["mirrors_python"] else "DIVERGED"
        note = ""
        if not v["mirrors_python"] and v["agrees_on_this_host"]:
            note = "  (agrees on this host by coincidence only)"
        print(
            f"  {v['scenario']:20s} {flag:9s} "
            f"rust_allowed={v['expected_allowed']}{note}"
        )
    if diverged:
        print(
            f"\n!! {len(diverged)} scenario(s) DIVERGE from the live Python "
            f"referee: {', '.join(diverged)}\n"
            f"   These vectors test the RUST kernel only. A green `cargo test` "
            f"does NOT mean the kernel agrees with `fleet guard`."
        )


if __name__ == "__main__":
    main()
