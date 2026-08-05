#!/usr/bin/env python3
"""PS-E shadow-parity: diff the Rust fleet-kernel against the Python
``fleet_watch.referee`` on the LIVE registry's real data.

READ THIS BEFORE TRUSTING A PASS. The Rust kernel does NOT implement the
OS-truth layer (socket-table port checks, GPU telemetry, the repo writer probe).
Divergence on those paths is EXPECTED and this script now says so explicitly
instead of reporting a bare percentage.

WHY THE OLD VERSION COULD PASS WHILE THE TWO IMPLEMENTATIONS DIVERGED. Its port
probes were the registry's own ports plus three arbitrary numbers. Registered
ports deny on both sides (the registry short-circuits first); arbitrary unused
ports allow on both sides. So the probe set never contained the one case that
distinguishes them — a port held by a LIVE but UNREGISTERED listener — and a
100% score meant only that the divergence had never been exercised. A parity
score that cannot go down is not a measurement. This version binds such a port
itself and asserts the disagreement appears.

Exit codes:
  0  every disagreement was an EXPECTED divergence and every expected
     divergence was actually observed
  1  an UNEXPECTED disagreement (a real regression), or an expected divergence
     that failed to appear (the probe set stopped exercising it)
  2  the Rust binary is missing

SAFETY: this never writes the live registry. ``check_port`` / ``check_gpu_budget``
read a read-only snapshot; ``check_repo`` (which GCs stale leases) runs each side
against its OWN throwaway snapshot copy. The live DB is only ever *copied from*.

Both sides use a single FIXED timestamp (Python's ``_now_iso`` is monkeypatched,
the Rust bin is passed the same value) so stale-lease aging is compared fairly.
"""
from __future__ import annotations

import contextlib
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile

REPO = str(__import__("pathlib").Path(__file__).resolve().parents[3])
sys.path.insert(0, REPO)
import fleet_watch.events as ev  # noqa: E402
import fleet_watch.referee as ref  # noqa: E402

LIVE = os.path.expanduser("~/.fleet-watch/registry.db")
BIN = f"{REPO}/rust/fleet-kernel/target/debug/kernel_shadow"
FIXED_TS = "2026-06-13T18:00:00+00:00"
ev._now_iso = lambda: FIXED_TS  # fair age comparison

# Paths where the Rust kernel is KNOWN not to implement Python's authority.
# A disagreement tagged with one of these is expected; anything else is a
# regression. Each entry must be OBSERVED at least once per run, or the probe
# set has stopped exercising it and the score is meaningless.
EXPECTED_DIVERGENCES = {
    "port_os_socket_table": (
        "referee.check_port consults the OS socket table; the Rust kernel "
        "answers from the registry alone, so a live UNREGISTERED listener "
        "denies in Python and allows in Rust"
    ),
    "gpu_live_telemetry": (
        "referee.check_gpu_budget combines the ledger with live VRAM residency "
        "and reports that provenance in its reason; the Rust kernel performs "
        "ledger arithmetic only"
    ),
    "repo_writer_probe": (
        "referee.check_repo_with_session attributes git write locks to live "
        "PIDs; the Rust kernel has no equivalent"
    ),
}


@contextlib.contextmanager
def _held_port():
    """Hold a real listener on an OS-assigned port — the case that separates
    the two implementations. Ephemeral so it never collides with a real
    service."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        yield int(sock.getsockname()[1])
    finally:
        sock.close()


def snapshot() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    shutil.copy2(LIVE, path)
    return path


def rust(db: str, *cmd) -> tuple[bool | None, str | None]:
    out = subprocess.run(
        [BIN, db, *map(str, cmd)], capture_output=True, text=True
    )
    if out.returncode != 0:
        return (None, f"<rust error: {out.stderr.strip()}>")
    d = json.loads(out.stdout)
    return (d["allowed"], d["reason"])


def pyd(d) -> tuple[bool, str]:
    return (bool(d.allowed), d.reason)


def main() -> int:
    if not os.path.exists(BIN):
        print(json.dumps({"error": f"rust bin missing: {BIN} (cargo build --bin kernel_shadow)"}))
        return 2

    results = []

    # --- read-only checks: one shared read-only snapshot ---
    snap = snapshot()
    conn = sqlite3.connect(f"file:{snap}?mode=ro", uri=True)
    held = [r[0] for r in conn.execute(
        "SELECT port FROM processes WHERE port IS NOT NULL")]
    for p in held + [9999, 8888, 65000]:
        py = pyd(ref.check_port(conn, p))
        rs = rust(snap, "check_port", p)
        results.append({"q": f"check_port({p})", "match": py == rs, "py": py, "rust": rs})

    # The probe the old version lacked: a port with a LIVE, UNREGISTERED
    # listener. This is the only port case where the two implementations must
    # disagree, so its absence made a 100% score unfalsifiable.
    with _held_port() as p:
        py = pyd(ref.check_port(conn, p))
        rs = rust(snap, "check_port", p)
        results.append({
            "q": f"check_port({p}) [live unregistered listener]",
            "match": py == rs,
            "py": py,
            "rust": rs,
            "expected_divergence": "port_os_socket_table",
        })

    for mb in [0, 1024, 65536, 200000]:
        py = pyd(ref.check_gpu_budget(conn, mb))
        rs = rust(snap, "check_gpu_budget", mb)
        results.append({
            "q": f"check_gpu_budget({mb})",
            "match": py == rs,
            "py": py,
            "rust": rs,
            # gpu_mb <= 0 returns before telemetry on both sides; every
            # positive claim reaches Python's telemetry layer and cannot match.
            **({} if mb <= 0 else {"expected_divergence": "gpu_live_telemetry"}),
        })
    # collect a couple of real repos from active leases for the check_repo probes
    repos = [r[0] for r in conn.execute(
        "SELECT DISTINCT repo_dir FROM session_leases "
        "WHERE status='ACTIVE' AND repo_dir IS NOT NULL AND repo_dir != '' LIMIT 3")]
    conn.close()
    os.remove(snap)

    # --- mutating check_repo: each side gets its OWN snapshot ---
    for repo in repos + ["/tmp/fk_nonexistent_repo"]:
        py_db, rs_db = snapshot(), snapshot()
        pc = sqlite3.connect(py_db)
        py = pyd(ref.check_repo_with_session(pc, repo, current_session_id=None))
        pc.commit()
        pc.close()
        rs = rust(rs_db, "check_repo", FIXED_TS, repo)
        results.append({"q": f"check_repo({repo})", "match": py == rs, "py": py, "rust": rs})
        os.remove(py_db)
        os.remove(rs_db)

    # The repo case that separates the two implementations: a live git writer in
    # an UNREGISTERED repo. Built in a throwaway directory — never a real repo,
    # because exercising this means creating a lock inside .git.
    with tempfile.TemporaryDirectory(prefix="fk_parity_repo_") as scratch:
        scratch_repo = os.path.join(scratch, "repo")
        os.makedirs(os.path.join(scratch_repo, ".git"))
        lock_path = os.path.join(scratch_repo, ".git", "index.lock")
        with open(lock_path, "w") as lock_handle:
            lock_handle.write("held by shadow_parity")
            lock_handle.flush()
            # The descriptor stays open for the duration of both probes, which
            # is what makes this a live writer rather than stale debris.
            py_db, rs_db = snapshot(), snapshot()
            pc = sqlite3.connect(py_db)
            py = pyd(
                ref.check_repo_with_session(pc, scratch_repo, current_session_id=None)
            )
            pc.commit()
            pc.close()
            rs = rust(rs_db, "check_repo", FIXED_TS, scratch_repo)
            results.append({
                "q": "check_repo(<scratch repo with a live git writer>)",
                "match": py == rs,
                "py": py,
                "rust": rs,
                "expected_divergence": "repo_writer_probe",
            })
            os.remove(py_db)
            os.remove(rs_db)

    total = len(results)
    matched = sum(1 for r in results if r["match"])

    # Classify every disagreement. Only a disagreement on a path we KNOW the
    # Rust kernel does not implement is acceptable; anything else is a
    # regression. Reporting one bare percentage conflated the two.
    regressions = [
        r for r in results if not r["match"] and not r.get("expected_divergence")
    ]
    expected_seen = {
        r["expected_divergence"] for r in results if not r["match"] and r.get("expected_divergence")
    }
    # A divergence we declared but did not observe means the probe set stopped
    # exercising it — the exact way the old script scored 100% while diverging.
    # This deliberately includes tags with NO probe at all: a divergence
    # declared and never tested is the same unverified claim in a new place.
    unexercised = sorted(tag for tag in EXPECTED_DIVERGENCES if tag not in expected_seen)
    # A probe tagged as an expected divergence that MATCHED is equally
    # suspicious: either the layer was ported, or the probe stopped reaching it.
    silently_agreeing = [
        r["q"] for r in results if r["match"] and r.get("expected_divergence")
    ]

    ok = not regressions and not unexercised and not silently_agreeing
    report = {
        "schema": "fleet_kernel_shadow_parity/v2",
        "fixed_ts": FIXED_TS,
        "verdict": "OK_WITH_DECLARED_DIVERGENCE" if ok else "FAIL",
        "kernel_is_behind_python_by": sorted(EXPECTED_DIVERGENCES),
        "note": (
            "The Rust kernel deliberately omits the OS-truth layer. A matching "
            "score here NEVER means the kernel agrees with the guard the "
            "operator runs; it means no UNEXPECTED disagreement was found."
        ),
        "total": total,
        "matched": matched,
        "regressions": regressions,
        "expected_divergences_observed": sorted(expected_seen),
        "expected_divergences_not_exercised": unexercised,
        "expected_divergences_that_unexpectedly_agreed": silently_agreeing,
        "probes": results,
    }
    print(json.dumps(report, indent=2, default=str))

    if not ok:
        banner = ["", "=" * 72, "SHADOW PARITY FAILED", "=" * 72]
        for r in regressions:
            banner.append(f"  REGRESSION  {r['q']}")
            banner.append(f"              python={r['py']}")
            banner.append(f"              rust  ={r['rust']}")
        for tag in unexercised:
            banner.append(
                f"  NOT EXERCISED  {tag}: {EXPECTED_DIVERGENCES[tag]}"
            )
        for q in silently_agreeing:
            banner.append(
                f"  UNEXPECTED AGREEMENT  {q} was expected to diverge but matched"
            )
        banner.append("=" * 72)
        print("\n".join(banner), file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
