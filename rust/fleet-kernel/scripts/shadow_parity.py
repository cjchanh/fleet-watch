#!/usr/bin/env python3
"""PS-E shadow-parity: diff the Rust fleet-kernel against the Python
``fleet_watch.referee`` on the LIVE registry's real data.

SAFETY: this never writes the live registry. ``check_port`` / ``check_gpu_budget``
read a read-only snapshot; ``check_repo`` (which GCs stale leases) runs each side
against its OWN throwaway snapshot copy. The live DB is only ever *copied from*.

Both sides use a single FIXED timestamp (Python's ``_now_iso`` is monkeypatched,
the Rust bin is passed the same value) so stale-lease aging is compared fairly.

Exit 0 iff every probe matches. Emits a JSON report to stdout.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile

REPO = "/Users/cj/Workspace/active/fleet-watch"
sys.path.insert(0, REPO)
import fleet_watch.events as ev  # noqa: E402
import fleet_watch.referee as ref  # noqa: E402

LIVE = os.path.expanduser("~/.fleet-watch/registry.db")
BIN = f"{REPO}/rust/fleet-kernel/target/debug/kernel_shadow"
FIXED_TS = "2026-06-13T18:00:00+00:00"
ev._now_iso = lambda: FIXED_TS  # fair age comparison


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
    for mb in [0, 1024, 65536, 200000]:
        py = pyd(ref.check_gpu_budget(conn, mb))
        rs = rust(snap, "check_gpu_budget", mb)
        results.append({"q": f"check_gpu_budget({mb})", "match": py == rs, "py": py, "rust": rs})
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

    total = len(results)
    matched = sum(1 for r in results if r["match"])
    report = {
        "schema": "fleet_kernel_shadow_parity/v1",
        "fixed_ts": FIXED_TS,
        "total": total,
        "matched": matched,
        "parity_pct": round(100 * matched / total, 1) if total else 0.0,
        "disagreements": [r for r in results if not r["match"]],
        "probes": results,
    }
    print(json.dumps(report, indent=2, default=str))
    return 0 if matched == total else 1


if __name__ == "__main__":
    sys.exit(main())
