#!/usr/bin/env python3
"""PS-E canary cycle — one shadow-parity run, recorded toward the
144-clean-cycle promotion gate.

Read-only: it invokes ``shadow_parity.py`` which only ever *copies* the live
registry (snapshots) — it never writes ``~/.fleet-watch/registry.db`` and never
touches the live ``fleet guard`` hot path. This is the safe canary: the Rust
kernel is exercised against the live fleet's real state on a schedule, and any
disagreement resets the clean streak.

Exit 0 on full parity, 1 on any disagreement (so launchd / a watcher can alert).
"""
from __future__ import annotations

import datetime
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SHADOW = os.path.join(HERE, "shadow_parity.py")
CANARY_DIR = os.path.expanduser("~/.governance/receipts/fleet-kernel/canary")
TALLY = os.path.join(CANARY_DIR, "tally.json")


def load_tally() -> dict:
    try:
        with open(TALLY) as fh:
            return json.load(fh)
    except Exception:
        return {"clean_streak": 0, "total_clean": 0, "total_dirty": 0, "target": 144, "runs": []}


def main() -> int:
    os.makedirs(CANARY_DIR, exist_ok=True)
    out = subprocess.run([sys.executable, SHADOW], capture_output=True, text=True)
    try:
        rep = json.loads(out.stdout)
    except Exception:
        rep = {"total": 0, "matched": 0, "parity_pct": 0.0, "error": (out.stderr or "")[:500]}

    clean = rep.get("total", 0) > 0 and rep.get("matched") == rep.get("total")
    tally = load_tally()
    if clean:
        tally["clean_streak"] = tally.get("clean_streak", 0) + 1
        tally["total_clean"] = tally.get("total_clean", 0) + 1
    else:
        tally["clean_streak"] = 0  # any disagreement resets the streak
        tally["total_dirty"] = tally.get("total_dirty", 0) + 1

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tally["runs"] = (tally.get("runs", []) + [{
        "ts": ts,
        "clean": clean,
        "parity_pct": rep.get("parity_pct"),
        "matched": rep.get("matched"),
        "total": rep.get("total"),
    }])[-200:]
    with open(TALLY, "w") as fh:
        json.dump(tally, fh, indent=2)
    with open(os.path.join(CANARY_DIR, f"run_{ts}.json"), "w") as fh:
        json.dump(rep, fh, indent=2, default=str)

    streak = tally["clean_streak"]
    target = tally.get("target", 144)
    print(
        f"canary: {'CLEAN' if clean else 'DIRTY'} "
        f"parity={rep.get('parity_pct')}% "
        f"clean_streak={streak}/{target} "
        f"({'PROMOTION-READY' if streak >= target else 'accumulating'})"
    )
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
