#!/usr/bin/env python3
"""Manual FleetWatch explanation sidecar.

This script is intentionally outside the launchd/FleetWatch zero-egress core.
It accepts one incident JSON object, redacts it, optionally calls a local
runtime (default: Ollama), validates the response, and prints a sanitized JSON
result.  It has no signal, shell, or executable-tool authority.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Make the repository package importable when this standalone script is invoked
# directly from a checkout (``python scripts/explain_incident.py``).
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fleet_watch.explanation import explain_incident


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="incident JSON file; default stdin")
    parser.add_argument("--runtime", default="ollama", help="local runtime executable")
    parser.add_argument("--model", default="local-model", help="local model name")
    parser.add_argument("--timeout", type=float, default=15.0)
    args = parser.parse_args()
    try:
        raw = args.input.read_text() if args.input else sys.stdin.read()
        incident = json.loads(raw)
        if not isinstance(incident, dict):
            raise ValueError("incident must be a JSON object")
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(json.dumps({"status": "UNKNOWN", "reason": f"input_{type(exc).__name__}"}))
        return 2
    result = explain_incident(
        incident,
        runtime=(args.runtime, "run"),
        model=args.model,
        timeout_seconds=args.timeout,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("status") == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
