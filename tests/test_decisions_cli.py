from __future__ import annotations

import json
import sqlite3

from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import events, registry


def test_decisions_cli_emits_structured_receipts_and_freshness(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")
    conn = registry.connect()
    events.log_event(
        conn,
        "PROCESS_DECISION",
        pid=123,
        detail={
            "context": "test",
            "receipt": {
                "policy_version": "fleet-watch/process-policy/v1",
                "observed_identity": {"pid": 123},
                "permitted_action": "advisory",
                "outcome": "denied",
            },
        },
    )
    conn.close()
    result = CliRunner().invoke(cli_module.cli, ["decisions", "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema_version"] == "fleet-watch/decision-feed/v1"
    assert payload["freshness"]["status"] in {"fresh", "stale", "unknown"}
    assert payload["receipts"][0]["observed_identity"]["pid"] == 123
