from __future__ import annotations

import os
import sys

from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import registry


def test_register_disposable_requires_explicit_identity_fields(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")
    result = CliRunner().invoke(
        cli_module.cli,
        [
            "register",
            "--pid",
            str(os.getpid()),
            "--name",
            "campaign-child",
            "--workstream",
            "test",
            "--disposable",
            "--owner-id",
            "campaign-test",
            "--executable",
            sys.executable,
        ],
    )
    assert result.exit_code == 0, result.output
    conn = registry.connect()
    row = registry.get_disposable_registration(conn, os.getpid())
    conn.close()
    assert row["owner_id"] == "campaign-test"
    assert row["service_class"] == "disposable"
