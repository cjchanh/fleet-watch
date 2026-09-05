"""B608 guard: heartbeat_external_resource column whitelist."""

from pathlib import Path

import pytest

from fleet_watch import registry


def _tmp_conn(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    monkeypatch.setattr(registry, "DB_PATH", tmp_path / "registry.db")
    return registry.connect(tmp_path / "registry.db")


def test_heartbeat_external_resource_updates_whitelisted_columns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The normal update path still works end-to-end on a tmp_path db."""
    conn = _tmp_conn(tmp_path, monkeypatch)
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-1",
        workstream="paper",
        name="Thunder abc123",
        status="RUNNING",
        metadata={"id": "0"},
    )

    assert (
        registry.heartbeat_external_resource(
            conn,
            provider="thunder",
            external_id="abc123",
            status="STOPPED",
            metadata={"id": "0", "reason": "test"},
        )
        is True
    )

    resource = registry.get_external_resource(conn, provider="thunder", external_id="abc123")
    assert resource is not None
    assert resource["status"] == "STOPPED"
    assert resource["metadata"]["reason"] == "test"
    assert resource["last_seen"] is not None


def test_update_whitelist_rejects_unknown_column():
    """The validator refuses any column outside the whitelist before SQL is built."""
    with pytest.raises(ValueError):
        registry._validate_external_resource_update_columns(["owner_pid"])
