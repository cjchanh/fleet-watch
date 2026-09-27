"""Event-query index migration preserves existing readers and stored evidence."""

import sqlite3

from fleet_watch import events, registry


INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_events_type_id "
    "ON events(event_type, id DESC);"
)
QUERY = (
    "SELECT id, timestamp, event_type, pid, workstream, detail, hash FROM events "
    "WHERE timestamp >= datetime(?, '-24 hours') AND event_type = ? "
    "ORDER BY id DESC LIMIT ?"
)


def legacy_database(tmp_path):
    path = tmp_path / "registry.db"
    conn = sqlite3.connect(path)
    conn.executescript(registry.SCHEMA.replace(INDEX_SQL, ""))
    for kind in ("REGISTER", "CONFLICT", "HEARTBEAT", "CONFLICT"):
        events.log_event(conn, kind, pid=123, workstream="fixture")
    conn.execute(
        "INSERT INTO session_leases (session_id, started_at, last_heartbeat_at) "
        "VALUES ('fixture', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')"
    )
    conn.commit()
    return path, conn


def migrate(path, tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "FLEET_DIR", tmp_path)
    return registry.connect(path)


def test_new_database_has_event_type_then_descending_id_index(tmp_path, monkeypatch):
    with migrate(tmp_path / "new.db", tmp_path, monkeypatch) as conn:
        columns = [r for r in conn.execute("PRAGMA index_xinfo(idx_events_type_id)") if r[5]]
        assert [(r[2], r[3]) for r in columns] == [("event_type", 0), ("id", 1)]


def test_existing_database_migrates_without_changing_events_or_leases(tmp_path, monkeypatch):
    path, conn = legacy_database(tmp_path)
    before_events = conn.execute("SELECT * FROM events ORDER BY id").fetchall()
    before_leases = conn.execute("SELECT * FROM session_leases").fetchall()
    conn.close()
    for _ in range(2):
        with migrate(path, tmp_path, monkeypatch) as reopened:
            assert reopened.execute("SELECT * FROM events ORDER BY id").fetchall() == before_events
            assert reopened.execute("SELECT * FROM session_leases").fetchall() == before_leases
            assert events.verify_chain(reopened) == (True, 4)
            assert len(reopened.execute("PRAGMA index_list(events)").fetchall()) == 1


def test_existing_get_events_results_are_identical_after_migration(tmp_path, monkeypatch):
    path, conn = legacy_database(tmp_path)
    cases = [(h, k, n) for h in (0, 24) for k in (None, "CONFLICT", "REGISTER", "ABSENT") for n in (1, 100)]
    expected = [events.get_events(conn, hours=h, event_type=k, limit=n) for h, k, n in cases]
    conn.close()
    with migrate(path, tmp_path, monkeypatch) as reopened:
        actual = [events.get_events(reopened, hours=h, event_type=k, limit=n) for h, k, n in cases]
        assert actual == expected
        events.log_event(reopened, "HEARTBEAT", pid=456)
        assert events.verify_chain(reopened) == (True, 5)


def test_conflict_query_changes_from_scan_to_index_search(tmp_path, monkeypatch):
    path, conn = legacy_database(tmp_path)
    args = ("2000-01-01T00:00:00+00:00", "CONFLICT", 100)
    before = conn.execute("EXPLAIN QUERY PLAN " + QUERY, args).fetchall()
    assert any("SCAN events" in r[3] for r in before)
    conn.close()
    with migrate(path, tmp_path, monkeypatch) as reopened:
        after = reopened.execute("EXPLAIN QUERY PLAN " + QUERY, args).fetchall()
        assert any("SEARCH events USING INDEX idx_events_type_id" in r[3] for r in after)
        assert len(reopened.execute(QUERY, args).fetchall()) == 2
