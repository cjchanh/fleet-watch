"""Tests for event logging with hash chain."""

import sqlite3

from fleet_watch import events, registry


def _fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 0)"
    )
    conn.commit()
    return conn


def test_log_event_basic():
    conn = _fresh_conn()
    eid = events.log_event(conn, "REGISTER", pid=1234, workstream="test",
                           detail={"name": "test-proc"})
    assert eid == 1


def test_event_hash_chain_genesis():
    conn = _fresh_conn()
    events.log_event(conn, "REGISTER", pid=1)
    row = conn.execute("SELECT prev_hash FROM events WHERE id = 1").fetchone()
    assert row[0] == events.GENESIS_HASH


def test_event_hash_chain_linked():
    conn = _fresh_conn()
    events.log_event(conn, "REGISTER", pid=1)
    events.log_event(conn, "HEARTBEAT", pid=1)
    rows = conn.execute("SELECT hash, prev_hash FROM events ORDER BY id").fetchall()
    assert rows[1][1] == rows[0][0]  # second event's prev_hash == first event's hash


def test_verify_chain_empty():
    conn = _fresh_conn()
    valid, count = events.verify_chain(conn)
    assert valid is True
    assert count == 0


def test_verify_chain_valid():
    conn = _fresh_conn()
    for i in range(5):
        events.log_event(conn, "REGISTER", pid=i)
    valid, count = events.verify_chain(conn)
    assert valid is True
    assert count == 5


def test_verify_chain_tampered():
    conn = _fresh_conn()
    events.log_event(conn, "REGISTER", pid=1)
    events.log_event(conn, "REGISTER", pid=2)
    # Tamper with the hash
    conn.execute("UPDATE events SET hash = 'tampered' WHERE id = 1")
    conn.commit()
    valid, count = events.verify_chain(conn)
    assert valid is False


def test_get_events_filtered():
    conn = _fresh_conn()
    events.log_event(conn, "REGISTER", pid=1)
    events.log_event(conn, "CONFLICT", pid=2)
    events.log_event(conn, "REGISTER", pid=3)
    result = events.get_events(conn, hours=1, event_type="CONFLICT")
    assert len(result) == 1
    assert result[0]["event_type"] == "CONFLICT"


def test_invalid_event_type():
    conn = _fresh_conn()
    try:
        events.log_event(conn, "INVALID_TYPE")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass
