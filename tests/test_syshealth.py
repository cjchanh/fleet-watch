"""Tests for system health monitoring."""

from fleet_watch import syshealth


def test_memory_state_returns_valid_data():
    """get_memory_state returns a populated MemoryState on macOS."""
    mem = syshealth.get_memory_state()
    assert mem.total_mb > 0
    assert mem.pressure_pct >= 0
    assert mem.pressure_pct <= 100
    assert mem.available_mb >= 0


def test_memory_state_dict():
    mem = syshealth.get_memory_state()
    d = mem.to_dict()
    assert "total_mb" in d
    assert "pressure_pct" in d
    assert "available_mb" in d
    assert d["available_mb"] == mem.free_mb + mem.inactive_mb


def test_session_processes_returns_list():
    """get_session_processes returns a list (may be empty in CI)."""
    sessions = syshealth.get_session_processes()
    assert isinstance(sessions, list)
    # If running inside Claude Code, should find at least this session
    for s in sessions:
        assert s.kind in ("claude-code", "codex")
        assert s.rss_mb >= 0
        assert s.pid > 0


def test_idle_processes_returns_list():
    idle = syshealth.get_idle_processes()
    assert isinstance(idle, list)
    for p in idle:
        assert "pid" in p
        assert "rss_mb" in p
        assert p["cpu_pct"] <= 1.0


def test_pressure_calculation():
    mem = syshealth.MemoryState(
        total_mb=131072,
        active_mb=40000,
        inactive_mb=20000,
        free_mb=30000,
        compressed_mb=10000,
        wired_mb=15000,
    )
    # pressure = (active + wired + compressed) / total = 65000/131072 = 49%
    assert mem.pressure_pct == 49
    assert mem.available_mb == 50000


def test_pressure_critical():
    mem = syshealth.MemoryState(
        total_mb=131072,
        active_mb=60000,
        inactive_mb=5000,
        free_mb=1000,
        compressed_mb=30000,
        wired_mb=25000,
    )
    # pressure = 115000/131072 = 87%
    assert mem.pressure_pct > 85
