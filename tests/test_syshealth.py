"""Tests for system health monitoring."""

from fleet_watch import syshealth


def test_memory_state_returns_valid_data():
    """get_memory_state returns a populated MemoryState on macOS."""
    mem = syshealth.get_memory_state()
    assert mem.total_mb > 0
    assert 0 <= mem.pressure_pct <= 100
    assert mem.available_mb >= 0


def test_memory_state_dict():
    mem = syshealth.get_memory_state()
    d = mem.to_dict()
    assert "total_mb" in d
    assert "pressure_pct" in d
    assert "available_mb" in d
    assert d["available_mb"] == mem.free_mb + mem.inactive_mb


def test_memory_state_zero_on_failure(monkeypatch):
    """Returns zeroed state when sysctl fails."""
    monkeypatch.setattr(
        syshealth.subprocess, "run",
        lambda *a, **kw: (_ for _ in ()).throw(FileNotFoundError),
    )
    mem = syshealth.get_memory_state()
    assert mem.total_mb == 0


def test_session_processes_returns_list():
    sessions = syshealth.get_session_processes()
    assert isinstance(sessions, list)
    for s in sessions:
        assert s.kind in ("claude-code", "codex")
        assert s.rss_mb >= 0
        assert s.pid > 0


def test_session_processes_custom_patterns():
    """Custom session patterns can match arbitrary processes."""
    sessions = syshealth.get_session_processes(patterns=[
        {"name": "Python", "kind": "python", "process_match": r"python3?"},
    ])
    assert isinstance(sessions, list)
    # At least our own test runner should match
    for s in sessions:
        assert s.kind == "python"


def test_session_processes_empty_patterns():
    """Empty pattern list discovers nothing."""
    sessions = syshealth.get_session_processes(patterns=[])
    # No patterns compiled → no matches possible
    assert isinstance(sessions, list)
    assert len(sessions) == 0


def test_session_processes_bad_regex():
    """Bad regex in pattern is skipped, not a crash."""
    sessions = syshealth.get_session_processes(patterns=[
        {"name": "Bad", "kind": "bad", "process_match": r"[invalid"},
    ])
    assert sessions == []


def test_idle_processes_returns_list():
    idle = syshealth.get_idle_processes()
    assert isinstance(idle, list)
    for p in idle:
        assert "pid" in p
        assert "rss_mb" in p
        assert p["cpu_pct"] <= syshealth.DEFAULT_IDLE_CPU_THRESHOLD


def test_idle_processes_custom_patterns():
    """Custom idle patterns are respected."""
    idle = syshealth.get_idle_processes(
        patterns=[r"this_will_never_match_anything_12345"],
    )
    assert idle == []


def test_idle_processes_custom_threshold():
    idle_strict = syshealth.get_idle_processes(threshold_cpu=0.0)
    idle_loose = syshealth.get_idle_processes(threshold_cpu=100.0)
    # Loose threshold should find at least as many as strict
    assert len(idle_loose) >= len(idle_strict)


def test_pressure_calculation():
    mem = syshealth.MemoryState(
        total_mb=131072,
        active_mb=40000,
        inactive_mb=20000,
        free_mb=30000,
        compressed_mb=10000,
        wired_mb=15000,
    )
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
    assert mem.pressure_pct > 85


def test_pressure_label():
    assert syshealth.pressure_label(50) == "OK"
    assert syshealth.pressure_label(75) == "ELEVATED"
    assert syshealth.pressure_label(90) == "CRITICAL"


def test_pressure_label_custom_thresholds():
    custom = {"elevated": 40, "critical": 60}
    assert syshealth.pressure_label(35, custom) == "OK"
    assert syshealth.pressure_label(50, custom) == "ELEVATED"
    assert syshealth.pressure_label(65, custom) == "CRITICAL"


def test_load_health_config_defaults():
    hc = syshealth.load_health_config(None)
    assert hc["session_patterns"] == syshealth.DEFAULT_SESSION_PATTERNS
    assert hc["idle_patterns"] == syshealth.DEFAULT_IDLE_PATTERNS
    assert hc["idle_cpu_threshold"] == syshealth.DEFAULT_IDLE_CPU_THRESHOLD


def test_load_health_config_overrides():
    config = {
        "session_patterns": [{"name": "X", "kind": "x", "process_match": "x"}],
        "idle_cpu_threshold": 5.0,
    }
    hc = syshealth.load_health_config(config)
    assert hc["session_patterns"] == config["session_patterns"]
    assert hc["idle_cpu_threshold"] == 5.0
    # idle_patterns falls back to default since not provided
    assert hc["idle_patterns"] == syshealth.DEFAULT_IDLE_PATTERNS
