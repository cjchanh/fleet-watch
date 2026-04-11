"""Tests for system health monitoring."""

from fleet_watch import registry, syshealth


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


def test_memory_state_unavailable_on_failure(monkeypatch):
    """Returns unavailable state when sysctl fails."""
    monkeypatch.setattr(
        syshealth.subprocess, "run",
        lambda *a, **kw: (_ for _ in ()).throw(FileNotFoundError),
    )
    mem = syshealth.get_memory_state()
    assert mem.total_mb == 0
    assert not mem.is_available
    assert mem.pressure_pct == -1
    assert syshealth.pressure_label(mem.pressure_pct) == "UNAVAILABLE"
    d = mem.to_dict()
    assert d["available"] is False


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


def test_session_processes_collapse_detached_codex_family(monkeypatch):
    monkeypatch.setattr(
        syshealth,
        "_ps_aux_lines",
        lambda: [
            ["cj", "61041", "0.0", "0.0", "0", "23552", "??", "S", "1:57PM", "0:00.10",
             "node /opt/homebrew/bin/codex --dangerously-bypass-approvals-and-sandbox"],
            ["cj", "61042", "61.7", "0.0", "0", "60416", "??", "R", "1:57PM", "10:29.00",
             "/opt/homebrew/lib/node_modules/@openai/codex/vendor/codex/codex --dangerously-bypass-approvals-and-sandbox"],
        ],
    )

    def fake_inspect(pid):
        mapping = {
            61041: {"pid": 61041, "alive": True, "inspectable": True, "ppid": 61009, "pgid": 61009, "tty": "??"},
            61042: {"pid": 61042, "alive": True, "inspectable": True, "ppid": 61041, "pgid": 61009, "tty": "??"},
            61009: {"pid": 61009, "alive": True, "inspectable": True, "ppid": 1, "pgid": 61009, "tty": "??"},
        }
        return mapping.get(pid)

    monkeypatch.setattr(registry, "_inspect_process", fake_inspect)
    monkeypatch.setattr(registry, "_is_parent_chain_detached", lambda pid: pid == 61009)

    sessions = syshealth.get_session_processes()

    assert len(sessions) == 1
    assert sessions[0].pid == 61042
    assert sessions[0].member_count == 2
    assert sessions[0].member_pids == [61041, 61042]
    assert sessions[0].classification == "detached_hot"
    assert sessions[0].attention is True
    assert sessions[0].rss_mb == (23552 + 60416) // 1024
    assert sessions[0].cpu_pct == 61.7


def test_session_processes_split_by_ppid_despite_shared_pgid(monkeypatch):
    """Two independent launches with same PGID but different PPIDs are separate sessions."""
    monkeypatch.setattr(
        syshealth,
        "_ps_aux_lines",
        lambda: [
            # PID 100, PPID 50, PGID 50 — session A member 1
            ["cj", "100", "10.0", "0.0", "0", "20480", "??", "S", "1:00PM", "1:00.00",
             "node /opt/homebrew/bin/codex --session-a"],
            # PID 200, PPID 50, PGID 50 — session A member 2
            ["cj", "200", "15.0", "0.0", "0", "30720", "??", "R", "1:00PM", "2:00.00",
             "node /opt/homebrew/bin/codex --session-a-child"],
            # PID 300, PPID 60, PGID 50 — session B (different PPID)
            ["cj", "300", "25.0", "0.0", "0", "40960", "??", "R", "1:05PM", "3:00.00",
             "node /opt/homebrew/bin/codex --session-b"],
        ],
    )

    def fake_inspect(pid):
        mapping = {
            100: {"pid": 100, "alive": True, "inspectable": True, "ppid": 50, "pgid": 50, "tty": "??"},
            200: {"pid": 200, "alive": True, "inspectable": True, "ppid": 50, "pgid": 50, "tty": "??"},
            300: {"pid": 300, "alive": True, "inspectable": True, "ppid": 60, "pgid": 50, "tty": "??"},
            50:  {"pid": 50, "alive": True, "inspectable": True, "ppid": 1, "pgid": 50, "tty": "??"},
            60:  {"pid": 60, "alive": True, "inspectable": True, "ppid": 1, "pgid": 50, "tty": "??"},
        }
        return mapping.get(pid)

    monkeypatch.setattr(registry, "_inspect_process", fake_inspect)
    monkeypatch.setattr(registry, "_is_parent_chain_detached", lambda pid: True)

    sessions = syshealth.get_session_processes()

    assert len(sessions) == 2

    by_pid = {s.pid: s for s in sessions}
    # Session A: PIDs 100+200 collapsed (same PGID + same PPID)
    session_a = by_pid.get(200)  # 200 has higher CPU
    assert session_a is not None
    assert session_a.member_count == 2
    assert sorted(session_a.member_pids) == [100, 200]

    # Session B: PID 300 alone (different PPID despite same PGID)
    session_b = by_pid.get(300)
    assert session_b is not None
    assert session_b.member_count == 1
    assert session_b.member_pids == [300]


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
    assert syshealth.pressure_label(-1) == "UNAVAILABLE"
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
