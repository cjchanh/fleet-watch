"""Tests for the referee — claim logic and budget enforcement."""

import contextlib
import errno
import os
import socket
import sqlite3

from fleet_watch import events, referee, registry


@contextlib.contextmanager
def _listener():
    """Hold a real TCP listener on an OS-assigned port for the block's life.

    Ephemeral rather than a fixed number: a fixed test port collides with
    whatever the host is actually running (MLX lives on :8100 here), and a
    test that fails because a real service is up is not reporting a defect.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        yield int(sock.getsockname()[1])
    finally:
        sock.close()


def _fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO gpu_budget (id, total_mb, reserve_mb, allocated_mb) "
        "VALUES (1, 131072, 16384, 0)"
    )
    conn.commit()
    return conn


def _free_ephemeral_port() -> int:
    """Bind 0, read the OS-assigned free port, release it. Race-tolerant for tests."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_port_available():
    conn = _fresh_conn()
    port = _free_ephemeral_port()
    d = referee.check_port(conn, port)
    assert d.allowed is True
    assert d.reason == "port available"


def test_port_taken():
    conn = _fresh_conn()
    port = _free_ephemeral_port()
    registry.register_process(conn, pid=1234, name="mlx", workstream="ws", port=port)
    d = referee.check_port(conn, port)
    assert d.allowed is False
    assert d.holder is not None
    assert d.holder["pid"] == 1234


def test_port_held_by_os_unregistered_is_denied():
    """Registry-empty + OS-held must refuse — the 8765 fail-open class.

    Proof shape (production): fleet guard --json --port 8765 returned
    allowed:true while socket.bind raised [Errno 48]. This test holds a real
    listener, leaves the registry empty, and asserts check_port refuses with
    the OS reason. Mutation-tested: without os_port_held in check_port, this
    assertion fails (allowed becomes True).
    """
    conn = _fresh_conn()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = int(listener.getsockname()[1])
    try:
        # Positive control: OS really holds it.
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.bind(("127.0.0.1", port))
            raise AssertionError("positive control failed: OS did not hold port")
        except OSError:
            pass
        finally:
            probe.close()

        d = referee.check_port(conn, port)
        assert d.allowed is False, (
            "registry was empty but OS held the port — must refuse, not "
            f"clear (got allowed={d.allowed!r} reason={d.reason!r})"
        )
        assert "not in fleet registry" in d.reason
        assert "registry absence is not availability" in d.reason
        assert d.holder is None
    finally:
        listener.close()


def test_os_port_held_true_when_listening():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = int(listener.getsockname()[1])
    try:
        assert referee.os_port_held(port) is True
    finally:
        listener.close()


def test_os_port_held_false_when_free():
    port = _free_ephemeral_port()
    assert referee.os_port_held(port) is False


def test_os_port_held_refuses_invalid_port():
    """Bad port numbers are not 'available' — fail closed."""
    assert referee.os_port_held(0) is True
    assert referee.os_port_held(-1) is True
    assert referee.os_port_held(70000) is True


# --- S1-a: a process must be able to register a port it already holds -------


def test_owner_pid_may_register_the_port_its_own_process_holds():
    """discover/register must be able to claim a port the caller itself holds.

    Failing case: the OS probe denied every live listener, because a listener
    holds its own port — `fleet discover` could register nothing at all and
    `fleet register --pid <self> --port <held>` was refused as "held by the
    OS". Positive control below proves the probe still has teeth on the same
    port for a caller that is NOT the holder.
    """
    conn = _fresh_conn()
    with _listener() as port:
        allowed = referee.check_port(conn, port, owner_pid=os.getpid())
        assert allowed.allowed is True, (
            "the holder of a port must be able to register it "
            f"(got reason={allowed.reason!r})"
        )

        # Positive control: identical call, no ownership declared. If this
        # also passed, the test above would prove nothing.
        anonymous = referee.check_port(conn, port)
        assert anonymous.allowed is False
        assert "registry absence is not availability" in anonymous.reason


def test_declared_owner_pid_must_actually_hold_the_port():
    """The ownership declaration is verified, not trusted.

    Without verification, any caller could pass an arbitrary --pid to switch
    the OS check off and write a false holder into the registry — which then
    reads back as an authoritative claim and makes `guard` name the wrong PID.
    """
    conn = _fresh_conn()
    with _listener() as port:
        real_owner = os.getpid()
        impostor = 2147483646  # not a live PID, certainly not the listener

        denied = referee.check_port(conn, port, owner_pid=impostor)
        assert denied.allowed is False, (
            "a PID that does not hold the port must not be able to claim it"
        )
        assert str(real_owner) in denied.reason, (
            f"the refusal should name the real holder (got {denied.reason!r})"
        )

        # Positive control: the true owner is still allowed, so the denial
        # above is about identity and not a blanket refusal.
        assert referee.check_port(conn, port, owner_pid=real_owner).allowed is True


def test_preflight_register_passes_ownership_through():
    """discover's actual call path, not just check_port in isolation."""
    conn = _fresh_conn()
    with _listener() as port:
        assert referee.preflight_register(conn, port=port, owner_pid=os.getpid()) == []

        # Positive control: the same preflight without ownership fails.
        failures = referee.preflight_register(conn, port=port)
        assert len(failures) == 1
        assert failures[0].allowed is False


def test_registered_rival_still_blocks_the_port_owner():
    """Ownership suppresses only the OS signal — never the registry half."""
    conn = _fresh_conn()
    with _listener() as port:
        registry.register_process(
            conn, pid=2147483646, name="rival", workstream="ws", port=port
        )
        d = referee.check_port(conn, port, owner_pid=os.getpid())
        assert d.allowed is False
        assert d.holder is not None
        assert d.holder["pid"] == 2147483646


# --- S1-c: a failed probe is not an available port -------------------------


def test_probe_fails_closed_when_sockets_cannot_be_created(monkeypatch):
    """Socket-creation failure must refuse, not fall through to available.

    Failing case (measured): the old ``except OSError: continue`` was scoped
    in its comment to "AF_INET6 unsupported" but caught every creation error
    for both families; with both continuing the loop reached ``return False``
    = available. Reproduced with RLIMIT_NOFILE lowered to 64 and fds consumed
    to EMFILE (errno 24): ``os_port_held(P)`` returned False for a port with
    a LIVE listener. Simulated deterministically here.
    """
    real_socket = socket.socket

    def _emfile(*args, **kwargs):
        raise OSError(errno.EMFILE, "Too many open files")

    with _listener() as port:
        # Positive control: with sockets working, this exact port probes held.
        assert referee.probe_port(port).status == referee.PORT_HELD

        monkeypatch.setattr(socket, "socket", _emfile)
        probe = referee.probe_port(port)
        assert probe.status == referee.PORT_UNDETERMINED, (
            "an unmeasurable port must not be reported as free "
            f"(got {probe.status!r})"
        )
        assert referee.os_port_held(port) is True

        conn = _fresh_conn()
        d = referee.check_port(conn, port)
        assert d.allowed is False
        assert "undetermined" in d.reason

        monkeypatch.setattr(socket, "socket", real_socket)


def test_one_family_failing_to_open_is_not_covered_by_the_other(monkeypatch):
    """Isolates the errno classification, not just the no-family backstop.

    A blanket ``continue`` on creation errors leaves a narrower hole that the
    "nothing could be probed" guard does not catch: if the AF_INET socket
    cannot be created but AF_INET6 can, the loop finishes having "probed"
    something and reports FREE — for a port with a live IPv4 listener. fd
    pressure is transient, so one family failing while the next succeeds is
    the realistic shape, not the all-or-nothing one.
    """
    real_socket = socket.socket

    def _ipv4_exhausted(family, *args, **kwargs):
        if family == socket.AF_INET:
            raise OSError(errno.EMFILE, "Too many open files")
        return real_socket(family, *args, **kwargs)

    with _listener() as port:  # IPv4-only listener
        assert referee.probe_port(port).status == referee.PORT_HELD  # control

        monkeypatch.setattr(socket, "socket", _ipv4_exhausted)
        probe = referee.probe_port(port)
        assert probe.status == referee.PORT_UNDETERMINED, (
            "the IPv6 probe cannot vouch for a port the IPv4 probe never "
            f"measured (got {probe.status!r})"
        )
        monkeypatch.setattr(socket, "socket", real_socket)


def test_unsupported_address_family_is_skipped_not_refused(monkeypatch):
    """The one creation error that MAY be skipped still is.

    Guards the fix from over-correcting: EAFNOSUPPORT (no IPv6 on this host)
    must fall through to the other family, not fail the whole probe closed.
    """
    real_socket = socket.socket

    def _no_ipv6(family, *args, **kwargs):
        if family == socket.AF_INET6:
            raise OSError(errno.EAFNOSUPPORT, "Address family not supported")
        return real_socket(family, *args, **kwargs)

    port = _free_ephemeral_port()
    monkeypatch.setattr(socket, "socket", _no_ipv6)
    assert referee.probe_port(port).status == referee.PORT_FREE


def test_probe_fails_closed_when_no_family_can_be_probed(monkeypatch):
    """Zero successful measurements is not evidence of a free port."""
    real_socket = socket.socket

    def _no_families(*args, **kwargs):
        raise OSError(errno.EAFNOSUPPORT, "Address family not supported")

    port = _free_ephemeral_port()
    # Positive control: this port is genuinely free when families work.
    assert referee.probe_port(port).status == referee.PORT_FREE

    monkeypatch.setattr(socket, "socket", _no_families)
    assert referee.probe_port(port).status == referee.PORT_UNDETERMINED
    monkeypatch.setattr(socket, "socket", real_socket)


# --- S2-d: "cannot test" is not "held" -------------------------------------


def test_privileged_port_is_not_reported_as_held(monkeypatch):
    """EACCES means we were not allowed to ask, not that a listener answered.

    Failing case: ``os_port_held(80)`` was True for an unprivileged prober
    because bind raised EACCES, and the emitted reason said the port was
    "held by the OS" — a true DENY carrying a false reason.
    """
    real_socket = socket.socket

    class _Refusing:
        def __init__(self, *args, **kwargs):
            self._sock = real_socket(*args, **kwargs)

        def bind(self, *args, **kwargs):
            raise OSError(errno.EACCES, "Permission denied")

        def close(self):
            self._sock.close()

    port = _free_ephemeral_port()
    # Positive control: unpatched, this port probes free — so the assertions
    # below are caused by the refusal, not by the port's state.
    assert referee.probe_port(port).status == referee.PORT_FREE

    monkeypatch.setattr(socket, "socket", _Refusing)
    probe = referee.probe_port(port)
    assert probe.status == referee.PORT_PRIVILEGED
    assert referee.os_port_held(port) is True, "still refuses — fail closed"

    conn = _fresh_conn()
    d = referee.check_port(conn, port)
    assert d.allowed is False, "an untestable port is still not claimable"
    assert "held by the OS" not in d.reason, (
        f"nothing was measured about a holder (got {d.reason!r})"
    )
    assert "could not be tested" in d.reason
    monkeypatch.setattr(socket, "socket", real_socket)


def test_real_privileged_port_is_not_called_held():
    """Same claim against a real privileged port, no stubbing."""
    if os.geteuid() == 0:
        return  # root can bind 80; the refusal under test cannot occur
    probe = referee.probe_port(80)
    if probe.status == referee.PORT_HELD:
        return  # something really is listening on 80 on this host
    assert probe.status == referee.PORT_PRIVILEGED
    assert "privilege" in probe.detail


# --- S2-e: suggestion and preemption must consult the OS too ---------------


def test_suggest_ports_never_offers_an_os_held_port():
    """Measured: check_port denied 55692 while suggest_ports offered it #1."""
    conn = _fresh_conn()
    with _listener() as port:
        assert referee.check_port(conn, port).allowed is False
        suggestions = referee.suggest_ports(
            conn, preferred_ports=[port], requested_port=None
        )
        assert port not in suggestions, (
            "suggested a port the same call path just refused"
        )

    # Positive control: once the listener is gone the port IS offered, so the
    # exclusion above was caused by the listener and not by the port being
    # filtered for some unrelated reason.
    reopened = referee.suggest_ports(conn, preferred_ports=[port], requested_port=None)
    assert reopened[0] == port


def test_preempt_refuses_a_port_it_cannot_preempt():
    """Registry-empty + OS-held is not "already free"."""
    conn = _fresh_conn()
    with _listener() as port:
        d = referee.preempt_port(conn, port, new_priority=5, reason="test")
        assert d.allowed is False, (
            f"an unregistered live listener is not free (got {d.reason!r})"
        )
        assert str(os.getpid()) in d.reason

    # Positive control: a genuinely free port still reports free.
    free_again = referee.preempt_port(conn, port, new_priority=5, reason="test")
    assert free_again.allowed is True
    assert "free" in free_again.reason


# --- S2-f: registry-only decisions must disclose their provenance ----------


def test_registry_only_decisions_disclose_their_provenance():
    """A number whose provenance is invisible is the defect.

    check_repo and check_gpu_budget cannot see an unregistered holder. That
    limit must stay written down where the next reader meets the function.
    """
    for func in (referee.check_repo, referee.check_repo_with_session):
        doc = func.__doc__ or ""
        assert "registered" in doc.lower()
        assert "allowed: true" in doc.lower()

    gpu_doc = referee.check_gpu_budget.__doc__ or ""
    assert "ledger" in gpu_doc.lower()
    assert "not gpu telemetry" in gpu_doc.lower()
    assert "invisible" in gpu_doc.lower()


def test_repo_available():
    conn = _fresh_conn()
    d = referee.check_repo(conn, "/tmp/test-repo")
    assert d.allowed is True


def test_repo_denied_by_external_resource():
    conn = _fresh_conn()
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-other",
        workstream="paper",
        name="Thunder abc123",
        repo_dir="/tmp/test-repo",
        status="RUNNING",
    )
    d = referee.check_repo(conn, "/tmp/test-repo")
    assert d.allowed is False
    assert d.holder is not None
    assert d.holder["provider"] == "thunder"


def test_repo_allowed_for_current_external_owner_session():
    conn = _fresh_conn()
    registry.register_external_resource(
        conn,
        provider="thunder",
        resource_type="instance",
        external_id="abc123",
        session_id="sess-current",
        workstream="paper",
        name="Thunder abc123",
        repo_dir="/tmp/test-repo",
        status="RUNNING",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-current")
    assert d.allowed is True


def test_repo_allows_active_cooperative_session_lease():
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn,
        "sess-other",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")
    assert d.allowed is True
    assert d.holders[0]["session_id"] == "sess-other"
    assert d.safe_mode == "declare --write-scope before editing"


def test_repo_denied_by_exclusive_session_lease():
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn,
        "sess-exclusive",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
        repo_lock_mode="exclusive",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")
    assert d.allowed is False
    assert d.holder is not None
    assert d.holder["session_id"] == "sess-exclusive"
    assert d.holder["repo_lock_mode"] == "exclusive"


def test_repo_denied_by_overlapping_write_scope():
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn,
        "sess-tools",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
        write_scopes=["tools/playwright"],
    )
    d = referee.check_repo_with_session(
        conn,
        "/tmp/test-repo",
        current_session_id="sess-mine",
        write_scopes=["tools/playwright/edit_post.py"],
    )
    assert d.allowed is False
    assert d.holder is not None
    assert d.holder["session_id"] == "sess-tools"
    assert any(path.endswith("tools/playwright") for path in d.overlap_paths)


def test_repo_allows_disjoint_write_scope():
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn,
        "sess-docs",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
        write_scopes=["docs"],
    )
    d = referee.check_repo_with_session(
        conn,
        "/tmp/test-repo",
        current_session_id="sess-mine",
        write_scopes=["tools/playwright"],
    )
    assert d.allowed is True
    assert d.holders[0]["session_id"] == "sess-docs"
    assert d.safe_mode == "cooperative-write"


def test_repo_allowed_for_current_session_lease():
    conn = _fresh_conn()
    registry.upsert_session_lease(
        conn,
        "sess-current",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-current")
    assert d.allowed is True
    assert "owned by current session" in d.reason


def test_repo_releases_cooperative_lease_with_dead_owner(monkeypatch):
    """Path C (DECOUPLE): a cooperative lease whose owner is PROVEN DEAD is
    released immediately, independent of heartbeat freshness. A dead owner is
    not a live advisory holder — the lease is closed and surfaced as stale.
    (Replaces the prior test asserting the buggy behavior of keeping a fresh
    dead-owner lease ACTIVE.)"""
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: False)
    # Fresh heartbeat (5s) — old behavior kept it ACTIVE; Path C closes it.
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 5 if ts else None)
    registry.upsert_session_lease(
        conn,
        "sess-dead-owner",
        owner_pid=99999,
        repo_dir="/tmp/test-repo",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")
    assert d.allowed is True
    assert d.holders == []
    assert d.stale_holders[0]["session_id"] == "sess-dead-owner"
    lease = registry.get_session_lease(conn, "sess-dead-owner")
    assert lease["status"] == "CLOSED"


def test_repo_allowed_when_owner_dead_and_heartbeat_stale(monkeypatch):
    """A lease whose owner PID is dead AND heartbeat is stale does not block the repo."""
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_pid_exists", lambda pid: False)
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 999 if ts else None)
    registry.upsert_session_lease(
        conn,
        "sess-dead-stale",
        owner_pid=99999,
        repo_dir="/tmp/test-repo",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")
    assert d.allowed is True
    assert d.stale_holders[0]["session_id"] == "sess-dead-stale"


def test_repo_cleans_ownerless_stale_session_lease(monkeypatch):
    conn = _fresh_conn()
    monkeypatch.setattr(registry, "_age_seconds", lambda ts: 999 if ts else None)
    registry.upsert_session_lease(
        conn,
        "sess-ownerless-stale",
        owner_pid=None,
        repo_dir="/tmp/test-repo",
    )

    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")

    assert d.allowed is True
    assert d.stale_holders[0]["session_id"] == "sess-ownerless-stale"
    lease = registry.get_session_lease(conn, "sess-ownerless-stale")
    assert lease["status"] == "CLOSED"


def test_repo_allowed_for_current_local_owner_session():
    """Same-session bypass works for local process repo locks, not just external."""
    conn = _fresh_conn()
    registry.register_process(
        conn,
        pid=os.getpid(),
        name="writer",
        workstream="test",
        repo_dir="/tmp/test-repo",
        session_id="sess-current",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-current")
    assert d.allowed is True
    assert "owned by current session" in d.reason


def test_repo_denied_for_different_local_session():
    """Different session is denied even for local processes."""
    conn = _fresh_conn()
    registry.register_process(
        conn,
        pid=os.getpid(),
        name="writer",
        workstream="test",
        repo_dir="/tmp/test-repo",
        session_id="sess-other",
    )
    d = referee.check_repo_with_session(conn, "/tmp/test-repo", current_session_id="sess-mine")
    assert d.allowed is False


def test_gpu_budget_fits():
    conn = _fresh_conn()
    d = referee.check_gpu_budget(conn, 50000)
    assert d.allowed is True


def test_gpu_budget_overflow():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="a", workstream="ws", gpu_mb=100000)
    d = referee.check_gpu_budget(conn, 50000)
    assert d.allowed is False
    assert "exceeded" in d.reason


def test_gpu_zero_always_ok():
    conn = _fresh_conn()
    d = referee.check_gpu_budget(conn, 0)
    assert d.allowed is True


def test_preflight_all_clear():
    conn = _fresh_conn()
    # Ephemeral: check_port consults the OS, so "all clear" needs a port this
    # test proved free rather than one a real service may be holding.
    failures = referee.preflight_register(
        conn, port=_free_ephemeral_port(), gpu_mb=1000, repo_dir="/tmp/r"
    )
    assert failures == []


def test_preflight_port_conflict():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="a", workstream="ws", port=8100)
    failures = referee.preflight_register(conn, port=8100)
    assert len(failures) == 1
    assert "8100" in failures[0].reason


def test_preflight_multiple_failures():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="a", workstream="ws", port=8100, gpu_mb=120000)
    failures = referee.preflight_register(conn, port=8100, gpu_mb=50000)
    assert len(failures) == 2


def test_claim_port_logs_event():
    conn = _fresh_conn()
    referee.claim_port(conn, _free_ephemeral_port())
    evts = events.get_events(conn, hours=1, event_type="CLAIM")
    assert len(evts) == 1


def test_claim_port_conflict_logs_event():
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="a", workstream="ws", port=8100)
    referee.claim_port(conn, 8100)
    evts = events.get_events(conn, hours=1, event_type="CONFLICT")
    assert len(evts) == 1


def test_preempt_higher_priority():
    conn = _fresh_conn()
    registry.register_process(conn, pid=2147483646, name="low", workstream="ws", port=8100, priority=2)
    # Use grace=0 for test speed — PID won't exist anyway
    d = referee.preempt_port(conn, 8100, new_priority=5, reason="test", grace_seconds=0)
    assert d.allowed is True
    # Port should be free now
    assert registry.get_process_by_port(conn, 8100) is None


def test_preempt_lower_priority_denied():
    conn = _fresh_conn()
    registry.register_process(conn, pid=2147483646, name="high", workstream="ws", port=8100, priority=5)
    d = referee.preempt_port(conn, 8100, new_priority=3, reason="test", grace_seconds=0)
    assert d.allowed is False


def test_preempt_empty_port():
    conn = _fresh_conn()
    # Ephemeral: preempt_port now consults the OS for the registry-empty case,
    # so a hardcoded port would fail whenever a real service occupies it.
    d = referee.preempt_port(conn, _free_ephemeral_port(), new_priority=5, reason="test")
    assert d.allowed is True
    assert "free" in d.reason


def test_suggest_ports_skips_taken_and_requested():
    conn = _fresh_conn()
    # Ephemeral for the same reason as above: suggestions are now OS-checked,
    # so the candidates must be ports this test proved free, not fixed numbers.
    taken_a, taken_b, offered, requested = (
        _free_ephemeral_port() for _ in range(4)
    )
    registry.register_process(conn, pid=1, name="held", workstream="ws", port=taken_a)
    registry.register_process(conn, pid=2, name="held2", workstream="ws", port=taken_b)

    ports = referee.suggest_ports(
        conn,
        preferred_ports=[taken_a, offered, taken_b, requested],
        requested_port=requested,
    )

    assert taken_a not in ports
    assert taken_b not in ports
    assert requested not in ports
    assert ports[0] == offered
