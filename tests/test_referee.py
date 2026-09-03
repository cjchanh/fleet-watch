"""Tests for the referee — claim logic and budget enforcement."""

import contextlib
import errno
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
from pathlib import Path

import pytest

from fleet_watch import events, referee, registry


@contextlib.contextmanager
def _live_child():
    """Yield the PID of a real, live child process for the block's life.

    Some guard paths act on a registered PID (SIGTERM in preempt_port) or
    verify its create-time. A synthetic never-existed PID makes those paths
    take the dead-holder branch, so a test that means to exercise the live
    path must supply a process that is actually alive.
    """
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        yield proc.pid
    finally:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)


@contextlib.contextmanager
def _git_repo_with_held_lock(tmp_path, lock_name: str = "index.lock"):
    """Create a git repo whose ``.git/<lock_name>`` is held open by a live process.

    This is what a git write in flight looks like on disk: the lockfile exists
    AND a live process holds its descriptor. Both halves matter — presence
    alone is also what week-old crash debris looks like.
    """
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    lock = repo / ".git" / lock_name
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import sys, time\n"
            "f = open(sys.argv[1], 'w')\n"
            "f.write('held')\n"
            "f.flush()\n"
            "sys.stdout.write('ready\\n')\n"
            "sys.stdout.flush()\n"
            "time.sleep(30)\n",
            str(lock),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "ready", "lock holder failed to start"
        # The descriptor is open by the time 'ready' is printed.
        deadline = time.monotonic() + 5
        while not lock.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        yield repo, proc.pid
    finally:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)


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


def test_decisions_disclose_their_remaining_provenance():
    """A number whose provenance is invisible is the defect.

    Both checks now consult an authority independent of the registry, so the
    old disclosure ("registry-only", "not GPU telemetry") would itself be a
    false claim. What must stay written down is the limit that is STILL real:
    check_repo cannot see a non-git writer, and check_gpu_budget reads a floor.
    """
    for func in (referee.check_repo, referee.check_repo_with_session):
        doc = func.__doc__ or ""
        assert "registered" in doc.lower()
        assert "allowed: true" in doc.lower()
        assert "git" in doc.lower(), "must name which writers it can and cannot see"

    gpu_doc = referee.check_gpu_budget.__doc__ or ""
    assert "ledger" in gpu_doc.lower()
    assert "telemetry" in gpu_doc.lower()
    assert "floor" in gpu_doc.lower(), "must disclose that the number under-counts"
    assert "not gpu telemetry" not in gpu_doc.lower(), (
        "stale disclosure: this function now reads telemetry"
    )


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


def test_normalize_write_scopes_rejects_repo_escape(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()

    with pytest.raises(ValueError, match="escapes repository root"):
        referee.normalize_write_scopes(str(repo), ["../outside"])


def test_normalize_write_scopes_accepts_repo_descendant(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()

    assert referee.normalize_write_scopes(str(repo), ["src/tool.py"]) == [
        str((repo / "src/tool.py").resolve())
    ]


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


# Port number for the preempt tests. Inert: preempt_port only probes the OS when
# the registry has NO holder, and these tests register one, so nothing is bound
# and no real service is consulted. (The previous fixture used 8100, which is
# MLX's port on this host — a test should not name a port the machine runs.)
_PREEMPT_FIXTURE_PORT = 4242


def test_preempt_higher_priority():
    conn = _fresh_conn()
    # A LIVE holder, not a synthetic dead PID. preempt_port now releases a
    # holder whose owner is provably gone BEFORE comparing priorities, so a
    # dead-PID fixture would satisfy this assertion without ever reaching the
    # priority logic it exists to cover.
    with _live_child() as pid:
        registry.register_process(
            conn, pid=pid, name="low", workstream="ws",
            port=_PREEMPT_FIXTURE_PORT, priority=2,
        )
        d = referee.preempt_port(
            conn, _PREEMPT_FIXTURE_PORT, new_priority=5, reason="test", grace_seconds=0
        )
    assert d.allowed is True
    assert "preempted" in d.reason
    assert registry.get_process_by_port(conn, _PREEMPT_FIXTURE_PORT) is None


def test_preempt_lower_priority_denied():
    conn = _fresh_conn()
    with _live_child() as pid:
        registry.register_process(
            conn, pid=pid, name="high", workstream="ws",
            port=_PREEMPT_FIXTURE_PORT, priority=5,
        )
        d = referee.preempt_port(
            conn, _PREEMPT_FIXTURE_PORT, new_priority=3, reason="test", grace_seconds=0
        )
    assert d.allowed is False
    assert "priority" in d.reason


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


# ── S1: repo occupancy must not be a registry lookup wearing a liveness probe ──
#
# The defect these cover, measured 2026-08-04: a live process holding
# <repo>/.git/index.lock open produced check_repo(...) -> allowed=True
# "repo available", because the only authority was registry.get_process_by_repo
# and the os.kill(pid, 0) probe was reachable only AFTER that returned a row.


def _stub_open_file_holders(monkeypatch, mapping: dict[str, dict[int, str]]):
    """Make the lsof LOOKUP deterministic without changing what the probe MEANS.

    Both verdicts below are decided by `probe_repo_writers`, but both were
    reached through a real `lsof` subprocess with a 10s timeout. Under a loaded
    full-suite run that call can exceed the timeout; `_open_file_holders` then
    returns None and the probe degrades HELD->UNDETERMINED and STALE->
    UNDETERMINED. The tests failed on the machine's load, not on the code —
    a flake that would eventually be silenced rather than read.

    The stub replaces only the external tool call, which is the one part of
    this path that carries no logic. Everything the tests are actually about
    still executes for real: descriptor-mode attribution, liveness filtering,
    the positive control (which builds a real temp file and queries it through
    this same seam, so it is exercised rather than faked), and the verdict
    selection. `referee._lsof_can_attribute_open_files` itself is unpatched.

    Real-lsof coverage is not removed by this: it lives, deliberately alone, in
    `test_repo_writer_probe_has_a_working_positive_control`, whose entire
    subject is that the real tool can attribute a real descriptor. This same
    seam is the one `test_referee_stale_lock_attribution.py` already uses.
    """

    def fake(path):
        key = str(path)
        if key in mapping:
            return dict(mapping[key])
        if Path(key).name.startswith("fleet_watch_lsof_control_"):
            # The probe's own positive-control file: the real lookup would find
            # this process holding it writable, so say exactly that.
            return {os.getpid(): "w"}
        return {}

    monkeypatch.setattr(referee, "_open_file_holders", fake)


def test_repo_denied_for_live_unregistered_git_writer(tmp_path, monkeypatch):
    """THE FAILING CASE. A git write in flight, nothing registered, must DENY.

    The lock holder is a REAL live process — the liveness filter in
    `probe_repo_writers` calls `registry._pid_exists` on it, so a fabricated
    pid would take the dead branch and the test would prove nothing. Only the
    lsof lookup is stubbed (see `_stub_open_file_holders`).
    """
    conn = _fresh_conn()
    with _git_repo_with_held_lock(tmp_path) as (repo, holder_pid):
        lock = repo / ".git" / "index.lock"
        _stub_open_file_holders(monkeypatch, {str(lock): {holder_pid: "w"}})
        d = referee.check_repo(conn, str(repo))
        assert d.allowed is False, (
            f"a live git writer must block, got allowed={d.allowed} "
            f"reason={d.reason!r}"
        )
        assert str(holder_pid) in d.reason, (
            f"the refusal must name the holding PID to be actionable, "
            f"got {d.reason!r}"
        )
        assert "index.lock" in d.reason


def test_repo_available_when_git_lock_is_stale_debris(tmp_path, monkeypatch):
    """POSITIVE CONTROL for the refusal — it must not fire on lock PRESENCE.

    Measured on this machine: ~/Workspace/active/flight-atlas/.git/index.lock,
    0 bytes, 171.6 hours old, zero open descriptors. A "lock exists => DENY"
    rule refuses a repo like that forever, which teaches the operator to ignore
    the guard. Only an ATTRIBUTED lock may deny.

    The empty-holder answer is what a real lsof returns for unheld debris; the
    stub supplies it directly so the verdict does not depend on how busy the
    machine is when the suite reaches this line.
    """
    conn = _fresh_conn()
    repo = tmp_path / "stale"
    (repo / ".git").mkdir(parents=True)
    (repo / ".git" / "index.lock").write_text("")  # nobody holds it open
    _stub_open_file_holders(monkeypatch, {str(repo / ".git" / "index.lock"): {}})

    probe = referee.probe_repo_writers(str(repo))
    assert probe.status == referee.REPO_WRITER_STALE, (
        f"unheld lock must read as stale debris, got {probe.status}: {probe.detail}"
    )
    d = referee.check_repo(conn, str(repo))
    assert d.allowed is True, f"stale lock must not refuse, got {d.reason!r}"


def test_stale_verdict_degrades_to_undetermined_when_the_control_cannot_run(
    tmp_path, monkeypatch
):
    """The flake's MECHANISM, pinned as behaviour rather than left to load.

    A slow `lsof` makes `_open_file_holders` return None inside the positive
    control, the control returns False, and an unheld lock reads UNDETERMINED
    instead of STALE. Direction is fail-closed (a DENY, never a widened
    allow), but it means the availability of a repo with debris on it is
    load-dependent. Recorded here so the degradation is a known contract and
    not a mystery failure in a future suite run.
    """
    conn = _fresh_conn()
    repo = tmp_path / "slow-lsof"
    (repo / ".git").mkdir(parents=True)
    (repo / ".git" / "index.lock").write_text("")

    def fake(path):
        key = str(path)
        if Path(key).name.startswith("fleet_watch_lsof_control_"):
            return None  # what a TimeoutExpired produces
        return {}

    monkeypatch.setattr(referee, "_open_file_holders", fake)

    probe = referee.probe_repo_writers(str(repo))
    assert probe.status == referee.REPO_WRITER_UNDETERMINED
    assert "not evidence of a stale lock" in probe.detail
    d = referee.check_repo(conn, str(repo))
    assert d.allowed is False, "an unverified empty answer must not allow"


def test_repo_writer_probe_has_a_working_positive_control():
    """The control that separates 'no holder' from 'lookup broken' must work.

    lsof exits 1 with empty stdout both when it finds nothing and when it
    fails, so the stale verdict above is only trustworthy if the same lookup
    can find a file this process is holding open.
    """
    if shutil.which("lsof") is None:
        pytest.skip("lsof not installed — the control genuinely cannot run here")
    assert referee._lsof_can_attribute_open_files() is True


def test_repo_writer_probe_fails_closed_when_lookup_unavailable(tmp_path, monkeypatch):
    """A lock we cannot attribute is UNDETERMINED, and undetermined must refuse."""
    conn = _fresh_conn()
    repo = tmp_path / "unknowable"
    (repo / ".git").mkdir(parents=True)
    (repo / ".git" / "index.lock").write_text("")

    monkeypatch.setattr(referee, "_open_file_holders", lambda path: None)
    probe = referee.probe_repo_writers(str(repo))
    assert probe.status == referee.REPO_WRITER_UNDETERMINED
    d = referee.check_repo(conn, str(repo))
    assert d.allowed is False, "an unmeasurable repo must not be reported available"
    assert "undetermined" in d.reason.lower()


def test_repo_writer_probe_fails_closed_when_control_fails(tmp_path, monkeypatch):
    """No holder found AND the control cannot confirm the lookup works => refuse.

    This is the branch that keeps the stale verdict honest: without it, a
    broken lookup would be indistinguishable from proven-stale and would
    silently return allow.
    """
    conn = _fresh_conn()
    repo = tmp_path / "uncontrolled"
    (repo / ".git").mkdir(parents=True)
    (repo / ".git" / "index.lock").write_text("")

    monkeypatch.setattr(referee, "_open_file_holders", lambda path: set())
    monkeypatch.setattr(referee, "_lsof_can_attribute_open_files", lambda: False)
    probe = referee.probe_repo_writers(str(repo))
    assert probe.status == referee.REPO_WRITER_UNDETERMINED
    assert referee.check_repo(conn, str(repo)).allowed is False


def test_repo_writer_probe_sees_worktree_gitdir(tmp_path):
    """A worktree's .git is a FILE pointing elsewhere — the parallel-session case."""
    separate_gitdir = tmp_path / "separate_gitdir"
    separate_gitdir.mkdir()
    worktree = tmp_path / "wt"
    worktree.mkdir()
    (worktree / ".git").write_text(f"gitdir: {separate_gitdir}\n")
    (separate_gitdir / "index.lock").write_text("")

    probe = referee.probe_repo_writers(str(worktree))
    assert probe.status != referee.REPO_WRITER_NONE, (
        "a worktree's gitdir must be followed, or every worktree is invisible"
    )
    assert str(separate_gitdir) in (probe.lock_path or "")


def test_repo_writer_probe_is_silent_on_non_git_paths(tmp_path):
    """No git dir means this probe has nothing to say — it must not manufacture
    a refusal for every non-repo path the guard is asked about."""
    conn = _fresh_conn()
    plain = tmp_path / "not_a_repo"
    plain.mkdir()
    assert referee.probe_repo_writers(str(plain)).status == referee.REPO_WRITER_NONE
    assert referee.check_repo(conn, str(plain)).allowed is True


# ── S1: a recycled PID is not a live holder ───────────────────────────────────
#
# os.kill(pid, 0) asks "does this integer name a live process", never "is it
# still the SAME process". The session-lease path has been create-time aware
# since Path C; the process path had no create-time recorded to compare against.


def _recycle(conn, pid: int) -> None:
    """Simulate PID reuse: the integer still names a live process, but not the
    one that registered — exactly what the OS does when it recycles a PID."""
    conn.execute(
        "UPDATE processes SET start_create_time = ? WHERE pid = ?",
        ("Thu Jan  1 00:00:00 1970", pid),
    )
    conn.commit()


def test_repo_released_when_holder_pid_was_recycled():
    """THE FAILING CASE. A live-but-different process must not hold the repo."""
    conn = _fresh_conn()
    with _live_child() as pid:
        registry.register_process(
            conn, pid=pid, name="ghost", workstream="ws",
            repo_dir="/tmp/fw-recycle-repo", session_id="sess-old",
        )
        _recycle(conn, pid)
        d = referee.check_repo(conn, "/tmp/fw-recycle-repo")
    assert d.allowed is True, (
        f"a recycled PID is not the holder; the repo must release, got {d.reason!r}"
    )
    assert registry.get_process_by_repo(conn, "/tmp/fw-recycle-repo") is None


def test_repo_stays_locked_for_a_genuinely_live_holder():
    """POSITIVE CONTROL. The recycle check must not release every holder."""
    conn = _fresh_conn()
    with _live_child() as pid:
        registry.register_process(
            conn, pid=pid, name="real", workstream="ws",
            repo_dir="/tmp/fw-live-repo", session_id="sess-other",
        )
        d = referee.check_repo(conn, "/tmp/fw-live-repo")
        assert d.allowed is False, (
            f"a live registered holder must still block, got {d.reason!r}"
        )
        assert str(pid) in d.reason


def test_preempt_does_not_signal_a_recycled_pid():
    """THE FAILING CASE with the worst blast radius: preempt SIGTERMs the PID
    it reads from the registry. If the owner already died, that signal lands on
    whatever unrelated process inherited the integer."""
    conn = _fresh_conn()
    with _live_child() as pid:
        registry.register_process(
            conn, pid=pid, name="ghost", workstream="ws",
            port=_PREEMPT_FIXTURE_PORT, priority=1,
        )
        _recycle(conn, pid)
        d = referee.preempt_port(
            conn, _PREEMPT_FIXTURE_PORT, new_priority=5, reason="test", grace_seconds=0
        )
        # The innocent process must be untouched.
        time.sleep(0.2)
        assert registry._pid_exists(pid) is True, (
            "preempt signalled a PID it could not prove was the holder"
        )
    assert d.allowed is True
    assert "not signalling" in d.reason
    assert registry.get_process_by_port(conn, _PREEMPT_FIXTURE_PORT) is None


def test_process_owner_alive_degrades_to_pid_existence_without_create_time():
    """Pre-migration rows carry no create-time. Missing evidence must degrade to
    'still alive / keep blocking', never to a release."""
    assert registry.process_owner_alive(
        {"pid": os.getpid(), "start_create_time": None}
    ) is True
    assert registry.process_owner_alive(
        {"pid": os.getpid(), "start_create_time": "Thu Jan  1 00:00:00 1970"}
    ) is False
    assert registry.process_owner_alive(None) is False


def test_register_process_records_owner_create_time():
    """The comparison above is only possible if the evidence is captured."""
    conn = _fresh_conn()
    registry.register_process(conn, pid=os.getpid(), name="self", workstream="ws")
    row = conn.execute(
        "SELECT start_create_time FROM processes WHERE pid = ?", (os.getpid(),)
    ).fetchone()
    assert row[0], "no create-time recorded — the recycle check has nothing to compare"


# ── S1: a GPU ledger that cannot see the device is not a GPU guard ────────────
#
# Measured 2026-08-04: Ollama held 11,601MB resident (qwen3:8b 11,249MB +
# nomic-embed-text 352MB) while the ledger reported 0MB allocated and
# check_gpu_budget(conn, 100000) returned allowed=True.


def _residency(mb: int) -> referee.GpuResidencyProbe:
    return referee.GpuResidencyProbe(
        referee.GPU_TELEMETRY_MEASURED, mb, f"test: {mb}MB resident", ("test",)
    )


_UNAVAILABLE = referee.GpuResidencyProbe(
    referee.GPU_TELEMETRY_UNAVAILABLE, 0, "test: telemetry unreadable", ("test",)
)


def test_gpu_denied_when_device_is_full_and_ledger_is_empty():
    """THE FAILING CASE. Empty ledger, device nearly full — must DENY."""
    conn = _fresh_conn()
    d = referee.check_gpu_budget(conn, 100000, residency=_residency(110000))
    assert d.allowed is False, (
        f"an unregistered consumer holding the device must block, got {d.reason!r}"
    )
    assert "110000" in d.reason


def test_gpu_allowed_when_device_has_room():
    """POSITIVE CONTROL. The telemetry path must not deny everything."""
    conn = _fresh_conn()
    d = referee.check_gpu_budget(conn, 8192, residency=_residency(352))
    assert d.allowed is True, f"a nearly-idle device must allow, got {d.reason!r}"
    assert "352" in d.reason


def test_gpu_fails_closed_when_telemetry_unreadable():
    """A failed measurement must never be served as the ledger's confident number."""
    conn = _fresh_conn()
    d = referee.check_gpu_budget(conn, 1024, residency=_UNAVAILABLE)
    assert d.allowed is False, (
        f"unreadable telemetry must refuse, not fall back to the ledger, "
        f"got {d.reason!r}"
    )
    assert "undetermined" in d.reason.lower()


def test_gpu_does_not_double_count_a_registered_consumer():
    """Ledger and telemetry describe the SAME memory for a registered runtime.
    Summing them would invent false refusals; max() is the honest floor.

    The request is chosen to STRADDLE the two arithmetics. Allocatable here is
    114688MB, so with 12000 declared and 12000 resident: max leaves 102688MB
    free and sum leaves 90688MB. A 90000MB request fits under both and proves
    nothing — the first version of this test used 90000 and a sum-instead-of-max
    mutation survived it. 100000MB fits only under max.
    """
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="ollama", workstream="ws", gpu_mb=12000)
    d = referee.check_gpu_budget(conn, 100000, residency=_residency(12000))
    assert d.allowed is True, (
        f"12000 declared + 12000 resident is 12000MB of real use, not 24000: "
        f"{d.reason!r}"
    )
    assert "102688MB available" in d.reason, (
        f"headroom must be computed from the larger of the two, not their sum: "
        f"{d.reason!r}"
    )


def test_gpu_telemetry_overrides_an_understated_declaration():
    """A process that declared 1000MB while holding 40000MB is counted at what
    it actually holds."""
    conn = _fresh_conn()
    registry.register_process(conn, pid=1, name="liar", workstream="ws", gpu_mb=1000)
    d = referee.check_gpu_budget(conn, 90000, residency=_residency(40000))
    assert d.allowed is False
    assert "40000" in d.reason


def test_ollama_probe_reads_refused_connection_as_a_real_zero(monkeypatch):
    """Nothing listening means Ollama is not running, which means it holds no
    VRAM. That is a determinate measurement, not an unknown."""
    def _refused(url, timeout=None):
        raise urllib.error.URLError(
            ConnectionRefusedError(errno.ECONNREFUSED, "refused")
        )

    monkeypatch.setattr(referee.urllib.request, "urlopen", _refused)
    probe = referee.probe_gpu_residency()
    assert probe.measured is True
    assert probe.resident_mb == 0


def test_ollama_probe_reads_timeout_as_unavailable(monkeypatch):
    """A timeout is a FAILED measurement and must not round down to zero."""
    def _timeout(url, timeout=None):
        raise TimeoutError("timed out")

    monkeypatch.setattr(referee.urllib.request, "urlopen", _timeout)
    probe = referee.probe_gpu_residency()
    assert probe.measured is False


def test_ollama_probe_refuses_a_model_with_no_readable_size(monkeypatch):
    """A resident model with an unreadable size would make the total UNDERSTATE
    consumption — the exact defect being fixed. Refuse instead."""
    class _Resp:
        status = 200

        def read(self):
            return json.dumps({"models": [{"name": "x", "size": 1}]}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(
        referee.urllib.request, "urlopen", lambda url, timeout=None: _Resp()
    )
    assert referee.probe_gpu_residency().measured is False


def test_ollama_probe_sums_resident_models(monkeypatch):
    """POSITIVE CONTROL for the parser: a well-formed reply must produce the
    real total, or every assertion above is vacuous."""
    class _Resp:
        status = 200

        def read(self):
            return json.dumps(
                {
                    "models": [
                        {"name": "qwen3:8b", "size_vram": 11249 * 1024 * 1024},
                        {"name": "nomic-embed-text", "size_vram": 352 * 1024 * 1024},
                    ]
                }
            ).encode()

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(
        referee.urllib.request, "urlopen", lambda url, timeout=None: _Resp()
    )
    probe = referee.probe_gpu_residency()
    assert probe.measured is True
    assert probe.resident_mb == 11601


def test_gpu_telemetry_only_ever_requests_the_listing_endpoint(monkeypatch):
    """READ-ONLY is load-bearing: a guard that loads a model to measure VRAM
    changes the thing it measures. Asserted on the URL actually requested —
    a source grep would be satisfied by a comment."""
    requested: list[str] = []

    class _Resp:
        status = 200

        def read(self):
            return json.dumps({"models": []}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def _capture(url, timeout=None):
        requested.append(url)
        return _Resp()

    monkeypatch.setattr(referee.urllib.request, "urlopen", _capture)
    referee.probe_gpu_residency()

    assert requested, "the probe made no request at all"
    for url in requested:
        assert url.startswith("http://127.0.0.1:"), f"non-loopback target {url!r}"
        assert url.endswith("/api/ps"), (
            f"only the listing endpoint may be called; got {url!r}"
        )


def test_referee_embeds_no_model_loading_url_literal():
    """Guards the same rule against a FUTURE call site: no URL literal in this
    module may name an endpoint that loads, pulls, or creates a model.

    Scans URL literals only — the prose above deliberately names those
    endpoints to explain why they are excluded.
    """
    source = Path(referee.__file__).read_text(encoding="utf-8")
    urls = re.findall(r"http://[^\s\"']+", source)
    assert urls, "no URL literal found — has the probe moved? (regex drift)"
    for url in urls:
        assert "/api/ps" in url, f"unexpected endpoint literal in referee: {url!r}"
