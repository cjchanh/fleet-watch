"""The operator seat may close their own leases; an agent may not close a peer's.

Incident 2026-09-03: the operator ran `fleet session close` from a second
Terminal tab and was DENIED. The lease owner was a `claude` CLI whose parent was
the zsh of a DIFFERENT tab, so the requesting shell was a SIBLING of the owner's
parent — not the owner, not a descendant, not an ancestor. Correct by the rule
as written, and wrong for a human at their own machine: it pushed the operator
into hand-deleting governance state twice in one night.

The arm added: same uid AND no agent runtime anywhere in the requester's
ancestry. These tests pin both halves — that the operator's sibling tab is
allowed, and that every agent-shaped requester is still bound to lineage.

Every process fact is injected. Nothing here reads the live process table or the
live registry.
"""
from __future__ import annotations

import sqlite3

import pytest

from fleet_watch import registry, syshealth


OPERATOR_UID = 501
OTHER_UID = 502

OWNER_PID = 21455          # the `claude` CLI holding the lease
OWNER_SHELL_PID = 21355    # tab A's zsh, the owner's parent
SIBLING_SHELL_PID = 31000  # tab B's zsh — the operator's other terminal
TERMINAL_PID = 900         # Terminal.app, parent of both shells
LAUNCHD_PID = 1

AGENT_CHILD_BASH_PID = 40000   # a bash spawned by the claude CLI
AGENT_TOOL_PID = 40001         # `fleet` run from that bash


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(registry.SCHEMA)
    conn.commit()
    return conn


def _open_lease(conn: sqlite3.Connection, session_id: str, owner_pid: int) -> None:
    registry.upsert_session_lease(
        conn,
        session_id,
        owner_pid=owner_pid,
        repo_dir="/tmp/lease-repo",
        repo_lock_mode="cooperative",
    )


# The machine as the operator's terminals actually look. Tab A spawned the
# agent; tab B is a sibling of tab A, which is the whole point of the incident.
PROCESS_TABLE: dict[int, dict[str, object]] = {
    LAUNCHD_PID: {"ppid": 0, "command": "/sbin/launchd"},
    TERMINAL_PID: {"ppid": LAUNCHD_PID, "command": "/System/Applications/Utilities/Terminal.app/Contents/MacOS/Terminal"},
    OWNER_SHELL_PID: {"ppid": TERMINAL_PID, "command": "-zsh"},
    SIBLING_SHELL_PID: {"ppid": TERMINAL_PID, "command": "-zsh"},
    OWNER_PID: {"ppid": OWNER_SHELL_PID, "command": "/Users/cj/.local/bin/claude --effort max"},
    AGENT_CHILD_BASH_PID: {"ppid": OWNER_PID, "command": "/bin/bash -c fleet session close"},
    AGENT_TOOL_PID: {"ppid": AGENT_CHILD_BASH_PID, "command": "/Users/cj/bin/fleet session close"},
}


@pytest.fixture
def machine(monkeypatch):
    """Inject the process table above; uid is OPERATOR_UID unless overridden."""
    table = {pid: dict(info) for pid, info in PROCESS_TABLE.items()}
    uids = {pid: OPERATOR_UID for pid in table}

    monkeypatch.setattr(registry, "_pid_exists", lambda pid: pid in table)
    monkeypatch.setattr(
        registry,
        "_inspect_process",
        lambda pid: (
            None
            if pid not in table
            else {
                "pid": pid,
                "alive": True,
                "inspectable": table[pid].get("inspectable", True),
                "ppid": table[pid]["ppid"],
                "pgid": pid,
                "tty": "ttys000",
            }
        ),
    )
    monkeypatch.setattr(
        registry, "_process_command", lambda pid: table.get(pid, {}).get("command")
    )
    monkeypatch.setattr(registry, "_process_uid", lambda pid: uids.get(pid))
    # The owner is alive and its identity is proven; this suite is about the
    # requester, and the dead-owner reap arm has its own tests.
    monkeypatch.setattr(registry, "_owner_identity_proven", lambda pid, ct: True)
    return {"table": table, "uids": uids}


class OperatorSeatIsAuthorized:
    pass


def test_operator_sibling_terminal_tab_may_close(machine):
    """The incident, inverted: tab B closes the lease tab A's agent holds."""
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", SIBLING_SHELL_PID)

    assert allowed is True, reason
    assert "operator seat" in reason


def test_terminal_app_itself_may_close(machine):
    """An ancestor is already allowed; the arm must not regress it."""
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", TERMINAL_PID)

    assert allowed is True, reason


def test_owner_and_descendant_still_authorized_by_lineage(machine):
    """The pre-existing arms answer first, with their own reasons."""
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", OWNER_PID)
    assert allowed is True
    assert "owner or a descendant" in reason

    allowed, reason = registry.authorize_session_close(
        conn, "sess-owner", AGENT_CHILD_BASH_PID
    )
    assert allowed is True
    assert "owner or a descendant" in reason


class AgentsStayBoundToLineage:
    pass


def test_agent_may_not_close_a_foreign_lease(machine):
    """The rule that must not soften: one agent cannot revoke another's lease."""
    conn = _conn()
    # A second, unrelated agent session owned by the operator's own uid.
    machine["table"][50000] = {"ppid": SIBLING_SHELL_PID, "command": "/Users/cj/.local/bin/claude --effort max"}
    machine["uids"][50000] = OPERATOR_UID
    _open_lease(conn, "sess-peer", 50000)

    # Requester is the first agent's Bash tool — same uid, but agent ancestry.
    allowed, reason = registry.authorize_session_close(
        conn, "sess-peer", AGENT_TOOL_PID
    )

    assert allowed is False
    assert "agent runtime" in reason


@pytest.mark.parametrize(
    "command",
    [
        "/Users/cj/.local/bin/claude --effort max",   # depth 1: the runtime itself
        "/opt/homebrew/bin/codex exec",
        "/usr/local/bin/opencode run",
        "/Users/cj/bin/grok chat",
        "python3 /Users/cj/.claude/hooks/fleet_guard_hook.py",  # runtime dir in the path
    ],
)
def test_every_roster_runtime_in_the_ancestry_declines_the_arm(machine, command):
    conn = _conn()
    machine["table"][60000] = {"ppid": SIBLING_SHELL_PID, "command": command}
    machine["table"][60001] = {"ppid": 60000, "command": "/Users/cj/bin/fleet session close"}
    machine["uids"][60000] = OPERATOR_UID
    machine["uids"][60001] = OPERATOR_UID
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", 60001)

    assert allowed is False, f"{command!r} was treated as the operator seat"
    assert "agent runtime" in reason


def test_agent_ancestry_is_found_at_depth(machine):
    """The walk is not depth-1: a runtime three hops up still declines the arm."""
    conn = _conn()
    machine["table"][70000] = {"ppid": AGENT_TOOL_PID, "command": "/bin/sh -c x"}
    machine["table"][70001] = {"ppid": 70000, "command": "/usr/bin/env python3"}
    machine["table"][70002] = {"ppid": 70001, "command": "/Users/cj/bin/fleet session close"}
    for pid in (70000, 70001, 70002):
        machine["uids"][pid] = OPERATOR_UID
    machine["table"][50001] = {"ppid": SIBLING_SHELL_PID, "command": "/Users/cj/.local/bin/claude --effort max"}
    machine["uids"][50001] = OPERATOR_UID
    _open_lease(conn, "sess-peer", 50001)

    allowed, reason = registry.authorize_session_close(conn, "sess-peer", 70002)

    assert allowed is False
    assert "agent runtime" in reason


class EveryUncertaintyDenies:
    pass


def test_different_uid_denies_before_the_arm_is_reached(machine):
    conn = _conn()
    machine["uids"][SIBLING_SHELL_PID] = OTHER_UID
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", SIBLING_SHELL_PID)

    assert allowed is False
    assert "uid" in reason


def test_unreadable_command_mid_walk_denies(machine):
    conn = _conn()
    machine["table"][SIBLING_SHELL_PID]["command"] = None
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", SIBLING_SHELL_PID)

    assert allowed is False
    assert "uninspectable" in reason


def test_uninspectable_ancestor_denies(machine):
    conn = _conn()
    machine["table"][TERMINAL_PID]["inspectable"] = False
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", SIBLING_SHELL_PID)

    assert allowed is False
    assert "uninspectable" in reason


def test_ppid_cycle_denies(machine):
    """A cycle OFF the owner's chain: unprovable, so no arm may authorize.

    The cycle is built between two fresh pids rather than by re-parenting the
    Terminal, because re-parenting would make the requester a genuine ancestor
    of the owner and the ancestor arm would authorize it correctly — proving
    nothing about the cycle.
    """
    conn = _conn()
    machine["table"][95000] = {"ppid": 95001, "command": "/bin/zsh"}
    machine["table"][95001] = {"ppid": 95000, "command": "/bin/zsh"}
    machine["uids"][95000] = OPERATOR_UID
    machine["uids"][95001] = OPERATOR_UID
    _open_lease(conn, "sess-owner", OWNER_PID)

    assert registry._agent_runtime_in_ancestry(95000) is None

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", 95000)

    assert allowed is False
    assert "uninspectable" in reason


def test_hop_budget_exhaustion_denies(machine):
    """A chain longer than the bound is unproven, not proven-clean."""
    chain_head = 80000
    prev = LAUNCHD_PID
    for i in range(registry.LINEAGE_MAX_HOPS + 5):
        pid = chain_head + i
        machine["table"][pid] = {"ppid": prev, "command": "/bin/zsh"}
        machine["uids"][pid] = OPERATOR_UID
        prev = pid

    assert registry._agent_runtime_in_ancestry(prev) is None


def test_unloadable_roster_denies(machine, monkeypatch):
    """A roster that cannot be read must never read as 'no agent'."""
    def _boom():
        raise ImportError("syshealth unavailable")

    monkeypatch.setattr(registry, "_agent_runtime_roster", _boom)
    assert registry._agent_runtime_in_ancestry(SIBLING_SHELL_PID) is None


def test_empty_roster_denies(machine, monkeypatch):
    monkeypatch.setattr(registry, "_agent_runtime_roster", lambda: [])
    assert registry._agent_runtime_in_ancestry(SIBLING_SHELL_PID) is None


class RosterHasOneSource:
    pass


def test_the_arm_reads_the_same_table_the_census_reads():
    """No drift: adding a runtime for the census arms authorization too."""
    assert registry._agent_runtime_roster() is syshealth.DEFAULT_SESSION_PATTERNS


def test_every_roster_entry_carries_both_reader_fields():
    for entry in syshealth.DEFAULT_SESSION_PATTERNS:
        assert entry.get("process_match"), entry
        assert entry.get("binary"), entry


def test_a_new_roster_entry_is_honoured_by_the_arm(machine, monkeypatch):
    """Positive control for the roster seam itself."""
    monkeypatch.setattr(
        registry,
        "_agent_runtime_roster",
        lambda: [{"name": "Vendor", "kind": "vendor", "process_match": r"/vendorbot\b", "binary": "vendorbot"}],
    )
    machine["table"][90000] = {"ppid": SIBLING_SHELL_PID, "command": "/opt/vendorbot --serve"}
    machine["table"][90001] = {"ppid": 90000, "command": "/Users/cj/bin/fleet"}
    machine["uids"][90000] = OPERATOR_UID
    machine["uids"][90001] = OPERATOR_UID

    assert registry._agent_runtime_in_ancestry(90001) is True
    # And the operator's own tab is still clean under that same roster.
    assert registry._agent_runtime_in_ancestry(SIBLING_SHELL_PID) is False


class ReapArmUnchanged:
    pass


def test_dead_owner_is_still_reapable_by_anyone(machine, monkeypatch):
    conn = _conn()
    monkeypatch.setattr(registry, "_owner_identity_proven", lambda pid, ct: False)
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", AGENT_TOOL_PID)

    assert allowed is True
    assert "provably dead" in reason
