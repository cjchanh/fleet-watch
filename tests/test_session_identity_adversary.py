"""Adversarial probes for the executable-identity census (commit 39687e9).

Independent-acceptance layer 3. Every probe proves its mechanism was reached
(positive control) before asserting the negative: for census rows, the control
is that the raw roster regex DOES hit the raw ps command text (base semantics
would have matched), while the identity window does not carry the token. A
probe that cannot show the control is vacuous and counts for nothing.

PLANTED_BAD fixtures are groomed rows engineered to look like an agent through
arguments alone. If any PLANTED_BAD row passes the validator as the mimicked
family, the batch is void.

Boundary behavior that is part of the fix's own contract is PINNED, not
asserted away: the interpreter-script slot is a documented window element, and
flag-shaped tokens (leading ``-``) are flag names by construction.

Every process fact is injected. Nothing here reads the live process table.
"""
from __future__ import annotations

import re
import sqlite3

import pytest

from fleet_watch import registry, syshealth


# --- injected process rows (same shape as test_session_identity_regression) --


def ps_row(pid: int, cmd: str) -> list[str]:
    return ["cj", str(pid), "0.0", "0.0", "0", "20480", "??", "S", "1:00PM", "1:00.00", cmd]


def fake_inspect(pid: int) -> dict:
    return {
        "pid": pid,
        "alive": True,
        "inspectable": True,
        "ppid": 1,
        "pgid": pid,
        "tty": "??",
    }


def census(monkeypatch, cmds: dict[int, str]) -> dict[int, str]:
    """Run the census over injected rows; return {pid: kind}."""
    monkeypatch.setattr(
        syshealth,
        "_ps_aux_lines",
        lambda: [ps_row(pid, cmd) for pid, cmd in cmds.items()],
    )
    monkeypatch.setattr(registry, "_inspect_process", fake_inspect)
    monkeypatch.setattr(registry, "_is_parent_chain_detached", lambda pid: False)
    sessions = syshealth.get_session_processes()
    return {s.pid: s.kind for s in sessions}


def window(cmd: str) -> str:
    return syshealth._census_identity_window(cmd)


# --- fixtures ---------------------------------------------------------------

ROSTER_RX = {p["kind"]: p["process_match"] for p in syshealth.DEFAULT_SESSION_PATTERNS}

# PLANTED_BAD #1: a groomed backup/archive row that mentions every roster
# family path in argument VALUES (flag values AND positionals). Base whole-
# command matching labeled this row an agent; it must be nobody. The literal
# /family paths are required so the raw roster regexes really do fire on the
# raw command text — that is the probe's positive control.
PLANTED_BAD_GROOMED_BACKUP = (
    "/usr/bin/rsync -av --partial "
    "--log-file=/codex-backups/rsync.log "
    "/Users/cj/claude-projects /mnt/devin-images /share/opencode-mirror "
    "--exclude '*.tmp'"
)

# PLANTED_BAD #2: a devin executable whose argument values mention BOTH /codex
# and /claude — roster order must not let either mention win over argv[0].
PLANTED_BAD_DEVIN_WITH_TWO_FAMILY_ARGS = (
    "/usr/local/bin/devin --note-file /tmp/claude-501/codex-handoff.md "
    "--workdir /tmp/devin-implants --model swe-2-max"
)

# A shell-wrapped real agent: documented fail-closed miss (shells are not
# interpreters, so the -c payload never reaches the window).
SH_WRAPPED_CODEX = 'sh -c /opt/homebrew/bin/codex --dangerously-bypass-approvals-and-sandbox'
NODE_LAUNCHED_CODEX = "node /opt/homebrew/bin/codex --dangerously-bypass-approvals-and-sandbox"

# Interpreter eval strings occupy the interpreter-script slot — the window's
# documented third element. Pinned, not called a mislabel: the eval string IS
# the script the interpreter executes, and the base behavior labeled these
# rows identically for every roster family.
NODE_EVAL_DEVIN = "node -e require('/devin/lib/client.js')"
PYTHON_EVAL_MULTIWORD = "python3 -c import shutil; shutil.copy('/codex/x', '/tmp/y')"

# A flag-shaped token (leading dash) is a flag NAME by construction — ps
# output cannot distinguish `-/codex/x` the flag from `-/codex/x` the value,
# and the window contract says flag names are identity. Pinned boundary.
DEVIN_DASH_PREFIXED_TOKEN = "/usr/local/bin/devin --include -/codex-notes/list.md"


# --- window mechanism (positive controls are the literal window contents) ---


def test_window_unit_non_interpreter_keeps_argv0_and_flag_names_only():
    cmd = PLANTED_BAD_GROOMED_BACKUP
    w = window(cmd)
    # control: the raw row DOES contain roster-family paths
    assert "/codex" in cmd and "/claude" in cmd and "/devin" in cmd
    # mechanism: argument values never entered the window
    assert w == "/usr/bin/rsync -av --partial --log-file --exclude"
    assert "/codex" not in w and "/claude" not in w and "/devin" not in w


def test_window_unit_interpreter_takes_first_nonflag_as_script():
    w = window(NODE_LAUNCHED_CODEX)
    assert w == "node /opt/homebrew/bin/codex --dangerously-bypass-approvals-and-sandbox"


def test_window_unit_shell_is_not_an_interpreter():
    w = window(SH_WRAPPED_CODEX)
    # mechanism: the script slot is empty for shells — `-c` is a flag name
    assert w == "sh -c --dangerously-bypass-approvals-and-sandbox"
    assert "codex" not in w


def test_window_unit_inline_flag_value_is_stripped_at_equals():
    w = window("/usr/local/bin/devin --workdir=/codex-20260926/repo")
    assert w == "/usr/local/bin/devin --workdir"


def test_window_unit_empty_command_has_empty_window():
    assert window("") == ""
    assert window("   ") == ""
    # control: a roster regex cannot match the empty window
    for rx in ROSTER_RX.values():
        assert re.search(rx, "") is None


# --- census: planted-bad rows must not pass as the mimicked family ----------


def test_planted_bad_groomed_backup_is_nobody(monkeypatch):
    """PLANTED_BAD: every family path mentioned in arguments; still nobody."""
    cmd = PLANTED_BAD_GROOMED_BACKUP
    # positive control: base whole-command semantics WOULD label this row
    assert re.search(ROSTER_RX["claude-code"], cmd) or re.search(ROSTER_RX["codex"], cmd)
    labels = census(monkeypatch, {30223: cmd})
    assert labels == {}
    # companion row: identical rsync with zero agent paths is also nobody —
    # the rejection is the WINDOW's verdict, not an accident of fixture shape
    clean = "/usr/bin/rsync -av --partial /src/docs /dst/docs"
    assert census(monkeypatch, {30224: clean}) == {}


def test_planted_bad_devin_two_family_args_labels_devin(monkeypatch):
    """PLANTED_BAD: /claude AND /codex argument values cannot relabel devin."""
    cmd = PLANTED_BAD_DEVIN_WITH_TWO_FAMILY_ARGS
    # positive control: raw row contains both foreign family tokens, and the
    # window carries only the devin identity
    assert "/claude" in cmd and "/codex" in cmd
    assert "/claude" not in window(cmd) and "/codex" not in window(cmd)
    labels = census(monkeypatch, {62279: cmd})
    assert labels == {62279: "devin"}


def test_shell_wrapped_agent_is_a_documented_miss_not_a_label(monkeypatch):
    """A real codex behind `sh -c` is MISSED — the fail-closed direction."""
    # positive control: the same payload launched under node IS counted
    assert census(monkeypatch, {101: NODE_LAUNCHED_CODEX}) == {101: "codex"}
    # and base semantics would have labeled the sh row as codex
    assert re.search(ROSTER_RX["codex"], SH_WRAPPED_CODEX)
    labels = census(monkeypatch, {102: SH_WRAPPED_CODEX})
    assert labels == {}


def test_interpreter_eval_string_occupies_script_slot(monkeypatch):
    """Boundary pin: `node -e STR` puts STR in the interpreter-script slot.

    The eval string IS the script the interpreter runs, so the window is doing
    its documented job. Asserted as mechanism truth, not as family verdict —
    and paired with the multi-word eval that fails closed below.
    """
    w = window(NODE_EVAL_DEVIN)
    assert w == "node -e require('/devin/lib/client.js')"
    labels = census(monkeypatch, {103: NODE_EVAL_DEVIN})
    assert labels == {103: "devin"}


def test_interpreter_eval_whose_first_token_is_benign_fails_closed(monkeypatch):
    """Multi-word eval: first non-flag token is `import`, not the path."""
    w = window(PYTHON_EVAL_MULTIWORD)
    assert w == "python3 -c import"
    labels = census(monkeypatch, {104: PYTHON_EVAL_MULTIWORD})
    assert labels == {}


def test_dash_prefixed_token_is_a_flag_name_by_construction(monkeypatch):
    """Boundary pin: `-/codex/x` is syntactically a flag name (identity).

    ps output cannot distinguish a value that starts with `-` from a flag;
    the window contract includes flag names. Positive control: the SAME row
    without the dash (`/codex/x` as a real value) is devin, unrelabeled.
    """
    clean = "/usr/local/bin/devin --include /codex-notes/list.md"
    assert census(monkeypatch, {105: clean}) == {105: "devin"}
    w = window(DEVIN_DASH_PREFIXED_TOKEN)
    assert w == "/usr/local/bin/devin --include -/codex-notes/list.md"
    labels = census(monkeypatch, {106: DEVIN_DASH_PREFIXED_TOKEN})
    assert labels == {106: "codex"}  # flag-position token, not an argument value


# --- custom-pattern API ------------------------------------------------------


def test_custom_pattern_explicit_command_scope_matches_whole_command(monkeypatch):
    """Explicit match_scope='command' keeps legacy semantics."""
    monkeypatch.setattr(
        syshealth, "_ps_aux_lines",
        lambda: [ps_row(4242, PLANTED_BAD_GROOMED_BACKUP)],
    )
    monkeypatch.setattr(registry, "_inspect_process", fake_inspect)
    monkeypatch.setattr(registry, "_is_parent_chain_detached", lambda pid: False)
    sessions = syshealth.get_session_processes(patterns=[
        {"name": "X", "kind": "x", "process_match": r"/codex", "match_scope": "command"},
    ])
    assert [s.kind for s in sessions] == ["x"]


def test_custom_pattern_opted_into_identity_scope_uses_window(monkeypatch):
    """A custom dict may OPT IN to identity scope — then it sees the window."""
    monkeypatch.setattr(
        syshealth, "_ps_aux_lines",
        lambda: [ps_row(4243, PLANTED_BAD_GROOMED_BACKUP)],
    )
    monkeypatch.setattr(registry, "_inspect_process", fake_inspect)
    monkeypatch.setattr(registry, "_is_parent_chain_detached", lambda pid: False)
    sessions = syshealth.get_session_processes(patterns=[
        {"name": "X", "kind": "x", "process_match": r"/codex", "match_scope": "identity"},
    ])
    assert sessions == []


# --- family walk: cycles terminate deterministically -------------------------


def _run_census_with_inspect(monkeypatch, rows, inspect):
    monkeypatch.setattr(
        syshealth, "_ps_aux_lines", lambda: [ps_row(pid, cmd) for pid, cmd in rows]
    )
    monkeypatch.setattr(registry, "_inspect_process", inspect)
    monkeypatch.setattr(registry, "_is_parent_chain_detached", lambda pid: False)
    return syshealth.get_session_processes()


def test_three_node_cycle_with_feeder_terminates(monkeypatch):
    """Cycle {20,30} plus feeder 10: terminates; cycle members group together."""
    rows = [
        (10, NODE_LAUNCHED_CODEX),
        (20, NODE_LAUNCHED_CODEX),
        (30, NODE_LAUNCHED_CODEX),
    ]
    ppid = {10: 20, 20: 30, 30: 20}

    def inspect(pid):
        return {
            "pid": pid, "alive": True, "inspectable": True,
            "ppid": ppid.get(pid, 1), "pgid": 50, "tty": "??",
        }

    sessions = _run_census_with_inspect(monkeypatch, rows, inspect)

    # positive control: normal acyclic grouping still works beside the cycle
    assert sorted(s.member_pids for s in sessions) == [[10], [20, 30]]
    assert len(sessions) == 2


def test_self_parented_row_terminates(monkeypatch):
    rows = [(77, NODE_LAUNCHED_CODEX)]

    def inspect(pid):
        return {
            "pid": pid, "alive": True, "inspectable": True,
            "ppid": 77, "pgid": 50, "tty": "??",  # self-parent
        }

    sessions = _run_census_with_inspect(monkeypatch, rows, inspect)
    assert len(sessions) == 1
    assert sessions[0].member_pids == [77]


def test_two_disjoint_cycles_same_pgid_stay_separate(monkeypatch):
    rows = [(20, NODE_LAUNCHED_CODEX), (30, NODE_LAUNCHED_CODEX),
            (70, NODE_LAUNCHED_CODEX), (80, NODE_LAUNCHED_CODEX)]
    ppid = {20: 30, 30: 20, 70: 80, 80: 70}

    def inspect(pid):
        return {
            "pid": pid, "alive": True, "inspectable": True,
            "ppid": ppid.get(pid, 1), "pgid": 50, "tty": "??",
        }

    sessions = _run_census_with_inspect(monkeypatch, rows, inspect)
    assert sorted(s.member_pids for s in sessions) == [[20, 30], [70, 80]]


def test_acyclic_chain_still_groups_by_external_ppid(monkeypatch):
    """Positive control: the bounded walk preserves normal grouping."""
    rows = [(100, NODE_LAUNCHED_CODEX), (200, NODE_LAUNCHED_CODEX)]
    ppid = {100: 50, 200: 100}

    def inspect(pid):
        return {
            "pid": pid, "alive": True, "inspectable": True,
            "ppid": ppid.get(pid, 1), "pgid": 50, "tty": "??",
        }

    sessions = _run_census_with_inspect(monkeypatch, rows, inspect)
    assert len(sessions) == 1
    assert sessions[0].member_pids == [100, 200]


# --- authorization: strictly stronger, never an operator path ----------------

OPERATOR_UID = 501
LAUNCHD_PID = 1
TERMINAL_PID = 900
OWNER_SHELL_PID = 21355
OWNER_PID = 21455
SIBLING_SHELL_PID = 31000


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


def inject_machine(monkeypatch, ancestry: dict[int, dict]):
    """Full ancestry table for the authorization walk."""
    table: dict[int, dict[str, object]] = {
        LAUNCHD_PID: {"ppid": 0, "command": "/sbin/launchd"},
        TERMINAL_PID: {
            "ppid": LAUNCHD_PID,
            "command": "/System/Applications/Utilities/Terminal.app/Contents/MacOS/Terminal",
        },
        OWNER_SHELL_PID: {"ppid": TERMINAL_PID, "command": "-zsh"},
        OWNER_PID: {"ppid": OWNER_SHELL_PID, "command": "/Users/demo/.local/bin/claude --effort max"},
        SIBLING_SHELL_PID: {"ppid": TERMINAL_PID, "command": "-zsh"},
    }
    table.update(ancestry)
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
    monkeypatch.setattr(registry, "_process_uid", lambda pid: OPERATOR_UID)
    monkeypatch.setattr(registry, "_owner_identity_proven", lambda pid, ct: True)


def test_devin_process_itself_cannot_close_peer_lease(monkeypatch):
    """The devin PID itself as requester — not even via a tool child."""
    inject_machine(monkeypatch, {
        62279: {"ppid": SIBLING_SHELL_PID, "command": "/usr/local/bin/devin --model swe-2-max"},
    })
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    # positive control: the human sibling tab keeps close authority
    allowed, reason = registry.authorize_session_close(conn, "sess-owner", SIBLING_SHELL_PID)
    assert allowed is True and "operator seat" in reason

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", 62279)
    assert allowed is False, reason
    assert "agent" in reason


def test_interpreter_wrapped_devin_ancestor_denies(monkeypatch):
    """`python3 /usr/bin/devin` in ancestry: binary-token match still catches it."""
    cmd = "python3 /usr/bin/devin --serve"
    # positive control: the authorization matcher sees the runtime token
    assert registry._command_is_agent_runtime(cmd, syshealth.DEFAULT_SESSION_PATTERNS)
    inject_machine(monkeypatch, {
        50001: {"ppid": SIBLING_SHELL_PID, "command": cmd},
        50002: {"ppid": 50001, "command": "/bin/bash -c fleet session close"},
    })
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", 50002)
    assert allowed is False, reason


def test_ppid_cycle_in_ancestry_denies_fail_closed(monkeypatch):
    """A PPID cycle in the requester's ancestry is uninspectable → DENY."""
    ancestry = {
        60001: {"ppid": 60002, "command": "-zsh"},
        60002: {"ppid": 60001, "command": "-zsh"},  # 60001 <-> 60002 cycle
        60003: {"ppid": 60001, "command": "/bin/bash -c fleet session close"},
    }
    inject_machine(monkeypatch, ancestry)
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    # positive control: point the requester at a clean ancestry → ALLOW
    allowed, reason = registry.authorize_session_close(conn, "sess-owner", SIBLING_SHELL_PID)
    assert allowed is True and "operator seat" in reason

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", 60003)
    assert allowed is False, reason
    assert "uninspectable" in reason or "fail-closed" in reason


def test_dead_owner_reaping_arm_unchanged(monkeypatch):
    """Positive control: provably-dead owner reap is still allowed for anyone."""
    inject_machine(monkeypatch, {
        70001: {"ppid": SIBLING_SHELL_PID, "command": "/usr/local/bin/devin acp"},
        70002: {"ppid": 70001, "command": "/bin/bash -c fleet session close"},
    })
    monkeypatch.setattr(registry, "_owner_identity_proven", lambda pid, ct: False)
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", 70002)
    assert allowed is True
    assert "provably dead" in reason


def test_unreadable_ancestry_command_denies(monkeypatch):
    """None from _process_command mid-chain must stay a DENY (fail-closed)."""
    inject_machine(monkeypatch, {
        80001: {"ppid": SIBLING_SHELL_PID, "command": None},  # unreadable
        80002: {"ppid": 80001, "command": "/bin/bash -c fleet session close"},
    })
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", 80002)
    assert allowed is False, reason
