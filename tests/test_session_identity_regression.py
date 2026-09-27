"""Census identity regression — label by executable identity, not argument mentions.

Proven problem (live, 2026-09-26): a Devin executable with a /codex-... path in
an argument was labelled Codex by the census (`/codex\b` searched the whole ps
command). The default roster also omitted Devin, so a Devin run with clean
arguments could read as the operator seat in ``authorize_session_close`` — an
agent with revocation authority.

These regressions pin both directions of the fix:
  * the CENSUS labels a process from its executable identity window
    (argv[0] + flag names + the interpreter's script), never from arbitrary
    argument values;
  * the AUTHORIZATION roster recognizes Devin as an agent runtime, which is
    strictly stronger: every previous conservative match and every DENY
    survives, and the operator-seat arm can no longer admit a Devin run.

PLANTED_BAD fixtures are adversarial rows the fixed validator must reject or
relabel. If any PLANTED_BAD row passes as the wrong family, the batch is void.
Custom-pattern semantics are pinned so the fix cannot silently narrow the
documented custom API (custom patterns intentionally match arbitrary commands).

Every process fact is injected. Nothing here reads the live process table.
"""
from __future__ import annotations

import sqlite3

import pytest

from fleet_watch import registry, syshealth


# --- injected process rows -------------------------------------------------


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


# --- real-shaped commands (sanitized; taken from live rows) ----------------

DEVIN_WITH_CODEX_PATH_ARG = (
    "/Users/cj/.local/bin/devin --prompt-file "
    "/Users/cj/.local/share/devin-mcp/prompts/composed/"
    "codex-devin-r4-review-20260925.composed.wO4lXEIF --model swe-2-max"
)
DEVIN_CLEAN = "/Users/cj/.local/bin/devin --prompt-file /tmp/clean.md --model swe-2-max"
DEVIN_INLINE_FLAG_VALUE = "/Users/cj/.local/bin/devin --workdir=/codex-20260926/repo"
RESTIC_BACKUP = (
    "/opt/homebrew/bin/restic -r sftp:pi:/mnt/archive26/t9/restic backup "
    "--tag hourly-leg-local /Users/cj/Documents "
    "--exclude=/Users/cj/Library/Application Support/Claude/claude-code-sessions "
    "--exclude=.git"
)
ZSH_SNAPSHOT_WITH_CODEX_ENV = (
    "/bin/zsh -c source /Users/cj/.claude/shell-snapshots/"
    "snapshot-zsh-1790453428831-839pcj.sh 2>/dev/null || true "
    "&& export PATH=/Users/cj/.local/share/codex/bin:$PATH"
)
BASH_DEVIN_DISPATCH_WITH_CLAUDE_SCRATCH = (
    "bash /Users/cj/ai/scripts/devin_dispatch.sh fire mr-craftfix-router-r3 "
    "/private/tmp/claude-501/-Users-cj/5c06b511-be4f-48c5-8d1f-e/work --mode smart --tmux"
)
NATIVE_CODEX = "/Users/cj/.local/bin/codex --resume abc123"
NODE_LAUNCHED_CODEX = "node /opt/homebrew/bin/codex --dangerously-bypass-approvals-and-sandbox"
VENDOR_CODEX = (
    "/opt/homebrew/lib/node_modules/@openai/codex/vendor/codex/codex "
    "--dangerously-bypass-approvals-and-sandbox"
)
CLAUDE_WITH_FLAGS = "/Users/cj/.local/bin/claude --effort max"
GROK_VERSIONED = "/usr/local/bin/grok-4.7 --scope web_and_x"
OPENCODE_CLI = "/Users/cj/.local/lib/node_modules/@opencode/cli/bin/opencode.exe serve --service"


# --- census: planted-bad rows must not relabel -----------------------------


def test_planted_bad_devin_with_codex_path_arg_is_devin_not_codex(monkeypatch):
    """THE proven bug: a Devin run whose prompt path contains /codex-..."""
    labels = census(monkeypatch, {62279: DEVIN_WITH_CODEX_PATH_ARG})
    assert labels == {62279: "devin"}


def test_planted_bad_mere_path_mention_is_not_an_agent(monkeypatch):
    """A backup tool that merely touches an agent's directory is not an agent."""
    labels = census(monkeypatch, {30223: RESTIC_BACKUP})
    assert labels == {}


def test_planted_bad_shell_snapshot_is_not_codex(monkeypatch):
    """A shell snapshot that exports CODEX_* env is not a Codex session."""
    labels = census(monkeypatch, {54363: ZSH_SNAPSHOT_WITH_CODEX_ENV})
    assert labels == {}


def test_planted_bad_dispatch_script_with_claude_scratch_path_is_not_claude(monkeypatch):
    """A dispatch harness whose scratch path contains /claude-501/ is not Claude."""
    labels = census(monkeypatch, {54014: BASH_DEVIN_DISPATCH_WITH_CLAUDE_SCRATCH})
    assert labels == {}


def test_flag_inline_value_mention_is_not_codex(monkeypatch):
    """--workdir=/codex-... is an argument value, not the process identity."""
    labels = census(monkeypatch, {70001: DEVIN_INLINE_FLAG_VALUE})
    assert labels == {70001: "devin"}


# --- census: legitimate variants stay observable (pass before AND after) ---


def test_native_codex_visible(monkeypatch):
    labels = census(monkeypatch, {100: NATIVE_CODEX})
    assert labels == {100: "codex"}


def test_node_launched_codex_visible(monkeypatch):
    labels = census(monkeypatch, {101: NODE_LAUNCHED_CODEX})
    assert labels == {101: "codex"}


def test_vendor_codex_visible(monkeypatch):
    labels = census(monkeypatch, {102: VENDOR_CODEX})
    assert labels == {102: "codex"}


def test_claude_with_flags_visible(monkeypatch):
    labels = census(monkeypatch, {103: CLAUDE_WITH_FLAGS})
    assert labels == {103: "claude-code"}


def test_grok_versioned_executable_visible(monkeypatch):
    labels = census(monkeypatch, {104: GROK_VERSIONED})
    assert labels == {104: "grok"}


def test_opencode_visible(monkeypatch):
    labels = census(monkeypatch, {105: OPENCODE_CLI})
    assert labels == {105: "opencode"}


def test_devin_clean_invocation_visible_as_devin(monkeypatch):
    labels = census(monkeypatch, {106: DEVIN_CLEAN})
    assert labels == {106: "devin"}


# --- custom-pattern API is not silently narrowed ---------------------------


def test_custom_patterns_keep_whole_command_semantics(monkeypatch):
    """Custom patterns intentionally match arbitrary commands — unchanged."""
    monkeypatch.setattr(
        syshealth,
        "_ps_aux_lines",
        lambda: [ps_row(4242, DEVIN_WITH_CODEX_PATH_ARG)],
    )
    monkeypatch.setattr(registry, "_inspect_process", fake_inspect)
    monkeypatch.setattr(registry, "_is_parent_chain_detached", lambda pid: False)
    sessions = syshealth.get_session_processes(
        patterns=[{"name": "X", "kind": "x", "process_match": r"/codex"}]
    )
    assert [s.kind for s in sessions] == ["x"]


def test_grouping_walk_survives_parent_cycle(monkeypatch):
    """A PPID cycle among matched rows must terminate, not hang the census.

    On the base behavior this loops forever in the family walk (found while
    authoring these tests with a self-parented row); after the fix the walk is
    bounded and the two members still collapse into one session.
    """
    monkeypatch.setattr(
        syshealth,
        "_ps_aux_lines",
        lambda: [ps_row(100, NATIVE_CODEX), ps_row(200, VENDOR_CODEX)],
    )

    def cycle_inspect(pid: int) -> dict:
        ppid = {100: 200, 200: 100}.get(pid, 1)
        return {
            "pid": pid,
            "alive": True,
            "inspectable": True,
            "ppid": ppid,
            "pgid": 50,
            "tty": "??",
        }

    monkeypatch.setattr(registry, "_inspect_process", cycle_inspect)
    monkeypatch.setattr(registry, "_is_parent_chain_detached", lambda pid: False)

    sessions = syshealth.get_session_processes()

    assert len(sessions) == 1
    assert sorted(sessions[0].member_pids) == [100, 200]


# --- roster shape ----------------------------------------------------------


def test_roster_contains_devin_with_binary_for_authorization():
    kinds = {p["kind"] for p in syshealth.DEFAULT_SESSION_PATTERNS}
    assert "devin" in kinds
    devin = next(p for p in syshealth.DEFAULT_SESSION_PATTERNS if p["kind"] == "devin")
    assert devin.get("binary") == "devin"


# --- authorization: the roster repair is strictly stronger -----------------

OPERATOR_UID = 501
LAUNCHD_PID = 1
TERMINAL_PID = 900
OWNER_SHELL_PID = 21355
OWNER_PID = 21455
SIBLING_SHELL_PID = 31000
DEVIN_PID = 62279
DEVIN_TOOL_PID = 62280


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


def inject_machine(monkeypatch, devin_command: str) -> None:
    """Terminal -> owner shell -> claude owner; sibling shell -> devin -> tool."""
    table: dict[int, dict[str, object]] = {
        LAUNCHD_PID: {"ppid": 0, "command": "/sbin/launchd"},
        TERMINAL_PID: {
            "ppid": LAUNCHD_PID,
            "command": "/System/Applications/Utilities/Terminal.app/Contents/MacOS/Terminal",
        },
        OWNER_SHELL_PID: {"ppid": TERMINAL_PID, "command": "-zsh"},
        SIBLING_SHELL_PID: {"ppid": TERMINAL_PID, "command": "-zsh"},
        OWNER_PID: {"ppid": OWNER_SHELL_PID, "command": "/Users/demo/.local/bin/claude --effort max"},
        DEVIN_PID: {"ppid": SIBLING_SHELL_PID, "command": devin_command},
        DEVIN_TOOL_PID: {"ppid": DEVIN_PID, "command": "/bin/bash -c fleet session close"},
    }
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


def test_devin_ancestry_is_not_the_operator_seat(monkeypatch):
    """Load-bearing: a Devin run with CLEAN arguments must not read as a human.

    Before the roster repair the ancestry walk found no agent runtime and the
    operator-seat arm ALLOWED the close — an agent holding peer revocation
    authority. This regression fails on the base behavior.
    """
    inject_machine(monkeypatch, DEVIN_CLEAN)
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", DEVIN_TOOL_PID)

    assert allowed is False, reason
    assert "agent" in reason


def test_planted_bad_devin_with_codex_arg_still_denied(monkeypatch):
    """Conservative direction preserved: the over-match still denies."""
    inject_machine(monkeypatch, DEVIN_WITH_CODEX_PATH_ARG)
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", DEVIN_TOOL_PID)

    assert allowed is False, reason


def test_operator_seat_still_allowed_without_any_agent(monkeypatch):
    """Positive control: the human sibling tab keeps its close authority."""
    table_extra = {}
    inject_machine(monkeypatch, DEVIN_CLEAN)
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", SIBLING_SHELL_PID)

    assert allowed is True, reason
    assert "operator seat" in reason
    assert table_extra == {}


def test_owner_lineage_arms_unaffected(monkeypatch):
    """Positive control: owner and descendant arms answer first, as before."""
    inject_machine(monkeypatch, DEVIN_CLEAN)
    conn = _conn()
    _open_lease(conn, "sess-owner", OWNER_PID)

    allowed, reason = registry.authorize_session_close(conn, "sess-owner", OWNER_PID)
    assert allowed is True, reason
    assert "owner or a descendant" in reason
