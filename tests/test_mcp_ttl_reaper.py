"""The MCP TTL reaper — cadence wrapper over the verified reclaim gate.

Spec 2627003. What these tests hold:

  * DRY-RUN IS THE DEFAULT. With candidates present and no flags, the payload's
    ``signals_attempted`` is empty and the gate's own dry-run receipt is written
    (a dry run that decided nothing would be indistinguishable from a clean host).
  * THE SIGNAL PATH IS BEHIND THE OPERATOR FLIP. ``--apply`` alone is refused and
    the run continues as a dry run; the path opens only with
    ``--i-understand-this-signals``, and even then the gate's denials stand.
  * FORCE IS NEVER AUTHORIZED. There is no flag for it and the call pins
    ``do_kill_force=False``, so this reaper cannot escalate to SIGKILL.
  * SUPERVISED KEEPERS ARE EXCLUDED AND NAMED. A ``com.cds.mcp-gateway*`` row is
    a keeper in the receipt, never a candidate.
  * RECEIPT BEFORE SIGNAL. The ordering assertion is made by the signal sender
    itself: it records how many PROCESS_DECISION rows existed at the moment it
    was called, so the interleaving is observed, not assumed.

THE PLANTED BADS (marked ``PLANTED BAD``): a live owning session, a recycled
target PID, a live session lease, and a supervised keeper. Each is driven through
the MOST AGGRESSIVE flag combination this reaper accepts
(``--apply --i-understand-this-signals``) and must come back with ZERO signals.
If any planted bad is ever signalled, the batch is void.

Determinism: every probe, clock, lease table, signal sender, supervision map,
process table and registry connection is injected. No test creates, observes or
signals a real process. The one real subprocess in the module (``ps``, read-only)
is exercised through a parsed string.
"""

from __future__ import annotations

import ast
import inspect
import json
import signal as signal_mod
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fleet_watch import process_policy as pp
from fleet_watch import ttl_reaper as T
from fleet_watch.discovery import mcp_orphan_detector as M

reap_mod = T.reap_mod

# Captured before any test patches it, so repeated wiring in one test never
# stacks a wrapper on top of the previous test's wrapper.
REAL_BUILD_CANDIDATES = M.build_candidates
REAL_SCAN_MCP_PROCESSES = M.scan_mcp_processes

TARGET = 4242
OWNER = 777
TARGET_CT = "CT-1"
OWNER_CT = "OCT-1"
START = 1_700_000_000.0
MCP_CMD = "python3 /cds/mcp_lexicon_engine_server.py"


# ── deterministic fakes ──────────────────────────────────────────────────────

class FakeClock:
    def __init__(self, start: float = START) -> None:
        self.t = start
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


class FakeWorld:
    """One fake kernel + registry, plus the audit log and the signal trap."""

    def __init__(self, *, start: float = START) -> None:
        self.clock = FakeClock(start)
        self.pids: dict[int, dict[str, Any]] = {}
        self.leases: list[dict[str, Any]] = []
        self.lease_alive: dict[str, bool] = {}
        self.raise_on: dict[str, BaseException] = {}
        self.signals: list[tuple[int, int]] = []
        # (events_already_logged, pid, sig) — the receipt-before-signal proof.
        self.signal_calls: list[tuple[int, int, int]] = []
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.caller_uid = 501
        self.self_pid = 1001
        self.parent_pid = 555

    def add(
        self,
        pid: int,
        *,
        create_time: str,
        exe: str = "/usr/bin/python3",
        uid: int = 501,
        ppid: int = 0,
        exists: bool = True,
    ) -> "FakeWorld":
        self.pids[pid] = {
            "create_time": create_time, "exe": exe, "uid": uid,
            "ppid": ppid, "exists": exists,
        }
        return self

    def orphan(self, *, owner_pid: int = OWNER, owner_exists: bool = False) -> "FakeWorld":
        return self.add(TARGET, create_time=TARGET_CT, ppid=owner_pid).add(
            owner_pid, create_time=OWNER_CT, ppid=1, exists=owner_exists,
        )

    def _guard(self, name: str) -> None:
        exc = self.raise_on.get(name)
        if exc is not None:
            raise exc

    def probes(self) -> pp.Probes:
        w = self

        def exists(pid: int) -> bool:
            w._guard("exists")
            return bool(w.pids.get(pid, {}).get("exists"))

        def create_time(pid: int) -> str | None:
            w._guard("create_time")
            row = w.pids.get(pid)
            if row is None or not row["exists"]:
                return None
            return row["create_time"]

        def exe(pid: int) -> str | None:
            w._guard("exe")
            row = w.pids.get(pid)
            return row["exe"] if row and row["exists"] else None

        def uid(pid: int) -> int | None:
            w._guard("uid")
            row = w.pids.get(pid)
            return row["uid"] if row and row["exists"] else None

        return pp.Probes(
            now=self.clock.now,
            sleep=self.clock.sleep,
            exists=exists,
            create_time=create_time,
            exe=exe,
            uid=uid,
            self_pid=lambda: self.self_pid,
            parent_pid=lambda: self.parent_pid,
            caller_uid=lambda: self.caller_uid,
        )

    def reclaim(self) -> pp.ReclaimProbes:
        w = self

        def ppid(pid: int) -> int | None:
            w._guard("ppid")
            row = w.pids.get(pid)
            if row is None or not row["exists"]:
                return None
            return row["ppid"]

        def active_leases() -> list[dict[str, Any]]:
            w._guard("active_leases")
            return list(w.leases)

        def lease_owner_alive(lease: dict[str, Any]) -> bool:
            w._guard("lease_owner_alive")
            return bool(w.lease_alive.get(str(lease.get("session_id"))))

        return pp.ReclaimProbes(
            ppid=ppid,
            exists=lambda pid: w.pids.get(pid, {}).get("exists", False),
            create_time=lambda pid: (w.pids.get(pid) or {}).get("create_time"),
            active_leases=active_leases,
            lease_owner_alive=lease_owner_alive,
        )

    def sender(self):
        """The signal trap. Records the audit-log depth AT THE MOMENT of the call."""
        w = self

        def send(pid: int, sig: int) -> None:
            w.signal_calls.append((len(w.events), pid, sig))
            w.signals.append((pid, sig))

        return send

    def log_event(self, kind: str, detail: dict[str, Any]) -> None:
        self.events.append((kind, detail))


class FakeConn:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _rows(world: FakeWorld, *, pid: int = TARGET, owner_pid: int = OWNER) -> list[dict[str, Any]]:
    """A ps row the detector will turn into a dead-session candidate."""
    return [{"pid": pid, "ppid": owner_pid, "rss_mb": 70,
             "cmd": MCP_CMD, "create_time": world.pids[pid]["create_time"]}]


def _idle(elapsed: int = 4000, cpu: float = 0.0) -> T.IdleSample:
    return T.IdleSample(pid=TARGET, elapsed_seconds=elapsed, cpu_pct=cpu)


def _wire(
    monkeypatch: pytest.MonkeyPatch,
    world: FakeWorld,
    rows: list[dict[str, Any]],
    *,
    supervision: dict[int, str] | None = None,
    samples: dict[int, T.IdleSample] | None = None,
    idle_error: str | None = None,
    scan_error: str | None = None,
) -> dict[str, Any]:
    """Inject every seam the reaper and the gate beneath it use.

    The supervisor map is patched on the DETECTOR, which is where both the
    keeper list and the candidate partition read it — so a test that changes it
    changes both halves, exactly as a real launchd state change would.
    """
    conn = FakeConn()
    events = world.events
    real_build = REAL_BUILD_CANDIDATES

    def build_with_the_fake_world(rows, *, exists=None, create_time=None, _w=world):
        """Inject the fake kernel into the DETECTOR's own split.

        ``scan_candidates`` takes ``exists``/``create_time`` seams; the reaper
        calls it with the production defaults, so the test wires the world's
        truth in here rather than letting a real ``ps`` decide the outcome.
        """
        return real_build(
            rows,
            exists=lambda pid: bool(_w.pids.get(pid, {}).get("exists")),
            create_time=lambda pid: (_w.pids.get(pid) or {}).get("create_time"),
        )

    monkeypatch.setattr(M, "build_candidates", build_with_the_fake_world)
    monkeypatch.setattr(
        reap_mod.mcp_orphan_detector, "scan_mcp_processes", lambda: (list(rows), scan_error),
    )
    monkeypatch.setattr(
        M, "_launchd_supervised_pids", lambda: supervision,
    )
    monkeypatch.setattr(
        reap_mod, "_mcp_identity_probes", world.probes,
    )
    monkeypatch.setattr(reap_mod, "_mcp_reclaim_probes", lambda _conn: world.reclaim())
    monkeypatch.setattr(reap_mod, "_mcp_signal_sender", world.sender)
    monkeypatch.setattr(reap_mod, "_get_conn", lambda: conn)
    monkeypatch.setattr(
        reap_mod.events, "log_event",
        lambda _conn, kind, **k: world.log_event(kind, k.get("detail", {})),
    )
    if idle_error is not None:
        idle_fn = lambda _pids: ({}, idle_error)  # noqa: E731
    else:
        table = {TARGET: _idle()} if samples is None else samples
        idle_fn = lambda _pids: (dict(table), None)  # noqa: E731
    return {"conn": conn, "idle_fn": idle_fn, "events": events}


def _run(monkeypatch: pytest.MonkeyPatch, world: FakeWorld, wired: dict[str, Any], **kwargs: Any):
    kwargs.setdefault("idle_probe", wired["idle_fn"])
    return T.run_once(**kwargs)


# ── structural checks on the module's own code (not its prose) ───────────────

def _tree() -> ast.Module:
    return ast.parse(inspect.getsource(T))


def _code_names() -> set[str]:
    """Every identifier and attribute the module's CODE can call or hold.

    Docstrings and comments are deliberately not in scope: the module's prose is
    allowed to describe SIGKILL, launchctl and force while its code holds no way
    to reach any of them.
    """
    names: set[str] = set()
    for node in ast.walk(_tree()):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
    return names


def _subprocess_run_calls() -> list[ast.Call]:
    return [
        node for node in ast.walk(_tree())
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "run"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "subprocess"
    ]


# ── AC1: dry-run default, candidates present, zero signals ───────────────────

def test_dry_run_reports_candidates_and_signals_nothing(monkeypatch):
    """The headline acceptance: candidates ARE present, signals_attempted == []."""
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))
    payload = _run(monkeypatch, world, wired)

    assert payload["mode"] == "dry-run"
    assert payload["signal_path"] == "closed:default_dry_run"
    assert len(payload["candidates"]) == 1, "the candidate must actually be there"
    assert payload["candidates"][0]["pid"] == TARGET
    assert payload["signals_attempted"] == []
    assert world.signals == []
    # The gate really was asked, and it really decided — a dry run that skipped
    # the gate could not tell a denied candidate from an unreported one.
    assert payload["gate_consulted"] is True
    assert payload["decisions_receipted"] == 1
    assert payload["verified"]["dry_run"] is True
    assert payload["verified"]["kill"] is False
    assert payload["verified"]["decisions"][0]["outcome"] == "reported"
    assert wired["conn"].closed is True
    # The decision IS in the hash chain, before anything could have been sent.
    assert [kind for kind, _ in world.events] == ["PROCESS_DECISION"]


def test_dry_run_candidate_carries_its_idle_and_ttl_facts(monkeypatch):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world), samples={TARGET: _idle(elapsed=4000)})
    payload = _run(monkeypatch, world, wired, ttl_seconds=600)
    candidate = payload["candidates"][0]
    assert candidate["elapsed_seconds"] == 4000
    assert candidate["cpu_pct"] == 0.0
    assert candidate["ttl_seconds"] == 600
    assert candidate["create_time"] == TARGET_CT
    assert candidate["source"] == "mcp"
    assert payload["signals_attempted"] == []


def test_a_young_candidate_is_held_with_a_reason_and_never_asked_about(monkeypatch):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world), samples={TARGET: _idle(elapsed=120)})
    payload = _run(monkeypatch, world, wired)
    assert payload["candidates"] == []
    assert payload["held"] == [
        {"pid": TARGET, "reason": "younger_than_ttl",
         "elapsed_seconds": 120, "cpu_pct": 0.0}
    ]
    assert payload["gate_consulted"] is False  # nothing to ask about
    assert world.events == [] and world.signals == []


def test_a_busy_candidate_is_held_even_when_older_than_the_ttl(monkeypatch):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world), samples={TARGET: _idle(elapsed=99999, cpu=4.5)})
    payload = _run(monkeypatch, world, wired)
    assert payload["candidates"] == []
    assert payload["held"][0]["reason"] == "busy_cpu"
    assert world.signals == []


def test_an_unreadable_idle_probe_holds_everything_it_cannot_prove(monkeypatch):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world), idle_error="ps_nonzero_exit")
    payload = _run(monkeypatch, world, wired)
    assert payload["idle_probe_error"] == "ps_nonzero_exit"
    assert payload["candidates"] == []
    assert payload["held"][0]["reason"] == "idle_unreadable"
    assert world.signals == []


def test_a_scan_failure_is_unknown_with_no_candidates_and_no_gate_call(monkeypatch):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world), scan_error="ps_nonzero_exit")
    payload = _run(monkeypatch, world, wired)
    assert payload["snapshot_error"] == "ps_nonzero_exit"
    assert payload["scanned"] == 0
    assert payload["candidates"] == [] and payload["keepers"] == []
    assert payload["gate_consulted"] is False
    assert world.signals == [] and world.events == []


def test_a_negative_ttl_is_refused_rather_than_reaping_everything(monkeypatch):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))
    with pytest.raises(T.TtlReaperError):
        _run(monkeypatch, world, wired, ttl_seconds=-1)


# ── AC2: the signal path is unreachable without the operator flip ────────────

def test_apply_without_the_operator_flip_is_refused_and_sends_nothing(monkeypatch):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))
    payload = _run(monkeypatch, world, wired, apply=True, operator_flip=False)
    assert payload["mode"] == "dry-run"
    assert payload["signal_path"] == f"refused:operator_flip_required:{T.SIGNAL_FLIP_FLAG}"
    assert payload["signals_attempted"] == []
    assert world.signals == []
    # Refused, not ignored: the candidates are still reported and still decided.
    assert len(payload["candidates"]) == 1
    assert payload["verified"]["kill"] is False


def test_the_flip_flag_alone_does_not_open_the_path(monkeypatch):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))
    payload = _run(monkeypatch, world, wired, apply=False, operator_flip=True)
    assert payload["signal_path"] == "closed:default_dry_run"
    assert payload["signals_attempted"] == [] and world.signals == []


@pytest.mark.parametrize(
    ("apply_requested", "operator_flip", "expected_open", "expected_prefix"),
    [
        (False, False, False, "closed:default_dry_run"),
        (False, True, False, "closed:default_dry_run"),
        (True, False, False, "refused:operator_flip_required"),
        (True, True, True, "open:operator_flip"),
    ],
)
def test_the_flip_resolves_both_ways(apply_requested, operator_flip, expected_open, expected_prefix):
    is_open, reason = T.resolve_signal_path(apply_requested, operator_flip)
    assert is_open is expected_open
    assert reason.startswith(expected_prefix)


def test_this_reaper_never_authorizes_force(monkeypatch):
    """No flag exists for force, and the gate call pins it off even when open.

    With both flags the gate DOES act on a provably-dead-session orphan — that
    is the operator's request being honoured — and the only signal that leaves is
    one SIGTERM. There is no second one.
    """
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))
    payload = _run(monkeypatch, world, wired, apply=True, operator_flip=True)
    assert payload["force_authorized"] is False
    assert payload["verified"]["kill_force"] is False
    assert "SIGKILL" not in T.build_parser().format_help()
    assert "force" not in T.build_parser().format_help().lower()
    assert world.signals == [(TARGET, int(signal_mod.SIGTERM))]
    assert [sig for _pid, sig in world.signals] == [int(signal_mod.SIGTERM)]
    # The fake world survives the SIGTERM, so the gate reports `survived` — and
    # reports it rather than escalating, which is the whole point.
    assert payload["verified"]["failed"][0]["action"] == "sigterm"
    assert payload["verified"]["failed"][0]["outcome"] == "survived"
    assert payload["verified"]["failed"][0]["reason"] == "grace_timeout_force_not_requested"


def test_the_parser_offers_no_force_flag_and_defaults_to_dry_run():
    args = T.build_parser().parse_args([])
    assert args.apply is False and args.operator_flip is False
    assert args.ttl_seconds == 600
    both = T.build_parser().parse_args(["--apply", T.SIGNAL_FLIP_FLAG])
    assert both.apply is True and both.operator_flip is True


def test_the_cli_default_invocation_sends_no_signal_even_with_candidates(monkeypatch, capsys):
    """The default invocation, through the real main(), with candidates present."""
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))
    monkeypatch.setattr(T, "read_idle_samples", wired["idle_fn"])
    assert T.main([]) == 0
    out = capsys.readouterr().out
    assert "DRY-RUN" in out and "signals=0" in out and "candidates=1" in out
    assert world.signals == [] and world.signal_calls == []
    # The decision was still receipted: a dry run that decided nothing would be
    # indistinguishable from a clean host.
    assert [kind for kind, _ in world.events] == ["PROCESS_DECISION"]


def test_the_cli_prints_the_full_receipt_as_json(monkeypatch, capsys):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))
    monkeypatch.setattr(T, "read_idle_samples", wired["idle_fn"])
    assert T.main(["--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "fleet-watch/mcp-ttl-reaper/v1"
    assert payload["signals_attempted"] == []
    assert world.signals == []


def test_the_cli_exits_non_zero_when_the_host_cannot_be_scanned(monkeypatch, capsys):
    world = FakeWorld().orphan()
    _wire(monkeypatch, world, _rows(world), scan_error="ps_nonzero_exit")
    assert T.main([]) == 1  # UNKNOWN is not a clean tick
    assert "ps_nonzero_exit" in capsys.readouterr().err
    assert world.signals == []


def test_the_cli_apply_without_the_flip_is_refused_through_main(monkeypatch, capsys):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))
    monkeypatch.setattr(T, "read_idle_samples", wired["idle_fn"])
    assert T.main(["--apply", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "dry-run"
    assert payload["signal_path"].startswith("refused:operator_flip_required")
    assert payload["signals_attempted"] == []
    assert world.signals == []


# ── AC4: the receipt precedes the signal (ordering observed, not assumed) ─────

def test_the_decision_receipt_is_in_the_chain_before_any_signal(monkeypatch):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))

    def send_and_exit(pid: int, sig: int) -> None:
        world.signal_calls.append((len(world.events), pid, sig))
        world.signals.append((pid, sig))
        world.pids[pid]["exists"] = False

    monkeypatch.setattr(reap_mod, "_mcp_signal_sender", lambda: send_and_exit)
    payload = _run(monkeypatch, world, wired, apply=True, operator_flip=True)

    assert len(world.signal_calls) == 1
    receipts_at_signal_time, pid, sig = world.signal_calls[0]
    assert receipts_at_signal_time == 1, "the plan receipt was not written before the signal"
    assert [kind for kind, _ in world.events][:1] == ["PROCESS_DECISION"]
    assert pid == TARGET and sig == int(signal_mod.SIGTERM)
    assert payload["decisions_receipted"] == 1
    assert payload["signals_attempted"] == [
        {"pid": TARGET, "signal": int(signal_mod.SIGTERM), "action": "sigterm",
         "outcome": "exited", "reason": "graceful_exit"}
    ]
    # plan first, then the outcome — the same order the gate has always used.
    assert [d["phase"] for _, d in world.events] == ["plan", "outcome"]


def test_a_refused_audit_writes_no_signal(monkeypatch):
    """No receipt in the chain ⇒ no signal. Fail-closed on the audit itself."""
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))

    def refuse(*_a, **_k):
        raise RuntimeError("chain refused the write")

    monkeypatch.setattr(reap_mod.events, "log_event", refuse)
    with pytest.raises(RuntimeError):
        _run(monkeypatch, world, wired, apply=True, operator_flip=True)
    assert world.signals == [] and world.signal_calls == []


# ── AC1b: supervised keepers are excluded and named in the receipt ────────────

def test_supervised_gateway_processes_are_keepers_not_candidates(monkeypatch):
    world = FakeWorld().orphan()
    rows = _rows(world) + [
        {"pid": 9001, "ppid": 1, "rss_mb": 40, "cmd": MCP_CMD, "create_time": "KCT-1"},
        {"pid": 9002, "ppid": 1, "rss_mb": 40, "cmd": MCP_CMD, "create_time": "KCT-2"},
    ]
    supervision = {
        9001: "com.cds.mcp-gateway",
        9002: "com.cds.mcp-gateway-session-memory",
    }
    wired = _wire(monkeypatch, world, rows, supervision=supervision)
    payload = _run(monkeypatch, world, wired)

    assert {k["pid"] for k in payload["keepers"]} == {9001, 9002}
    assert {k["label"] for k in payload["keepers"]} == {
        "com.cds.mcp-gateway", "com.cds.mcp-gateway-session-memory",
    }
    assert all("cmd" in k for k in payload["keepers"])
    # Excluded from the candidate set — a keeper is never a reclaim target.
    assert {c["pid"] for c in payload["candidates"]} == {TARGET}
    assert 9001 not in {c["pid"] for c in payload["candidates"]}
    assert 9002 not in {c["pid"] for c in payload["candidates"]}
    assert payload["supervision_map"] == "read"
    assert world.signals == []


def test_an_unlabeled_supervised_process_is_not_claimed_as_a_keeper(monkeypatch):
    """The keeper list is label-scoped, but the EXCLUSION is not: the detector's
    own supervision partition holds any supervised pid, named or not."""
    world = FakeWorld().orphan()
    rows = _rows(world) + [
        {"pid": 9003, "ppid": 1, "rss_mb": 40, "cmd": MCP_CMD, "create_time": "KCT-3"},
    ]
    wired = _wire(monkeypatch, world, rows, supervision={9003: "com.cds.something-else"})
    payload = _run(monkeypatch, world, wired)
    assert payload["keepers"] == []
    assert 9003 not in {c["pid"] for c in payload["candidates"]}
    assert world.signals == []


def test_an_unreadable_supervision_map_fails_closed_and_says_so(monkeypatch):
    """No supervision map: the reaper cannot tell a keeper from an orphan, so it
    holds everything reparented and reports the map as unreadable."""
    world = FakeWorld().orphan()
    rows = _rows(world) + [
        {"pid": 9004, "ppid": 1, "rss_mb": 40, "cmd": MCP_CMD, "create_time": "KCT-4"},
    ]
    wired = _wire(monkeypatch, world, rows, supervision=None)
    payload = _run(monkeypatch, world, wired)
    assert payload["supervision_map"] == "unreadable"
    assert payload["keepers"] == []
    assert 9004 not in {c["pid"] for c in payload["candidates"]}
    assert world.signals == []


def test_a_healthy_host_with_nothing_to_reap_never_opens_the_gate(monkeypatch):
    world = FakeWorld()
    wired = _wire(monkeypatch, world, [], supervision={})
    payload = _run(monkeypatch, world, wired)
    assert payload["scanned"] == 0
    assert payload["gate_consulted"] is False
    assert payload["signals_attempted"] == []
    assert world.events == [] and world.signals == []
    assert wired["conn"].closed is False  # no connection was ever opened


# ── the TTL split itself (pure) ──────────────────────────────────────────────

def _candidate(pid: int = TARGET, owner: int = OWNER) -> M.MCPCandidate:
    return M.MCPCandidate(
        pid=pid, ppid=owner, rss_mb=70, cmd=MCP_CMD, create_time=TARGET_CT,
        owner=M.MCPOwnerSnapshot(
            session_pid=owner, session_create_time=OWNER_CT,
            reparented=False, alive_at_scan=False,
        ),
        evidence=("owning session absent at scan time",),
    )


def test_a_candidate_younger_than_the_ttl_is_held():
    expired, held = T.plan_ttl(
        [_candidate()], {TARGET: _idle(elapsed=599)}, ttl_seconds=600, idle_cpu_pct=1.0)
    assert expired == [] and held[0].reason == "younger_than_ttl"


def test_a_candidate_exactly_at_the_ttl_expires():
    expired, held = T.plan_ttl(
        [_candidate()], {TARGET: _idle(elapsed=600)}, ttl_seconds=600, idle_cpu_pct=1.0)
    assert [c.pid for c in expired] == [TARGET] and held == []


def test_cpu_exactly_at_the_bar_counts_as_busy():
    _expired, held = T.plan_ttl(
        [_candidate()], {TARGET: _idle(elapsed=99999, cpu=1.0)}, ttl_seconds=600, idle_cpu_pct=1.0)
    assert held[0].reason == "busy_cpu"


def test_a_candidate_with_no_idle_sample_is_unreadable_not_expired():
    _expired, held = T.plan_ttl(
        [_candidate()], {}, ttl_seconds=600, idle_cpu_pct=1.0)
    assert held[0] == T.Held(TARGET, "idle_unreadable", None, None)


def test_keeper_rows_needs_a_readable_map_and_never_guesses_one():
    rows = [{"pid": 7, "cmd": MCP_CMD}]
    assert T.keeper_rows(rows, None) == []
    assert T.keeper_rows(rows, {}) == []
    assert T.keeper_rows(rows, {7: "com.cds.mcp-gateway"}) == [
        T.Keeper(pid=7, label="com.cds.mcp-gateway", cmd=MCP_CMD)
    ]


# ── the idle probe: one read-only bulk ps ────────────────────────────────────

class _PsResult:
    def __init__(self, stdout: str, returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode


def test_idle_probe_parses_etime_and_keeps_only_the_wanted_pids(monkeypatch):
    stdout = (
        f"{TARGET} 01:06:40 0.0\n"
        f"{OWNER} 3-04:05:06 12.5\n"
    )
    captured: dict[str, Any] = {}

    def fake_run(cmd, **kw):
        captured["cmd"] = cmd
        return _PsResult(stdout)

    monkeypatch.setattr(T.subprocess, "run", fake_run)
    samples, error = T.read_idle_samples([TARGET])
    assert error is None
    assert samples == {TARGET: T.IdleSample(pid=TARGET, elapsed_seconds=4000, cpu_pct=0.0)}
    assert "etime" in " ".join(captured["cmd"]) and "pcpu" in " ".join(captured["cmd"])


def test_idle_probe_reports_a_failed_ps_as_unknown_not_as_idle(monkeypatch):
    monkeypatch.setattr(T.subprocess, "run", lambda *a, **k: _PsResult("", returncode=1))
    samples, error = T.read_idle_samples([TARGET])
    assert samples == {} and error == "ps_nonzero_exit"

    def boom(*_a, **_k):
        raise subprocess.SubprocessError("ps died")

    monkeypatch.setattr(T.subprocess, "run", boom)
    samples, error = T.read_idle_samples([TARGET])
    assert samples == {} and error == "ps_SubprocessError"


def test_idle_probe_asks_nothing_when_there_is_nothing_to_ask_about(monkeypatch):
    def explode(*_a, **_k):  # pragma: no cover - must never run
        raise AssertionError("ps must not run for an empty candidate set")

    monkeypatch.setattr(T.subprocess, "run", explode)
    assert T.read_idle_samples([]) == ({}, None)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("45", 45), ("01:02:03", 3723), ("3-04:05:06", 273906), ("00:00:30", 30), ("junk", None)],
)
def test_etime_forms(raw, expected):
    assert T.parse_etime(raw) == expected


# ── AC3: the launchd plist is generated, lint-clean, and installs nothing ─────

def test_the_generated_plist_is_lint_clean_and_mirrors_the_resource_reaper(tmp_path):
    path = T.write_plist(tmp_path / "ttl" / "com.cds.mcp-ttl-reaper.plist")
    body = path.read_text(encoding="utf-8")

    result = subprocess.run(
        ["/usr/bin/plutil", "-lint", str(path)], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout
    assert "<string>com.cds.mcp-ttl-reaper</string>" in body
    assert "<integer>600</integer>" in body          # 10-minute cadence
    assert "<false/>" in body                        # RunAtLoad: cadence, not boot-time kill
    assert "-m" in body and "fleet_watch.ttl_reaper" in body
    # Paths are absolute: launchd does not expand `~` in a plist.
    assert "~" not in body.split("<key>StandardOutPath</key>")[1].split("</dict>")[0]
    assert str(Path.home()) in body
    # The resource reaper's shape: pinned PATH, no inherited environment.
    assert "PATH" in body and "EnvironmentVariables" in body
    # Generated into a temp dir: nothing was installed.
    assert str(path).startswith(str(tmp_path))


def test_the_generator_has_no_launchd_call_site():
    """`generate` is the whole promise: the module never loads, installs or
    reloads a job, so it can hold no launchctl/bootstrap call site at all.

    Checked on the PARSED code, not the text: a docstring that says "never
    calls launchctl" must not read as a call to it.
    """
    names = _code_names()
    assert "launchctl" not in names
    assert "bootstrap" not in names
    assert "bootout" not in names
    assert not (Path.home() / "Library" / "LaunchAgents" / "com.cds.mcp-ttl-reaper.plist").exists()
    # The only subprocess the module runs is the one read-only idle probe.
    calls = _subprocess_run_calls()
    assert len(calls) == 1
    unparsed = ast.unparse(calls[0].args[0])
    assert "PS_BIN" in unparsed and "etime" in unparsed


def test_the_generated_plist_writes_no_launchd_state():
    """The generator's only side effect is the file it was handed."""
    before = sorted(p.name for p in (Path.home() / "Library" / "LaunchAgents").glob("*.plist"))
    body = T.render_plist()
    after = sorted(p.name for p in (Path.home() / "Library" / "LaunchAgents").glob("*.plist"))
    assert before == after
    assert "mcp-ttl-reaper" in body  # the label is in the text, not on disk
    assert not (Path.home() / "Library" / "LaunchAgents" / "com.cds.mcp-ttl-reaper.plist").exists()


def test_the_cli_can_emit_the_plist_without_installing_it(tmp_path, capsys):
    target = tmp_path / "com.cds.mcp-ttl-reaper.plist"
    assert T.main(["--emit-plist", str(target)]) in (0, 1)
    assert target.exists()
    assert str(target) in capsys.readouterr().out


# ── the summary line ─────────────────────────────────────────────────────────

def test_the_summary_names_the_signal_path_and_counts(monkeypatch):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))
    payload = _run(monkeypatch, world, wired)
    summary = T.format_summary(payload)
    assert "DRY-RUN" in summary
    assert "signal_path=closed:default_dry_run" in summary
    assert "candidates=1" in summary and "signals=0" in summary


def test_the_payload_is_json_serialisable(monkeypatch):
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))
    payload = _run(monkeypatch, world, wired)
    json.dumps(payload)  # the receipt is the wire format
    assert payload["schema_version"] == "fleet-watch/mcp-ttl-reaper/v1"


# ── the batch tripwire: every planted bad, the most aggressive flags ─────────

def test_every_planted_bad_in_this_batch_is_zero_signalled_at_max_aggression(monkeypatch):
    """BATCH VOID TRIPWIRE. Each planted bad is driven through the most
    aggressive flags this reaper accepts (``apply=True, operator_flip=True`` —
    force is not offered) and the whole batch must come back with ZERO signals.
    If any planted bad is ever signalled, this test fails and the batch is void.
    """
    def planted_bad_worlds() -> list[tuple[str, FakeWorld, list[dict[str, Any]], dict[int, str]]]:
        out: list[tuple[str, FakeWorld, list[dict[str, Any]], dict[int, str]]] = []

        # PLANTED BAD 1 — the owning session is ALIVE. Never a candidate at all.
        live_owner = FakeWorld().orphan(owner_exists=True)
        out.append(("live_owning_session", live_owner, _rows(live_owner), {}))

        # PLANTED BAD 2 — the target PID was recycled after the scan.
        recycled = FakeWorld().orphan()
        rows = _rows(recycled)
        recycled.pids[TARGET]["create_time"] = "CT-REUSED"
        out.append(("target_pid_recycled", recycled, rows, {}))

        # PLANTED BAD 3 — a live session lease still references the session.
        leased = FakeWorld().orphan()
        leased.leases = [{"session_id": "sess-live", "owner_pid": OWNER}]
        leased.lease_alive["sess-live"] = True
        out.append(("live_session_lease", leased, _rows(leased), {}))

        # PLANTED BAD 4 — a launchd-supervised gateway process (a keeper) and
        # nothing else. A reparented row is exactly what a broken keeper rule
        # would let through, so the whole case turns on the exclusion holding
        # under the most aggressive flags there are.
        keeper = FakeWorld().add(9001, create_time="KCT-1", ppid=1)
        keeper_rows = [
            {"pid": 9001, "ppid": 1, "rss_mb": 40, "cmd": MCP_CMD, "create_time": "KCT-1"},
        ]
        out.append((
            "supervised_keeper", keeper, keeper_rows, {9001: "com.cds.mcp-gateway"},
        ))
        return out

    verdicts: dict[str, str] = {}
    for name, world, rows, supervision in planted_bad_worlds():
        _wire(monkeypatch, world, rows, supervision=supervision)
        payload = _run(monkeypatch, world, {"idle_fn": lambda _p: ({TARGET: _idle()}, None)},
                       apply=True, operator_flip=True)
        assert payload["signals_attempted"] == [], f"{name} was signalled"
        assert world.signals == [], f"{name} reached the sender: {world.signals}"
        if name == "supervised_keeper":
            # A keeper never becomes a candidate, so it never reaches the gate.
            assert [k["pid"] for k in payload["keepers"]] == [9001]
            assert payload["candidates"] == []
            assert payload["gate_consulted"] is False
            verdicts[name] = "excluded_as_supervised_keeper"
        elif name == "live_owning_session":
            # Inherited from the detector: a live parent is never a candidate.
            assert payload["candidates"] == []
            verdicts[name] = "excluded_live_owning_session"
        else:
            # These two DO reach the gate, and the gate is what refuses them.
            assert payload["candidates"], f"{name} should have reached the gate to be denied"
            assert payload["decisions_receipted"] == 1
            verdicts[name] = payload["verified"]["decisions"][0]["reason"]
            assert payload["verified"]["exited"] == []
            assert payload["verified"]["denied"], f"{name} produced no denial receipt"

    assert verdicts == {
        "live_owning_session": "excluded_live_owning_session",
        "target_pid_recycled": "scan_create_time_mismatch",
        "live_session_lease": "live_session_lease:sess-live",
        "supervised_keeper": "excluded_as_supervised_keeper",
    }


def test_the_only_signal_call_site_is_the_verified_gate(monkeypatch):
    """The reaper has no signalling code of its own: the gate is reached through
    exactly one call, and every signal observed is the gate's doing.

    The dry run asks the gate not to kill and produces zero signals. The flipped
    run asks it to, and the one SIGTERM that appears is the gate acting on a
    provably-dead-session orphan — not the reaper signalling.
    """
    seen: list[tuple[str, bool]] = []
    real_pass = reap_mod.verified_reclaim_pass

    def recording_pass(conn, **kwargs):
        seen.append(("verified_reclaim_pass", bool(kwargs.get("do_kill"))))
        return real_pass(conn, **kwargs)

    monkeypatch.setattr(T.reap_mod, "verified_reclaim_pass", recording_pass)
    world = FakeWorld().orphan()
    wired = _wire(monkeypatch, world, _rows(world))

    _run(monkeypatch, world, wired)
    assert world.signals == []  # the dry run asked, and nothing was sent
    receipts_before = len(world.events)

    _run(monkeypatch, world, wired, apply=True, operator_flip=True)
    assert seen == [
        ("verified_reclaim_pass", False),
        ("verified_reclaim_pass", True),
    ]
    assert world.signals == [(TARGET, int(signal_mod.SIGTERM))]
    # And it went through the trap the gate was handed, never around it — after
    # this run's own plan receipt was already in the chain.
    assert world.signal_calls == [
        (receipts_before + 1, TARGET, int(signal_mod.SIGTERM))
    ]


def test_the_reaper_module_holds_no_signal_call_site_of_its_own():
    """Grep-proof, as a test: the reaper has no signalling construct anywhere in
    its parsed code — no os.kill, no signal import, no SIGTERM/SIGKILL constant,
    no pkill. The only signal in this path lives in the verified gate.

    Parsed, not string-matched: the docstring is allowed to SAY SIGKILL while
    promising never to send one; the code may not be able to.
    """
    tree = _tree()
    names = _code_names()
    assert "kill" not in names
    assert "killpg" not in names
    assert "SIGTERM" not in names and "SIGKILL" not in names
    assert "pkill" not in names
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    assert "signal" not in imported
    # What it does call, by name: the gate.
    assert "verified_reclaim_pass" in inspect.getsource(T)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
