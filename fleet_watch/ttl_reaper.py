"""MCP TTL reaper — a CADENCE wrapper around the existing verified reclaim gate.

Spec 2627003 (the 2026-09-26 orphan recon's Fix 5). The accumulating class it
closes: MCP stdio servers that outlive the session that spawned them, including
the duplicate children a long-lived app-server re-spawns on reconnect. The old
mitigation was a hand-run script; this is the same class of work on a 10-minute
cadence, governed.

THE LOAD-BEARING PROPERTY: this module has no signal path of its own. It
computes WHICH dead-session MCP servers have outlived their TTL, and hands that
set to :func:`fleet_watch.commands.reap.verified_reclaim_pass` — the same
function behind ``fleet reap --mcp --verify`` — which re-proves identity
(pid + kernel create-time + executable + uid), owner death (session registry +
parent chain) and lease state at the moment of action, and writes a
PROCESS_DECISION receipt before any signal. There is exactly one signal call
site in this path and it lives in the verified gate, not here. If this module
were deleted, nothing would lose the ability to decide; the gate is the gate.

THREE PROPERTIES THIS MODULE OWNS:

1. **Dry-run is the default, and it is not a smaller apply.** With no flags the
   gate is consulted with ``do_kill=False``, so it writes its decisions and
   sends nothing. ``signals_attempted`` in the payload is the receipt of that:
   it is empty in every dry run, even with candidates present.

2. **The signal path is behind an explicit operator flip.** ``--apply`` on its
   own is REFUSED (``signal_path: refused:operator_flip_required``) and the run
   continues as a dry run, so the request is always reported and never acted on.
   The path opens only when the operator also passes
   ``--i-understand-this-signals``. Even then it is a REQUEST to evaluate, not
   a licence to signal: the gate's denials still stand, and ``do_kill_force`` is
   hard-pinned ``False`` here — this reaper never escalates to SIGKILL.

3. **Supervised keepers are excluded, and named in the receipt.** A
   launchd-supervised daemon (``com.cds.mcp-gateway*``) is a keeper, never a
   candidate: its ``ppid=1`` is supervision, and killing it is precisely the
   event ``KeepAlive`` respawns from. Keepers are reported under ``keepers`` with
   the label that protected them, so the receipt says what was held and why
   rather than silently omitting it. An unreadable supervision map fails CLOSED
   (``supervision_map: unreadable``): the detector's own partition then holds
   every reparented row, because a kill-facing path must never act on a
   distinction it could not read.

Everything the module touches is injectable — the process-table scan, the
supervision map, the idle probe, the clock, the lease table, the signal sender,
the registry connection — so the tests never create, observe or signal a real
process. The only real subprocess in production is one read-only ``ps``.
"""

from __future__ import annotations

import argparse
import importlib
import json
import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from fleet_watch.constants import PS_BIN
from fleet_watch.discovery import mcp_orphan_detector as detector

# The MODULE, not the Click command: `fleet_watch.commands` re-exports `reap`,
# so the attribute `fleet_watch.commands.reap` is a Command. Resolving through
# importlib is what gets the module the verified gate actually lives in, and it
# keeps the module's seams (probes, sender, connection) patchable at call time.
reap_mod = importlib.import_module("fleet_watch.commands.reap")

TTL_REAPER_SCHEMA = "fleet-watch/mcp-ttl-reaper/v1"

# 600s = 10 minutes: the same idle default the com.cds.resource-reaper cadence
# uses, and the same number the generated plist carries as StartInterval.
DEFAULT_TTL_SECONDS = 600

# A server above this lifetime-average CPU is working, not idle. Same bar the
# lexicon leak reaper has used, so a candidate set does not change meaning when
# the cadence takes over from the hand-run script.
DEFAULT_IDLE_CPU_PCT = 1.0

# The operator flip. ``--apply`` without this is refused, never downgraded.
SIGNAL_FLIP_FLAG = "--i-understand-this-signals"

# A supervised keeper is never a reclaim target. Prefix-matched against the
# launchd label, so a new com.cds.mcp-gateway* job is covered by name.
KEEPER_LABEL_PREFIXES = ("com.cds.mcp-gateway",)

PLIST_LABEL = "com.cds.mcp-ttl-reaper"
PLIST_START_INTERVAL = DEFAULT_TTL_SECONDS
PLIST_PYTHON = "/Users/cj/Workspace/active/fleet-watch/.venv/bin/python3"
PLIST_MODULE_ARGS = ("-m", "fleet_watch.ttl_reaper", "--json")
PLIST_LOG_DIR = "~/Library/Logs/fleet-watch"
# BSD `ps -o etime=` renders SS, MM:SS, HH:MM:SS or DD-HH:MM:SS.
_ETIME_RE = re.compile(
    r"^(?:(?:(?P<days>\d+)-)?(?P<hours>\d+):)?(?P<minutes>\d+):(?P<seconds>\d+)$"
)


class TtlReaperError(RuntimeError):
    """Fail-closed reaper error: the run stops rather than reaping on a guess."""


@dataclass(frozen=True)
class IdleSample:
    """One process's idle facts as the kernel reports them right now."""

    pid: int
    elapsed_seconds: int
    cpu_pct: float


@dataclass(frozen=True)
class Held:
    """A candidate the TTL did NOT expire, with the reason it was held.

    Reported, never dropped: a receipt that says only what it would act on
    cannot answer "why is that one still there?".
    """

    pid: int
    reason: str
    elapsed_seconds: int | None
    cpu_pct: float | None


@dataclass(frozen=True)
class Keeper:
    """A launchd-supervised MCP server held out of the candidate set."""

    pid: int
    label: str
    cmd: str


def parse_etime(value: str) -> int | None:
    """Parse a BSD ``ps`` elapsed-time string into seconds, or None if unknown."""
    value = value.strip()
    match = _ETIME_RE.match(value)
    if match is None:
        # Bare seconds form, e.g. "45".
        return int(value) if value.isdigit() else None
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return ((days * 24 + hours) * 60 + minutes) * 60 + seconds


def read_idle_samples(
    pids: Sequence[int],
) -> tuple[dict[int, IdleSample], str | None]:
    """One read-only bulk ``ps`` for the idle facts of ``pids``.

    Returns ``(samples, error)``. A failed probe is an error string and no
    samples — never an empty dict that would read as "every candidate is
    unreadable, therefore nothing to do". The caller holds on the error.
    """
    wanted = {int(pid) for pid in pids}
    if not wanted:
        return {}, None
    try:
        result = subprocess.run(
            [PS_BIN, "-eo", "pid=,etime=,pcpu="],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {}, f"ps_{type(exc).__name__}"
    if result.returncode != 0:
        return {}, "ps_nonzero_exit"
    samples: dict[int, IdleSample] = {}
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) != 3:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        if pid not in wanted:
            continue
        elapsed = parse_etime(parts[1])
        try:
            cpu_pct = float(parts[2])
        except ValueError:
            continue
        if elapsed is None:
            continue
        samples[pid] = IdleSample(pid=pid, elapsed_seconds=elapsed, cpu_pct=cpu_pct)
    return samples, None


def keeper_rows(
    rows: Sequence[Mapping[str, Any]],
    supervised: Mapping[int, str] | None,
) -> list[Keeper]:
    """The supervised keepers among ``rows``, by launchd label.

    ``supervised=None`` means the map could not be read: there is no keeper list
    to report, and the caller records ``supervision_map: unreadable``. This
    function never invents a keeper from an unreadable label.
    """
    if supervised is None:
        return []
    keepers: list[Keeper] = []
    for row in rows:
        try:
            pid = int(row.get("pid", 0) or 0)
        except (TypeError, ValueError):
            continue
        label = supervised.get(pid)
        if not label or not label.startswith(KEEPER_LABEL_PREFIXES):
            continue
        keepers.append(Keeper(pid=pid, label=label, cmd=str(row.get("cmd", ""))))
    return keepers


def plan_ttl(
    candidates: Sequence[detector.MCPCandidate],
    samples: Mapping[int, IdleSample],
    *,
    ttl_seconds: int,
    idle_cpu_pct: float,
) -> tuple[list[detector.MCPCandidate], list[Held]]:
    """Split candidates into TTL-expired and held. PURE: no I/O, no signal.

    A candidate is expired only when it is BOTH older than the TTL and below the
    idle-CPU bar. Anything unreadable, busy, or merely young is held with a
    named reason — an unreadable idle probe is never read as "expired".
    """
    if ttl_seconds < 0:
        raise TtlReaperError("--ttl-seconds must be >= 0")
    expired: list[detector.MCPCandidate] = []
    held: list[Held] = []
    for candidate in candidates:
        sample = samples.get(candidate.pid)
        if sample is None:
            held.append(Held(candidate.pid, "idle_unreadable", None, None))
        elif sample.cpu_pct >= idle_cpu_pct:
            held.append(Held(candidate.pid, "busy_cpu", sample.elapsed_seconds, sample.cpu_pct))
        elif sample.elapsed_seconds < ttl_seconds:
            held.append(
                Held(candidate.pid, "younger_than_ttl", sample.elapsed_seconds, sample.cpu_pct)
            )
        else:
            expired.append(candidate)
    return expired, held


def resolve_signal_path(apply_requested: bool, operator_flip: bool) -> tuple[bool, str]:
    """Is the signal path open? ``(open, reason)`` — fail-closed on one flag.

    ``--apply`` is a REQUEST to evaluate. Without the operator flip the path is
    refused and the run continues as a dry run: the request is reported in the
    receipt, and nothing is signalled.
    """
    if not apply_requested:
        return False, "closed:default_dry_run"
    if not operator_flip:
        return False, f"refused:operator_flip_required:{SIGNAL_FLIP_FLAG}"
    return True, "open:operator_flip"


def run_once(
    *,
    apply: bool = False,
    operator_flip: bool = False,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    idle_cpu_pct: float = DEFAULT_IDLE_CPU_PCT,
    scan: Callable[[], tuple[list[dict[str, Any]], str | None]] | None = None,
    supervised: Callable[[], Mapping[int, str] | None] | None = None,
    idle_probe: Callable[
        [Sequence[int]], tuple[dict[int, IdleSample], str | None]
    ] | None = None,
    conn: Any = None,
    conn_factory: Callable[[], Any] | None = None,
    probes: Any = None,
    reclaim: Any = None,
    sender: Any = None,
) -> dict[str, Any]:
    """One TTL reaper tick. Returns the receipt payload; never raises on a probe.

    Phase order mirrors the gate it calls: observe (scan, supervision, idle),
    decide TTL, then HAND OFF. The handoff is the only thing in this function
    that can lead to a signal, and it asks the verified gate to do it.
    """
    signalling, signal_reason = resolve_signal_path(apply, operator_flip)
    payload: dict[str, Any] = {
        "schema_version": TTL_REAPER_SCHEMA,
        "mode": "apply" if signalling else "dry-run",
        "signal_path": signal_reason,
        "force_authorized": False,
        "ttl_seconds": ttl_seconds,
        "idle_cpu_pct": idle_cpu_pct,
        "keeper_label_prefixes": list(KEEPER_LABEL_PREFIXES),
        "scanned": 0,
        "supervision_map": "unreadable",
        "snapshot_error": None,
        "idle_probe_error": None,
        "candidates": [],
        "held": [],
        "keepers": [],
        "gate_consulted": False,
        "decisions_receipted": 0,
        "signals_attempted": [],
        "verified": None,
    }

    scan_fn = scan or detector.scan_mcp_processes
    rows, scan_error = scan_fn()
    if scan_error:
        # Fail-closed: an unscannable host is UNKNOWN, never "nothing to do".
        payload["snapshot_error"] = scan_error
        return payload
    payload["scanned"] = len(rows)

    supervised_fn = supervised or detector._launchd_supervised_pids
    supervision = supervised_fn()
    payload["supervision_map"] = "read" if supervision is not None else "unreadable"
    payload["keepers"] = [asdict(k) for k in keeper_rows(rows, supervision)]

    # The detector's own partition is the load-bearing split (a live parent is
    # never a candidate, and supervised pids are held) — inherited, not
    # re-implemented here.
    candidates, _error = detector.scan_candidates(rows=rows)

    idle_fn = idle_probe or read_idle_samples
    samples, idle_error = idle_fn([c.pid for c in candidates])
    if idle_error:
        payload["idle_probe_error"] = idle_error
    expired, held = plan_ttl(
        candidates, samples, ttl_seconds=ttl_seconds, idle_cpu_pct=idle_cpu_pct
    )
    payload["candidates"] = [
        {
            **c.to_dict(),
            "elapsed_seconds": samples[c.pid].elapsed_seconds,
            "cpu_pct": samples[c.pid].cpu_pct,
            "ttl_seconds": ttl_seconds,
        }
        for c in expired
    ]
    payload["held"] = [asdict(h) for h in held]

    if not expired:
        # Nothing to ask the gate about: it is not consulted, and nothing is
        # signalled. This is the steady state on a healthy host.
        return payload

    owns_conn = conn is None
    db = conn if conn is not None else (conn_factory or reap_mod._get_conn)()
    try:
        verified, plans = reap_mod.verified_reclaim_pass(
            db,
            do_kill=signalling,
            # Hard-pinned: this reaper never escalates to SIGKILL.
            do_kill_force=False,
            probes=probes,
            reclaim=reclaim,
            sender=sender,
            pids=[c.pid for c in expired],
        )
    finally:
        if owns_conn:
            db.close()

    payload["gate_consulted"] = True
    payload["verified"] = verified
    payload["decisions_receipted"] = len(plans)
    # Derived from the gate's audited OUTCOME receipts, not from its plans: a
    # plan never carries a signal, only the executed outcome does. Anything the
    # gate actually sent is here, and nothing else.
    payload["signals_attempted"] = [
        {
            "pid": d.get("pid"),
            "signal": d.get("signal"),
            "action": d.get("action"),
            "outcome": d.get("outcome"),
            "reason": d.get("reason"),
        }
        for d in (verified.get("exited", []) + verified.get("failed", []))
        if d.get("signal") is not None
    ]
    return payload


# ── the launchd cadence (generated, never installed) ──────────────────────────

_PLIST_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
\t<key>EnvironmentVariables</key>
\t<dict>
\t\t<key>PATH</key>
\t\t<string>{path}</string>
\t</dict>
\t<key>Label</key>
\t<string>{label}</string>
\t<key>ProgramArguments</key>
\t<array>
{program_arguments}\t</array>
\t<key>RunAtLoad</key>
\t<false/>
\t<key>StandardErrorPath</key>
\t<string>{stderr_path}</string>
\t<key>StandardOutPath</key>
\t<string>{stdout_path}</string>
\t<key>StartInterval</key>
\t<integer>{interval}</integer>
</dict>
</plist>
"""


def render_plist(
    *,
    label: str = PLIST_LABEL,
    python: str = PLIST_PYTHON,
    module_args: Sequence[str] = PLIST_MODULE_ARGS,
    interval: int = PLIST_START_INTERVAL,
    log_dir: str = PLIST_LOG_DIR,
    path_env: str = "/opt/homebrew/bin:/Users/cj/.local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
) -> str:
    """Render the com.cds.mcp-ttl-reaper plist, mirroring com.cds.resource-reaper.

    Same shape as the resource reaper: PATH pinned in the environment (never
    inherited), a single ProgramArguments entry, ``RunAtLoad`` false so an
    install is a cadence rather than a boot-time kill, and StartInterval
    600s. The generated file is a TEMPLATE — this module never writes to
    ~/Library/LaunchAgents and never calls launchctl, so generating it changes
    nothing about what is loaded. Install precondition (the operator's step, not
    this module's): ``log_dir`` must exist before the job is loaded, because
    launchd will not create it.
    """
    arguments = [python, *module_args]
    body = "".join(f"\t\t<string>{arg}</string>\n" for arg in arguments)
    # launchd does NOT expand `~` in a plist path — a literal tilde becomes a
    # file named "~" in the job's working directory. Expand it here so the
    # generated file is correct as written.
    log_base = str(Path(log_dir).expanduser())
    return _PLIST_TEMPLATE.format(
        path=path_env,
        label=label,
        program_arguments=body,
        stdout_path=f"{log_base}/mcp-ttl-reaper.log",
        stderr_path=f"{log_base}/mcp-ttl-reaper.err",
        interval=interval,
    )


def write_plist(destination: Path, **kwargs: Any) -> Path:
    """Write the generated plist to ``destination`` and return the path.

    The only side effect in the module: it creates the destination's parent
    directory and writes the file. It never installs anything.
    """
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_plist(**kwargs), encoding="utf-8")
    return destination


# ── the entry point the generated plist runs ─────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--json", action="store_true", help="Print the full JSON receipt."
    )
    parser.add_argument(
        "--ttl-seconds", type=int, default=DEFAULT_TTL_SECONDS,
        help=f"Idle TTL a candidate must outlive. Default {DEFAULT_TTL_SECONDS}.",
    )
    parser.add_argument(
        "--idle-cpu-pct", type=float, default=DEFAULT_IDLE_CPU_PCT,
        help=f"CPU%% at or above which a candidate is held as busy. Default {DEFAULT_IDLE_CPU_PCT}.",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="REQUEST the signal path. Refused without the operator flip below.",
    )
    parser.add_argument(
        SIGNAL_FLIP_FLAG, dest="operator_flip", action="store_true",
        help="Operator flip: acknowledge that --apply sends real signals.",
    )
    parser.add_argument(
        "--emit-plist", metavar="PATH", default=None,
        help="Write the launchd plist template to PATH. Generates only; installs nothing.",
    )
    return parser


def format_summary(payload: Mapping[str, Any]) -> str:
    return (
        f"{payload['mode'].upper()} "
        f"signal_path={payload['signal_path']} "
        f"scanned={payload['scanned']} "
        f"candidates={len(payload['candidates'])} "
        f"held={len(payload['held'])} "
        f"keepers={len(payload['keepers'])} "
        f"decisions={payload['decisions_receipted']} "
        f"signals={len(payload['signals_attempted'])}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.emit_plist:
        print(str(write_plist(Path(args.emit_plist))))
    try:
        payload = run_once(
            apply=args.apply,
            operator_flip=args.operator_flip,
            ttl_seconds=args.ttl_seconds,
            idle_cpu_pct=args.idle_cpu_pct,
        )
    except TtlReaperError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(format_summary(payload))
        for error in (payload["snapshot_error"], payload["idle_probe_error"]):
            if error:
                print(f"WARNING: {error}", file=sys.stderr)
    # UNKNOWN is non-zero: an unscannable host must not read as a clean tick.
    return 1 if (payload["snapshot_error"] or payload["idle_probe_error"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
