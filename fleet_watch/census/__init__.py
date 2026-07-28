"""fleet_watch.census — "what boots and runs on this Mac, and what's stale".

A deterministic, read-only census of every persistence and runtime surface on
the machine: launchd user agents, /Library daemons, live processes, TCP
listeners, cron, login items, brew services, and Fleet Watch's own registry.
Each item gets a status, a verdict, and the exact evidence behind them.

No LLM, no network, no writes outside the receipt directory. Fleet Watch never
kills anything — ``close_command`` is advisory text for the operator.

Public surface::

    run_census()                  -> CensusResult (probe, judge, write receipt)
    build_payload(snapshot)       -> contract-shaped dict
    render_launchd_plist(path)    -> staged plist text (installs nothing)

Receipt contract: ``docs/fleet-census-receipt-contract-v1.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape as xml_escape

from fleet_watch.census.domains import (
    DOMAIN_IDS,
    CensusDomain,
    CensusItem,
    build_domains,
)
from fleet_watch.census.probes import SystemSnapshot, collect_snapshot
from fleet_watch.census.rank import (
    DEFAULT_TOP_N,
    format_rank_line,
    rank_investigate,
)
from fleet_watch.census.receipt import (
    LATEST_NAME,
    RECEIPT_DIR,
    SCHEMA_VERSION,
    CensusRefusal,
    compute_drift,
    load_latest,
    now_iso,
    validate,
    write_receipt,
)
from fleet_watch.census.verdicts import VERDICTS

__all__ = [
    "CensusDomain",
    "CensusItem",
    "CensusRefusal",
    "CensusResult",
    "DEFAULT_TOP_N",
    "DOMAIN_IDS",
    "INSTALL_COMMAND",
    "LATEST_NAME",
    "LAUNCHD_LABEL",
    "RECEIPT_DIR",
    "SCHEMA_VERSION",
    "SystemSnapshot",
    "build_payload",
    "collect_snapshot",
    "format_rank_line",
    "rank_investigate",
    "render_launchd_plist",
    "run_census",
    "validate",
]

LAUNCHD_LABEL = "io.fleet-watch.census"
STAGED_PLIST_PATH = "~/Library/LaunchAgents/io.fleet-watch.census.plist"
DEFAULT_FLEET_BIN = "/usr/local/bin/fleet"

#: The operator installs the recurring job. `fleet census` never bootstraps it —
#: process control is an operator gate, not an agent action.
INSTALL_COMMAND = (
    f"fleet census --emit-launchd-plist > {STAGED_PLIST_PATH} && "
    f"launchctl bootstrap gui/$(id -u) {STAGED_PLIST_PATH}"
)
UNINSTALL_COMMAND = (
    f"launchctl bootout gui/$(id -u)/{LAUNCHD_LABEL} && rm {STAGED_PLIST_PATH}"
)


@dataclass(frozen=True)
class CensusResult:
    payload: dict[str, Any]
    dated_path: Path | None = None
    latest_path: Path | None = None
    refusal: list[str] | None = None

    @property
    def ok(self) -> bool:
        return self.refusal is None

    @property
    def item_count(self) -> int:
        return int(self.payload.get("totals", {}).get("items", 0))


def _rollup(domains: list[CensusDomain]) -> dict[str, int]:
    totals = {"items": 0} | {verdict: 0 for verdict in sorted(VERDICTS)}
    for domain in domains:
        totals["items"] += len(domain.items)
        for verdict, count in domain.verdict_counts().items():
            totals[verdict] += count
    return totals


def build_payload(
    snapshot: SystemSnapshot,
    receipt_dir: Path = RECEIPT_DIR,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build the contract-shaped payload, including drift against latest.json."""
    domains = build_domains(snapshot)
    domain_dicts = [domain.to_dict() for domain in domains]
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at or now_iso(),
        "host": snapshot.machine.host,
        "machine": snapshot.machine.to_dict(),
        "totals": _rollup(domains),
        "domains": domain_dicts,
        # Always present (possibly empty): top investigate by RAM/CPU/failure
        # cost so operators and the ORDER boot pane don't drown in flat ambers.
        "ranked_investigate": rank_investigate(domain_dicts, top_n=DEFAULT_TOP_N),
        "probes": [probe.to_dict() for probe in snapshot.probes],
        "drift": {
            "prior_receipt": None,
            "prior_status": "absent",
            "excluded_domains": [],
            "new_items": [],
            "disappeared": [],
            "verdict_changes": [],
        },
    }
    prior, prior_path, prior_status = load_latest(receipt_dir)
    payload["drift"] = compute_drift(payload, prior, prior_path, prior_status)
    return payload


def run_census(
    registry_processes: list[dict[str, Any]] | None = None,
    receipt_dir: Path = RECEIPT_DIR,
    write: bool = True,
    snapshot: SystemSnapshot | None = None,
    deep: bool = False,
) -> CensusResult:
    """Probe the machine, judge every surface, and emit a receipt.

    A payload that fails contract validation (including the degenerate
    zero-item case) is a refusal: nothing is written and ``latest.json`` keeps
    whatever good receipt it already held.
    """
    snap = (
        snapshot
        if snapshot is not None
        else collect_snapshot(registry_processes, deep=deep)
    )
    payload = build_payload(snap, receipt_dir=receipt_dir)

    errors = validate(payload)
    if errors:
        return CensusResult(payload=payload, refusal=errors)
    if not write:
        return CensusResult(payload=payload)

    try:
        dated_path, latest_path = write_receipt(payload, receipt_dir)
    except (CensusRefusal, OSError) as exc:
        reason = exc.errors if isinstance(exc, CensusRefusal) else [f"write failed: {exc}"]
        return CensusResult(payload=payload, refusal=reason)
    return CensusResult(payload=payload, dated_path=dated_path, latest_path=latest_path)


def render_launchd_plist(
    executable: str = DEFAULT_FLEET_BIN,
    hour: int = 9,
    minute: int = 0,
) -> str:
    """Render the staged launchd plist. Writes nothing, loads nothing.

    The in-repo copy at ``contrib/launchd/io.fleet-watch.census.plist`` is this
    text with the default executable path; run ``fleet census
    --emit-launchd-plist`` to get it with the path resolved for this machine.
    """
    log_path = "/tmp/fleet-census.log"
    # The path is interpolated into XML, and '&' or '<' are legal in filenames.
    # An unescaped one emits a plist that launchd cannot parse.
    executable = xml_escape(executable)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" '
        '"http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
        "<!--\n"
        f"  {LAUNCHD_LABEL} : daily fleet census at "
        f"{hour:02d}:{minute:02d} local.\n"
        "  Staged, not installed. Fleet Watch never bootstraps a launchd job;\n"
        "  process control is an operator action. The install and uninstall\n"
        "  lines are printed on stderr by 'fleet census' when it emits this\n"
        "  file, and are recorded in docs/fleet-census-receipt-contract-v1.md.\n"
        "  (XML comments cannot contain a double hyphen, so the command lines\n"
        "  cannot live in here without corrupting the plist.)\n"
        "-->\n"
        '<plist version="1.0">\n'
        "<dict>\n"
        "    <key>Label</key>\n"
        f"    <string>{LAUNCHD_LABEL}</string>\n"
        "    <key>ProgramArguments</key>\n"
        "    <array>\n"
        f"        <string>{executable}</string>\n"
        "        <string>census</string>\n"
        "        <string>--quiet</string>\n"
        "    </array>\n"
        "    <key>StartCalendarInterval</key>\n"
        "    <dict>\n"
        "        <key>Hour</key>\n"
        f"        <integer>{hour}</integer>\n"
        "        <key>Minute</key>\n"
        f"        <integer>{minute}</integer>\n"
        "    </dict>\n"
        "    <key>RunAtLoad</key>\n"
        "    <false/>\n"
        "    <key>Nice</key>\n"
        "    <integer>19</integer>\n"
        "    <key>ProcessType</key>\n"
        "    <string>Background</string>\n"
        "    <key>StandardOutPath</key>\n"
        f"    <string>{log_path}</string>\n"
        "    <key>StandardErrorPath</key>\n"
        f"    <string>{log_path}</string>\n"
        "</dict>\n"
        "</plist>\n"
    )
