from __future__ import annotations

import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import click

from fleet_watch import autonomous as autonomous_mod
from fleet_watch import boot_map as boot_map_mod
from fleet_watch import census as census_mod
from fleet_watch import claude_lease_twin
from fleet_watch import counters, discover as discover_mod
from fleet_watch import events, gpu_estimator, referee, registry, reporter, runaway, syshealth
from fleet_watch.cli_support import (
    DEFAULT_REPORT_BUDGET_S,
    REPORT_ATTEMPT_MARKER,
    REPORT_BUDGET_ENV,
    REPORT_MIN_INTERVAL_ENV,
    STATUS_DISCOVERY_TIMEOUT_SECONDS,
    _ack,
    _build_guard_payload,
    _build_reconcile_payload,
    _census_registry_rows,
    _cooperative_alternative,
    _default_owner_pid,
    _documents_root,
    _executable_supports_census,
    _extract_json_document,
    _float_env,
    _get_conn,
    _holder_conflict_text,
    _holder_text,
    _is_documents_path,
    _is_fleet_owned,
    _load_tnr_instances,
    _mark_report_attempt,
    _mcp_reap_candidates,
    _mcp_surface_lines,
    _notify_attention,
    _notify_conflict,
    _publish_report_after_ack,
    _reject_negative_gpu,
    _render_census,
    _render_guard,
    _render_launchd_plist,
    _report_budget_seconds,
    _report_is_fresh,
    _report_min_interval_seconds,
    _repo_unblock_command,
    _resolved_session_id,
    _run_bounded,
    _run_runaway_tick,
    _terminate_orphan,
)
from fleet_watch.discovery import mcp_orphan_detector, ollama_runners, orphan_detector
from fleet_watch.guards import memory_pressure

@click.command("boot-coverage")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def boot_coverage(as_json: bool):
    """Cross-check fleet-registered processes against launchd-loaded services."""
    from fleet_watch import boot_coverage as boot_coverage_mod

    conn = _get_conn()
    procs = registry.get_all_processes(conn)
    conn.close()

    if not procs:
        payload = {
            "schema_version": boot_coverage_mod.SCHEMA_VERSION,
            "generated_utc": boot_coverage_mod._now_iso(),
            "processes_assessed": 0,
            "by_verdict": {},
            "results": [],
        }
        if as_json:
            click.echo(json.dumps(payload, indent=2, sort_keys=True))
        else:
            click.echo("No registered processes to assess.")
        return

    payload = boot_coverage_mod.run(procs, as_json=as_json)

    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return

    click.echo(f"Boot Persistence Coverage ({payload['processes_assessed']} processes):\n")
    for r in payload["results"]:
        flag = {"HAS_PERSISTENCE": "PERSIST", "NO_PERSISTENCE_WILL_DIE_ON_REBOOT": "NO_PERSIST", "PLIST_PRESENT_BUT_UNLOADED": "UNLOADED"}[r["verdict"]]
        port_str = f":{r['port']}" if r.get("port") else ""
        click.echo(f"  [{flag}] PID {r['pid']} ({r['name']}{port_str})")
        if r.get("suggested_plist"):
            suggested_path = Path.home() / "Library" / "LaunchAgents" / f"{r['suggested_label']}.plist"
            click.echo(f"         Suggested plist: {suggested_path}")

    summary = ", ".join(f"{v} {k}" for k, v in sorted(payload["by_verdict"].items()))
    click.echo(f"\nSummary: {summary}")
    click.echo(f"Receipt: {payload.get('receipt_path', 'N/A')}")


@click.command()
@click.option("--json", "as_json", is_flag=True, help="Emit the full receipt as JSON")
@click.option("--quiet", is_flag=True, help="Print only totals and the receipt path")
@click.option("--no-receipt", is_flag=True, help="Judge only; write no receipt")
@click.option(
    "--receipt-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Override the receipt directory",
)
@click.option(
    "--deep",
    is_flag=True,
    help="Wait longer on slow probes (sfltool login items) for full coverage",
)
@click.option(
    "--emit-launchd-plist",
    is_flag=True,
    help="Print the staged daily-census launchd plist and exit; installs nothing",
)
def census(
    as_json: bool,
    quiet: bool,
    no_receipt: bool,
    receipt_dir: Path | None,
    deep: bool,
    emit_launchd_plist: bool,
):
    """Census what boots and runs on this machine, and what is stale."""
    if emit_launchd_plist:
        executable = shutil.which("fleet") or census_mod.DEFAULT_FLEET_BIN
        log_path = os.path.join(tempfile.gettempdir(), "fleet-census.log")
        click.echo(census_mod.render_launchd_plist(executable, log_path=log_path), nl=False)
        # stderr so the plist can be redirected to a file while the operator
        # still sees what to run. Fleet Watch does not install it.
        click.echo(
            "\nPrinted only — `--emit-launchd-plist` never installs anything itself.\n"
            "A `launchctl bootstrap` you run (or piped after this) is what installs. "
            "From scratch:\n"
            f"  {census_mod.INSTALL_COMMAND}\n"
            "To remove it:\n"
            f"  {census_mod.UNINSTALL_COMMAND}",
            err=True,
        )
        if not _executable_supports_census(executable):
            # A launchd job pointing at a `fleet` that predates this command
            # would fail silently every morning. Say so before it is installed.
            click.echo(
                f"\nWARNING: {executable} does not support `census` — the staged job "
                "would fail on every run.\n"
                "  That binary is a separate installation from this source tree. "
                "Reinstall it first, e.g.\n"
                "    pipx reinstall fleet-watch   (or: pipx install --force "
                f"{Path(__file__).resolve().parent.parent})",
                err=True,
            )
        return

    try:
        result = census_mod.run_census(
            registry_processes=_census_registry_rows(),
            receipt_dir=receipt_dir or census_mod.RECEIPT_DIR,
            write=not no_receipt,
            deep=deep,
        )
    except Exception as exc:  # noqa: BLE001 - fail closed, never traceback
        # The staged launchd job runs this unattended; a raw traceback there is
        # an unreadable failure. Refuse out loud instead.
        click.echo(
            f"REFUSAL: census crashed before producing a receipt "
            f"({type(exc).__name__}: {exc}).",
            err=True,
        )
        click.echo("  latest.json was left untouched.", err=True)
        sys.exit(1)

    if result.refusal:
        if as_json:
            click.echo(
                json.dumps(
                    {
                        "schema_version": census_mod.SCHEMA_VERSION,
                        "refusal": result.refusal,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            click.echo("REFUSAL: census receipt not written.", err=True)
            for error in result.refusal:
                click.echo(f"  - {error}", err=True)
            click.echo(
                "  latest.json was left untouched (never clobber good with bad).",
                err=True,
            )
        sys.exit(1)

    if as_json:
        click.echo(json.dumps(result.payload, indent=2, sort_keys=True))
        return

    totals = result.payload["totals"]
    if quiet:
        click.echo(
            f"census {totals['items']} items "
            f"(keep {totals['keep']}, investigate {totals['investigate']}, "
            f"close {totals['close']}, remove {totals['remove']}) -> "
            f"{result.dated_path or 'no receipt'}"
        )
        return

    click.echo("\n".join(_render_census(result.payload, result)))


@click.command("boot-map")
@click.option(
    "--receipt",
    "receipt_path",
    default=None,
    type=click.Path(),
    help=f"Census receipt JSON (default: {boot_map_mod.DEFAULT_RECEIPT_PATH})",
)
@click.option(
    "--out",
    "out_dir",
    default=None,
    type=click.Path(),
    help=f"Output directory (default: {boot_map_mod.DEFAULT_OUT_DIR})",
)
@click.option("--json", "as_json", is_flag=True, help="Emit the build receipt as JSON")
def boot_map(receipt_path: str | None, out_dir: str | None, as_json: bool):
    """Build the boot map: census receipt -> graph JSON + local 3D HTML.

    Deterministic and offline: same receipt produces the same graph and the same
    self-contained page. Exits 3 (fail-closed) when the receipt is absent,
    unparseable, structurally invalid, or degenerate.
    """
    try:
        built = boot_map_mod.build(receipt=receipt_path, out_dir=out_dir)
    except boot_map_mod.BootMapError as exc:
        click.echo(f"REFUSAL: {exc}", err=True)
        sys.exit(3)

    if as_json:
        click.echo(json.dumps(built, indent=2, sort_keys=True))
        return

    stats = built["stats"]
    source = built["source_receipt"]
    click.echo(
        f"Boot map built from {source['path']}\n"
        f"  census:  {source['item_count']} items / {source['domain_count']} domains"
        f" (host {source['host'] or 'unnamed'}, {source['generated_at'] or 'undated'})\n"
        f"  graph:   {stats['node_count']} nodes / {stats['edge_count']} edges"
    )
    for kind, count in stats["counts_by_kind"].items():
        click.echo(f"    {kind:<9} {count}")
    verdicts = ", ".join(f"{k} {v}" for k, v in stats["counts_by_verdict"].items())
    click.echo(f"  verdicts: {verdicts}")
    if built["warnings"]:
        click.echo(f"  warnings: {len(built['warnings'])} (kept + flagged, never dropped)")
    for output in built["outputs"]:
        click.echo(f"  wrote {output['path']}  sha256 {output['sha256'][:16]}…")
    html = next((o["path"] for o in built["outputs"] if o["path"].endswith(".html")), None)
    if html:
        click.echo(f"View: open {html}")
