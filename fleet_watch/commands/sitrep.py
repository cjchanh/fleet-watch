from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
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

@click.command("sitrep")
@click.option("--json", "as_json", is_flag=True, help="Emit the full sitrep as JSON")
@click.option(
    "--owner",
    default=None,
    help="GitHub login or org to list (default: repositories owned by the gh viewer)",
)
@click.option(
    "--limit",
    type=click.IntRange(1, 100),
    default=30,
    show_default=True,
    help="Maximum repositories to include (GitHub page size; truncated if more exist)",
)
@click.option("--no-receipt", is_flag=True, help="Print only; write no receipt")
@click.option(
    "--receipt-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Override the receipt directory",
)
def sitrep(as_json: bool, owner: str | None, limit: int, no_receipt: bool, receipt_dir: Path | None):
    """Read-only GitHub fleet sitrep. No clone, no tokens, no invented SHA."""
    from fleet_watch import github_sitrep as sitrep_mod

    try:
        result = sitrep_mod.run_sitrep(
            owner=owner,
            limit=limit,
            receipt_dir=receipt_dir or sitrep_mod.RECEIPT_DIR,
            write=not no_receipt,
        )
    except sitrep_mod.SitrepRefusal as exc:
        if as_json:
            click.echo(
                json.dumps(
                    {
                        "schema_version": sitrep_mod.SCHEMA_VERSION,
                        "refusal": list(exc.errors),
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            click.echo("REFUSAL: GitHub fleet sitrep not written.", err=True)
            for error in exc.errors:
                click.echo(f"  - {error}", err=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 - fail closed, never traceback
        click.echo(
            f"REFUSAL: sitrep crashed before producing a receipt "
            f"({type(exc).__name__}: {sitrep_mod.redact(str(exc))}).",
            err=True,
        )
        sys.exit(1)

    if as_json:
        click.echo(json.dumps(result.payload, indent=2, sort_keys=True))
        return

    click.echo("\n".join(sitrep_mod.render_sitrep(result)))
