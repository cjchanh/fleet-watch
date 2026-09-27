"""``fleet reflex`` — findings to spec queue, with verify-on-land.

Observe-and-emit only. Nothing in this group changes process state; the
loop drafts a spec and records a receipt, and the governed policy paths keep
every process-control authority.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click

from fleet_watch import reflex


def _paths(
    queue_dir: str | None,
    receipts_dir: str | None,
) -> tuple[Path, Path]:
    return (
        Path(queue_dir) if queue_dir else reflex.QUEUE_DIR,
        Path(receipts_dir) if receipts_dir else reflex.RECEIPT_DIR,
    )


def _resolve_findings(findings_file: str | None) -> tuple[list[Any], list[dict[str, Any]]]:
    """Findings from an injectable source file, or from the live registry."""
    if findings_file:
        try:
            return reflex.findings_from_file(Path(findings_file)), []
        except (OSError, ValueError) as exc:
            return [], [{"source": findings_file, "reason": f"{type(exc).__name__}: {exc}"}]
    findings, errors = reflex.collect_live_findings()
    return findings, errors


def _probe_from_file(path: str) -> reflex.Probe:
    """Build a re-check probe from a JSON map of finding_id -> observation."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        payload = {
            str(row.get("finding_id")): row
            for row in payload
            if isinstance(row, dict)
        }
    table: dict[str, Any] = payload if isinstance(payload, dict) else {}

    def probe(finding_id: str) -> reflex.Recheck:
        row = table.get(finding_id)
        if row is None:
            return reflex.Recheck(present=False, detail={"source": "probe_file"})
        if isinstance(row, dict):
            return reflex.Recheck(
                present=bool(row.get("present")),
                detail=row.get("detail") or {"source": "probe_file"},
            )
        return reflex.Recheck(present=bool(row), detail={"source": "probe_file"})

    return probe


@click.group()
def reflex_group():
    """Convert Fleet Watch findings into queued specs, and re-check them on land."""
    pass


@reflex_group.command("emit")
@click.option("--write", "do_write", is_flag=True, help="Write spec + receipt (default: dry-run)")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
@click.option("--findings-file", default=None, help="JSON file of raw finding rows (injectable source)")
@click.option("--ticks", "min_ticks", type=int, default=reflex.DEFAULT_MIN_TICKS,
              help=f"Ticks an event-shaped denial must survive (default {reflex.DEFAULT_MIN_TICKS})")
@click.option("--queue-dir", default=None, help="Spec queue directory")
@click.option("--receipts-dir", default=None, help="Receipt directory")
@click.option("--repo", default=reflex.DEFAULT_REPO, help="Repo path recorded in each spec")
@click.option("--goal-id", default=None, help="Canonical goal id bound in spec front-matter and receipt")
def emit_command(
    do_write: bool,
    as_json: bool,
    findings_file: str | None,
    min_ticks: int,
    queue_dir: str | None,
    receipts_dir: str | None,
    repo: str,
    goal_id: str | None,
):
    """Queue one spec per open finding and record an emission receipt."""
    queue, receipts = _paths(queue_dir, receipts_dir)
    findings, errors = _resolve_findings(findings_file)
    report = reflex.emit(
        findings,
        queue_dir=queue,
        receipts_dir=receipts,
        write=do_write,
        min_ticks=min_ticks,
        repo=repo,
        goal_id=goal_id,
    )
    payload = report.to_dict()
    if errors:
        payload["source_errors"] = errors

    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return

    click.echo(f"Reflex emit — mode={payload['mode']} min_ticks={payload['min_ticks']}")
    for row in payload["emitted"]:
        verb = "queued" if row["wrote"] else "would queue"
        click.echo(f"  {verb} {row['spec_id']}  <- {row['finding_id']} [{row['kind']}]")
    for row in payload["rejected"]:
        click.echo(f"  rejected {row['finding_id']} [{row['kind']}]: {row['reason']}")
    for err in errors:
        click.echo(f"  source error: {err['source']}: {err['reason']}", err=True)
    if not payload["emitted"] and not payload["rejected"]:
        click.echo("No findings observed.")


@reflex_group.command("verify")
@click.option("--write", "do_write", is_flag=True, help="Persist receipts + re-queued specs (default: dry-run)")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
@click.option("--probe-file", default=None, help="JSON map of finding_id -> observation (injectable probe)")
@click.option("--ttl", "ttl_seconds", type=int, default=reflex.DEFAULT_TTL_SECONDS,
              help=f"Seconds a finding may persist before re-queue (default {reflex.DEFAULT_TTL_SECONDS})")
@click.option("--queue-dir", default=None, help="Spec queue directory")
@click.option("--receipts-dir", default=None, help="Receipt directory")
@click.option("--repo", default=reflex.DEFAULT_REPO, help="Repo path recorded in a re-queued spec")
def verify_command(
    do_write: bool,
    as_json: bool,
    probe_file: str | None,
    ttl_seconds: int,
    queue_dir: str | None,
    receipts_dir: str | None,
    repo: str,
):
    """Re-test every open receipt: clear it, hold it, or re-queue it."""
    queue, receipts = _paths(queue_dir, receipts_dir)
    probe: reflex.Probe = reflex.default_probe
    if probe_file:
        try:
            probe = _probe_from_file(probe_file)
        except (OSError, ValueError) as exc:
            payload = {
                "schema_version": reflex.SCHEMA_VERSION,
                "error": f"probe_file_unreadable: {type(exc).__name__}: {exc}",
            }
            if as_json:
                click.echo(json.dumps(payload, indent=2, sort_keys=True))
                return
            click.echo(f"Reflex verify — probe file unreadable: {exc}", err=True)
            return

    report = reflex.verify_open(
        probe=probe,
        receipts_dir=receipts,
        queue_dir=queue,
        write=do_write,
        ttl_seconds=ttl_seconds,
        repo=repo,
    )
    payload = report.to_dict()

    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True))
        return

    click.echo(f"Reflex verify — mode={payload['mode']} ttl={payload['ttl_seconds']}s")
    for row in payload["checked"]:
        click.echo(f"  {row['finding_id']}  {row['outcome']}  age={row['age_seconds']}s")
    for row in payload["escalated"]:
        click.echo(f"  re-queued {row['spec_id']} after {row['age_seconds']}s — {row['escalation_note']}")
    for row in payload["errors"]:
        click.echo(f"  probe error: {row['finding_id']}: {row['reason']}", err=True)
    if not payload["checked"]:
        click.echo("No open receipts.")


@reflex_group.command("status")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
@click.option("--findings-file", default=None, help="JSON file of raw finding rows (injectable source)")
@click.option("--ttl", "ttl_seconds", type=int, default=reflex.DEFAULT_TTL_SECONDS,
              help=f"Seconds a finding may persist before re-queue (default {reflex.DEFAULT_TTL_SECONDS})")
@click.option("--queue-dir", default=None, help="Spec queue directory")
@click.option("--receipts-dir", default=None, help="Receipt directory")
def status_command(
    as_json: bool,
    findings_file: str | None,
    ttl_seconds: int,
    queue_dir: str | None,
    receipts_dir: str | None,
):
    """List open findings, their queued spec ids, and receipt freshness."""
    queue, receipts = _paths(queue_dir, receipts_dir)
    findings, errors = _resolve_findings(findings_file)
    payload = reflex.build_status(
        findings=findings,
        queue_dir=queue,
        receipts_dir=receipts,
        ttl_seconds=ttl_seconds,
    )
    if errors:
        payload["source_errors"] = errors
    if as_json:
        click.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return
    click.echo(reflex.status_text(payload))


# The group is registered as `reflex` on the root CLI.
reflex_group.name = "reflex"
