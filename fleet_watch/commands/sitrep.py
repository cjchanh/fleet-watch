from __future__ import annotations

import json
import sys
from pathlib import Path

import click


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
