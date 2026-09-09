from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import click

from fleet_watch.cli_support import (
    _render_launchd_plist,
)

@click.command("install-launchd")
@click.option("--interval", type=int, default=60, help="Seconds between scans")
@click.option(
    "--output",
    "output_path",
    type=click.Path(path_type=Path),
    default=Path.home() / "Library/LaunchAgents/io.fleet-watch.plist",
    help="Where to write the plist",
)
@click.option("--load/--no-load", default=True, help="Load the agent after writing")
def install_launchd(interval: int, output_path: Path, load: bool):
    """Write a launchd plist with the real fleet executable path."""
    executable = shutil.which("fleet")
    if executable is None:
        click.echo("fleet executable not found in PATH", err=True)
        sys.exit(1)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(_render_launchd_plist(executable, interval))
    click.echo(f"Written: {output_path}")

    if not load:
        return

    subprocess.run(
        ["launchctl", "unload", str(output_path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    result = subprocess.run(
        ["launchctl", "load", str(output_path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode != 0:
        click.echo(result.stderr.strip() or result.stdout.strip(), err=True)
        sys.exit(result.returncode)

    click.echo("Loaded: io.fleet-watch")
