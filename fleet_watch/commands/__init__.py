"""CLI subcommand modules. ``register_all`` owns Click registration order."""

from __future__ import annotations

from fleet_watch.commands.census import boot_coverage, boot_map, census
from fleet_watch.commands.discover import (
    clean,
    discover,
    heartbeat,
    register,
    release,
    share_repo,
    watch,
)
from fleet_watch.commands.guard import check, claim, context, guard
from fleet_watch.commands.launchd import install_launchd
from fleet_watch.commands.pkill import pkill, preempt
from fleet_watch.commands.reap import reap, reap_sessions
from fleet_watch.commands.runaway import runaway_scan
from fleet_watch.commands.session import session
from fleet_watch.commands.sitrep import sitrep
from fleet_watch.commands.status import (
    changelog,
    health,
    history,
    reconcile,
    report,
    stale,
    status,
)
from fleet_watch.commands.thunder import thunder


def register_all(cli) -> None:
    """Attach subcommands in the historical ``cli.py`` definition order."""
    for command in (
        status,
        register,
        check,
        guard,
        claim,
        release,
        pkill,
        share_repo,
        heartbeat,
        session,
        preempt,
        report,
        history,
        stale,
        reconcile,
        reap,
        reap_sessions,
        clean,
        context,
        discover,
        watch,
        install_launchd,
        health,
        boot_coverage,
        changelog,
        thunder,
        runaway_scan,
        census,
        boot_map,
        sitrep,
    ):
        cli.add_command(command)
