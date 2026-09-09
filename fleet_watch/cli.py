"""Click CLI for Fleet Watch."""

from __future__ import annotations

import json
import sys

import click

from fleet_watch import cli_support as _cli_support
from fleet_watch.commands import register_all
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
from fleet_watch.commands.session import (
    session,
    session_check,
    session_close,
    session_ensure,
    session_heartbeat,
    session_list,
    session_start,
)
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
from fleet_watch.commands.thunder import (
    thunder,
    thunder_claim,
    thunder_heartbeat,
    thunder_release,
    thunder_sync,
)

# Re-export helpers and imported names tests reach via fleet_watch.cli
for _name, _value in vars(_cli_support).items():
    if _name.startswith("__"):
        continue
    globals()[_name] = _value
del _name, _value

_GROUP_FLAGS = frozenset({"-h", "--help", "-V", "--version"})


def _resolved_subcommand(argv: list[str]) -> str | None:
    """First non-option token after the group — the Click subcommand name.

    Group options on this CLI are flags only (``--help`` / ``--version``).
    Token membership is not enough: ``pkill guard --json`` and
    ``register --name guard --json`` contain the string ``guard`` without
    ever running a guard check.
    """
    for token in argv:
        if token == "--":
            return None
        if token in _GROUP_FLAGS or token.startswith("-"):
            continue
        return token
    return None


def _is_guard_json(argv: list[str]) -> bool:
    # Only the ``guard`` subcommand. ``context`` is a post-parse alias that
    # invokes guard with as_json=True after Click accepts context's own
    # (empty) option set; a usage error on ``context`` is not a guard check
    # and must stay Click usage text + exit 2.
    return _resolved_subcommand(argv) == "guard" and "--json" in argv


class FleetGroup(click.Group):
    """Keep ``fleet guard --json`` on the JSON contract through Click usage errors.

    Click raises ``UsageError`` during parameter conversion (negative GPU,
    non-integer port, etc.) before the command body runs. The guard contract
    says ``--json`` always emits ``{"allowed": false, ...}``, never usage text.
    """

    def main(self, args=None, **kwargs):
        argv = sys.argv[1:] if args is None else list(args)
        if _is_guard_json(argv):
            kwargs = dict(kwargs)
            # standalone_mode=False lets us catch ClickException and emit the
            # JSON deny. Click then no longer converts Exit/Abort to SystemExit:
            # Exit is returned as the integer code, Abort is re-raised. Map
            # both back so ctx.exit(n) still yields process exit n.
            kwargs["standalone_mode"] = False
            try:
                rv = super().main(args=args, **kwargs)
            except click.exceptions.Exit as exc:
                sys.exit(exc.exit_code)
            except click.Abort:
                sys.exit(1)
            except click.ClickException as exc:
                click.echo(
                    json.dumps(
                        {
                            "allowed": False,
                            "reason": f"usage_error: {exc.format_message()}",
                        }
                    )
                )
                sys.exit(1)
            if rv not in (None, 0):
                sys.exit(rv)
            return rv
        return super().main(args=args, **kwargs)


@click.group(cls=FleetGroup)
@click.version_option(package_name="fleet-watch")
def cli():
    """Fleet Watch — local process governance, plus read-only GitHub fleet sitrep."""
    pass


register_all(cli)


# Helpers live in cli_support; command modules import them by name.
# Tests that need to intercept a command-path helper patch the command
# module (importlib.import_module("fleet_watch.commands.<group>"), not the
# shadowed Click object on fleet_watch.commands) or cli_support. Direct
# helper calls on this module still work via the re-export loop above.


def main():
    """Run the Fleet Watch CLI entrypoint."""
    cli()


if __name__ == "__main__":
    main()
