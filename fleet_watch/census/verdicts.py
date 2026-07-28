"""census.verdicts — deterministic status/verdict heuristics.

Pure functions: facts in, :class:`Judgment` out. No I/O, no clock, no
randomness — the same snapshot always yields the same verdicts, which is what
makes drift between two receipts a real signal rather than model noise.

Every judgment carries the ``rule`` that produced it so a receipt testifies
*which* heuristic fired, not just what it concluded.

Fleet Watch never kills anything: ``close_command`` is advisory text for the
operator, emitted into the receipt and never executed.
"""

from __future__ import annotations

from dataclasses import dataclass

from fleet_watch.census.probes import (
    TARGET_UNVERIFIABLE,
    BrewService,
    CronEntry,
    LaunchctlEntry,
    Listener,
    LoginItem,
    ParsedPlist,
    ProcInfo,
)

STATUSES = frozenset(
    {"running", "idle-loaded", "dead", "failing", "stale", "orphan", "unknown"}
)
VERDICTS = frozenset({"keep", "investigate", "close", "remove"})

#: Label fragments that name work meant to happen once. A one-shot that is
#: still loaded long after its job is done is clutter, not coverage.
ONE_SHOT_FRAGMENTS = (
    "bootstrap",
    "install",
    "migrate",
    "migration",
    "once",
    "oneshot",
    "one-shot",
    "setup",
    "provision",
    "firstrun",
    "first-run",
)

#: A listener idle this long with no launchd job behind it is a leftover.
STALE_IDLE_SECONDS = 7 * 24 * 3600
STALE_CPU_PERCENT = 0.1

#: Cluster thresholds (count of processes / aggregate resident MB).
LARGE_CLUSTER_PROCS = 50
LARGE_CLUSTER_RSS_MB = 4096
HIGH_CPU_PERCENT = 90.0


@dataclass(frozen=True)
class Judgment:
    status: str
    verdict: str
    reason: str
    rule: str
    close_command: str | None = None

    def __post_init__(self) -> None:
        if self.status not in STATUSES:
            raise ValueError(f"invalid status: {self.status}")
        if self.verdict not in VERDICTS:
            raise ValueError(f"invalid verdict: {self.verdict}")


def _is_one_shot(label: str) -> bool:
    lowered = label.lower()
    return any(fragment in lowered for fragment in ONE_SHOT_FRAGMENTS)


def _bootout(label: str, uid_token: str = "$(id -u)") -> str:
    return f"launchctl bootout gui/{uid_token}/{label}"


# --------------------------------------------------------------------------
# user LaunchAgents
# --------------------------------------------------------------------------


def judge_launchd_agent(
    plist: ParsedPlist,
    entry: LaunchctlEntry | None,
    disabled: bool,
    target_exists: bool | None,
    display_target: str | None = None,
    target_note: str = "",
    rule_prefix: str = "user-agent",
) -> Judgment:
    """Verdict for one launchd *agent* — a job whose state `launchctl list` knows.

    Covers ``~/Library/LaunchAgents`` and ``/Library/LaunchAgents``: both load
    into the calling user's GUI domain, so launchctl is authoritative for both.

    Order matters: unparseable beats everything (we know least), then a missing
    target (the job cannot possibly work), then live launchctl state.
    """
    label = plist.label
    target = display_target if display_target is not None else plist.target

    if plist.parse_error is not None:
        return Judgment(
            status="unknown",
            verdict="investigate",
            reason=f"plist could not be parsed ({plist.parse_error}); state unknown.",
            rule=f"{rule_prefix}/unparseable",
        )

    if plist.is_empty_stub:
        return Judgment(
            status="unknown",
            verdict="investigate",
            reason="plist is an empty stub (no Label, no Program) and can never load.",
            rule=f"{rule_prefix}/empty-stub",
        )

    if target_exists is False:
        detail = f" ({target_note})" if target_note else ""
        return Judgment(
            status="stale",
            verdict="remove",
            reason=(
                f"target {target!r} does not exist on disk{detail}; the job can "
                "never run."
            ),
            rule=f"{rule_prefix}/missing-target",
            close_command=f"{_bootout(label)} && rm {plist.path}",
        )

    if target_exists is None:
        if target_note.startswith(TARGET_UNVERIFIABLE):
            return Judgment(
                status="unknown",
                verdict="investigate",
                reason=f"target could not be checked ({target_note}).",
                rule=f"{rule_prefix}/target-unverifiable",
            )
        return Judgment(
            status="unknown",
            verdict="investigate",
            reason="plist declares no Program or ProgramArguments; nothing to run.",
            rule=f"{rule_prefix}/no-target-declared",
        )

    if entry is None:
        if disabled or plist.disabled_in_plist:
            return Judgment(
                status="dead",
                verdict="keep",
                reason=(
                    "plist on disk, explicitly disabled — a deliberate operator "
                    "decision, not drift."
                ),
                rule=f"{rule_prefix}/disabled-on-purpose",
            )
        return Judgment(
            status="dead",
            verdict="investigate",
            reason="plist on disk but the label is absent from launchctl (never loaded).",
            rule=f"{rule_prefix}/not-loaded",
        )

    if entry.pid is not None:
        return Judgment(
            status="running",
            verdict="keep",
            reason=f"loaded and running as pid {entry.pid}.",
            rule=f"{rule_prefix}/running",
        )

    exit_code = entry.last_exit
    if exit_code is not None and exit_code != 0:
        if exit_code < 0:
            detail = f"last run was killed by signal {abs(exit_code)}"
        else:
            detail = f"last run exited {exit_code}"
        return Judgment(
            status="failing",
            verdict="investigate",
            reason=f"loaded but {detail}; the job is not completing cleanly.",
            rule=f"{rule_prefix}/nonzero-exit",
        )

    if _is_one_shot(label):
        return Judgment(
            status="idle-loaded",
            verdict="investigate",
            reason=(
                "label names a one-shot task and it is loaded but idle at exit 0 — "
                "likely spent and no longer needed."
            ),
            rule=f"{rule_prefix}/spent-one-shot",
            close_command=_bootout(label),
        )

    return Judgment(
        status="idle-loaded",
        verdict="keep",
        reason="loaded, on-demand, last exit clean.",
        rule=f"{rule_prefix}/idle-clean",
    )


def judge_orphan_label(entry: LaunchctlEntry) -> Judgment:
    """A label loaded in launchctl with no plist in any itemized directory."""
    if entry.pid is not None:
        return Judgment(
            status="orphan",
            verdict="investigate",
            reason=(
                f"running as pid {entry.pid} but no plist for this label exists in "
                "any scanned directory; its definition is unaccounted for."
            ),
            rule="orphan/loaded-running-no-plist",
            close_command=f"launchctl print gui/$(id -u)/{entry.label}",
        )
    return Judgment(
        status="orphan",
        verdict="investigate",
        reason=(
            "loaded in launchctl but no plist for this label exists in any scanned "
            "directory; its definition is unaccounted for."
        ),
        rule="orphan/loaded-no-plist",
        close_command=f"launchctl print gui/$(id -u)/{entry.label}",
    )


# --------------------------------------------------------------------------
# global daemons (/Library) — no sudo, so load state is inferred from ps
# --------------------------------------------------------------------------


def judge_global_daemon(
    plist: ParsedPlist,
    target_exists: bool | None,
    matched_pid: int | None,
    display_target: str | None = None,
    target_note: str = "",
) -> Judgment:
    """Verdict for a ``/Library/LaunchDaemons`` job.

    System-domain load state is not readable without sudo, so "running" here
    means "a matching process is live in ps", and every idle verdict says so.
    """
    target = display_target if display_target is not None else plist.target

    if plist.parse_error is not None:
        return Judgment(
            status="unknown",
            verdict="investigate",
            reason=f"plist could not be parsed ({plist.parse_error}); state unknown.",
            rule="global-daemon/unparseable",
        )

    if target_exists is False:
        detail = f" ({target_note})" if target_note else ""
        return Judgment(
            status="stale",
            verdict="remove",
            reason=(
                f"target {target!r} does not exist on disk{detail}; the daemon can "
                "never run (removal needs sudo)."
            ),
            rule="global-daemon/missing-target",
            close_command=(
                f"sudo launchctl bootout system/{plist.label} && sudo rm {plist.path}"
            ),
        )

    if target_exists is None:
        if target_note.startswith(TARGET_UNVERIFIABLE):
            return Judgment(
                status="unknown",
                verdict="investigate",
                reason=f"target could not be checked ({target_note}).",
                rule="global-daemon/target-unverifiable",
            )
        return Judgment(
            status="unknown",
            verdict="investigate",
            reason="plist declares no Program or ProgramArguments; nothing to run.",
            rule="global-daemon/no-target-declared",
        )

    if matched_pid is not None:
        return Judgment(
            status="running",
            verdict="keep",
            reason=f"a matching process is live (pid {matched_pid}).",
            rule="global-daemon/running",
        )

    persistent = {"RunAtLoad", "KeepAlive"} & set(plist.triggers)
    if persistent:
        return Judgment(
            status="dead",
            verdict="investigate",
            reason=(
                f"declares {'/'.join(sorted(persistent))} but no matching process is "
                "live; it should be running and is not."
            ),
            rule="global-daemon/should-be-running",
        )

    return Judgment(
        status="idle-loaded",
        verdict="keep",
        reason=(
            "on-demand daemon with a valid target and no live process; system-domain "
            "load state is not verifiable without sudo."
        ),
        rule="global-daemon/idle-unverified",
    )


# --------------------------------------------------------------------------
# network listeners
# --------------------------------------------------------------------------


def judge_listener(
    listener: Listener,
    proc: ProcInfo | None,
    launchd_backed: bool,
) -> Judgment:
    if proc is None:
        return Judgment(
            status="unknown",
            verdict="investigate",
            reason=(
                f"lsof reports pid {listener.pid} listening on tcp/{listener.port} but "
                "that pid is absent from the ps snapshot; the two probes disagree."
            ),
            rule="listener/pid-not-in-ps",
        )

    exposed = listener.is_wildcard
    idle = (
        proc.etime_seconds >= STALE_IDLE_SECONDS
        and proc.cpu_percent < STALE_CPU_PERCENT
    )

    if idle and not launchd_backed:
        days = proc.etime_seconds // 86400
        suffix = " and is bound to all interfaces" if exposed else ""
        return Judgment(
            status="stale",
            verdict="close",
            reason=(
                f"listener has been up {days}d at {proc.cpu_percent:.1f}% CPU with no "
                f"launchd job behind it{suffix}; nothing will restart it, so closing "
                "it costs no configuration."
            ),
            rule="listener/stale-unmanaged",
            close_command=f"kill {proc.pid}",
        )

    if exposed:
        return Judgment(
            status="running",
            verdict="investigate",
            reason=(
                f"bound to {listener.address}:{listener.port} — reachable beyond "
                "loopback; confirm that exposure is intended."
            ),
            rule="listener/bound-all-interfaces",
        )

    return Judgment(
        status="running",
        verdict="keep",
        reason=f"loopback listener on tcp/{listener.port} with a live owning process.",
        rule="listener/loopback-live",
    )


# --------------------------------------------------------------------------
# processes
# --------------------------------------------------------------------------


def judge_process_cluster(count: int, rss_mb: int, cpu_percent: float) -> Judgment:
    if count >= LARGE_CLUSTER_PROCS:
        return Judgment(
            status="running",
            verdict="investigate",
            reason=(
                f"{count} processes in one cluster holding {rss_mb} MB resident; "
                "large fan-out worth confirming."
            ),
            rule="process/large-cluster-count",
        )
    if rss_mb >= LARGE_CLUSTER_RSS_MB:
        return Judgment(
            status="running",
            verdict="investigate",
            reason=f"cluster holds {rss_mb} MB resident across {count} processes.",
            rule="process/large-cluster-rss",
        )
    return Judgment(
        status="running",
        verdict="keep",
        reason=f"{count} process(es), {rss_mb} MB resident; within normal bounds.",
        rule="process/normal",
    )


def judge_zombie(proc: ProcInfo) -> Judgment:
    return Judgment(
        status="dead",
        verdict="investigate",
        reason=(
            f"zombie process (stat {proc.stat}); its parent pid {proc.ppid} has not "
            "reaped it."
        ),
        rule="process/zombie",
    )


def judge_high_cpu(proc: ProcInfo) -> Judgment:
    return Judgment(
        status="running",
        verdict="investigate",
        reason=(
            f"pid {proc.pid} is at {proc.cpu_percent:.1f}% CPU in this snapshot; "
            "confirm it is intended work and not a runaway."
        ),
        rule="process/high-cpu",
    )


# --------------------------------------------------------------------------
# cron / login items / brew
# --------------------------------------------------------------------------


def judge_cron_entry(entry: CronEntry, target_exists: bool | None) -> Judgment:
    if target_exists is False:
        return Judgment(
            status="stale",
            verdict="remove",
            reason="the script this cron line invokes does not exist on disk.",
            rule="cron/missing-target",
            close_command=f"crontab -e   # delete the line: {entry.line}",
        )
    if target_exists is None:
        return Judgment(
            status="unknown",
            verdict="investigate",
            reason="could not resolve an executable path from this cron line.",
            rule="cron/unresolved-target",
        )
    return Judgment(
        status="idle-loaded",
        verdict="keep",
        reason=f"scheduled ({entry.schedule}) and its target exists.",
        rule="cron/scheduled",
    )


def judge_login_item(item: LoginItem, target_exists: bool | None) -> Judgment:
    if target_exists is False:
        return Judgment(
            status="stale",
            verdict="remove",
            reason=(
                f"login item points at {item.executable!r}, which does not exist "
                "on disk."
            ),
            rule="login-item/missing-target",
            close_command="open 'x-apple.systempreferences:com.apple.LoginItems-Settings.extension'",
        )
    if not item.enabled:
        return Judgment(
            status="dead",
            verdict="keep",
            reason="registered but disabled; it will not launch at login.",
            rule="login-item/disabled",
        )
    return Judgment(
        status="idle-loaded",
        verdict="keep",
        reason="enabled login item; launches at login.",
        rule="login-item/enabled",
    )


def judge_brew_service(service: BrewService) -> Judgment:
    status = service.status.lower()
    if status in {"error", "unknown"}:
        return Judgment(
            status="failing",
            verdict="investigate",
            reason=f"brew reports status {service.status!r}.",
            rule="brew/error",
            close_command=f"brew services info {service.name}",
        )
    if status in {"started", "scheduled"}:
        return Judgment(
            status="running",
            verdict="keep",
            reason=f"brew service is {service.status}.",
            rule="brew/started",
        )
    return Judgment(
        status="idle-loaded",
        verdict="keep",
        reason=f"brew service is present but {service.status}.",
        rule="brew/stopped",
    )


# --------------------------------------------------------------------------
# Fleet Watch's own registry
# --------------------------------------------------------------------------


def judge_registry_process(pid: int | None, alive: bool) -> Judgment:
    if pid is None:
        return Judgment(
            status="unknown",
            verdict="investigate",
            reason="registry row carries no pid; it cannot be checked against ps.",
            rule="registry/no-pid",
        )
    if alive:
        return Judgment(
            status="running",
            verdict="keep",
            reason=(
                f"registered pid {pid} is live in ps; identity is not cross-checked "
                "(the processes table records no create-time, so a recycled pid "
                "would read as alive)."
            ),
            rule="registry/live",
        )
    return Judgment(
        status="dead",
        verdict="investigate",
        reason=(
            f"registry holds pid {pid} but no such process exists; the registry is "
            "stale against reality."
        ),
        rule="registry/dead-pid",
        close_command="fleet clean",
    )
