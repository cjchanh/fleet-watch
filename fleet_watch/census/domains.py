"""census.domains — turn a :class:`SystemSnapshot` into contract-shaped domains.

Six domains, fixed order and fixed ``domain_id`` slugs (the machine key
consumers should bind to; ``domain`` is prose and may be reworded):

  1. ``user-launch-agents``   ~/Library/LaunchAgents + launchctl cross-ref
  2. ``global-daemons``       /Library/LaunchDaemons + /Library/LaunchAgents
  3. ``processes``            live process census, clustered
  4. ``network-listeners``    TCP listeners attributed to owning processes
  5. ``cron-login-items``     crontab, login items, brew services
  6. ``fleet-layer``          Fleet Watch registry + tmux sessions

Every builder is a pure function of the snapshot. Fail-closed: an item that
cannot be understood is emitted as ``unknown``/``investigate``, never dropped.
"""

from __future__ import annotations

import shlex
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from fleet_watch.census import verdicts
from fleet_watch.census.probes import (
    GLOBAL_LAUNCH_AGENTS,
    GLOBAL_LAUNCH_DAEMONS,
    USER_LAUNCH_AGENTS,
    ParsedPlist,
    ProcInfo,
    SystemSnapshot,
    cluster_key,
    is_synthetic_label,
    resolve_job_target,
    resolve_target,
)

#: Reported process clusters, ranked by aggregate resident memory.
TOP_CLUSTERS = 20

_HOME = str(Path.home())


def _tilde(path: str | Path) -> str:
    text = str(path)
    return "~" + text[len(_HOME) :] if text.startswith(_HOME) else text


@dataclass(frozen=True)
class CensusItem:
    label: str
    path: str
    status: str
    evidence: str
    verdict: str
    reason: str
    rule: str
    resource: str | None = None
    close_command: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "label": self.label,
            "path": self.path,
            "status": self.status,
            "evidence": self.evidence,
            "verdict": self.verdict,
            "reason": self.reason,
            "rule": self.rule,
        }
        if self.resource is not None:
            payload["resource"] = self.resource
        if self.close_command is not None:
            payload["close_command"] = self.close_command
        return payload


@dataclass
class CensusDomain:
    domain_id: str
    domain: str
    summary: str
    totals: dict[str, Any] = field(default_factory=dict)
    items: list[CensusItem] = field(default_factory=list)

    def verdict_counts(self) -> dict[str, int]:
        counts = {v: 0 for v in sorted(verdicts.VERDICTS)}
        for item in self.items:
            counts[item.verdict] += 1
        return counts

    def to_dict(self) -> dict[str, Any]:
        totals = dict(self.totals)
        totals.update(self.verdict_counts())
        totals["items"] = len(self.items)
        return {
            "domain_id": self.domain_id,
            "domain": self.domain,
            "summary": self.summary,
            "totals": totals,
            "items": [item.to_dict() for item in self.items],
        }


def _item(
    label: str,
    path: str,
    evidence: str,
    judgment: verdicts.Judgment,
    resource: str | None = None,
) -> CensusItem:
    return CensusItem(
        label=label,
        path=path,
        status=judgment.status,
        evidence=evidence,
        verdict=judgment.verdict,
        reason=judgment.reason,
        rule=judgment.rule,
        resource=resource,
        close_command=judgment.close_command,
    )


# --------------------------------------------------------------------------
# 1. user LaunchAgents
# --------------------------------------------------------------------------


def _plist_evidence(plist: ParsedPlist, target: str | None, note: str) -> str:
    if plist.parse_error is not None:
        return f"plistlib.load({_tilde(plist.path)}) raised {plist.parse_error}"
    return (
        f"plistlib.load({_tilde(plist.path)}): triggers={'+'.join(plist.triggers)}, "
        f"target={target or '(none declared)'} ({note})"
    )


def _count_rules(items: list[CensusItem], suffix: str) -> int:
    """Derive a domain counter from the items themselves, not a parallel tally."""
    return sum(1 for item in items if item.rule.endswith(suffix))


def build_user_launch_agents(snapshot: SystemSnapshot) -> CensusDomain:
    items: list[CensusItem] = []
    on_disk_labels: set[str] = set()
    not_loaded = 0
    nonzero_exit = 0

    for plist in sorted(snapshot.user_agents, key=lambda p: (p.label, str(p.path))):
        on_disk_labels.add(plist.label)
        target, target_exists, note = resolve_job_target(plist)
        entry = snapshot.launchctl.get(plist.label)
        disabled = snapshot.disabled.get(plist.label, False)
        judgment = verdicts.judge_launchd_agent(
            plist, entry, disabled, target_exists, target, note
        )

        if entry is None:
            not_loaded += 1
        elif entry.last_exit not in (None, 0):
            nonzero_exit += 1

        if entry is None:
            launchctl_note = "launchctl list: label absent (not loaded)"
        else:
            pid = entry.pid if entry.pid is not None else "-"
            launchctl_note = f"launchctl list: PID {pid}, last exit {entry.last_exit}"
        disabled_note = "; launchctl print-disabled: disabled" if disabled else ""

        items.append(
            _item(
                label=plist.label,
                path=_tilde(plist.path),
                evidence=(
                    f"{_plist_evidence(plist, target, note)}. "
                    f"{launchctl_note}{disabled_note}."
                ),
                judgment=judgment,
            )
        )

    # Both-direction orphan check: loaded labels with no plist anywhere we scan.
    # Synthetic `application.*` labels are excluded — launchd mints one per
    # running GUI app and they have no plist by design, so counting them as
    # orphans would bury the real finding under 20 rows of noise.
    known_labels = (
        on_disk_labels
        | {p.label for p in snapshot.global_daemons}
        | set(snapshot.system_labels)
    )
    for label in sorted(snapshot.launchctl):
        if label in known_labels or is_synthetic_label(label):
            continue
        entry = snapshot.launchctl[label]
        items.append(
            _item(
                label=label,
                path="(no plist found in any scanned directory)",
                evidence=(
                    f"launchctl list: PID {entry.pid if entry.pid is not None else '-'}, "
                    f"last exit {entry.last_exit}. No matching *.plist in "
                    f"{_tilde(USER_LAUNCH_AGENTS)}, {GLOBAL_LAUNCH_DAEMONS}, "
                    f"{GLOBAL_LAUNCH_AGENTS}, or the OS plist directories "
                    "(matched by filename stem)."
                ),
                judgment=verdicts.judge_orphan_label(entry),
            )
        )

    missing_target = _count_rules(items, "/missing-target")
    unparseable = _count_rules(items, "/unparseable")
    orphans = sum(1 for item in items if item.rule.startswith("orphan/"))

    summary = (
        f"{len(snapshot.user_agents)} plist file(s) in {_tilde(USER_LAUNCH_AGENTS)}; "
        f"{len(on_disk_labels) - not_loaded} loaded in launchctl, {not_loaded} present "
        f"but not loaded. {missing_target} declare a target that is missing from disk, "
        f"{nonzero_exit} report a non-zero last exit, {unparseable} could not be "
        f"parsed. Reverse orphan check found {orphans} loaded label(s) with no plist "
        "in any scanned directory. "
        f"{snapshot.probe_evidence('launchctl list')}."
    )
    if not items:
        summary = (
            f"No launch agents found: {_tilde(USER_LAUNCH_AGENTS)} is absent or empty "
            f"and {snapshot.probe_evidence('launchctl list')}."
        )

    return CensusDomain(
        domain_id="user-launch-agents",
        domain="user LaunchAgents (~/Library/LaunchAgents)",
        summary=summary,
        totals={
            "plist_files_on_disk": len(snapshot.user_agents),
            "not_loaded": not_loaded,
            "missing_target_executable": missing_target,
            "nonzero_last_exit": nonzero_exit,
            "unparseable_plists": unparseable,
            "orphan_loaded_no_plist": orphans,
        },
        items=items,
    )


# --------------------------------------------------------------------------
# 2. global daemons
# --------------------------------------------------------------------------


def _match_process(target: str | None, processes: list[ProcInfo]) -> ProcInfo | None:
    """Lowest-pid process that plausibly is this target.

    Tries the exact argv[0] path, then the executable basename, then the target
    appearing anywhere in the command line (app-bundle daemons often re-exec
    under a different argv[0]).
    """
    if not target:
        return None
    name = Path(target).name
    if not name:
        return None
    for proc in processes:
        if proc.argv0 == target:
            return proc
    for proc in processes:
        if Path(proc.argv0).name == name:
            return proc
    # Whole-token match only. An unanchored substring test would match any
    # unrelated process that merely mentions this path in its arguments (an
    # editor, `tail`, a backup job) and report a dead daemon as running.
    for proc in processes:
        if target in proc.command.split():
            return proc
    return None


def build_global_daemons(snapshot: SystemSnapshot) -> CensusDomain:
    """Third-party jobs under /Library.

    ``/Library/LaunchAgents`` loads into the calling user's GUI domain, so
    `launchctl list` is authoritative for those. ``/Library/LaunchDaemons``
    lives in the system domain, which is not readable without sudo — those fall
    back to process matching, and every such item says so in its evidence.
    """
    items: list[CensusItem] = []
    apple_skipped = 0
    agents = 0

    for plist in sorted(snapshot.global_daemons, key=lambda p: (p.label, str(p.path))):
        if plist.label.startswith("com.apple."):
            apple_skipped += 1
            continue
        target, target_exists, note = resolve_job_target(plist)

        is_agent = plist.path.parent == GLOBAL_LAUNCH_AGENTS
        if is_agent:
            agents += 1
            entry = snapshot.launchctl.get(plist.label)
            judgment = verdicts.judge_launchd_agent(
                plist,
                entry,
                snapshot.disabled.get(plist.label, False),
                target_exists,
                target,
                note,
                rule_prefix="global-agent",
            )
            if entry is None:
                state_note = "launchctl list: label absent (not loaded)"
            else:
                pid = entry.pid if entry.pid is not None else "-"
                state_note = (
                    f"launchctl list: PID {pid}, last exit {entry.last_exit}"
                )
            resource = "system-wide launch agent (user GUI domain)"
        else:
            proc = _match_process(target, snapshot.processes)
            judgment = verdicts.judge_global_daemon(
                plist, target_exists, proc.pid if proc else None, target, note
            )
            state_note = (
                f"ps -Ao: pid {proc.pid} runs {Path(proc.argv0).name}"
                if proc
                else "ps -Ao: no process matches this target (system-domain load "
                "state needs sudo)"
            )
            resource = "system daemon"

        items.append(
            _item(
                label=plist.label,
                path=str(plist.path),
                evidence=f"{_plist_evidence(plist, target, note)}. {state_note}.",
                judgment=judgment,
                resource=resource,
            )
        )

    missing_target = _count_rules(items, "/missing-target")
    summary = (
        f"{len(items)} third-party job(s) under /Library: {agents} agent(s) in "
        f"{GLOBAL_LAUNCH_AGENTS} (launchctl is authoritative) and "
        f"{len(items) - agents} daemon(s) in {GLOBAL_LAUNCH_DAEMONS} (system-domain "
        "load state needs sudo, so state is inferred from ps and each item says so). "
        f"{apple_skipped} com.apple.* entries skipped as OS noise; {missing_target} "
        "declare a missing target."
    )
    if not items:
        summary = (
            f"No third-party jobs found in {GLOBAL_LAUNCH_DAEMONS} or "
            f"{GLOBAL_LAUNCH_AGENTS} (probe returned nothing)."
        )

    return CensusDomain(
        domain_id="global-daemons",
        domain="global-daemons (/Library/LaunchDaemons + /Library/LaunchAgents)",
        summary=summary,
        totals={
            "thirdparty_plists": len(items),
            "library_launch_agents": agents,
            "library_launch_daemons": len(items) - agents,
            "apple_entries_skipped": apple_skipped,
            "missing_target_executable": missing_target,
        },
        items=items,
    )


# --------------------------------------------------------------------------
# 3. live processes
# --------------------------------------------------------------------------


def build_processes(snapshot: SystemSnapshot) -> CensusDomain:
    procs = snapshot.processes
    clusters: dict[str, list[ProcInfo]] = defaultdict(list)
    for proc in procs:
        clusters[cluster_key(proc.command)].append(proc)

    ranked = sorted(
        clusters.items(),
        key=lambda kv: (-sum(p.rss_kb for p in kv[1]), kv[0]),
    )

    items: list[CensusItem] = []
    for name, members in ranked[:TOP_CLUSTERS]:
        rss_mb = sum(p.rss_kb for p in members) // 1024
        cpu = sum(p.cpu_percent for p in members)
        oldest = max((p.etime_seconds for p in members), default=0)
        sample = ", ".join(str(p.pid) for p in sorted(members, key=lambda p: p.pid)[:5])
        items.append(
            _item(
                label=name,
                path=sorted(members, key=lambda p: p.pid)[0].argv0 or "(unknown)",
                evidence=(
                    f"ps -Ao: {len(members)} process(es) in this cluster, pids "
                    f"[{sample}], aggregate RSS {rss_mb} MB, aggregate CPU "
                    f"{cpu:.1f}%, oldest up {oldest // 3600}h."
                ),
                judgment=verdicts.judge_process_cluster(len(members), rss_mb, cpu),
                resource=f"{len(members)} procs / {rss_mb} MB RSS / {cpu:.1f}% CPU",
            )
        )

    zombies = [p for p in procs if p.is_zombie]
    for proc in zombies:
        items.append(
            _item(
                label=f"{cluster_key(proc.command)} (pid {proc.pid})",
                path=proc.argv0 or "(unknown)",
                evidence=f"ps -Ao: stat={proc.stat}, ppid={proc.ppid}, rss={proc.rss_kb} KB.",
                judgment=verdicts.judge_zombie(proc),
                resource="zombie process",
            )
        )

    hot = sorted(
        (p for p in procs if p.cpu_percent >= verdicts.HIGH_CPU_PERCENT),
        key=lambda p: (-p.cpu_percent, p.pid),
    )
    for proc in hot:
        items.append(
            _item(
                label=f"{cluster_key(proc.command)} (pid {proc.pid})",
                path=proc.argv0 or "(unknown)",
                evidence=(
                    f"ps -Ao: {proc.cpu_percent:.1f}% CPU, {proc.rss_kb // 1024} MB RSS, "
                    f"up {proc.etime_seconds // 60}m, command: {proc.command[:160]}"
                ),
                judgment=verdicts.judge_high_cpu(proc),
                resource=f"{proc.cpu_percent:.1f}% CPU",
            )
        )

    summary = (
        f"{len(procs)} live process(es) grouped into {len(clusters)} cluster(s); the "
        f"top {min(TOP_CLUSTERS, len(clusters))} by resident memory are itemized, plus "
        f"{len(zombies)} zombie(s) and {len(hot)} process(es) at or above "
        f"{verdicts.HIGH_CPU_PERCENT:.0f}% CPU. Cluster membership is deterministic: "
        "app bundles collapse to <App>.app, generic runtimes are qualified by their "
        f"first non-flag argument. {snapshot.ps_unparsed_lines} ps line(s) could not "
        f"be parsed and are not represented anywhere in this census. "
        f"{snapshot.probe_evidence('ps -Ao')}."
    )
    if not procs:
        summary = f"No process snapshot available: {snapshot.probe_evidence('ps -Ao')}."

    return CensusDomain(
        domain_id="processes",
        domain="live process census",
        summary=summary,
        totals={
            "total_processes": len(procs),
            "clusters": len(clusters),
            "clusters_itemized": min(TOP_CLUSTERS, len(clusters)),
            "zombies": len(zombies),
            "high_cpu_processes": len(hot),
            "ps_unparsed_lines": snapshot.ps_unparsed_lines,
            "cores": snapshot.machine.cores,
            "ram_gb": snapshot.machine.ram_gb,
        },
        items=items,
    )


# --------------------------------------------------------------------------
# 4. network listeners
# --------------------------------------------------------------------------


def build_network_listeners(snapshot: SystemSnapshot) -> CensusDomain:
    by_pid = {p.pid: p for p in snapshot.processes}
    launchd_pids = snapshot.launchd_pids
    items: list[CensusItem] = []
    exposed = 0

    for listener in snapshot.listeners:
        proc = by_pid.get(listener.pid)
        launchd_backed = listener.pid in launchd_pids or (
            proc is not None and proc.ppid == 1
        )
        if listener.is_wildcard:
            exposed += 1
        judgment = verdicts.judge_listener(listener, proc, launchd_backed)
        if proc is None:
            ps_note = "ps -Ao: pid not present"
            path = "(owning process not found)"
        else:
            ps_note = (
                f"ps -Ao: up {proc.etime_seconds // 86400}d "
                f"{(proc.etime_seconds % 86400) // 3600}h at {proc.cpu_percent:.1f}% "
                f"CPU, {proc.rss_kb // 1024} MB RSS, "
                f"{'launchd-backed' if launchd_backed else 'no launchd job behind it'}"
            )
            path = proc.command[:200]
        items.append(
            _item(
                label=f"{listener.command}:{listener.port}",
                path=path,
                evidence=(
                    f"lsof -nP -iTCP -sTCP:LISTEN: pid {listener.pid} "
                    f"({listener.command}) bound {listener.address}:{listener.port}. "
                    f"{ps_note}."
                ),
                judgment=judgment,
                resource=f"tcp/{listener.port}",
            )
        )

    summary = (
        f"{len(items)} TCP listener(s) after collapsing dual-stack duplicates on the "
        f"same (pid, port); {exposed} bound to all interfaces rather than loopback. "
        "Each listener is attributed to its owning process via ps and checked for a "
        f"launchd job behind it. {snapshot.probe_evidence('lsof')}."
    )
    if not items:
        summary = f"No TCP listeners observed: {snapshot.probe_evidence('lsof')}."

    return CensusDomain(
        domain_id="network-listeners",
        domain="network-listeners",
        summary=summary,
        totals={"tcp_listeners": len(items), "bound_all_interfaces": exposed},
        items=items,
    )


# --------------------------------------------------------------------------
# 5. cron, login items, brew services
# --------------------------------------------------------------------------


def _cron_target(command: str) -> str | None:
    """First executable token of a cron command line, quoting respected.

    `"/Signal Check/refresh.sh"` and `/Signal\\ Check/refresh.sh` are both valid
    cron syntax for ONE path containing a space. Splitting on whitespace would
    truncate them into a path that does not exist and mark a healthy job for
    removal — the same failure shape already fixed for launchd interpreters.
    """
    try:
        tokens = shlex.split(command)
    except ValueError:
        tokens = command.split()
    for token in tokens:
        if token and not token.startswith("-"):
            return token
    return None


def build_cron_login_items(snapshot: SystemSnapshot) -> CensusDomain:
    items: list[CensusItem] = []

    for entry in snapshot.cron:
        target = _cron_target(entry.command)
        _, exists = resolve_target(target)
        items.append(
            _item(
                label=f"crontab: {target or entry.command[:40]}",
                path=target or "(unresolved)",
                evidence=(
                    f"crontab -l: {entry.line[:200]} | schedule={entry.schedule} | "
                    f"target={'EXISTS' if exists else 'MISSING' if exists is False else 'UNRESOLVED'}"
                ),
                judgment=verdicts.judge_cron_entry(entry, exists),
                resource="cron",
            )
        )

    for item in sorted(snapshot.login_items, key=lambda i: (i.uid, i.name)):
        _, exists = resolve_target(item.executable)
        items.append(
            _item(
                label=f"login item: {item.name}",
                path=item.executable or "(no executable path recorded)",
                evidence=(
                    f"sfltool dumpbtm: uid={item.uid}, type={item.item_type}, "
                    f"disposition={item.disposition}, identifier={item.identifier}, "
                    f"target={'EXISTS' if exists else 'MISSING' if exists is False else 'not declared'}"
                ),
                judgment=verdicts.judge_login_item(item, exists),
                resource="login item",
            )
        )

    for service in sorted(snapshot.brew, key=lambda s: s.name):
        if service.status.lower() == "none":
            continue
        items.append(
            _item(
                label=f"brew service: {service.name}",
                path=service.plist or "(no plist recorded)",
                evidence=(
                    f"brew services list: {service.name} status={service.status} "
                    f"user={service.user or '-'}"
                ),
                judgment=verdicts.judge_brew_service(service),
                resource="brew service",
            )
        )

    active_brew = sum(1 for s in snapshot.brew if s.status.lower() != "none")
    summary = (
        f"{len(snapshot.cron)} crontab entr(ies), {len(snapshot.login_items)} login "
        f"item(s) from the background task manager, and {active_brew} active brew "
        f"service(s) out of {len(snapshot.brew)} known. "
        f"{snapshot.probe_evidence('crontab')}; {snapshot.probe_evidence('sfltool')}; "
        f"{snapshot.probe_evidence('services list')}."
    )
    if not items:
        summary = (
            "No cron entries, login items, or active brew services found. "
            f"{snapshot.probe_evidence('crontab')}; {snapshot.probe_evidence('sfltool')}."
        )

    return CensusDomain(
        domain_id="cron-login-items",
        domain="cron, login items, periodic, brew services",
        summary=summary,
        totals={
            "crontab_entries": len(snapshot.cron),
            "login_items": len(snapshot.login_items),
            "brew_services_known": len(snapshot.brew),
            "brew_services_active": active_brew,
        },
        items=items,
    )


# --------------------------------------------------------------------------
# 6. fleet layer
# --------------------------------------------------------------------------


def build_fleet_layer(snapshot: SystemSnapshot) -> CensusDomain:
    items: list[CensusItem] = []
    live_pids = snapshot.pids
    dead_rows = 0

    for row in snapshot.registry_processes:
        pid = row.get("pid")
        pid = pid if isinstance(pid, int) else None
        alive = pid in live_pids if pid is not None else False
        if pid is not None and not alive:
            dead_rows += 1
        name = str(row.get("name") or "unnamed")
        port = row.get("port")
        items.append(
            _item(
                label=f"fleet registry: {name}",
                path=str(row.get("start_cmd") or "(no start command recorded)"),
                evidence=(
                    f"fleet registry row: pid={pid}, workstream={row.get('workstream')}, "
                    f"port={port}. ps -Ao: pid "
                    f"{'present' if alive else 'absent'}."
                ),
                judgment=verdicts.judge_registry_process(pid, alive),
                resource=f"tcp/{port}" if port else "fleet-registered process",
            )
        )

    for session in snapshot.tmux:
        items.append(
            _item(
                label=f"tmux: {session.split(':', 1)[0]}",
                path="(tmux server session)",
                evidence=f"tmux ls: {session}",
                judgment=verdicts.Judgment(
                    status="running",
                    verdict="keep",
                    reason="tmux session is alive and holding its panes.",
                    rule="tmux/alive",
                ),
                resource="tmux session",
            )
        )

    summary = (
        f"{len(snapshot.registry_processes)} process(es) tracked in the Fleet Watch "
        f"registry ({dead_rows} holding a pid that no longer exists) and "
        f"{len(snapshot.tmux)} tmux session(s). "
        f"{snapshot.probe_evidence('tmux ls')}."
    )
    if not items:
        summary = (
            "Fleet Watch registry holds no processes and no tmux sessions are open — "
            f"probe returned nothing. {snapshot.probe_evidence('tmux ls')}."
        )

    return CensusDomain(
        domain_id="fleet-layer",
        domain="fleet layer (Fleet Watch registry + tmux sessions)",
        summary=summary,
        totals={
            "registry_rows": len(snapshot.registry_processes),
            "registry_dead_pids": dead_rows,
            "tmux_sessions": len(snapshot.tmux),
        },
        items=items,
    )


#: Fixed order — consumers may rely on both the order and the domain_id slugs.
DOMAIN_BUILDERS: tuple[Callable[[SystemSnapshot], CensusDomain], ...] = (
    build_user_launch_agents,
    build_global_daemons,
    build_processes,
    build_network_listeners,
    build_cron_login_items,
    build_fleet_layer,
)

DOMAIN_IDS: tuple[str, ...] = (
    "user-launch-agents",
    "global-daemons",
    "processes",
    "network-listeners",
    "cron-login-items",
    "fleet-layer",
)


def build_domains(snapshot: SystemSnapshot) -> list[CensusDomain]:
    return [builder(snapshot) for builder in DOMAIN_BUILDERS]
