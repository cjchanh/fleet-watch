"""census.probes — read-only, timeout-bounded system probes for `fleet census`.

Every probe is deterministic, read-only, and never raises. A probe that cannot
run returns a :class:`ProbeResult` with ``ok=False`` and an error string so the
census can testify "probe returned nothing" instead of inventing items
(Craft Gate pillar 1 — fail-closed).

No network, no LLM, no writes. Parsing is separated from execution so every
parser is unit-testable against captured fixtures.
"""

from __future__ import annotations

import platform
import plistlib
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence
from xml.parsers.expat import ExpatError

DEFAULT_TIMEOUT = 15.0

USER_LAUNCH_AGENTS = Path.home() / "Library" / "LaunchAgents"
GLOBAL_LAUNCH_DAEMONS = Path("/Library/LaunchDaemons")
GLOBAL_LAUNCH_AGENTS = Path("/Library/LaunchAgents")
SYSTEM_PLIST_DIRS = (
    Path("/System/Library/LaunchAgents"),
    Path("/System/Library/LaunchDaemons"),
)

# Runtimes whose argv[0] says nothing useful about what is actually running:
# `python3.14 foo.py` clusters as "foo.py", not as "every python on the box".
RUNTIME_WRAPPER_RE = re.compile(
    r"^(?:python[\d.]*|node|deno|bun|ruby|perl|sh|bash|zsh|dash|ksh|env|uv|git"
    r"|osascript|java)$",
    re.IGNORECASE,
)

#: launchd synthesises `application.<bundle-id>.<n>.<n>` labels for every running
#: GUI app. They have no plist anywhere by design, so they are not orphans.
SYNTHETIC_LABEL_PREFIXES = ("application.", "com.apple.")

#: `sfltool dumpbtm` reads the background-task database and routinely stalls for
#: minutes on a loaded machine. Keep the default census fast and honest about the
#: gap; `--deep` buys full coverage when the operator wants it.
LOGIN_ITEM_TIMEOUT = 30.0
LOGIN_ITEM_DEEP_TIMEOUT = 180.0


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of one external probe. Never raises; failure is data."""

    command: str
    ok: bool
    stdout: str = ""
    returncode: int | None = None
    error: str | None = None

    @property
    def evidence(self) -> str:
        if self.ok:
            return f"`{self.command}` -> {len(self.stdout.splitlines())} lines"
        return f"`{self.command}` -> probe returned nothing ({self.error})"

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": self.command,
            "ok": self.ok,
            "lines": len(self.stdout.splitlines()) if self.ok else 0,
            "error": self.error,
        }


def run_probe(argv: Sequence[str], timeout: float = DEFAULT_TIMEOUT) -> ProbeResult:
    """Run a read-only command. Returns a ProbeResult; never raises."""
    command = " ".join(argv)
    try:
        proc = subprocess.run(
            list(argv),
            capture_output=True,
            text=True,
            # `ps` echoes raw argv, which is not guaranteed to be valid UTF-8.
            # Strict decoding would raise UnicodeDecodeError — not an OSError,
            # so it would escape this handler and break "probes never raise".
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError:
        return ProbeResult(command, False, error="executable not found")
    except PermissionError:
        return ProbeResult(command, False, error="permission denied")
    except subprocess.TimeoutExpired:
        return ProbeResult(command, False, error=f"timed out after {timeout:g}s")
    except OSError as exc:  # pragma: no cover - defensive
        return ProbeResult(command, False, error=f"os error: {exc}")

    if proc.returncode != 0 and not proc.stdout.strip():
        detail = (proc.stderr or "").strip().splitlines()
        reason = detail[0][:200] if detail else "no output"
        return ProbeResult(
            command,
            False,
            stdout=proc.stdout,
            returncode=proc.returncode,
            error=f"exit {proc.returncode}: {reason}",
        )
    return ProbeResult(command, True, stdout=proc.stdout, returncode=proc.returncode)


def _to_int(raw: str) -> int | None:
    raw = raw.strip()
    if not raw or raw == "-":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


# --------------------------------------------------------------------------
# launchctl
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class LaunchctlEntry:
    """One row of `launchctl list` in the calling user's domain."""

    label: str
    pid: int | None
    last_exit: int | None


def parse_launchctl_list(stdout: str) -> dict[str, LaunchctlEntry]:
    """Parse `launchctl list` (PID / Status / Label, tab-separated)."""
    entries: dict[str, LaunchctlEntry] = {}
    for line in stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            parts = line.split()
        if len(parts) < 3:
            continue
        label = parts[-1].strip()
        if not label or label == "Label":
            continue
        entries[label] = LaunchctlEntry(
            label=label,
            pid=_to_int(parts[0]),
            last_exit=_to_int(parts[1]),
        )
    return entries


_DISABLED_RE = re.compile(r'"([^"]+)"\s*=>\s*(\w+)')


def parse_print_disabled(stdout: str) -> dict[str, bool]:
    """Parse `launchctl print-disabled <domain>` into {label: disabled?}."""
    disabled: dict[str, bool] = {}
    for match in _DISABLED_RE.finditer(stdout):
        label, value = match.group(1), match.group(2).lower()
        disabled[label] = value in {"disabled", "true", "1"}
    return disabled


def launchctl_list(timeout: float = DEFAULT_TIMEOUT) -> tuple[
    dict[str, LaunchctlEntry], ProbeResult
]:
    result = run_probe(["launchctl", "list"], timeout=timeout)
    return (parse_launchctl_list(result.stdout) if result.ok else {}), result


def launchctl_print_disabled(
    domain: str, timeout: float = DEFAULT_TIMEOUT
) -> tuple[dict[str, bool], ProbeResult]:
    result = run_probe(["launchctl", "print-disabled", domain], timeout=timeout)
    return (parse_print_disabled(result.stdout) if result.ok else {}), result


# --------------------------------------------------------------------------
# launchd plists
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedPlist:
    """A launchd job definition read off disk.

    ``parse_error`` set means the file exists but could not be understood — the
    item is still surfaced (unknown/investigate), never dropped.
    """

    path: Path
    label: str
    target: str | None = None
    program_arguments: tuple[str, ...] = ()
    triggers: tuple[str, ...] = ()
    disabled_in_plist: bool = False
    working_directory: str | None = None
    keys: tuple[str, ...] = ()
    parse_error: str | None = None

    @property
    def is_empty_stub(self) -> bool:
        return self.parse_error is None and not self.keys


def _infer_triggers(data: dict[str, Any]) -> tuple[str, ...]:
    triggers: list[str] = []
    if data.get("RunAtLoad"):
        triggers.append("RunAtLoad")
    if data.get("KeepAlive"):
        triggers.append("KeepAlive")
    interval = data.get("StartInterval")
    if isinstance(interval, int):
        triggers.append(f"StartInterval={interval}s")
    if data.get("StartCalendarInterval"):
        triggers.append("StartCalendarInterval")
    if data.get("WatchPaths"):
        triggers.append("WatchPaths")
    if data.get("QueueDirectories"):
        triggers.append("QueueDirectories")
    if data.get("Sockets"):
        triggers.append("Sockets")
    if data.get("StartOnMount"):
        triggers.append("StartOnMount")
    return tuple(triggers) if triggers else ("on-demand",)


def _target_of(data: dict[str, Any]) -> tuple[str | None, tuple[str, ...]]:
    args_raw = data.get("ProgramArguments")
    args: tuple[str, ...] = ()
    if isinstance(args_raw, list):
        args = tuple(str(a) for a in args_raw)
    program = data.get("Program") or data.get("BundleProgram")
    if isinstance(program, str) and program.strip():
        return program.strip(), args
    if args:
        return args[0], args
    return None, args


def parse_plist_file(path: Path) -> ParsedPlist:
    """Read one launchd plist. Unreadable/corrupt files return parse_error."""
    try:
        with open(path, "rb") as handle:
            data = plistlib.load(handle)
    except (OSError, plistlib.InvalidFileException, ExpatError, ValueError) as exc:
        return ParsedPlist(
            path=path, label=path.stem, parse_error=f"{type(exc).__name__}: {exc}"
        )

    if not isinstance(data, dict):
        return ParsedPlist(
            path=path,
            label=path.stem,
            parse_error="plist root is not a dictionary",
        )

    label_raw = data.get("Label")
    label = label_raw.strip() if isinstance(label_raw, str) and label_raw.strip() else path.stem
    target, args = _target_of(data)
    workdir = data.get("WorkingDirectory")
    return ParsedPlist(
        path=path,
        label=label,
        target=target,
        program_arguments=args,
        triggers=_infer_triggers(data),
        disabled_in_plist=bool(data.get("Disabled")),
        working_directory=workdir if isinstance(workdir, str) and workdir else None,
        keys=tuple(sorted(str(k) for k in data)),
    )


def read_plist_dir(directory: Path) -> list[ParsedPlist]:
    """Parse every ``*.plist`` in a directory. Missing dir -> empty list."""
    try:
        if not directory.is_dir():
            return []
        paths = sorted(directory.glob("*.plist"))
    except OSError:
        return []
    return [parse_plist_file(p) for p in paths]


def system_plist_labels(dirs: Sequence[Path] = SYSTEM_PLIST_DIRS) -> frozenset[str]:
    """Label set for OS-shipped jobs, by filename stem.

    Used only to suppress false "orphan" findings for labels loaded from
    directories the census does not itemize. Stems are used rather than a full
    parse because these are ~900 vendor files whose Label always matches the
    filename; the heuristic is recorded in the domain evidence.
    """
    labels: set[str] = set()
    for directory in dirs:
        try:
            if not directory.is_dir():
                continue
            for path in directory.glob("*.plist"):
                labels.add(path.stem)
        except OSError:
            continue
    return frozenset(labels)


def resolve_target(target: str | None) -> tuple[Path | None, bool | None]:
    """Resolve a plist target to a path and existence flag.

    Returns ``(None, None)`` when the job declares no target at all — that is
    "undeclared", which is a different finding from "declared but missing".
    ``exists()`` follows symlinks, so a link to a deleted script reads False.
    """
    if not target:
        return None, None
    expanded = Path(target).expanduser()
    if not expanded.is_absolute():
        found = shutil.which(target)
        if found is None:
            return expanded, False
        expanded = Path(found)
    try:
        return expanded, expanded.exists()
    except OSError:
        return expanded, False


#: `resolve_job_target` note meaning "we could not check this target at all".
#: Distinct from "declared and missing" — an unverifiable target must never be
#: reported as healthy, and must never be reported as a removal candidate.
TARGET_UNVERIFIABLE = "unverifiable"


def _path_candidates(arg: str) -> list[str]:
    """Ways to read one argument as a path: whole argument, then first token.

    ``bash "/Signal Check/refresh.sh"`` is ONE path containing a space, while
    ``bash "/x/run.sh --flag"`` is a command whose first token is the path.
    Truncating at the space unconditionally reported a healthy job as missing
    and marked it for removal.
    """
    candidates = [arg]
    first_token = arg.split()[0] if arg.split() else arg
    if first_token != arg:
        candidates.append(first_token)
    return candidates


def _is_relative(candidate: str) -> bool:
    return not candidate.startswith(("/", "~"))


#: Absolute (or ~) path-shaped tokens inside an inline shell command.
_ABS_PATH_RE = re.compile(r"(?<![\w=])((?:/|~/)[^\s'\";|&)>]+)")


def _inline_command_paths(command: str) -> list[str]:
    return _ABS_PATH_RE.findall(command)


def _resolve_inline_command(interpreter: str, command: str) -> tuple[str, bool | None, str]:
    """Judge `sh -c "<command>"`, which is a program, not a path.

    We cannot execute a shell parser, so the best available evidence is the
    absolute paths the command references. Missing ones are worth a look but
    are never a removal candidate: an absolute path in a shell line can just as
    easily be an output file that does not exist yet.
    """
    referenced = _inline_command_paths(command)
    if not referenced:
        return (
            command,
            None,
            f"{TARGET_UNVERIFIABLE}: {interpreter} runs an inline command that "
            "references no absolute path",
        )
    missing = [path for path in referenced if resolve_target(path)[1] is False]
    if missing:
        return (
            missing[0],
            None,
            f"{TARGET_UNVERIFIABLE}: {interpreter} runs an inline command "
            f"referencing {missing[0]!r}, which is missing",
        )
    return (
        referenced[0],
        True,
        f"inline {interpreter} command; all {len(referenced)} referenced "
        "absolute path(s) exist",
    )


def _resolve_interpreter_target(
    interpreter_path: str, args: Sequence[str], workdir: str | None, depth: int = 0
) -> tuple[str | None, bool | None, str]:
    """Work out what an interpreter actually runs.

    Only an ABSOLUTE script path can produce "missing" (and therefore a removal
    candidate). A relative argument may be a module name, a subcommand, or a
    path resolved against a WorkingDirectory we cannot see — guessing "missing"
    there is how a healthy job gets marked for deletion.
    """
    interpreter = Path(interpreter_path).name

    index = 0
    while index < len(args):
        arg = args[index]
        if not arg:
            index += 1
            continue

        if interpreter == "env" and "=" in arg and not arg.startswith(("-", "/", "~")):
            index += 1  # env VAR=VALUE ... prefix assignment
            continue

        if arg in {"-c", "-e", "--command"}:
            if index + 1 < len(args):
                return _resolve_inline_command(interpreter, args[index + 1])
            return (
                arg,
                None,
                f"{TARGET_UNVERIFIABLE}: {interpreter} was given {arg} with no command",
            )

        if arg == "-m":
            module = args[index + 1] if index + 1 < len(args) else "(none)"
            return (
                f"-m {module}",
                None,
                f"{TARGET_UNVERIFIABLE}: {interpreter} runs the module {module!r}, "
                "which is an import name and not a file path",
            )

        if arg.startswith("-"):
            index += 1
            continue

        # env re-dispatches to the real interpreter.
        if interpreter == "env" and depth < 2:
            resolved, _ = resolve_target(arg)
            name = Path(arg).name
            if RUNTIME_WRAPPER_RE.match(name):
                return _resolve_interpreter_target(
                    str(resolved or arg), args[index + 1 :], workdir, depth + 1
                )

        if _is_relative(arg):
            if workdir:
                for candidate in _path_candidates(arg):
                    joined = str(Path(workdir) / candidate)
                    if resolve_target(joined)[1]:
                        return joined, True, f"run by {interpreter}; script exists"
            return (
                arg,
                None,
                f"{TARGET_UNVERIFIABLE}: {interpreter} runs the relative argument "
                f"{arg!r}, which may be a subcommand or a path this plist does not "
                "anchor",
            )

        for candidate in _path_candidates(arg):
            if resolve_target(candidate)[1]:
                return candidate, True, f"run by {interpreter}; script exists"
        return (
            arg,
            False,
            f"interpreter {interpreter} exists but the script it runs is missing",
        )

    return (
        interpreter_path,
        None,
        f"{TARGET_UNVERIFIABLE}: {interpreter} was given no argument to run",
    )


def resolve_job_target(plist: ParsedPlist) -> tuple[str | None, bool | None, str]:
    """Resolve what a job *actually* runs, seeing through interpreter wrappers.

    ``/bin/bash /Users/cj/bin/watchdog.sh`` reports on ``watchdog.sh``, not on
    bash — otherwise every broken script hides behind an interpreter that always
    exists. Returns ``(display_target, exists, note)`` where ``exists`` is
    ``None`` for "could not be checked", which is neither healthy nor removable.
    """
    primary = plist.target
    if not primary:
        return None, None, "no Program or ProgramArguments declared"

    _, primary_exists = resolve_target(primary)
    if primary_exists is False:
        return primary, False, "declared target missing from disk"

    if RUNTIME_WRAPPER_RE.match(Path(primary).name):
        return _resolve_interpreter_target(
            primary, plist.program_arguments[1:], plist.working_directory
        )

    return primary, primary_exists, "target exists"


def is_synthetic_label(label: str) -> bool:
    """True for launchd labels that never have a plist on disk by design."""
    return label.startswith(SYNTHETIC_LABEL_PREFIXES)


# --------------------------------------------------------------------------
# processes
# --------------------------------------------------------------------------

_PS_FORMAT = "pid=,ppid=,stat=,etime=,pcpu=,rss=,user=,command="
_PS_RE = re.compile(
    r"^\s*(\d+)\s+(\d+)\s+(\S+)\s+(\S+)\s+([\d.]+)\s+(\d+)\s+(\S+)\s+(.*)$"
)


@dataclass(frozen=True)
class ProcInfo:
    pid: int
    ppid: int
    stat: str
    etime_seconds: int
    cpu_percent: float
    rss_kb: int
    user: str
    command: str

    @property
    def is_zombie(self) -> bool:
        return self.stat.upper().startswith("Z")

    @property
    def argv0(self) -> str:
        return self.command.split(" ", 1)[0] if self.command else ""


def parse_etime(raw: str) -> int:
    """Parse ps ``etime`` (``[[DD-]HH:]MM:SS``) into seconds. Unparseable -> 0."""
    raw = raw.strip()
    days = 0
    if "-" in raw:
        day_part, _, raw = raw.partition("-")
        try:
            days = int(day_part)
        except ValueError:
            return 0
    parts = raw.split(":")
    try:
        values = [int(p) for p in parts]
    except ValueError:
        return 0
    while len(values) < 3:
        values.insert(0, 0)
    if len(values) > 3:
        return 0
    hours, minutes, seconds = values
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def parse_ps(stdout: str) -> tuple[list[ProcInfo], int]:
    """Parse `ps -Ao ...`. Returns ``(processes, unparsed_line_count)``.

    Unparsed lines are counted rather than silently discarded: a process that
    vanishes from the snapshot also vanishes from listener attribution and
    daemon matching, and a census that cannot say so is not testifying.
    """
    procs: list[ProcInfo] = []
    unparsed = 0
    for line in stdout.splitlines():
        match = _PS_RE.match(line)
        if match is None:
            if line.strip():
                unparsed += 1
            continue
        pid, ppid, stat, etime, pcpu, rss, user, command = match.groups()
        procs.append(
            ProcInfo(
                pid=int(pid),
                ppid=int(ppid),
                stat=stat,
                etime_seconds=parse_etime(etime),
                cpu_percent=float(pcpu),
                rss_kb=int(rss),
                user=user,
                command=command.strip(),
            )
        )
    return sorted(procs, key=lambda p: p.pid), unparsed


def ps_snapshot(
    timeout: float = DEFAULT_TIMEOUT,
) -> tuple[list[ProcInfo], int, ProbeResult]:
    result = run_probe(["ps", "-Ao", _PS_FORMAT], timeout=timeout)
    if not result.ok:
        return [], 0, result
    procs, unparsed = parse_ps(result.stdout)
    return procs, unparsed, result


def cluster_key(command: str) -> str:
    """Deterministic process-cluster name for a full argv string.

    Real app bundles collapse to ``<App>.app``; generic runtimes are qualified
    by their first non-flag argument, so 116 ``git fsmonitor--daemon`` processes
    read as one cluster rather than as "git", and framework-embedded pythons
    cluster by the script they run rather than all collapsing to "Python.app".
    """
    if not command:
        return "unknown"
    tokens = command.split()
    argv0 = tokens[0]

    if ".framework/" not in argv0:
        app_match = re.findall(r"([^/]+)\.app/", argv0)
        if app_match:
            return f"{app_match[-1]}.app"

    base = Path(argv0).name or argv0
    if RUNTIME_WRAPPER_RE.match(base):
        rest = tokens[1:]
        for index, token in enumerate(rest):
            if token == "-c":
                # Inline code: the next token is source, not a script name.
                return f"{base} -c (inline)"
            if token == "-m":
                module = rest[index + 1] if index + 1 < len(rest) else "(module)"
                return f"{base} -m {module}"
            if token.startswith("-"):
                continue
            return f"{base} {Path(token).name or token}"
    return base


# --------------------------------------------------------------------------
# network listeners
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Listener:
    pid: int
    command: str
    address: str
    port: int

    @property
    def is_loopback(self) -> bool:
        return self.address in {"127.0.0.1", "::1", "localhost"}

    @property
    def is_wildcard(self) -> bool:
        return self.address in {"*", "0.0.0.0", "::"}


def _split_hostport(raw: str) -> tuple[str, int] | None:
    raw = raw.strip()
    if not raw or ":" not in raw:
        return None
    host, _, port_raw = raw.rpartition(":")
    host = host.strip("[]") or "*"
    try:
        return host, int(port_raw)
    except ValueError:
        return None


def parse_lsof_fields(stdout: str) -> list[Listener]:
    """Parse `lsof -F pcn` records into deduped (pid, port) listeners."""
    seen: dict[tuple[int, int], Listener] = {}
    pid: int | None = None
    command = ""
    for line in stdout.splitlines():
        if not line:
            continue
        tag, value = line[0], line[1:]
        if tag == "p":
            pid = _to_int(value)
            command = ""
        elif tag == "c":
            command = value.strip()
        elif tag == "n" and pid is not None:
            hostport = _split_hostport(value)
            if hostport is None:
                continue
            address, port = hostport
            key = (pid, port)
            existing = seen.get(key)
            # Prefer the most exposed binding when a pid listens on both stacks.
            if existing is None or (
                address in {"*", "0.0.0.0", "::"} and not existing.is_wildcard
            ):
                seen[key] = Listener(
                    pid=pid, command=command or "unknown", address=address, port=port
                )
    return [seen[k] for k in sorted(seen)]


def tcp_listeners(timeout: float = DEFAULT_TIMEOUT) -> tuple[list[Listener], ProbeResult]:
    result = run_probe(
        ["lsof", "-nP", "-iTCP", "-sTCP:LISTEN", "-F", "pcn"], timeout=timeout
    )
    return (parse_lsof_fields(result.stdout) if result.ok else []), result


# --------------------------------------------------------------------------
# cron / login items / brew
# --------------------------------------------------------------------------

_CRON_SPECIAL = ("@reboot", "@daily", "@hourly", "@weekly", "@monthly", "@yearly", "@annually", "@midnight")


@dataclass(frozen=True)
class CronEntry:
    line: str
    schedule: str
    command: str


def parse_crontab(stdout: str) -> list[CronEntry]:
    entries: list[CronEntry] = []
    for raw in stdout.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line.split()[0] and not line.startswith("@"):
            continue  # environment assignment, not a job
        if line.startswith("@"):
            schedule, _, command = line.partition(" ")
            if schedule.lower() not in _CRON_SPECIAL:
                continue
        else:
            fields = line.split(None, 5)
            if len(fields) < 6:
                continue
            schedule = " ".join(fields[:5])
            command = fields[5]
        command = command.strip()
        if not command:
            continue
        entries.append(CronEntry(line=line, schedule=schedule, command=command))
    return entries


def crontab_entries(timeout: float = DEFAULT_TIMEOUT) -> tuple[list[CronEntry], ProbeResult]:
    result = run_probe(["crontab", "-l"], timeout=timeout)
    return (parse_crontab(result.stdout) if result.ok else []), result


@dataclass(frozen=True)
class LoginItem:
    uid: int
    name: str
    item_type: str
    disposition: str
    identifier: str
    executable: str | None = None

    @property
    def enabled(self) -> bool:
        return "enabled" in self.disposition.lower()


_BTM_UID_RE = re.compile(r"Records for UID\s+(-?\d+)")
_BTM_ITEM_RE = re.compile(r"^\s*#\d+:\s*$")
_BTM_FIELD_RE = re.compile(r"^\s*([A-Za-z ]+):\s*(.*)$")
LOGIN_ITEM_TYPES = ("login item", "app")


def parse_btm(stdout: str, types: Sequence[str] = LOGIN_ITEM_TYPES) -> list[LoginItem]:
    """Parse `sfltool dumpbtm` into login-item records of the requested types."""
    items: list[LoginItem] = []
    uid = -1
    current: dict[str, str] = {}

    def flush() -> None:
        if not current:
            return
        item_type = current.get("Type", "").strip()
        if not any(t in item_type for t in types):
            return
        name = current.get("Name", "").strip()
        identifier = current.get("Identifier", "").strip()
        if name in {"", "(null)"}:
            name = identifier or "(unnamed login item)"
        executable = current.get("Executable Path", "").strip()
        items.append(
            LoginItem(
                uid=uid,
                name=name,
                item_type=item_type,
                disposition=current.get("Disposition", "").strip(),
                identifier=identifier,
                executable=None if executable in {"", "(null)"} else executable,
            )
        )

    for line in stdout.splitlines():
        uid_match = _BTM_UID_RE.search(line)
        if uid_match:
            flush()
            current = {}
            uid = int(uid_match.group(1))
            continue
        if _BTM_ITEM_RE.match(line):
            flush()
            current = {}
            continue
        field_match = _BTM_FIELD_RE.match(line)
        if field_match and current is not None:
            current[field_match.group(1).strip()] = field_match.group(2).strip()
    flush()
    return items


def login_items(
    timeout: float = LOGIN_ITEM_TIMEOUT,
) -> tuple[list[LoginItem], ProbeResult]:
    result = run_probe(["sfltool", "dumpbtm"], timeout=timeout)
    return (parse_btm(result.stdout) if result.ok else []), result


@dataclass(frozen=True)
class BrewService:
    name: str
    status: str
    user: str | None
    plist: str | None


def parse_brew_services(stdout: str) -> list[BrewService]:
    services: list[BrewService] = []
    for raw in stdout.splitlines():
        line = raw.rstrip()
        if not line.strip() or line.startswith("Name "):
            continue
        fields = line.split()
        if len(fields) < 2:
            continue
        name, status = fields[0], fields[1]
        user = fields[2] if len(fields) > 2 else None
        plist = fields[3] if len(fields) > 3 else None
        services.append(BrewService(name=name, status=status, user=user, plist=plist))
    return services


def brew_services(timeout: float = 30.0) -> tuple[list[BrewService], ProbeResult]:
    brew = shutil.which("brew")
    if brew is None:
        return [], ProbeResult("brew services list", False, error="brew not installed")
    result = run_probe([brew, "services", "list"], timeout=timeout)
    return (parse_brew_services(result.stdout) if result.ok else []), result


def parse_tmux_sessions(stdout: str) -> list[str]:
    return [line.strip() for line in stdout.splitlines() if line.strip()]


def tmux_sessions(timeout: float = DEFAULT_TIMEOUT) -> tuple[list[str], ProbeResult]:
    result = run_probe(["tmux", "ls"], timeout=timeout)
    return (parse_tmux_sessions(result.stdout) if result.ok else []), result


# --------------------------------------------------------------------------
# machine
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class MachineInfo:
    host: str
    os: str
    cores: int | None = None
    ram_gb: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"os": self.os, "cores": self.cores, "ram_gb": self.ram_gb}


def machine_info(timeout: float = 5.0) -> tuple[MachineInfo, list[ProbeResult]]:
    probes: list[ProbeResult] = []
    host = (platform.node() or "unknown").split(".")[0]
    os_name = f"{platform.system()} {platform.release()}".strip()

    cores: int | None = None
    ram_gb: int | None = None
    result = run_probe(["sysctl", "-n", "hw.ncpu", "hw.memsize"], timeout=timeout)
    probes.append(result)
    if result.ok:
        values = result.stdout.split()
        if len(values) >= 1:
            cores = _to_int(values[0])
        if len(values) >= 2:
            memsize = _to_int(values[1])
            ram_gb = round(memsize / (1024**3)) if memsize else None
    if cores is None:
        import os as _os

        cores = _os.cpu_count()
    return MachineInfo(host=host, os=os_name, cores=cores, ram_gb=ram_gb), probes


# --------------------------------------------------------------------------
# snapshot
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class SystemSnapshot:
    """Everything the census read, in one immutable value.

    Domain builders take a snapshot, never the live system, so every verdict is
    reproducible from captured input in tests.
    """

    machine: MachineInfo
    user_agents: list[ParsedPlist] = field(default_factory=list)
    global_daemons: list[ParsedPlist] = field(default_factory=list)
    system_labels: frozenset[str] = frozenset()
    launchctl: dict[str, LaunchctlEntry] = field(default_factory=dict)
    disabled: dict[str, bool] = field(default_factory=dict)
    processes: list[ProcInfo] = field(default_factory=list)
    ps_unparsed_lines: int = 0
    listeners: list[Listener] = field(default_factory=list)
    cron: list[CronEntry] = field(default_factory=list)
    login_items: list[LoginItem] = field(default_factory=list)
    brew: list[BrewService] = field(default_factory=list)
    tmux: list[str] = field(default_factory=list)
    registry_processes: list[dict[str, Any]] = field(default_factory=list)
    probes: list[ProbeResult] = field(default_factory=list)

    def probe_evidence(self, needle: str) -> str:
        for probe in self.probes:
            if needle in probe.command:
                return probe.evidence
        return f"`{needle}` -> probe not run"

    @property
    def pids(self) -> frozenset[int]:
        return frozenset(p.pid for p in self.processes)

    @property
    def launchd_pids(self) -> frozenset[int]:
        return frozenset(e.pid for e in self.launchctl.values() if e.pid is not None)


def collect_snapshot(
    registry_processes: list[dict[str, Any]] | None = None,
    gui_domain: str | None = None,
    deep: bool = False,
) -> SystemSnapshot:
    """Run every probe once and freeze the result. Read-only.

    ``deep=True`` raises the login-item probe timeout; the default keeps the
    census fast and testifies the gap instead of waiting minutes on `sfltool`.
    """
    probes: list[ProbeResult] = []

    machine, machine_probes = machine_info()
    probes.extend(machine_probes)

    launchctl_entries, launchctl_probe = launchctl_list()
    probes.append(launchctl_probe)

    import os as _os

    domain = gui_domain or f"gui/{_os.getuid()}"
    disabled, disabled_probe = launchctl_print_disabled(domain)
    probes.append(disabled_probe)

    processes, ps_unparsed, ps_probe = ps_snapshot()
    probes.append(ps_probe)

    listeners, lsof_probe = tcp_listeners()
    probes.append(lsof_probe)

    cron, cron_probe = crontab_entries()
    probes.append(cron_probe)

    items, btm_probe = login_items(
        timeout=LOGIN_ITEM_DEEP_TIMEOUT if deep else LOGIN_ITEM_TIMEOUT
    )
    probes.append(btm_probe)

    brew, brew_probe = brew_services()
    probes.append(brew_probe)

    sessions, tmux_probe = tmux_sessions()
    probes.append(tmux_probe)

    return SystemSnapshot(
        machine=machine,
        user_agents=read_plist_dir(USER_LAUNCH_AGENTS),
        global_daemons=(
            read_plist_dir(GLOBAL_LAUNCH_DAEMONS) + read_plist_dir(GLOBAL_LAUNCH_AGENTS)
        ),
        system_labels=system_plist_labels(),
        launchctl=launchctl_entries,
        disabled=disabled,
        processes=processes,
        ps_unparsed_lines=ps_unparsed,
        listeners=listeners,
        cron=cron,
        login_items=items,
        brew=brew,
        tmux=sessions,
        registry_processes=list(registry_processes or []),
        probes=probes,
    )
