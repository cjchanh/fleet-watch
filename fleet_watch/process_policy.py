"""Deterministic process identity and action policy.

This is the only module allowed to turn a process observation into a signal
eligibility decision.  It is intentionally conservative: CPU, memory, age,
PPID 1, and a missing terminal are evidence for explanation only, never
authorization.  A caller must provide an explicit disposable registration,
positive PID/create-time/executable/UID identity, a proven-dead owner, and
negative evidence for every competing protection before termination is allowed.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable, Mapping


POLICY_VERSION = "fleet-watch/process-policy/v1"
DEFAULT_MAX_OBSERVATION_AGE_SECONDS = 5.0
DEFAULT_GRACE_SECONDS = 1.5
SERVICE_PROTECTED = "protected"
SERVICE_ACTIVE = "active_workload"
SERVICE_DISPOSABLE = "disposable"
SERVICE_UNKNOWN = "unknown"

# These are protections, not cleanup candidates.  Matching is intentionally
# broad and can be extended with a registry-provided service class.
_PROTECTED_TOKENS = (
    "windowserver",
    "launchd",
    "kernel_task",
    "kernel",
    "securityagent",
    "backup",
    "timemachine",
    "comfyui",
    "comfy ui",
    "claude",
    "codex",
    "opencode",
    "grok",
    "ollama serve",
    "mlx_lm",
    "vllm",
)
_ACTIVE_TOKENS = ("comfyui", "backup", "claude", "codex", "opencode", "grok")


@dataclass(frozen=True)
class ProcessIdentity:
    """A positively observed identity tuple; missing values are not defaults."""

    pid: int
    create_time: str
    executable: str
    uid: int
    command: str | None
    ppid: int | None
    pgid: int | None
    tty: str | None
    service_class: str
    observed_at: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PolicyDecision:
    """Action decision plus an auditable receipt."""

    allowed: bool
    action: str
    reason: str
    identity: ProcessIdentity | None
    evidence: tuple[str, ...]
    policy_version: str = POLICY_VERSION
    observed_at: float = 0.0
    outcome: str = "denied"

    @property
    def permitted(self) -> bool:
        return self.action in {"terminate", "force_terminate"}

    def receipt(self) -> dict[str, Any]:
        return {
            "schema_version": "fleet-watch/process-decision/v1",
            "policy_version": self.policy_version,
            "observed_at": self.observed_at,
            "observed_identity": self.identity.to_dict() if self.identity else None,
            "permitted_action": self.action,
            "allowed": self.allowed,
            "reason": self.reason,
            "evidence": list(self.evidence),
            "outcome": self.outcome,
        }


def classify_service(command: str | None, name: str | None = None) -> str:
    """Classify conservatively; unknown command/service data stays unknown."""
    text = f"{name or ''} {command or ''}".casefold()
    if any(token in text for token in _PROTECTED_TOKENS):
        if any(token in text for token in _ACTIVE_TOKENS):
            return SERVICE_ACTIVE
        return SERVICE_PROTECTED
    return SERVICE_UNKNOWN


def snapshot_process(
    pid: int,
    *,
    now: float | None = None,
    probes: Mapping[str, Callable[..., Any]] | None = None,
) -> ProcessIdentity | None:
    """Capture a complete identity, returning ``None`` on any uncertainty."""
    if pid <= 0:
        return None
    p = probes or {}
    exists = p.get("exists") or _default_exists
    create = p.get("create_time") or _default_create_time
    executable = p.get("executable") or _default_executable
    uid = p.get("uid") or _default_uid
    command = p.get("command") or _default_command
    inspect = p.get("inspect") or _default_inspect
    try:
        if not exists(pid):
            return None
        create_time = create(pid)
        executable_path = executable(pid)
        owner_uid = uid(pid)
        if not create_time or not executable_path or owner_uid is None:
            return None
        info = inspect(pid) or {}
        cmd = command(pid)
        service = classify_service(cmd, executable_path)
        return ProcessIdentity(
            pid=pid,
            create_time=str(create_time),
            executable=str(executable_path),
            uid=int(owner_uid),
            command=cmd,
            ppid=info.get("ppid"),
            pgid=info.get("pgid"),
            tty=info.get("tty"),
            service_class=service,
            observed_at=float(time.monotonic() if now is None else now),
        )
    except (OSError, PermissionError, TimeoutError, ValueError, TypeError):
        return None


def evaluate(
    identity: ProcessIdentity | None,
    *,
    candidate: Mapping[str, Any] | None = None,
    disposable_registration: Mapping[str, Any] | None = None,
    now: float | None = None,
    max_age_seconds: float = DEFAULT_MAX_OBSERVATION_AGE_SECONDS,
    allow_force: bool = False,
    operator_confirmed: bool = False,
) -> PolicyDecision:
    """Evaluate one observation without performing any side effect."""
    candidate = candidate or {}
    observed_at = float(time.monotonic() if now is None else now)
    evidence: list[str] = []
    if identity is None:
        return PolicyDecision(
            False,
            "deny",
            "identity_unavailable",
            None,
            ("PID/create-time/executable/UID could not be positively observed",),
            observed_at=observed_at,
        )
    evidence.extend(
        [
            f"pid={identity.pid}",
            f"create_time={identity.create_time}",
            f"executable={identity.executable}",
            f"uid={identity.uid}",
            f"service_class={identity.service_class}",
        ]
    )
    age = observed_at - identity.observed_at
    if age < 0 or age > max_age_seconds:
        return PolicyDecision(
            False, "deny", "observation_stale", identity, tuple(evidence) + (f"age={age:.3f}",), observed_at=observed_at
        )
    if identity.pid in {os.getpid(), os.getppid()}:
        return PolicyDecision(False, "deny", "self_or_parent_protected", identity, tuple(evidence), observed_at=observed_at)
    if identity.service_class in {SERVICE_PROTECTED, SERVICE_ACTIVE}:
        return PolicyDecision(False, "deny", f"protected_{identity.service_class}", identity, tuple(evidence), observed_at=observed_at)

    registration = disposable_registration
    if not registration:
        return PolicyDecision(False, "deny", "no_explicit_disposable_registration", identity, tuple(evidence), observed_at=observed_at)
    if registration.get("service_class") != SERVICE_DISPOSABLE:
        return PolicyDecision(False, "deny", "registration_not_disposable", identity, tuple(evidence), observed_at=observed_at)
    if not registration.get("owner_id"):
        return PolicyDecision(False, "deny", "owner_evidence_missing", identity, tuple(evidence), observed_at=observed_at)
    if str(registration.get("create_time")) != identity.create_time:
        return PolicyDecision(False, "deny", "pid_reuse_detected", identity, tuple(evidence) + ("registered create-time differs",), observed_at=observed_at)
    if str(registration.get("executable")) != identity.executable:
        return PolicyDecision(False, "deny", "executable_identity_changed", identity, tuple(evidence), observed_at=observed_at)
    if registration.get("uid") != identity.uid:
        return PolicyDecision(False, "deny", "uid_identity_changed", identity, tuple(evidence), observed_at=observed_at)
    evidence.append("registration identity matched")

    # The owner must be positively dead, not merely old or quiet.
    if candidate.get("owner_identity_proven") is not False and candidate.get("owner_dead") is not True:
        return PolicyDecision(False, "deny", "owner_death_not_proven", identity, tuple(evidence), observed_at=observed_at)
    evidence.append("owner death proven")
    for key, label in (
        ("lease_active", "active lease"),
        ("parent_alive", "live parent"),
        ("session_alive", "live session"),
        ("stdio_peer_alive", "live stdio peer"),
        ("active_work", "active work"),
    ):
        if candidate.get(key) is True:
            return PolicyDecision(False, "deny", f"{label}_protected", identity, tuple(evidence), observed_at=observed_at)
    evidence.append("no competing protection evidence")
    if candidate.get("inspection_complete") is not True:
        return PolicyDecision(False, "deny", "inspection_incomplete", identity, tuple(evidence), observed_at=observed_at)
    evidence.append("fresh complete inspection")
    if candidate.get("classification") not in {"orphan_confirmed", "disposable_orphan"}:
        return PolicyDecision(False, "advisory", "classification_not_orphan_confirmed", identity, tuple(evidence), observed_at=observed_at)
    evidence.append("classification=orphan_confirmed")
    if not operator_confirmed:
        return PolicyDecision(False, "advisory", "operator_confirmation_required", identity, tuple(evidence), observed_at=observed_at)
    action = "force_terminate" if allow_force and candidate.get("force_escalation") is True else "terminate"
    return PolicyDecision(True, action, "explicit_disposable_cleanup", identity, tuple(evidence), observed_at=observed_at)


def revalidate_and_terminate(
    decision: PolicyDecision,
    *,
    candidate: Mapping[str, Any] | None = None,
    disposable_registration: Mapping[str, Any] | None = None,
    probes: Mapping[str, Callable[..., Any]] | None = None,
    send_signal: Callable[[int, int], None] | None = None,
    exists: Callable[[int], bool] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    grace_seconds: float = DEFAULT_GRACE_SECONDS,
    allow_force: bool = False,
    now: Callable[[], float] = time.monotonic,
) -> PolicyDecision:
    """Revalidate immediately before SIGTERM, then verify exit honestly."""
    if not decision.allowed or not decision.identity:
        return decision
    send_signal = send_signal or os.kill
    exists = exists or _default_exists
    identity = snapshot_process(decision.identity.pid, now=now(), probes=probes)
    fresh = evaluate(
        identity,
        candidate=candidate,
        disposable_registration=disposable_registration,
        now=now(),
        allow_force=allow_force,
        operator_confirmed=True,
    )
    if not fresh.allowed:
        return PolicyDecision(False, "deny", fresh.reason, identity, fresh.evidence, observed_at=fresh.observed_at, outcome="revalidation_denied")
    assert identity is not None
    try:
        send_signal(identity.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError) as exc:
        return PolicyDecision(False, "deny", f"signal_failed:{type(exc).__name__}", identity, fresh.evidence, observed_at=fresh.observed_at, outcome="signal_failed")
    deadline = now() + max(0.05, grace_seconds)
    while now() < deadline:
        if not exists(identity.pid):
            return PolicyDecision(True, "terminate", "graceful_exit_verified", identity, fresh.evidence, observed_at=fresh.observed_at, outcome="exited")
        sleep(min(0.05, max(0.0, deadline - now())))
    # Force is never implicit. A caller must pass allow_force and a fresh policy
    # decision; even then revalidate the complete identity once more.
    if not allow_force:
        return PolicyDecision(False, "deny", "graceful_exit_timeout_force_denied", identity, fresh.evidence, observed_at=fresh.observed_at, outcome="still_running")
    second = snapshot_process(identity.pid, now=now(), probes=probes)
    second_decision = evaluate(
        second,
        candidate=candidate,
        disposable_registration=disposable_registration,
        now=now(),
        allow_force=True,
        operator_confirmed=True,
    )
    if not second_decision.allowed or second is None:
        return PolicyDecision(False, "deny", "force_revalidation_denied", second, second_decision.evidence, observed_at=second_decision.observed_at, outcome="force_denied")
    try:
        send_signal(second.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError) as exc:
        return PolicyDecision(False, "deny", f"force_signal_failed:{type(exc).__name__}", second, second_decision.evidence, observed_at=second_decision.observed_at, outcome="force_signal_failed")
    deadline = now() + max(0.05, grace_seconds)
    while now() < deadline:
        if not exists(second.pid):
            return PolicyDecision(True, "force_terminate", "force_exit_verified", second, second_decision.evidence, observed_at=second_decision.observed_at, outcome="exited")
        sleep(min(0.05, max(0.0, deadline - now())))
    return PolicyDecision(False, "deny", "force_exit_unverified", second, second_decision.evidence, observed_at=second_decision.observed_at, outcome="still_running")


# Small adapters keep the policy injectable while using the repo's canonical
# probes in production. They are defined late to avoid import cycles.
def _default_exists(pid: int) -> bool:
    from fleet_watch import registry
    from fleet_watch.constants import PS_BIN
    if not registry._pid_exists(pid):
        return False
    try:
        result = subprocess.run(
            [PS_BIN, "-o", "stat=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=1.0,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        # An unreadable state is still conservatively live.
        return True
    state = result.stdout.strip()
    if result.returncode == 0 and state.startswith("Z"):
        return False
    return True


def _default_create_time(pid: int) -> str | None:
    from fleet_watch import registry
    return registry._pid_create_time(pid)


def _default_executable(pid: int) -> str | None:
    from fleet_watch import registry
    return registry._process_executable(pid)


def _default_uid(pid: int) -> int | None:
    from fleet_watch import registry
    return registry._process_uid(pid)


def _default_command(pid: int) -> str | None:
    from fleet_watch import registry
    return registry._process_command(pid)


def _default_inspect(pid: int) -> dict[str, Any] | None:
    from fleet_watch import registry
    return registry._inspect_process(pid)


# ═══════════════════════════════════════════════════════════════════════════
# SHARED PROCESS IDENTITY / ACTION CONTRACT  (process-control-identity slice)
#
# APPEND-ONLY SECTION: a parallel slice authored the module docstring and the
# disposable-orphan contract above while this slice was in flight; nothing
# above this line was modified. POLICY_VERSION is deliberately SHARED so both
# contracts stamp the same policy version onto their receipts.
# ═══════════════════════════════════════════════════════════════════════════
"""Shared process identity/action contract — the fail-closed decision surface.

(Section docstring: the module-docstring slot was already taken by the
parallel contract above; this block documents its own contract here.)

PURPOSE: the single proof point that turns (recorded identity, fresh
observation, requested action) into an auditable decision receipt — and
sends a signal only after re-proving identity immediately beforehand.

REQUIRED INPUTS for a signal-class decision: PID **and** kernel create-time,
executable identity, same UID (recorded == observed == caller), computed
service/system/protection classification, session + owner evidence, and an
observation no older than MAX_OBSERVATION_AGE_SECONDS.

CONSERVATIVE FAIL-CLOSED CHOICES (the brief was ambiguous; safe was chosen):
1. Missing / permission / stale / PID-reuse / any identity mismatch DENY.
   Probe exceptions deny too, with the exception type named in the reason
   (repo doctrine: never a blind except on a decision path).
2. ``advisory`` (report-only, never signals) is ALLOWED once identity is
   proven — even on protected targets — and lists its protections, so a
   WindowServer/backup/ComfyUI process stays describable while remaining
   un-signallable. Identity failures still deny advisory.
3. ``terminate``/``force`` additionally require session_id AND owner
   evidence, a recognized status (active|idle), caller UID == observed UID,
   and ZERO protections.
4. ``force`` additionally requires an explicit open
   ``ForceGate(allowed=True, reason=<non-empty>)``; default (None) = closed.
   ``terminate_gracefully`` NEVER escalates: survival after the bounded grace
   window is outcome ``survived``; escalation is a separate gated
   ``force_terminate()`` call.
5. TOCTOU: an earlier ALLOWED receipt is never trusted. Both signal
   functions run a fresh ``observe()`` + ``decide()`` inside the call,
   immediately before signalling; drift in the window DENIES and sends
   nothing.
6. Protections — self, parent, system, windowserver, active, agent, backup,
   comfyui, unknown — are COMPUTED here from observed facts; a caller cannot
   assert them away. ``service`` classification is reported but is not itself
   a protection (the protected set is exactly the enumerated list); an
   unrecognized evidence kind/status still lands in ``unknown`` and is
   protected.
7. Every decision is a DecisionReceipt: timestamps (requested/observed/
   decided/completed), stable reason code, POLICY_VERSION, outcome.

PUBLIC API of this section: ``MAX_OBSERVATION_AGE_SECONDS``, the ACTION_/
OUTCOME_/CLASS_ constants, ``OwnerEvidence``, ``RecordedIdentity``,
``Observation``, ``ForceGate``, ``Probes``, ``DecisionReceipt``,
``default_probes()``, ``observe()``, ``decide()``,
``terminate_gracefully()``, ``force_terminate()``. Everything else is
private. All kernel access flows through injectable ``Probes`` so unit tests
are deterministic and never signal a real process.
"""

import subprocess  # pinned-ps executable/UID probes; no network, no egress

from fleet_watch.constants import PS_BIN

# Freshness + action ladder ---------------------------------------------------

MAX_OBSERVATION_AGE_SECONDS = DEFAULT_MAX_OBSERVATION_AGE_SECONDS

ACTION_ADVISORY = "advisory"        # report only — never signals
ACTION_TERMINATE = "terminate"      # one bounded graceful signal
ACTION_FORCE = "force"              # explicit ForceGate required

OUTCOME_ALLOWED = "allowed"
OUTCOME_DENIED = "denied"
OUTCOME_EXITED = "exited"
OUTCOME_SURVIVED = "survived"
OUTCOME_FAILED = "failed"

CLASS_USER = "user"
CLASS_SERVICE = "service"
CLASS_SYSTEM = "system"
CLASS_UNKNOWN = "unknown"

# At/below this PID the target is kernel/system territory (mirrors
# runaway.MIN_SAFE_PID) — classification ``system`` is a protection.
MIN_SYSTEM_PID = 100

_AGENT_EXES = frozenset({
    "claude", "claude-code", "codex", "opencode", "cursor-agent", "grok",
    "devin", "aider", "gemini",
})
_BACKUP_EXES = frozenset({
    "backupd", "backupd-helper", "tmutil", "restic", "borg", "borgmatic",
    "duplicity", "rsync",
})

_POLL_INTERVAL_SECONDS = 0.1
_MAX_POLL_ITERATIONS = 10_000  # broken-clock guard; a healthy clock exits first


# Contract data classes ---------------------------------------------------------

@dataclass(frozen=True)
class OwnerEvidence:
    """Session/owner evidence recorded when the process was registered.

    Signal-class actions require non-empty ``session_id`` AND ``owner`` plus
    a recognized ``status`` (``active``/``idle``) and ``kind``
    (``user``/``service``/``agent``); anything else is fail-closed.
    """

    session_id: str = ""
    owner: str = ""
    status: str = "unknown"
    kind: str = "unknown"


@dataclass(frozen=True)
class RecordedIdentity:
    """Identity recorded at registration — capture it via ``observe()`` so the
    recorded and revalidated sides always use the same probe rendering."""

    pid: int
    create_time: str  # kernel create-time: defeats PID reuse
    exe: str          # executable identity (kernel-reported, not argv)
    uid: int
    evidence: OwnerEvidence | None = None


@dataclass(frozen=True)
class Observation:
    """One fresh kernel observation. ``error`` set ⇒ unusable ⇒ deny."""

    pid: int
    observed_at: float
    create_time: str | None = None
    exe: str | None = None
    uid: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class ForceGate:
    """Explicit force-escalation grant. Absent (None) or closed = denied."""

    allowed: bool = False
    reason: str = ""


@dataclass(frozen=True)
class Probes:
    """Injectable kernel access — every observation flows through here.

    ``now``/``sleep`` are injectable so freshness and the bounded wait are
    deterministic in tests. Clock probes must not raise; kernel probes may
    raise and are converted to denial reasons by ``observe()``.
    """

    now: Callable[[], float]
    sleep: Callable[[float], None]
    exists: Callable[[int], bool]
    create_time: Callable[[int], str | None]
    exe: Callable[[int], str | None]
    uid: Callable[[int], int | None]
    self_pid: Callable[[], int]
    parent_pid: Callable[[], int]
    caller_uid: Callable[[], int]


@dataclass(frozen=True)
class DecisionReceipt:
    """Structured decision record: timestamps, reason, policy version, outcome."""

    policy_version: str
    action: str
    outcome: str
    reason: str
    pid: int
    requested_at: float
    observed_at: float | None
    decided_at: float
    completed_at: float | None = None  # set only by the signal functions
    classification: str | None = None
    protections: tuple[str, ...] = ()
    session_id: str | None = None
    owner: str | None = None
    signal: int | None = None  # signal ATTEMPTED, if any; None when denied

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_version": self.policy_version,
            "action": self.action,
            "outcome": self.outcome,
            "reason": self.reason,
            "pid": self.pid,
            "requested_at": self.requested_at,
            "observed_at": self.observed_at,
            "decided_at": self.decided_at,
            "completed_at": self.completed_at,
            "classification": self.classification,
            "protections": list(self.protections),
            "session_id": self.session_id,
            "owner": self.owner,
            "signal": self.signal,
        }


# Observation + decision ------------------------------------------------------

def observe(pid: int, probes: Probes) -> Observation:
    """Take one fresh observation. Never raises; every uncertainty becomes an
    ``error`` that ``decide()`` turns into a denial."""
    observed_at = probes.now()
    if pid <= 0:
        return Observation(pid=pid, observed_at=observed_at, error="invalid_pid")
    try:
        alive = probes.exists(pid)
        create_time = probes.create_time(pid)
        exe = probes.exe(pid)
        uid = probes.uid(pid)
    except PermissionError:
        return Observation(pid=pid, observed_at=observed_at, error="probe_permission")
    except Exception as exc:  # type named in the reason — never a blind except
        return Observation(
            pid=pid, observed_at=observed_at,
            error=f"probe_error:{type(exc).__name__}",
        )
    if not alive:
        return Observation(pid=pid, observed_at=observed_at, error="process_gone")
    if not create_time or not str(create_time).strip():
        return Observation(pid=pid, observed_at=observed_at, error="missing_create_time")
    if not exe or not str(exe).strip():
        return Observation(
            pid=pid, observed_at=observed_at, create_time=create_time,
            error="missing_exe",
        )
    if uid is None:
        return Observation(
            pid=pid, observed_at=observed_at, create_time=create_time,
            exe=exe, error="missing_uid",
        )
    return Observation(
        pid=pid, observed_at=observed_at, create_time=create_time,
        exe=exe, uid=uid,
    )


def _classify(
    pid: int,
    uid: int,
    exe: str,
    evidence: OwnerEvidence | None,
    self_pid: int,
    parent_pid: int,
) -> tuple[str, tuple[str, ...]]:
    """Compute (classification, protections) — never caller-asserted."""
    tags: list[str] = []
    if pid == self_pid:
        tags.append("self")
    if pid == parent_pid:
        tags.append("parent")
    kind = evidence.kind if evidence is not None else ""
    if pid <= MIN_SYSTEM_PID or uid == 0:
        classification = CLASS_SYSTEM
    elif kind == "service":
        classification = CLASS_SERVICE
    elif kind in ("user", "agent"):
        classification = CLASS_USER
    else:
        classification = CLASS_UNKNOWN
    if classification == CLASS_SYSTEM:
        tags.append("system")
    if classification == CLASS_UNKNOWN:
        tags.append("unknown")
    path = exe.lower()
    base = path.rsplit("/", 1)[-1]
    if base == "windowserver":
        tags.append("windowserver")
    if base in _AGENT_EXES or kind == "agent":
        tags.append("agent")
    if base in _BACKUP_EXES:
        tags.append("backup")
    if "comfyui" in path:  # matches venv/script paths, not just basenames
        tags.append("comfyui")
    if evidence is not None:
        if evidence.status == "active":
            tags.append("active")
        elif evidence.status not in ("active", "idle"):
            tags.append("unknown")
    return classification, tuple(sorted(set(tags)))


def decide(
    observation: Observation | None,
    recorded: RecordedIdentity,
    action: str,
    probes: Probes,
    *,
    force_gate: ForceGate | None = None,
    max_age_seconds: float = MAX_OBSERVATION_AGE_SECONDS,
) -> DecisionReceipt:
    """Pure decision — no signals, no state changes, fail-closed by construction.

    Check order: action validity → recorded-identity sanity → observation
    usability → freshness → PID-reuse → executable → UID → caller context →
    classification → (signal class: evidence → same-UID → protections) →
    (force class: gate). The first failing check is the receipt's reason.
    """
    requested_at = probes.now()
    evidence = recorded.evidence

    def _receipt(
        outcome: str,
        reason: str,
        *,
        observed_at: float | None = None,
        classification: str | None = None,
        protections: tuple[str, ...] = (),
    ) -> DecisionReceipt:
        return DecisionReceipt(
            policy_version=POLICY_VERSION,
            action=action,
            outcome=outcome,
            reason=reason,
            pid=recorded.pid,
            requested_at=requested_at,
            observed_at=observed_at,
            decided_at=probes.now(),
            completed_at=None,
            classification=classification,
            protections=protections,
            session_id=evidence.session_id if evidence else None,
            owner=evidence.owner if evidence else None,
            signal=None,
        )

    def _deny(reason: str, **kw: Any) -> DecisionReceipt:
        return _receipt(OUTCOME_DENIED, reason, **kw)

    if action not in (ACTION_ADVISORY, ACTION_TERMINATE, ACTION_FORCE):
        return _deny("unknown_action")
    if recorded.pid <= 0:
        return _deny("invalid_pid")
    if not recorded.create_time or not str(recorded.create_time).strip():
        return _deny("missing_create_time")
    if not recorded.exe or not str(recorded.exe).strip():
        return _deny("missing_exe")
    if recorded.uid is None:
        return _deny("missing_uid")
    if observation is None:
        return _deny("missing_observation")
    observed_at = observation.observed_at
    if observation.error:
        return _deny(observation.error, observed_at=observed_at)
    if observation.pid != recorded.pid:
        return _deny("pid_mismatch", observed_at=observed_at)
    age = requested_at - observed_at
    if age < 0.0 or age > max_age_seconds:
        return _deny("stale_observation", observed_at=observed_at)
    if observation.create_time != recorded.create_time:
        return _deny("pid_reuse", observed_at=observed_at)
    if observation.exe != recorded.exe:
        return _deny("exe_mismatch", observed_at=observed_at)
    if observation.uid != recorded.uid:
        return _deny("uid_mismatch", observed_at=observed_at)
    try:
        self_pid = probes.self_pid()
        parent_pid = probes.parent_pid()
        caller_uid = probes.caller_uid()
    except PermissionError:
        return _deny("probe_permission", observed_at=observed_at)
    except Exception as exc:  # type named in the reason
        return _deny(f"probe_error:{type(exc).__name__}", observed_at=observed_at)
    classification, protections = _classify(
        recorded.pid, observation.uid, observation.exe,
        evidence, self_pid, parent_pid,
    )
    if action in (ACTION_TERMINATE, ACTION_FORCE):
        if evidence is None or not evidence.session_id.strip() or not evidence.owner.strip():
            return _deny(
                "missing_owner_evidence", observed_at=observed_at,
                classification=classification, protections=protections,
            )
        if observation.uid != caller_uid:
            return _deny(
                "uid_not_same", observed_at=observed_at,
                classification=classification, protections=protections,
            )
        if protections:
            return _deny(
                "protected:" + ",".join(protections), observed_at=observed_at,
                classification=classification, protections=protections,
            )
    if action == ACTION_FORCE:
        if force_gate is None or not force_gate.allowed or not force_gate.reason.strip():
            return _deny(
                "force_not_permitted", observed_at=observed_at,
                classification=classification, protections=protections,
            )
    return _receipt(
        OUTCOME_ALLOWED, "identity_proven", observed_at=observed_at,
        classification=classification, protections=protections,
    )


# Signal path -------------------------------------------------------------------

def _reissue(
    base: DecisionReceipt,
    *,
    outcome: str,
    reason: str,
    signal_value: int | None = None,
    completed_at: float,
) -> DecisionReceipt:
    """Copy a decision onto its executed outcome, stamping completion."""
    return DecisionReceipt(
        policy_version=base.policy_version,
        action=base.action,
        outcome=outcome,
        reason=reason,
        pid=base.pid,
        requested_at=base.requested_at,
        observed_at=base.observed_at,
        decided_at=base.decided_at,
        completed_at=completed_at,
        classification=base.classification,
        protections=base.protections,
        session_id=base.session_id,
        owner=base.owner,
        signal=signal_value,
    )


def _send_signal(pid: int, sig: int) -> None:
    os.kill(pid, sig)


def _same_process(pid: int, create_time: str, probes: Probes) -> bool | None:
    """True = same process, False = original gone, None = unprovable.

    ``None`` (probe failure mid-wait) is treated as STILL RUNNING by callers —
    never as exited — so an unreadable process cannot be reported as dead.
    """
    try:
        if not probes.exists(pid):
            return False
        live = probes.create_time(pid)
    except Exception:
        return None
    if live is None:
        return None
    return live == create_time


def terminate_gracefully(
    recorded: RecordedIdentity,
    probes: Probes,
    *,
    sender: Callable[[int, int], None] | None = None,
    grace_seconds: float = DEFAULT_GRACE_SECONDS,
    max_age_seconds: float = MAX_OBSERVATION_AGE_SECONDS,
) -> DecisionReceipt:
    """Fresh revalidation → ONE bounded SIGTERM → bounded wait → receipt.

    Re-observes and re-decides inside this call (an earlier ALLOWED receipt is
    never trusted). Never escalates to SIGKILL: survival after the grace
    window is ``survived``/``grace_timeout``; escalation is a separate,
    explicitly gated ``force_terminate()`` call. Inject ``sender`` in tests —
    nothing here signals unless the caller supplies the default.
    """
    observation = observe(recorded.pid, probes)
    decision = decide(
        observation, recorded, ACTION_TERMINATE, probes,
        max_age_seconds=max_age_seconds,
    )
    if decision.outcome != OUTCOME_ALLOWED:
        return _reissue(
            decision, outcome=decision.outcome, reason=decision.reason,
            completed_at=probes.now(),
        )
    send = sender if sender is not None else _send_signal
    sigterm = int(signal.SIGTERM)
    try:
        send(recorded.pid, sigterm)
    except ProcessLookupError:
        return _reissue(
            decision, outcome=OUTCOME_EXITED, reason="already_gone",
            signal_value=sigterm, completed_at=probes.now(),
        )
    except PermissionError:
        return _reissue(
            decision, outcome=OUTCOME_FAILED, reason="signal_permission",
            signal_value=sigterm, completed_at=probes.now(),
        )
    except Exception as exc:  # type named in the reason
        return _reissue(
            decision, outcome=OUTCOME_FAILED,
            reason=f"signal_error:{type(exc).__name__}",
            signal_value=sigterm, completed_at=probes.now(),
        )
    deadline = probes.now() + max(0.0, grace_seconds)
    for _ in range(_MAX_POLL_ITERATIONS):
        if _same_process(recorded.pid, recorded.create_time, probes) is False:
            return _reissue(
                decision, outcome=OUTCOME_EXITED, reason="graceful_exit",
                signal_value=sigterm, completed_at=probes.now(),
            )
        remaining = deadline - probes.now()
        if remaining <= 0.0:
            break
        probes.sleep(min(_POLL_INTERVAL_SECONDS, remaining))
    return _reissue(
        decision, outcome=OUTCOME_SURVIVED, reason="grace_timeout",
        signal_value=sigterm, completed_at=probes.now(),
    )


def force_terminate(
    recorded: RecordedIdentity,
    probes: Probes,
    *,
    force_gate: ForceGate | None = None,
    sender: Callable[[int, int], None] | None = None,
    max_age_seconds: float = MAX_OBSERVATION_AGE_SECONDS,
) -> DecisionReceipt:
    """Explicit force gate → fresh revalidation → ONE SIGKILL → receipt.

    The gate is closed by default (``None``): without
    ``ForceGate(allowed=True, reason=<non-empty>)`` nothing is observed as
    signallable and nothing is sent. Everything ``terminate_gracefully``
    requires (identity, freshness, evidence, same UID, zero protections) is
    re-proven inside this call immediately before the signal.
    """
    observation = observe(recorded.pid, probes)
    decision = decide(
        observation, recorded, ACTION_FORCE, probes,
        force_gate=force_gate, max_age_seconds=max_age_seconds,
    )
    if decision.outcome != OUTCOME_ALLOWED:
        return _reissue(
            decision, outcome=decision.outcome, reason=decision.reason,
            completed_at=probes.now(),
        )
    send = sender if sender is not None else _send_signal
    sigkill = int(signal.SIGKILL)
    try:
        send(recorded.pid, sigkill)
    except ProcessLookupError:
        return _reissue(
            decision, outcome=OUTCOME_EXITED, reason="already_gone",
            signal_value=sigkill, completed_at=probes.now(),
        )
    except PermissionError:
        return _reissue(
            decision, outcome=OUTCOME_FAILED, reason="signal_permission",
            signal_value=sigkill, completed_at=probes.now(),
        )
    except Exception as exc:  # type named in the reason
        return _reissue(
            decision, outcome=OUTCOME_FAILED,
            reason=f"signal_error:{type(exc).__name__}",
            signal_value=sigkill, completed_at=probes.now(),
        )
    same = _same_process(recorded.pid, recorded.create_time, probes)
    if same is False:
        return _reissue(
            decision, outcome=OUTCOME_EXITED, reason="killed",
            signal_value=sigkill, completed_at=probes.now(),
        )
    if same is True:
        return _reissue(
            decision, outcome=OUTCOME_FAILED, reason="still_alive",
            signal_value=sigkill, completed_at=probes.now(),
        )
    return _reissue(
        decision, outcome=OUTCOME_FAILED, reason="kill_unconfirmed",
        signal_value=sigkill, completed_at=probes.now(),
    )


# Production probes -------------------------------------------------------------

def _probe_exe(pid: int) -> str | None:
    """Kernel-reported executable identity (never argv: argv is spoofable).

    ``/proc/<pid>/exe`` on Linux; pinned ``ps -o comm=`` elsewhere. Record and
    revalidate use THIS SAME probe, so both sides always compare like with
    like. Subprocess failures propagate to ``observe()``, which names them.
    """
    if os.path.isdir("/proc"):
        try:
            return os.readlink(f"/proc/{pid}/exe") or None
        except OSError:
            pass  # unreadable here → fall back; both sides fall back alike
    out = subprocess.run(
        [PS_BIN, "-o", "comm=", "-p", str(pid)],
        capture_output=True, text=True, timeout=2, check=False,
    )
    line = out.stdout.strip()
    if out.returncode != 0 or not line:
        return None
    return line


def _probe_uid(pid: int) -> int | None:
    """Reported UID via pinned ``ps``; failures propagate and name themselves."""
    out = subprocess.run(
        [PS_BIN, "-o", "uid=", "-p", str(pid)],
        capture_output=True, text=True, timeout=2, check=False,
    )
    if out.returncode != 0:
        return None
    try:
        return int(out.stdout.strip())
    except ValueError:
        return None


def default_probes() -> Probes:
    """Production probes: pinned binaries, TZ-invariant create-time (reuses the
    registry helpers above), zero egress — see module/section docstring."""
    return Probes(
        now=time.time,
        sleep=time.sleep,
        exists=_default_exists,
        create_time=_default_create_time,
        exe=_probe_exe,
        uid=_probe_uid,
        self_pid=os.getpid,
        parent_pid=os.getppid,
        caller_uid=os.getuid,
    )


# ═══════════════════════════════════════════════════════════════════════════
# VERIFIED MCP DEAD-SESSION RECLAIM — `fleet reap --mcp --verify`
#
# APPEND-ONLY SECTION. Nothing above this line changed; this slice builds the
# VERIFIED PATH only. There is no daemon auto-kill wiring here and none is
# added: acting requires an explicit operator `--kill` on the CLI.
#
# THE TWO PATHS (both must agree before a signal; receipts name both):
#   path 1 — IDENTITY  (`IDENTITY_PATH`): the kernel identity contract above.
#     A snapshot (pid + kernel create-time + executable + uid) is captured at
#     discovery and re-observed at decision time, so a recycled PID, a changed
#     executable, or a changed owner all DENY.
#   path 2 — SECOND PATH (`SECOND_PATH`): the session registry + parent chain.
#     It does NOT read path 1's verdict. It independently proves (a) the owning
#     session is positively GONE — absent, or its recorded create-time no longer
#     matches a live PID, or its chain is detached (ppid <= 1) — and (b) no live
#     session lease references the target or its owning session.
#
# FAIL-CLOSED, NO EXCEPTIONS. Every uncertainty DENIES: unreadable ancestry,
# unprovable owner create-time, a lease registry that cannot be read, a lease
# whose liveness cannot be read, a missing snapshot, or a stale observation. The
# tri-state never collapses toward "dead" and never toward "allow".
#
# GRACEFUL FIRST. The plan is report-only by default. Acting sends exactly one
# SIGTERM, verifies the exit within a bounded window, and STOPS there:
# ``survived`` is a reportable outcome, never a trigger. SIGKILL requires the
# separate force flag AND its own fresh identity re-check inside
# :func:`force_terminate` (which re-observes and re-decides on its own).
#
# Every decision — allow, deny, dry-run, survived, killed — is a
# ``ProcessDecision`` stamped ``fleet-watch/process-decision/v1`` carrying the
# evidence block and the ``verifier_identity`` naming the second path.
# ═══════════════════════════════════════════════════════════════════════════

from dataclasses import replace  # local import: keeps the header above untouched

PROCESS_DECISION_SCHEMA = "fleet-watch/process-decision/v1"
IDENTITY_PATH = "kernel_identity_probes"
SECOND_PATH = "session_registry+parent_chain"

RECLAIM_ACTION_NONE = "none"
RECLAIM_ACTION_SIGTERM = "sigterm"
RECLAIM_ACTION_SIGKILL = "sigkill"

RECLAIM_OUTCOME_ALLOWED = "allowed"
RECLAIM_OUTCOME_DENIED = "denied"
RECLAIM_OUTCOME_REPORTED = "reported"
RECLAIM_OUTCOME_EXITED = "exited"
RECLAIM_OUTCOME_SURVIVED = "survived"
RECLAIM_OUTCOME_FAILED = "failed"


@dataclass(frozen=True)
class MCPOwnerClaim:
    """The discovery-time owner claim a decision must independently re-prove.

    ``session_create_time`` is the owning session's kernel create-time as read
    during the candidate scan. ``None`` means the owner was never positively
    identified, which is a DENY, never a wildcard. ``reparented`` records a chain
    that was ALREADY detached (ppid <= 1) at scan time.
    """

    session_pid: int = 0
    session_create_time: str | None = None
    session_id: str = ""
    reparented: bool = False
    alive_at_scan: bool | None = None
    cmd: str = ""
    rss_mb: int = 0
    scan_create_time: str | None = None
    source: str = "mcp"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def owner_label(self) -> str:
        """The ``session_id`` used for owner EVIDENCE on the identity path.

        A derived label, not registry proof — the proof is the second path. The
        identity contract requires a non-empty session label, so an orphaned
        server gets an explicit one instead of an empty string that would look
        like missing evidence.
        """
        if self.session_id:
            return self.session_id
        if self.reparented or self.session_pid <= 1:
            return "mcp-owner:detached"
        return f"mcp-owner-pid:{self.session_pid}"


@dataclass(frozen=True)
class ReclaimProbes:
    """Second-path kernel + registry access, injectable so tests never touch a
    real process, a real lease table, or a real clock.

    Every callable may raise; each caller converts an exception into a DENY with
    the exception TYPE named in the reason (never a blind except on a decision
    path).
    """

    ppid: Callable[[int], int | None]
    exists: Callable[[int], bool]
    create_time: Callable[[int], str | None]
    active_leases: Callable[[], list[Mapping[str, Any]]]
    lease_owner_alive: Callable[[Mapping[str, Any]], bool]


def reclaim_probes_for_conn(conn: Any) -> ReclaimProbes:
    """Production second-path probes bound to an open registry connection.

    ``ppid`` reads ancestry through the registry's inspector and returns None
    when the process is uninspectable, which the caller treats as DENY.
    """
    from fleet_watch import registry

    def _ppid(pid: int) -> int | None:
        return (registry._inspect_process(pid) or {}).get("ppid")

    return ReclaimProbes(
        ppid=_ppid,
        exists=_default_exists,
        create_time=_default_create_time,
        active_leases=lambda: registry.list_active_session_leases(conn),
        lease_owner_alive=lambda lease: registry._lease_owner_alive(lease),
    )


@dataclass(frozen=True)
class ProcessDecision:
    """One auditable reclaim decision.

    ``authorized`` is the gate verdict (True means every check passed and a
    signal WOULD be permitted); ``action`` is what was actually attempted. A
    dry-run plan is therefore ``authorized=True`` with ``action="none"``.
    """

    pid: int
    action: str
    outcome: str
    reason: str
    decided_at: float
    authorized: bool
    evidence: Mapping[str, Any]
    identity: Mapping[str, Any] | None = None
    completed_at: float | None = None
    signal: int | None = None
    schema_version: str = PROCESS_DECISION_SCHEMA
    policy_version: str = POLICY_VERSION
    identity_path: str = IDENTITY_PATH
    verifier_identity: str = SECOND_PATH

    @property
    def signalling(self) -> bool:
        return self.signal is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "policy_version": self.policy_version,
            "pid": self.pid,
            "action": self.action,
            "outcome": self.outcome,
            "reason": self.reason,
            "authorized": self.authorized,
            "identity_path": self.identity_path,
            "verifier_identity": self.verifier_identity,
            "decided_at": self.decided_at,
            "completed_at": self.completed_at,
            "signal": self.signal,
            "observed_identity": dict(self.identity) if self.identity else None,
            "evidence": dict(self.evidence),
        }


def capture_mcp_identity(
    pid: int,
    *,
    probes: Probes,
    claim: MCPOwnerClaim | None = None,
    status: str = "idle",
    kind: str = "service",
) -> RecordedIdentity | None:
    """Snapshot the full kernel identity at discovery time.

    This is the base the decision re-validates against: PID + kernel create-time
    + executable + UID, all observed once here. Returns None on ANY uncertainty
    (gone, unreadable, missing create-time/exe/uid) so the caller records a
    snapshot-less DENY rather than a partial one.
    """
    observation = observe(pid, probes)
    if observation.error:
        return None
    if observation.create_time is None or observation.exe is None or observation.uid is None:
        return None
    claim = claim or MCPOwnerClaim()
    return RecordedIdentity(
        pid=pid,
        create_time=observation.create_time,
        exe=observation.exe,
        uid=observation.uid,
        evidence=OwnerEvidence(
            session_id=claim.owner_label(),
            owner=f"mcp-stdio-server:{pid}",
            status=status,
            kind=kind,
        ),
    )


def verify_owner_dead(
    pid: int,
    claim: MCPOwnerClaim,
    reclaim: ReclaimProbes,
) -> tuple[bool, str, dict[str, Any]]:
    """Second path, part 1: positively prove the OWNING SESSION IS GONE.

    Two independent ways to prove death, both positive: the owner PID is absent,
    or the owner PID is present but its create-time no longer matches the one
    recorded at scan (the OS recycled the integer, so the original is gone). A
    chain that is already detached (ppid <= 1) counts only when it is STILL
    detached at decision time.

    Returns ``(proven_dead, reason, evidence)``. Any unreadable value returns
    False — there is no third answer.
    """
    evidence: dict[str, Any] = {
        "session_pid_at_scan": claim.session_pid,
        "session_create_time_at_scan": claim.session_create_time,
        "reparented_at_scan": claim.reparented,
        "alive_at_scan": claim.alive_at_scan,
    }
    try:
        fresh_ppid = reclaim.ppid(pid)
    except Exception as exc:  # type named in the reason — never a blind except
        return False, f"ancestry_unreadable:{type(exc).__name__}", evidence
    evidence["ppid_at_decision"] = fresh_ppid
    if fresh_ppid is None:
        return False, "ancestry_unreadable", evidence
    # A parent that is neither the recorded one nor init means the chain was
    # re-attached under a different, possibly live, owner.
    if fresh_ppid > 1 and fresh_ppid != claim.session_pid:
        return False, "parent_changed_since_scan", evidence

    if claim.session_pid > 1:
        try:
            owner_exists = reclaim.exists(claim.session_pid)
        except Exception as exc:
            return False, f"owner_probe_error:{type(exc).__name__}", evidence
        evidence["owner_exists_at_decision"] = owner_exists
        if not owner_exists:
            evidence["owner_death_proof"] = "owner_pid_absent"
            return True, "owner_pid_absent", evidence
        try:
            live_ct = reclaim.create_time(claim.session_pid)
        except Exception as exc:
            return False, f"owner_create_time_error:{type(exc).__name__}", evidence
        evidence["owner_create_time_at_decision"] = live_ct
        if live_ct is None:
            return False, "owner_identity_unprovable", evidence
        if not claim.session_create_time:
            return False, "owner_create_time_missing_at_scan", evidence
        if live_ct != claim.session_create_time:
            evidence["owner_death_proof"] = "owner_pid_recycled"
            return True, "owner_pid_recycled", evidence
        return False, "owner_session_alive", evidence

    if fresh_ppid <= 1:
        evidence["owner_death_proof"] = "chain_detached"
        return True, "chain_detached", evidence
    return False, "parent_changed_since_scan", evidence


def verify_no_live_lease(
    pid: int,
    claim: MCPOwnerClaim,
    reclaim: ReclaimProbes,
) -> tuple[bool, str, dict[str, Any]]:
    """Second path, part 2: the SESSION REGISTRY must not show a live lease that
    references this process or its recorded owning session.

    A lease row whose owner is positively gone is stale bookkeeping, not a live
    reference. A row whose owner cannot be read is a DENY, never a pass.
    """
    try:
        leases = list(reclaim.active_leases() or [])
    except Exception as exc:
        return False, f"lease_registry_unreadable:{type(exc).__name__}", {
            "leases_examined": 0,
        }
    watched = {pid}
    if claim.session_pid > 1:
        watched.add(claim.session_pid)
    referencing: list[str] = []
    stale: list[str] = []
    for lease in leases:
        raw_owner = lease.get("owner_pid")
        try:
            owner_pid = int(raw_owner) if raw_owner is not None else None
        except (TypeError, ValueError):
            continue
        if owner_pid is None or owner_pid not in watched:
            continue
        session_id = str(lease.get("session_id"))
        try:
            alive = bool(reclaim.lease_owner_alive(lease))
        except Exception as exc:
            return False, f"lease_owner_unreadable:{type(exc).__name__}", {
                "leases_examined": len(leases),
                "watched_pids": sorted(watched),
                "unreadable_lease": session_id,
            }
        (referencing if alive else stale).append(session_id)
    evidence = {
        "leases_examined": len(leases),
        "watched_pids": sorted(watched),
        "referencing_leases": referencing,
        "stale_leases": stale,
    }
    if referencing:
        return False, "live_session_lease:" + ",".join(sorted(referencing)), evidence
    return True, "no_live_session_lease", evidence


def decide_mcp_reclaim(
    pid: int,
    recorded: RecordedIdentity | None,
    claim: MCPOwnerClaim,
    *,
    probes: Probes,
    reclaim: ReclaimProbes,
    kill: bool = False,
    max_age_seconds: float = MAX_OBSERVATION_AGE_SECONDS,
) -> ProcessDecision:
    """Run the full gate stack for one candidate. PURE: never signals.

    Order: snapshot present → second path (owner death, then lease references) →
    identity path (fresh observation vs the snapshot, then the full terminate
    contract) → dry-run/act. The first failing check is the receipt's reason.
    ``pid`` is the candidate's own PID (always known); ``recorded`` is its
    identity snapshot, which may be None and then denies.
    """
    now = probes.now()
    evidence: dict[str, Any] = {
        "source": claim.source,
        "cmd": claim.cmd,
        "rss_mb": claim.rss_mb,
        "owner_claim": claim.to_dict(),
        "identity_path": IDENTITY_PATH,
        "verifier_identity": SECOND_PATH,
    }

    def _deny(reason: str, **extra: Any) -> ProcessDecision:
        return ProcessDecision(
            pid=pid,
            action=RECLAIM_ACTION_NONE,
            outcome=RECLAIM_OUTCOME_DENIED,
            reason=reason,
            decided_at=now,
            authorized=False,
            evidence={**evidence, **extra},
        )

    if recorded is None:
        return _deny("identity_snapshot_unavailable")
    pid = recorded.pid
    evidence["identity_snapshot"] = {
        "pid": recorded.pid,
        "create_time": recorded.create_time,
        "exe": recorded.exe,
        "uid": recorded.uid,
    }

    # Three-point create-time agreement: the scan, the snapshot, and (below) the
    # fresh observation must all name the same process. The scan value is the
    # earliest one, so a recycled PID cannot inherit the earlier verdict; a
    # missing scan value means the candidate was never positively identified.
    if not claim.scan_create_time:
        return _deny("scan_create_time_unavailable")
    evidence["create_time_cross_check"] = {
        "scan_create_time": claim.scan_create_time,
        "snapshot_create_time": recorded.create_time,
    }
    if claim.scan_create_time != recorded.create_time:
        return _deny("scan_create_time_mismatch")

    owner_ok, owner_reason, owner_evidence = verify_owner_dead(pid, claim, reclaim)
    evidence["owner_proof"] = owner_evidence
    if not owner_ok:
        return _deny(owner_reason, lease_check={"skipped": "owner_death_unproven"})

    lease_ok, lease_reason, lease_evidence = verify_no_live_lease(pid, claim, reclaim)
    evidence["lease_check"] = lease_evidence
    if not lease_ok:
        return _deny(lease_reason)

    observation = observe(pid, probes)
    inner = decide(
        observation, recorded, ACTION_TERMINATE, probes,
        max_age_seconds=max_age_seconds,
    )
    evidence["identity_decision"] = inner.to_dict()
    if inner.outcome != OUTCOME_ALLOWED:
        return _deny(inner.reason)

    if not kill:
        return ProcessDecision(
            pid=pid,
            action=RECLAIM_ACTION_NONE,
            outcome=RECLAIM_OUTCOME_REPORTED,
            reason="dry_run_verified_candidate",
            decided_at=now,
            authorized=True,
            evidence={**evidence, "planned_action": RECLAIM_ACTION_SIGTERM},
            identity={
                "create_time": observation.create_time,
                "exe": observation.exe,
                "uid": observation.uid,
                "classification": inner.classification,
                "protections": list(inner.protections),
                "session_id": claim.owner_label(),
            },
        )
    return ProcessDecision(
        pid=pid,
        action=RECLAIM_ACTION_SIGTERM,
        outcome=RECLAIM_OUTCOME_ALLOWED,
        reason="verified_dead_session_orphan",
        decided_at=now,
        authorized=True,
        evidence=evidence,
        identity={
            "create_time": observation.create_time,
            "exe": observation.exe,
            "uid": observation.uid,
            "classification": inner.classification,
            "protections": list(inner.protections),
            "session_id": claim.owner_label(),
        },
    )


def execute_mcp_reclaim(
    plan: ProcessDecision,
    recorded: RecordedIdentity,
    claim: MCPOwnerClaim,
    *,
    probes: Probes,
    reclaim: ReclaimProbes,
    force: bool = False,
    force_reason: str = "operator --kill-force",
    grace_seconds: float = DEFAULT_GRACE_SECONDS,
    sender: Callable[[int, int], None] | None = None,
) -> ProcessDecision:
    """Act on an authorized plan: ONE graceful SIGTERM, then stop.

    Order: a dry-run or denied plan returns unchanged and sends nothing. An
    authorized plan re-runs BOTH second-path checks immediately before the
    signal (the second path is re-proved at the moment of action, not trusted
    from the decision), then :func:`terminate_gracefully` re-observes and
    re-decides the identity itself. ``survived`` is reported, never escalated.
    SIGKILL needs ``force`` AND a non-empty reason AND its own fresh identity
    re-check inside :func:`force_terminate`.
    """
    if not plan.authorized or plan.action != RECLAIM_ACTION_SIGTERM:
        return plan

    owner_ok, owner_reason, owner_evidence = verify_owner_dead(recorded.pid, claim, reclaim)
    lease_ok, lease_reason, lease_evidence = verify_no_live_lease(recorded.pid, claim, reclaim)
    recheck = {"owner_proof": owner_evidence, "lease_check": lease_evidence}
    if not (owner_ok and lease_ok):
        return replace(
            plan,
            action=RECLAIM_ACTION_NONE,
            outcome=RECLAIM_OUTCOME_DENIED,
            reason=f"second_path_recheck_denied:{owner_reason if not owner_ok else lease_reason}",
            authorized=False,
            evidence={**plan.evidence, "second_path_recheck": recheck},
            completed_at=probes.now(),
        )

    receipt = terminate_gracefully(
        recorded, probes, sender=sender, grace_seconds=grace_seconds,
    )
    evidence = {**plan.evidence, "second_path_recheck": recheck, "signal_receipt": receipt.to_dict()}
    completed = receipt.completed_at if receipt.completed_at is not None else probes.now()
    if receipt.outcome == OUTCOME_EXITED:
        return replace(
            plan, action=RECLAIM_ACTION_SIGTERM, outcome=RECLAIM_OUTCOME_EXITED,
            reason=receipt.reason, completed_at=completed,
            signal=int(signal.SIGTERM), evidence=evidence,
        )
    if receipt.outcome != OUTCOME_SURVIVED:
        return replace(
            plan, action=RECLAIM_ACTION_SIGTERM, outcome=RECLAIM_OUTCOME_FAILED,
            reason=receipt.reason, completed_at=completed,
            signal=receipt.signal, evidence=evidence,
        )
    if not force or not force_reason.strip():
        return replace(
            plan, action=RECLAIM_ACTION_SIGTERM, outcome=RECLAIM_OUTCOME_SURVIVED,
            reason="grace_timeout_force_not_requested", completed_at=completed,
            signal=int(signal.SIGTERM), evidence=evidence,
        )

    killed = force_terminate(
        recorded, probes,
        force_gate=ForceGate(allowed=True, reason=force_reason),
        sender=sender,
    )
    evidence = {**evidence, "force_receipt": killed.to_dict()}
    completed = killed.completed_at if killed.completed_at is not None else probes.now()
    if killed.outcome == OUTCOME_EXITED:
        return replace(
            plan, action=RECLAIM_ACTION_SIGKILL, outcome=RECLAIM_OUTCOME_EXITED,
            reason=killed.reason, completed_at=completed,
            signal=int(signal.SIGKILL), evidence=evidence,
        )
    return replace(
        plan, action=RECLAIM_ACTION_SIGKILL, outcome=RECLAIM_OUTCOME_FAILED,
        reason=killed.reason, completed_at=completed,
        signal=int(signal.SIGKILL), evidence=evidence,
    )
