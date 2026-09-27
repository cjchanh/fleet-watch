"""Fleet Watch Reflex — findings to spec queue with verify-on-land.

OBSERVE AND EMIT ONLY. This module reads state other modules already
published, screens it, and writes a queued spec plus a JSON receipt. It holds
no process-control authority: nothing here calls into the OS process layer,
and no authority to do so is accepted from a caller. Process control stays in
the governed policy paths (``fleet_watch.process_policy``, ``commands/reap.py``).

Shape of the loop:

1. ``emit``  — screen findings, write one spec per open finding, one receipt
   per finding. Dry-run by default: nothing on disk changes without ``write``.
2. ``verify`` — re-test every open receipt. Cleared -> ``verified=True``.
   Persisting past the TTL -> re-queue a new spec carrying an escalation note.
3. ``status`` — open findings, their queued spec ids, receipt freshness.

Every boundary is injectable — queue dir, receipts dir, the finding source, the
re-check probe and the clock — so the tests run hermetically in a temp dir and
never read the live registry.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "fleet-watch/reflex/v1"
RECEIPT_SCHEMA_VERSION = "fleet-watch/reflex-receipt/v1"
STATUS_SCHEMA_VERSION = "fleet-watch/reflex-status/v1"
SPEC_TEMPLATE_VERSION = "fleet-watch/reflex-spec/v1"

QUEUE_DIR = Path.home() / "ai" / "specs" / "queue"
RECEIPT_DIR = Path.home() / ".fleet-watch" / "receipts" / "reflex"
DEFAULT_REPO = "/Users/cj/Workspace/active/fleet-watch"

DEFAULT_MIN_TICKS = 3
DEFAULT_TTL_SECONDS = 3600
DEFAULT_DENIAL_LOOKBACK_HOURS = 24

FINDING_KINDS = (
    "heartbeat_stale",
    "dead_session_orphan",
    "runaway",
    "process_decision_denial",
)

# Only event-shaped findings need to survive N observations. The other three
# are derived from current state, so one observation is the whole observation
# and re-counting them would only delay a real finding.
TICK_GATED_KINDS = frozenset({"process_decision_denial"})

# Any truthy value under one of these keys means the row is owned by a
# supervisor (launchd KeepAlive, an explicit keeper, ...). A supervised
# process is not an orphan; emitting it is the false positive this loop must
# never produce, so it is rejected before a spec can be drafted.
SUPERVISION_KEYS = (
    "supervised",
    "supervision",
    "supervisor",
    "supervisor_label",
    "launchd_label",
    "keepalive",
    "keeper",
)

TUESDAY_PRINCIPLES = (
    "Spec before execution",
    "Evidence or refuse",
    "Smallest executable unit",
    "Operator remains sovereign",
    "Read-before-write",
)

OUT_OF_SCOPE = (
    "Execution or automated code landing of this work",
    "Commits or lifecycle changes",
    "Modification of shipped specs",
    "Any process-control call from the reflex path (authority stays governed)",
)

Clock = Callable[[], float]


@dataclass(frozen=True)
class Finding:
    """One observed problem. ``ticks`` is how many observations it survived."""

    finding_id: str = ""
    kind: str = ""
    summary: str = ""
    severity: str = "medium"
    detail: Mapping[str, Any] = field(default_factory=dict)
    ticks: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "finding_id": self.finding_id,
            "kind": self.kind,
            "summary": self.summary,
            "severity": self.severity,
            "detail": dict(self.detail),
            "ticks": self.ticks,
        }


@dataclass(frozen=True)
class Screening:
    """Why a finding did or did not clear the emitter's gate."""

    emit: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {"emit": self.emit, "reason": self.reason}


@dataclass(frozen=True)
class Recheck:
    """Result of re-testing one finding for ``verify``."""

    present: bool
    detail: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"present": self.present, "detail": dict(self.detail)}


Probe = Callable[[str], Recheck]


@dataclass
class EmitReport:
    """What one ``emit`` pass decided, per finding."""

    mode: str
    min_ticks: int
    queue_dir: Path
    receipts_dir: Path
    emitted: list[dict[str, Any]] = field(default_factory=list)
    rejected: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "mode": self.mode,
            "min_ticks": self.min_ticks,
            "queue_dir": str(self.queue_dir),
            "receipts_dir": str(self.receipts_dir),
            "counts": {
                "emitted": len(self.emitted),
                "rejected": len(self.rejected),
            },
            "emitted": self.emitted,
            "rejected": self.rejected,
        }


@dataclass
class VerifyReport:
    """What one ``verify`` pass decided, per open receipt."""

    mode: str
    ttl_seconds: int
    receipts_dir: Path
    queue_dir: Path
    checked: list[dict[str, Any]] = field(default_factory=list)
    cleared: list[str] = field(default_factory=list)
    persisting: list[str] = field(default_factory=list)
    escalated: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "mode": self.mode,
            "ttl_seconds": self.ttl_seconds,
            "queue_dir": str(self.queue_dir),
            "receipts_dir": str(self.receipts_dir),
            "counts": {
                "checked": len(self.checked),
                "cleared": len(self.cleared),
                "persisting": len(self.persisting),
                "escalated": len(self.escalated),
                "errors": len(self.errors),
            },
            "checked": self.checked,
            "cleared": self.cleared,
            "persisting": self.persisting,
            "escalated": self.escalated,
            "errors": self.errors,
        }


# ── small pure helpers ──────────────────────────────────────────────────────


def _canonical(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="seconds")


def _day(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")


# Explicitly negative spellings only. A supervision marker is a label as often
# as a flag ("com.cj.ollama-runner" is as much a supervision fact as ``True``),
# so anything non-empty and not a negative spelling counts as supervised.
_FALSEY_STRINGS = frozenset({"", "0", "false", "no", "off", "none", "null"})


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in _FALSEY_STRINGS
    return bool(value)


def slug(text: str, *, limit: int = 40) -> str:
    """Filesystem-safe fragment: lower-case alnum and dashes only."""
    out = [c.lower() if c.isalnum() else "-" for c in str(text)]
    collapsed: list[str] = []
    for c in out:
        if c == "-" and (not collapsed or collapsed[-1] == "-"):
            continue
        collapsed.append(c)
    return "".join(collapsed).strip("-")[:limit] or "unknown"


def evidence_sha256(finding: Finding | Mapping[str, Any]) -> str:
    """Stable hash over the finding's evidence — the receipt's proof anchor."""
    detail = finding.detail if isinstance(finding, Finding) else finding.get("detail")
    return _sha256(_canonical(dict(detail or {})))


def finding_key(finding: Finding) -> str:
    """Stable per-finding identity: receipt file name and dedupe key."""
    return f"reflex-{slug(finding.kind)}-{_sha256(finding.finding_id)[:10]}"


def next_spec_id(receipt_key: str, index: int) -> str:
    """First emission keeps the bare key; every later one gets a suffix.

    Deterministic, so a re-queue never clobbers the spec it supersedes.
    """
    return receipt_key if index <= 1 else f"{receipt_key}-{index}"


def _identity_token(row: Mapping[str, Any]) -> str:
    for key in ("session_id", "process_key", "key", "pid", "repo_dir"):
        value = row.get(key)
        if value not in (None, "", {}):
            return f"{key}={value}"
    return f"row={_sha256(_canonical(dict(row)))[:12]}"


def _default_summary(kind: str, row: Mapping[str, Any]) -> str:
    name = row.get("name") or row.get("workstream") or "unknown"
    ident = _identity_token(row)
    return f"{kind or 'unknown'} finding: {name} ({ident})"


def finding_from_row(row: Mapping[str, Any] | None) -> Finding:
    """Normalize a raw source row into a :class:`Finding`.

    Total by design: a malformed row becomes a Finding the screen rejects,
    never an exception, so one bad row cannot abort a whole pass.
    """
    data: Mapping[str, Any] = row if isinstance(row, Mapping) else {}
    kind = str(data.get("kind") or data.get("type") or "").strip()
    raw_detail = data.get("detail")
    if raw_detail is None:
        raw_detail = data.get("evidence")
    detail = raw_detail if isinstance(raw_detail, Mapping) else {}
    if raw_detail is not None and not isinstance(raw_detail, Mapping):
        detail = {"value": raw_detail}

    finding_id = str(data.get("finding_id") or "").strip()
    if not finding_id:
        finding_id = f"{kind or 'unknown'}:{_identity_token(data)}"

    raw_ticks = data.get("ticks", data.get("ticks_observed", 1))
    try:
        ticks = int(raw_ticks)
    except (TypeError, ValueError):
        ticks = 0

    summary = str(data.get("summary") or "").strip() or _default_summary(kind, data)
    severity = str(data.get("severity") or "medium").strip().lower() or "medium"
    return Finding(
        finding_id=finding_id,
        kind=kind,
        summary=summary,
        severity=severity,
        detail=detail,
        ticks=max(0, ticks),
    )


def _is_supervised(finding: Finding) -> bool:
    return any(_truthy(finding.detail.get(key)) for key in SUPERVISION_KEYS)


def screen_finding(
    finding: Finding,
    *,
    open_receipts: Mapping[str, Mapping[str, Any]] | None = None,
    min_ticks: int = DEFAULT_MIN_TICKS,
) -> Screening:
    """Decide whether one finding may become a queued spec.

    Rejection is the interesting branch. The loop's worst failure is a false
    positive, so anything unproven is dropped with a named reason rather than
    drafted. The reason string is recorded in the report, so a dropped finding
    is visible instead of silently missing.
    """
    receipts = open_receipts or {}
    threshold = max(1, int(min_ticks))

    if finding.kind not in FINDING_KINDS:
        return Screening(False, f"unknown_kind:{finding.kind or 'missing'}")
    if not finding.finding_id:
        return Screening(False, "malformed:missing_finding_id")
    if not finding.summary:
        return Screening(False, "malformed:missing_summary")
    if not finding.detail:
        return Screening(False, "insufficient_evidence:empty_detail")
    if _is_supervised(finding):
        # The planted bad: a supervised keeper reads as an orphan to a naive
        # parent-PID heuristic. It is not one.
        return Screening(False, "supervised_keeper")
    if finding.kind == "dead_session_orphan" and finding.detail.get("orphan_confirmed") is not True:
        return Screening(False, "identity_unproven:orphan_not_confirmed")
    if finding.kind in TICK_GATED_KINDS and finding.ticks < threshold:
        return Screening(False, f"below_tick_threshold:{finding.ticks}<{threshold}")

    existing = receipts.get(finding_key(finding))
    if existing is not None and not existing.get("verified"):
        return Screening(False, f"already_open:{existing.get('spec_id')}")
    return Screening(True, "ok")


# ── spec template ───────────────────────────────────────────────────────────


def _acceptance_criteria(finding: Finding, sha: str) -> list[str]:
    return [
        f"`fleet reflex status --json` lists finding `{finding.finding_id}` as open "
        f"with spec id `{finding_key(finding)}` and receipt evidence hash `{sha[:12]}`.",
        "The observation stops repeating, fixed in the governed path that published "
        "the finding — the reflex path performs no process-control call.",
        "`fleet reflex verify --json` either marks the receipt verified or re-queues "
        "it with an escalation note, and `python3 -m pytest -q` stays green.",
    ]


def _objective(finding: Finding) -> str:
    survived = (
        f"survived {finding.ticks} observations"
        if finding.kind in TICK_GATED_KINDS
        else "repeats on every observation"
    )
    return (
        f"Fleet Watch observed `{finding.kind}` finding `{finding.finding_id}` and it "
        f"{survived}. Land the smallest change that stops the observation: the fix "
        "belongs in the governed path that published the finding, not in the reflex "
        "loop, which observes and emits only."
    )


def render_spec(
    finding: Finding,
    *,
    spec_id: str,
    created: str,
    repo: str = DEFAULT_REPO,
    evidence_hash: str = "",
    escalation_note: str | None = None,
    goal_id: str | None = None,
) -> str:
    """Render the fixed Tuesday-style spec for one finding."""
    sha = evidence_hash or evidence_sha256(finding)
    title = f"Fleet Watch reflex: {finding.summary}"
    lines: list[str] = [
        "---",
        f"spec_id: {spec_id}",
        f"template_version: {SPEC_TEMPLATE_VERSION}",
        f"finding_id: {finding.finding_id}",
        f"finding_kind: {finding.kind}",
        f'title: "{title}"',
        "status: BLOCKED",
        "priority: 50",
        "owner: cj",
        "classification: governance",
        "class: governance_writes",
        "surface: fleet-watch",
        "tuesday_standard: true",
        "autopilot_eligible: false",
        "slaunch_eligible: false",
        "operator_gated: false",
        "depends_on: []",
        f"repo: {repo}",
        f"created: {created}",
        f"evidence_sha256: {sha}",
        *([f"goal_id: {goal_id}"] if goal_id else []),
        f"ticks_observed: {finding.ticks}",
        f"severity: {finding.severity}",
    ]
    if escalation_note:
        lines.append(f'escalation_note: "{escalation_note}"')
    lines += [
        "provenance:",
        f'  from: "fleet reflex emit — {finding.kind} {finding.finding_id}"',
        "---",
        "",
        f"# {title}",
        "",
        "## Objective",
        "",
        _objective(finding),
        "",
        "## In Scope",
        "",
        f"- Drafted from finding: {finding.finding_id} ({finding.kind})",
        f"- Severity: {finding.severity}",
        "- Domain: fleet-watch",
        "",
        "## Out of Scope",
        "",
        *[f"- {item}" for item in OUT_OF_SCOPE],
        "",
        "## Acceptance Criteria",
        "",
    ]
    lines += [f"{i}. {text}" for i, text in enumerate(_acceptance_criteria(finding, sha), 1)]
    lines += [
        "",
        "## Tuesday Bar Principles",
        "",
        *[f"- {item}" for item in TUESDAY_PRINCIPLES],
        "",
        "## Evidence Requirements",
        "",
        "- Verification command or test proving completion",
        "- Receipt emitted on completion",
        "- No unverified claims in closeout",
        "",
        "## Stop Conditions",
        "",
        "- Work completed with evidence, or",
        "- Blocker encountered requiring operator decision, or",
        "- Spec found to be underspecified on implementation attempt",
        "",
        "## Evidence",
        "",
        f"- evidence sha256: {sha}",
        f"- finding detail: `{_canonical(dict(finding.detail))}`",
    ]
    if escalation_note:
        lines += ["", "## Escalation", "", f"- {escalation_note}"]
    lines += [
        "",
        "## Notes",
        "",
        "Drafted by `fleet reflex emit`. Operator review required before running.",
        "",
    ]
    return "\n".join(lines)


# ── receipts ────────────────────────────────────────────────────────────────


def load_receipts(receipts_dir: Path) -> dict[str, dict[str, Any]]:
    """Index every receipt by its stable finding key. Unreadable files skipped."""
    receipts: dict[str, dict[str, Any]] = {}
    if not receipts_dir.is_dir():
        return receipts
    for path in sorted(receipts_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        key = payload.get("receipt_key")
        if isinstance(key, str) and key:
            receipts[key] = payload
    return receipts


def write_receipt(receipts_dir: Path, receipt: Mapping[str, Any]) -> Path:
    receipts_dir.mkdir(parents=True, exist_ok=True)
    key = slug(str(receipt.get("receipt_key") or "unknown"))
    path = receipts_dir / f"{key}.json"
    path.write_text(json.dumps(dict(receipt), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _build_receipt(
    finding: Finding,
    *,
    receipt_key: str,
    spec_id: str,
    spec_path: Path,
    spec_text: str,
    sha: str,
    now: float,
    min_ticks: int,
    previous: Mapping[str, Any] | None = None,
    escalation_note: str | None = None,
    goal_id: str | None = None,
) -> dict[str, Any]:
    prior = dict(previous or {})
    emissions = list(prior.get("emissions") or [])
    emissions.append(
        {
            "spec_id": spec_id,
            "spec_path": str(spec_path),
            "emitted_at": _iso(now),
            "emitted_at_epoch": now,
            "escalation_note": escalation_note,
        }
    )
    return {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "receipt_key": receipt_key,
        "finding_id": finding.finding_id,
        "kind": finding.kind,
        "severity": finding.severity,
        "summary": finding.summary,
        "spec_id": spec_id,
        "spec_path": str(spec_path),
        "spec_sha256": _sha256(spec_text),
        "emitted_at": _iso(now),
        "emitted_at_epoch": now,
        "evidence_sha256": sha,
        "goal_id": goal_id,
        "evidence": dict(finding.detail),
        "ticks_observed": finding.ticks,
        "min_ticks": min_ticks,
        "requeues": int(prior.get("requeues") or 0),
        "escalation_note": escalation_note,
        "verified": False,
        "verified_at": None,
        "emissions": emissions,
        "rechecks": list(prior.get("rechecks") or []),
    }


def spec_fresh(receipt: Mapping[str, Any]) -> bool:
    """Receipt freshness: the queued spec still exists and still matches."""
    spec_path = receipt.get("spec_path")
    expected = receipt.get("spec_sha256")
    if not spec_path or not expected:
        return False
    path = Path(str(spec_path))
    if not path.is_file():
        return False
    try:
        return _sha256(path.read_text(encoding="utf-8")) == expected
    except OSError:
        return False


# ── emit ────────────────────────────────────────────────────────────────────


def emit(
    findings: Iterable[Finding | Mapping[str, Any]],
    *,
    queue_dir: Path = QUEUE_DIR,
    receipts_dir: Path = RECEIPT_DIR,
    write: bool = False,
    min_ticks: int = DEFAULT_MIN_TICKS,
    clock: Clock = time.time,
    repo: str = DEFAULT_REPO,
    goal_id: str | None = None,
) -> EmitReport:
    """Screen findings and queue one spec per open finding.

    Dry-run by default: ``write=False`` touches nothing on disk and reports
    what each emission would have produced. Receipts are written with the spec,
    never ahead of it — an open receipt suppresses re-emission, so a dry run
    that left one behind would silently block the real emission.
    """
    report = EmitReport(
        mode="write" if write else "dry-run",
        min_ticks=max(1, int(min_ticks)),
        queue_dir=Path(queue_dir),
        receipts_dir=Path(receipts_dir),
    )
    open_receipts = load_receipts(Path(receipts_dir))

    for raw in findings:
        finding = raw if isinstance(raw, Finding) else finding_from_row(raw)
        screening = screen_finding(finding, open_receipts=open_receipts, min_ticks=report.min_ticks)
        if not screening.emit:
            report.rejected.append(
                {
                    "finding_id": finding.finding_id,
                    "kind": finding.kind,
                    "summary": finding.summary,
                    "ticks": finding.ticks,
                    **screening.to_dict(),
                }
            )
            continue

        now = clock()
        key = finding_key(finding)
        prior = open_receipts.get(key)
        index = len((prior or {}).get("emissions") or []) + 1
        spec_id = next_spec_id(key, index)
        sha = evidence_sha256(finding)
        spec_text = render_spec(
            finding,
            spec_id=spec_id,
            created=_day(now),
            repo=repo,
            evidence_hash=sha,
            goal_id=goal_id,
        )
        spec_path = Path(queue_dir) / f"{spec_id}.md"
        receipt = _build_receipt(
            finding,
            receipt_key=key,
            spec_id=spec_id,
            spec_path=spec_path,
            spec_text=spec_text,
            sha=sha,
            goal_id=goal_id,
            now=now,
            min_ticks=report.min_ticks,
            previous=prior,
        )
        receipt_path = Path(receipts_dir) / f"{slug(key)}.json"
        if write:
            Path(queue_dir).mkdir(parents=True, exist_ok=True)
            spec_path.write_text(spec_text, encoding="utf-8")
            written = write_receipt(Path(receipts_dir), receipt)
            open_receipts[key] = receipt
        else:
            written = receipt_path

        report.emitted.append(
            {
                "finding_id": finding.finding_id,
                "kind": finding.kind,
                "severity": finding.severity,
                "summary": finding.summary,
                "ticks": finding.ticks,
                "spec_id": spec_id,
                "spec_path": str(spec_path),
                "spec_sha256": receipt["spec_sha256"],
                "receipt_path": str(written),
                "evidence_sha256": sha,
                "emitted_at": receipt["emitted_at"],
                "wrote": bool(write),
            }
        )
    return report


# ── verify ──────────────────────────────────────────────────────────────────


def default_probe(finding_id: str) -> Recheck:
    """Re-test a finding against the live source (observation only)."""
    live, errors = collect_live_findings()
    for finding in live:
        if finding.finding_id == finding_id:
            return Recheck(present=True, detail={"source": "live", **dict(finding.detail)})
    return Recheck(
        present=False,
        detail={"source": "live", "source_errors": errors},
    )


def verify_open(
    *,
    probe: Probe = default_probe,
    receipts_dir: Path = RECEIPT_DIR,
    queue_dir: Path = QUEUE_DIR,
    write: bool = False,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    clock: Clock = time.time,
    repo: str = DEFAULT_REPO,
) -> VerifyReport:
    """Re-test every open receipt; clear it, hold it, or re-queue it.

    Each re-check appends an entry to the receipt's own ``rechecks`` history
    (timestamp, outcome, evidence hash), so the receipt for a finding is also
    the receipt for every re-check of it.
    """
    report = VerifyReport(
        mode="write" if write else "dry-run",
        ttl_seconds=int(ttl_seconds),
        receipts_dir=Path(receipts_dir),
        queue_dir=Path(queue_dir),
    )

    for key, receipt in sorted(load_receipts(Path(receipts_dir)).items()):
        if receipt.get("verified"):
            continue
        finding_id = str(receipt.get("finding_id") or "")
        now = clock()
        try:
            result = probe(finding_id)
        except Exception as exc:  # noqa: BLE001 — a failed re-check is data, not a crash
            report.errors.append(
                {
                    "finding_id": finding_id,
                    "receipt_key": key,
                    "reason": f"probe_error:{type(exc).__name__}:{exc}",
                }
            )
            continue

        age = now - float(receipt.get("emitted_at_epoch") or now)
        entry: dict[str, Any] = {
            "at": _iso(now),
            "at_epoch": now,
            "present": result.present,
            "age_seconds": round(age, 3),
            "ttl_seconds": report.ttl_seconds,
            "detail": dict(result.detail),
        }
        updated = dict(receipt)

        if not result.present:
            entry["outcome"] = "cleared"
            updated["verified"] = True
            updated["verified_at"] = entry["at"]
            report.cleared.append(finding_id)
        elif age < report.ttl_seconds:
            entry["outcome"] = "persists"
            report.persisting.append(finding_id)
        else:
            entry["outcome"] = "escalated"
            requeues = int(receipt.get("requeues") or 0) + 1
            spec_id = next_spec_id(key, len(receipt.get("emissions") or []) + 1)
            note = (
                f"finding {finding_id} persisted {int(age)}s past the "
                f"{report.ttl_seconds}s verify TTL after {requeues} re-queue(s); "
                f"re-queued as {spec_id}"
            )
            finding = Finding(
                finding_id=finding_id,
                kind=str(receipt.get("kind") or ""),
                summary=str(receipt.get("summary") or ""),
                severity=str(receipt.get("severity") or "medium"),
                detail=receipt.get("evidence") or {},
                ticks=int(receipt.get("ticks_observed") or 1),
            )
            spec_text = render_spec(
                finding,
                spec_id=spec_id,
                created=_day(now),
                repo=repo,
                evidence_hash=str(receipt.get("evidence_sha256") or ""),
                escalation_note=note,
                goal_id=receipt.get("goal_id"),
            )
            spec_path = Path(queue_dir) / f"{spec_id}.md"
            if write:
                Path(queue_dir).mkdir(parents=True, exist_ok=True)
                spec_path.write_text(spec_text, encoding="utf-8")
            updated = _build_receipt(
                finding,
                receipt_key=key,
                spec_id=spec_id,
                spec_path=spec_path,
                spec_text=spec_text,
                sha=str(receipt.get("evidence_sha256") or ""),
                now=now,
                min_ticks=int(receipt.get("min_ticks") or DEFAULT_MIN_TICKS),
                previous=receipt,
                escalation_note=note,
                goal_id=receipt.get("goal_id"),
            )
            updated["requeues"] = requeues
            report.escalated.append(
                {
                    "finding_id": finding_id,
                    "receipt_key": key,
                    "spec_id": spec_id,
                    "spec_path": str(spec_path),
                    "requeues": requeues,
                    "age_seconds": round(age, 3),
                    "escalation_note": note,
                }
            )
            entry["spec_id"] = spec_id

        # The receipt is the receipt for this re-check, so the history entry is
        # written only once the outcome is final.
        updated["rechecks"] = [*(receipt.get("rechecks") or []), entry]
        if write:
            write_receipt(Path(receipts_dir), updated)
        report.checked.append(
            {
                "finding_id": finding_id,
                "receipt_key": key,
                "spec_id": updated.get("spec_id"),
                "outcome": entry["outcome"],
                "age_seconds": round(age, 3),
                "verified": bool(updated.get("verified")),
                "wrote": bool(write),
            }
        )
    return report


# ── status ──────────────────────────────────────────────────────────────────


def build_status(
    *,
    findings: Iterable[Finding | Mapping[str, Any]] | None = None,
    queue_dir: Path = QUEUE_DIR,
    receipts_dir: Path = RECEIPT_DIR,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    clock: Clock = time.time,
) -> dict[str, Any]:
    """Open findings, their queued spec ids, and receipt freshness."""
    now = clock()
    receipts = load_receipts(Path(receipts_dir))
    open_findings: list[dict[str, Any]] = []
    verified = 0

    for key, receipt in sorted(receipts.items()):
        fresh = spec_fresh(receipt)
        age = now - float(receipt.get("emitted_at_epoch") or now)
        if receipt.get("verified"):
            verified += 1
            continue
        open_findings.append(
            {
                "finding_id": receipt.get("finding_id"),
                "kind": receipt.get("kind"),
                "severity": receipt.get("severity"),
                "summary": receipt.get("summary"),
                "receipt_key": key,
                "spec_id": receipt.get("spec_id"),
                "spec_path": receipt.get("spec_path"),
                "emitted_at": receipt.get("emitted_at"),
                "age_seconds": round(age, 3),
                "receipt_fresh": fresh,
                "ttl_exceeded": age >= int(ttl_seconds),
                "ticks_observed": receipt.get("ticks_observed"),
                "requeues": receipt.get("requeues"),
                "escalated": bool(receipt.get("escalation_note")),
                "rechecks": len(receipt.get("rechecks") or []),
                "source": "receipt",
            }
        )

    seen = {str(row.get("finding_id")) for row in open_findings}
    pending: list[dict[str, Any]] = []
    for raw in findings or []:
        finding = raw if isinstance(raw, Finding) else finding_from_row(raw)
        if finding.finding_id in seen:
            continue
        seen.add(finding.finding_id)
        screening = screen_finding(finding, open_receipts=receipts)
        pending.append(
            {
                "finding_id": finding.finding_id,
                "kind": finding.kind,
                "summary": finding.summary,
                "ticks": finding.ticks,
                "emit_ready": screening.emit,
                "reason": screening.reason,
                "source": "observed",
            }
        )

    return {
        "schema_version": STATUS_SCHEMA_VERSION,
        "generated_at": _iso(now),
        "queue_dir": str(queue_dir),
        "receipts_dir": str(receipts_dir),
        "ttl_seconds": int(ttl_seconds),
        "counts": {
            "open_findings": len(open_findings),
            "verified_findings": verified,
            "total_receipts": len(receipts),
            "observed_not_queued": len(pending),
        },
        "open_findings": open_findings,
        "observed_not_queued": pending,
    }


def status_text(status: Mapping[str, Any]) -> str:
    counts = status.get("counts") or {}
    lines = [
        f"Reflex status — {counts.get('open_findings', 0)} open, "
        f"{counts.get('verified_findings', 0)} verified, "
        f"{counts.get('observed_not_queued', 0)} observed-not-queued",
        f"queue: {status.get('queue_dir')}",
        f"receipts: {status.get('receipts_dir')}",
    ]
    open_findings = status.get("open_findings") or []
    if not open_findings:
        lines.append("No open findings.")
    for row in open_findings:
        freshness = "fresh" if row.get("receipt_fresh") else "STALE"
        flags = [freshness]
        if row.get("ttl_exceeded"):
            flags.append("PAST-TTL")
        if row.get("escalated"):
            flags.append(f"requeued x{row.get('requeues')}")
        lines.append(
            f"  {row.get('finding_id')}  [{row.get('kind')}]  spec={row.get('spec_id')}  "
            f"age={row.get('age_seconds')}s  {' '.join(flags)}"
        )
    pending = status.get("observed_not_queued") or []
    for row in pending:
        lines.append(
            f"  (observed) {row.get('finding_id')}  [{row.get('kind')}]  {row.get('reason')}"
        )
    return "\n".join(lines)


# ── finding sources ─────────────────────────────────────────────────────────


def findings_from_file(path: Path) -> list[Finding]:
    """Load raw finding rows from a JSON file (the injectable source)."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, Mapping):
        rows: Sequence[Any] = payload.get("findings") or []
    else:
        rows = payload
    return [finding_from_row(row) for row in rows if isinstance(row, Mapping)]


def _live_heartbeat_stale(conn, errors) -> list[Finding]:
    from fleet_watch import registry

    try:
        rows = registry.get_stale_processes(conn)
    except Exception as exc:  # noqa: BLE001 — an unreadable source is reported, not raised
        errors.append({"source": "heartbeat_stale", "reason": f"{type(exc).__name__}: {exc}"})
        return []
    return [
        Finding(
            finding_id=f"heartbeat_stale:pid={row.get('pid')}",
            kind="heartbeat_stale",
            summary=(
                f"heartbeat stale: {row.get('name')} (pid {row.get('pid')}) "
                f"— {row.get('stale_seconds')}s"
            ),
            severity="medium",
            detail={
                "pid": row.get("pid"),
                "name": row.get("name"),
                "classification": row.get("classification"),
                "stale_seconds": row.get("stale_seconds"),
                "session_id": row.get("session_id"),
                "evidence": row.get("evidence") or [],
            },
        )
        for row in rows
    ]


def _live_dead_session_orphan(conn, errors) -> list[Finding]:
    from fleet_watch import registry

    try:
        leases = registry.list_active_session_leases(conn)
    except Exception as exc:  # noqa: BLE001
        errors.append({"source": "dead_session_orphan", "reason": f"{type(exc).__name__}: {exc}"})
        return []

    supervised: dict[int, str] | None
    try:
        from fleet_watch.discovery import mcp_orphan_detector

        supervised = mcp_orphan_detector._launchd_supervised_pids()
    except Exception as exc:  # noqa: BLE001 — unreadable supervision map must not
        # manufacture an orphan. Fail closed: no dead-session-orphan finding is
        # emitted this tick, and the reason is on the record.
        errors.append(
            {"source": "supervision_map", "reason": f"{type(exc).__name__}: {exc}"}
        )
        supervised = None

    findings: list[Finding] = []
    for lease in leases:
        try:
            owner_alive = registry._lease_owner_alive(lease)
        except Exception as exc:  # noqa: BLE001 — keep blocking on inspection error
            errors.append({"source": "lease_liveness", "reason": f"{type(exc).__name__}: {exc}"})
            continue
        if owner_alive:
            continue
        owner_pid = lease.get("owner_pid")
        label = None
        if supervised is not None and owner_pid is not None:
            label = supervised.get(int(owner_pid))
        elif supervised is None:
            continue
        findings.append(
            Finding(
                finding_id=f"dead_session_orphan:session={lease.get('session_id')}",
                kind="dead_session_orphan",
                summary=(
                    f"dead session owner: {lease.get('session_id')} "
                    f"(pid {owner_pid}) holds a lease"
                ),
                severity="high",
                detail={
                    "session_id": lease.get("session_id"),
                    "owner_pid": owner_pid,
                    "repo_dir": lease.get("repo_dir"),
                    "orphan_confirmed": True,
                    "supervisor_label": label,
                    "owner_alive": False,
                },
            )
        )
    return findings


def _live_runaway(errors) -> list[Finding]:
    from fleet_watch import runaway

    try:
        flagged = runaway.scan_runaways()
    except Exception as exc:  # noqa: BLE001
        errors.append({"source": "runaway", "reason": f"{type(exc).__name__}: {exc}"})
        return []
    return [
        Finding(
            finding_id=f"runaway:pid={proc.pid}",
            kind="runaway",
            summary=f"runaway cpu: {proc.name} (pid {proc.pid}) at {proc.cpu_pct:.1f}%",
            severity="high",
            detail=proc.to_dict(),
        )
        for proc in flagged
    ]


def _live_process_decision_denials(conn, errors, *, lookback_hours: int) -> list[Finding]:
    """Group denied PROCESS_DECISION receipts and count their observations.

    A denial that survives N ticks is a real blocker; a single denial is often
    a correct policy call. Counting the ticks is the difference between a
    finding and noise.
    """
    from fleet_watch import events

    try:
        rows = events.get_events(
            conn, hours=lookback_hours, event_type="PROCESS_DECISION", limit=1000
        )
    except Exception as exc:  # noqa: BLE001
        errors.append({"source": "process_decision_denial", "reason": f"{type(exc).__name__}: {exc}"})
        return []

    groups: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = {}
    for row in rows:
        detail = row.get("detail") or {}
        receipt = detail.get("receipt") or {}
        if receipt.get("authorized") is not False:
            continue
        key = (receipt.get("pid"), receipt.get("action"), receipt.get("reason"))
        groups.setdefault(key, []).append(row)

    findings: list[Finding] = []
    for (pid, action, reason), entries in groups.items():
        entries.sort(key=lambda r: str(r.get("timestamp") or ""))
        latest = entries[-1]
        detail_payload = (latest.get("detail") or {}).get("receipt") or {}
        findings.append(
            Finding(
                finding_id=f"process_decision_denial:pid={pid}:action={action}",
                kind="process_decision_denial",
                summary=f"process decision denied: pid {pid} {action} — {reason}",
                severity="high",
                detail={
                    "pid": pid,
                    "action": action,
                    "reason": reason,
                    "ticks": len(entries),
                    "first_seen": entries[0].get("timestamp"),
                    "last_seen": latest.get("timestamp"),
                    "event_hash": latest.get("hash"),
                    "receipt": detail_payload,
                },
                ticks=len(entries),
            )
        )
    return findings


def collect_live_findings(
    *,
    denial_lookback_hours: int = DEFAULT_DENIAL_LOOKBACK_HOURS,
) -> tuple[list[Finding], list[dict[str, Any]]]:
    """Read every finding source from the live registry.

    Returns ``(findings, errors)``. Each source is isolated: one unreadable
    source degrades that kind only, and the reason is returned rather than
    swallowed. Observation is read-only — nothing here changes process state.
    """
    errors: list[dict[str, Any]] = []
    findings: list[Finding] = []
    conn = None
    try:
        from fleet_watch import registry

        conn = registry.connect()
    except Exception as exc:  # noqa: BLE001 — no registry means no queue-state findings
        errors.append({"source": "registry", "reason": f"{type(exc).__name__}: {exc}"})
        return findings, errors

    try:
        findings.extend(_live_heartbeat_stale(conn, errors))
        findings.extend(_live_dead_session_orphan(conn, errors))
        findings.extend(_live_process_decision_denials(
            conn, errors, lookback_hours=denial_lookback_hours
        ))
    finally:
        conn.close()
    findings.extend(_live_runaway(errors))
    return findings, errors
