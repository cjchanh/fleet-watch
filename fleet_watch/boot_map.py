"""boot_map — fleet-census receipt -> boot graph -> local sovereign 3D render.

Answers one operator question: *what boots on this machine, and where does it go?*

Pipeline (deterministic, no LLM, no network, no OS mutation):

    census receipt JSON  ->  parse + validate (fail-closed)
                         ->  node/link graph JSON
                         ->  ONE self-contained HTML file (zero external assets)

Input contract: ``fleet-census/v1`` (see the Boot Map section of README). Two accepted
shapes: the full receipt document (``{"schema_version", "domains", ...}``) and a
bare ``domains`` array. Anything else is a REFUSAL, never a partial graph.

Graph conventions follow the Phase 1C no-LLM structural map
(``~/ai/scripts/cds_graph_phase1c_no_llm_map.py``): a ``{"nodes": [...],
"links": [...]}`` document, ``kind:<sha256[:16]>`` node ids, and links carrying
``relation`` / ``confidence`` / ``confidence_score`` / ``weight``. One
deliberate deviation: Phase 1C links carry ``source_file`` (the file that
produced the edge); a census has no source file, so links carry ``source_ref``
(the census item label that produced the edge) instead of mislabelling it.

Fail-closed rules (Craft Gate pillar 1):
  * Missing / unreadable / unparseable receipt -> ``BootMapError``.
  * Structurally wrong receipt (bad root type, non-dict item, unlabelled item)
    -> ``BootMapError``.
  * Degenerate receipt (zero domains or zero items) -> ``BootMapError``. A
    real-but-empty render is a refusal condition, not output.
  * Missing ``status`` / ``verdict`` on an item is NEVER a silent drop: the item
    is kept, coerced to ``unknown`` / ``investigate`` (the "a human must look"
    side), and a warning is recorded in the graph.
  * Off-enum values are kept verbatim and flagged ``*_valid: false`` — the graph
    never launders bad input into good-looking input.

Determinism: ``graph.json`` and ``index.html`` contain no wall-clock and no
randomness. Same receipt bytes -> byte-identical artifacts. The build receipt
(``receipt.json``) is the only artifact carrying a timestamp.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fleet_watch.boot_map_view import render_html

SCHEMA_VERSION = "fleet-watch/boot-map-graph/v1"
RECEIPT_SCHEMA_VERSION = "fleet-watch/boot-map-receipt/v1"
CENSUS_SCHEMA_VERSION = "fleet-census/v1"

DEFAULT_RECEIPT_PATH = Path.home() / ".governance" / "receipts" / "fleet-census" / "latest.json"
DEFAULT_OUT_DIR = Path.home() / ".governance" / "graph" / "boot-map"

STATUS_VALUES = ("running", "idle-loaded", "dead", "failing", "stale", "orphan", "unknown")
VERDICT_VALUES = ("keep", "investigate", "close", "remove")
#: Worst-wins ordering used to derive a verdict for nodes the census does not
#: rate directly (targets, ports, repos). Unknown verdicts rate as "investigate".
VERDICT_SEVERITY = {"keep": 0, "investigate": 1, "close": 2, "remove": 3}
DEFAULT_SEVERITY = 1

NODE_KINDS = ("host", "domain", "job", "process", "listener", "unit", "target", "port", "repo", "area")
RELATIONS = ("contains", "launches", "references", "listens_on", "lives_in", "managed_by")

#: Absolute-path first segments we accept. Anything else that pattern-matches a
#: path (``/api/ps``, ``2/12``) is dropped and counted, never silently graphed.
_PATH_ROOTS = frozenset(
    {
        "Users", "Applications", "Library", "System", "usr", "opt", "private",
        "tmp", "var", "bin", "sbin", "etc", "dev", "Volumes", "cores", "Network",
    }
)
_SYSTEM_AREA_PREFIXES = (
    "/Applications", "/Library", "/System", "/usr", "/opt", "/private",
    "/tmp", "/var", "/bin", "/sbin", "/etc", "/dev", "/Volumes", "/cores", "/Network",
)
#: System roots where the second segment is the meaningful unit
#: (``/opt/homebrew``, ``/Applications/Foo.app``), not the root itself.
_TWO_SEGMENT_AREAS = frozenset({"/Applications", "/opt", "/private", "/Volumes"})
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
_ALL_INTERFACE_HOSTS = frozenset({"0.0.0.0", "*", "::"})

# The `~/` alternative must be first and must consume its slash, or `~/Library/x`
# matches from the inner `/` and is mis-rooted into the SYSTEM `/Library`.
_PATH_RE = re.compile(r"(?:~/|/)[A-Za-z0-9._+\-][A-Za-z0-9._+\-/@]*")
_ENDPOINT_RE = re.compile(
    r"(?<![\w.])(\*|0\.0\.0\.0|127\.0\.0\.1|localhost|\[::1\]|::1|\d{1,3}(?:\.\d{1,3}){3}):(\d{1,5})(?![\w.])"
)
_USER_HOME_RE = re.compile(r"^/Users/[^/]+/")
_LAUNCHD_LABEL_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_\-]*(?:\.[A-Za-z0-9_\-]+){2,}$")
_PID_RE = re.compile(r"(?<![\w])[Pp][Ii][Dd]\s*[= ]\s*(\d{1,7})(?![\w])")

#: Suffixes that mean "this path is data the job touches", not "this path is the
#: thing the job runs".
_REFERENCE_SUFFIXES = frozenset(
    {".log", ".err", ".out", ".txt", ".json", ".plist", ".db", ".sqlite", ".sqlite3",
     ".md", ".yaml", ".yml", ".toml", ".cfg", ".ini", ".csv", ".pid", ".lock", ".sock"}
)
_EXECUTABLE_SUFFIXES = frozenset(
    {"", ".py", ".sh", ".bash", ".zsh", ".js", ".mjs", ".rb", ".pl", ".ts", ".bin", ".app"}
)

#: Caps keep an adversarial / verbose evidence string from exploding the graph.
MAX_EVIDENCE_PATHS_PER_ITEM = 8
MAX_EVIDENCE_ENDPOINTS_PER_ITEM = 6
MAX_EVIDENCE_CHARS = 1400


class BootMapError(RuntimeError):
    """Fail-closed boot-map error. Never raised for a merely sparse census."""


# --------------------------------------------------------------------------- #
# parsing / validation
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class CensusItem:
    """One census row, normalized. Nothing is dropped; bad values are flagged."""

    domain: str
    label: str
    path: str
    resource: str
    status: str
    verdict: str
    reason: str
    evidence: str
    close_command: str
    status_valid: bool
    verdict_valid: bool

    @property
    def severity(self) -> int:
        return VERDICT_SEVERITY.get(self.verdict, DEFAULT_SEVERITY)


@dataclass(frozen=True)
class DomainRecord:
    name: str
    summary: str
    totals: dict[str, Any]
    items: tuple[CensusItem, ...]


@dataclass
class ParsedCensus:
    schema_version: str
    generated_at: str
    host: str
    machine: dict[str, Any]
    totals: dict[str, Any]
    shape: str
    domains: tuple[DomainRecord, ...] = ()
    warnings: list[dict[str, str]] = field(default_factory=list)

    @property
    def items(self) -> list[CensusItem]:
        return [item for domain in self.domains for item in domain.items]


def _as_text(value: Any, *, limit: int | None = None) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(value, sort_keys=True, default=str)
    text = text.strip()
    if limit is not None and len(text) > limit:
        text = text[: limit - 1].rstrip() + "…"
    return text


def parse_census(data: Any, *, source: str = "<memory>") -> ParsedCensus:
    """Normalize + validate a census receipt. Raises ``BootMapError`` fail-closed."""
    if isinstance(data, list):
        raw_domains: Any = data
        header: dict[str, Any] = {}
        shape = "domains-array"
    elif isinstance(data, dict):
        header = data
        raw_domains = data.get("domains")
        shape = "document"
        if not isinstance(raw_domains, list):
            raise BootMapError(
                f"{source}: receipt object has no 'domains' array"
                f" (got {type(raw_domains).__name__})"
            )
    else:
        raise BootMapError(
            f"{source}: receipt root must be an object or a domains array,"
            f" got {type(data).__name__}"
        )

    if not raw_domains:
        raise BootMapError(
            f"{source}: degenerate census — zero domains."
            " An empty map is a refusal, not a render."
        )

    warnings: list[dict[str, str]] = []
    domains: list[DomainRecord] = []
    seen_domain_names: set[str] = set()

    for index, raw_domain in enumerate(raw_domains):
        if not isinstance(raw_domain, dict):
            raise BootMapError(
                f"{source}: domains[{index}] must be an object,"
                f" got {type(raw_domain).__name__}"
            )
        name = _as_text(raw_domain.get("domain"))
        if not name:
            raise BootMapError(f"{source}: domains[{index}] has no 'domain' name")
        if name in seen_domain_names:
            name = f"{name} #{index}"
            warnings.append(
                {"code": "duplicate_domain_name", "detail": f"domains[{index}] renamed to {name!r}"}
            )
        seen_domain_names.add(name)

        raw_items = raw_domain.get("items")
        if raw_items is None:
            raw_items = []
            warnings.append({"code": "domain_without_items", "detail": f"{name}: no 'items' key"})
        if not isinstance(raw_items, list):
            raise BootMapError(f"{source}: {name}: 'items' must be an array, got {type(raw_items).__name__}")

        items: list[CensusItem] = []
        for item_index, raw_item in enumerate(raw_items):
            if not isinstance(raw_item, dict):
                raise BootMapError(
                    f"{source}: {name}: items[{item_index}] must be an object, got {type(raw_item).__name__}"
                )
            label = _as_text(raw_item.get("label"))
            if not label:
                raise BootMapError(f"{source}: {name}: items[{item_index}] has no 'label'")

            status = _as_text(raw_item.get("status"))
            status_valid = status in STATUS_VALUES
            if not status:
                status, status_valid = "unknown", True
                warnings.append({"code": "missing_status", "detail": f"{name}: {label}"})
            elif not status_valid:
                warnings.append({"code": "off_enum_status", "detail": f"{name}: {label}: {status!r}"})

            verdict = _as_text(raw_item.get("verdict"))
            verdict_valid = verdict in VERDICT_VALUES
            if not verdict:
                # Fail-closed direction: an unrated item needs a human, not a pass.
                verdict, verdict_valid = "investigate", True
                warnings.append({"code": "missing_verdict", "detail": f"{name}: {label}"})
            elif not verdict_valid:
                warnings.append({"code": "off_enum_verdict", "detail": f"{name}: {label}: {verdict!r}"})

            items.append(
                CensusItem(
                    domain=name,
                    label=label,
                    path=_as_text(raw_item.get("path")),
                    resource=_as_text(raw_item.get("resource")),
                    status=status,
                    verdict=verdict,
                    reason=_as_text(raw_item.get("reason"), limit=600),
                    evidence=_as_text(raw_item.get("evidence"), limit=MAX_EVIDENCE_CHARS),
                    close_command=_as_text(raw_item.get("close_command"), limit=400),
                    status_valid=status_valid,
                    verdict_valid=verdict_valid,
                )
            )

        domains.append(
            DomainRecord(
                name=name,
                summary=_as_text(raw_domain.get("summary"), limit=900),
                totals=raw_domain.get("totals") if isinstance(raw_domain.get("totals"), dict) else {},
                items=tuple(items),
            )
        )

    total_items = sum(len(domain.items) for domain in domains)
    if total_items == 0:
        raise BootMapError(
            f"{source}: degenerate census — {len(domains)} domain(s), 0 items."
            " Refusing to render an empty map."
        )

    machine = header.get("machine") if isinstance(header.get("machine"), dict) else {}
    totals = header.get("totals") if isinstance(header.get("totals"), dict) else {}
    return ParsedCensus(
        schema_version=_as_text(header.get("schema_version")) or CENSUS_SCHEMA_VERSION,
        generated_at=_as_text(header.get("generated_at")),
        host=_as_text(header.get("host")),
        machine=machine,
        totals=totals,
        shape=shape,
        domains=tuple(domains),
        warnings=warnings,
    )


def load_census(path: Path) -> ParsedCensus:
    """Read + validate a census receipt from disk. Fail-closed on every error."""
    try:
        raw = path.read_bytes()
    except FileNotFoundError as exc:
        raise BootMapError(f"NO CENSUS RECEIPT at {path}") from exc
    except OSError as exc:
        raise BootMapError(f"cannot read census receipt {path}: {exc}") from exc
    if not raw.strip():
        raise BootMapError(f"{path}: census receipt is empty")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise BootMapError(f"{path}: census receipt is not valid JSON: {exc}") from exc
    return parse_census(data, source=str(path))


# --------------------------------------------------------------------------- #
# extraction helpers (deterministic, documented, capped)
# --------------------------------------------------------------------------- #


def node_id(kind: str, key: str) -> str:
    return f"{kind}:" + hashlib.sha256(f"{kind}|{key}".encode("utf-8")).hexdigest()[:16]


def normalize_path(raw: str) -> str | None:
    """Canonicalize one path token, or return ``None`` if it is not a real path."""
    token = raw.strip().strip("'\"`,;")
    while token and token[-1] in ".,;:)]}>'\"":
        token = token[:-1]
    if len(token) > 1 and token.endswith("/"):
        token = token.rstrip("/")
    if not token or len(token) < 4:
        return None
    token = _USER_HOME_RE.sub("~/", token)
    if token.startswith("~/"):
        rest = token[2:]
        return "~/" + rest if rest else None
    if not token.startswith("/"):
        return None
    parts = [p for p in token.split("/") if p]
    if len(parts) < 2 or parts[0] not in _PATH_ROOTS:
        return None
    return "/" + "/".join(parts)


def extract_paths(text: str) -> list[str]:
    """All distinct real paths in ``text``, in first-seen order."""
    found: list[str] = []
    seen: set[str] = set()
    for match in _PATH_RE.finditer(text or ""):
        norm = normalize_path(match.group(0))
        if norm and norm not in seen:
            seen.add(norm)
            found.append(norm)
    return found


def extract_endpoints(text: str) -> list[tuple[str, int]]:
    """All distinct ``(host, port)`` endpoints in ``text``, in first-seen order."""
    found: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for match in _ENDPOINT_RE.finditer(text or ""):
        host = match.group(1)
        if host == "[::1]":
            host = "::1"
        try:
            port = int(match.group(2))
        except ValueError:  # pragma: no cover - regex guarantees digits
            continue
        if not 1 <= port <= 65535:
            continue
        key = (host, port)
        if key not in seen:
            seen.add(key)
            found.append(key)
    return found


def exposure_for(host: str) -> str:
    if host in _LOOPBACK_HOSTS:
        return "loopback"
    if host in _ALL_INTERFACE_HOSTS:
        return "all-interfaces"
    return "bound"


def container_for(path: str) -> tuple[str, str] | None:
    """Map a path to its owning ``(kind, root)`` — ``repo`` for user trees,
    ``area`` for system trees. ``None`` when the path has no meaningful root."""
    parts = [p for p in path.split("/") if p and p != "~"]
    if path.startswith("~/"):
        if not parts:
            return None
        if parts[0] == "Workspace" and len(parts) >= 2:
            if parts[1] in ("active", "worktrees") and len(parts) >= 3:
                return ("repo", f"~/Workspace/{parts[1]}/{parts[2]}")
            return ("repo", f"~/Workspace/{parts[1]}")
        return ("repo", f"~/{parts[0]}")
    for prefix in _SYSTEM_AREA_PREFIXES:
        if path == prefix or path.startswith(prefix + "/"):
            if prefix in _TWO_SEGMENT_AREAS and len(parts) >= 2:
                return ("area", f"{prefix}/{parts[1]}")
            return ("area", prefix)
    return None


def target_relation(path: str) -> str:
    """``launches`` if the path is a runnable thing, else ``references``."""
    name = path.rsplit("/", 1)[-1]
    suffix = ("." + name.rsplit(".", 1)[-1].lower()) if "." in name else ""
    if ".app/" in path or path.endswith(".app"):
        return "launches"
    if suffix in _REFERENCE_SUFFIXES:
        return "references"
    if suffix in _EXECUTABLE_SUFFIXES:
        return "launches"
    if "/bin/" in path or "/libexec/" in path or "/sbin/" in path or "/MacOS/" in path:
        return "launches"
    return "references"


def classify_item(item: CensusItem) -> str:
    """Node kind for a census row. Ordered, per-item, never per-domain-only."""
    domain = item.domain.lower()
    if item.path.endswith(".plist") or _LAUNCHD_LABEL_RE.match(item.label):
        return "job"
    blob = f"{item.label} {item.resource}"
    if "listener" in domain or "port" in domain:
        return "listener"
    if extract_endpoints(blob) and ("network" in domain or "listener" in domain):
        return "listener"
    if _PID_RE.search(blob) or "process" in domain:
        return "process"
    if "listener" in item.label.lower():
        return "listener"
    return "unit"


# --------------------------------------------------------------------------- #
# graph build
# --------------------------------------------------------------------------- #


class _Graph:
    def __init__(self) -> None:
        self.nodes: dict[str, dict[str, Any]] = {}
        self.edges: dict[tuple[str, str, str], dict[str, Any]] = {}

    def node(self, kind: str, key: str, label: str, **attrs: Any) -> str:
        nid = node_id(kind, key)
        node = self.nodes.get(nid)
        if node is None:
            node = {"id": nid, "kind": kind, "key": key, "label": label}
            node.update({k: v for k, v in attrs.items() if v not in (None, "", [], {})})
            self.nodes[nid] = node
        else:
            for k, v in attrs.items():
                if v in (None, "", [], {}):
                    continue
                node.setdefault(k, v)
        return nid

    def edge(self, source: str, target: str, relation: str, confidence: str, source_ref: str) -> None:
        if source == target:
            return
        key = (source, target, relation)
        existing = self.edges.get(key)
        if existing is None:
            self.edges[key] = {
                "source": source,
                "target": target,
                "relation": relation,
                "confidence": confidence,
                "confidence_score": 1.0 if confidence == "EXTRACTED" else 0.6,
                "source_ref": source_ref,
                "weight": 1.0,
            }
        else:
            existing["weight"] += 1.0


def build_graph(census: ParsedCensus, *, source_path: str = "", source_sha256: str = "") -> dict[str, Any]:
    """Transform a validated census into a ``{"nodes", "links"}`` graph document."""
    graph = _Graph()
    stats: dict[str, int] = {
        "items": 0,
        "evidence_paths_capped": 0,
        "evidence_endpoints_capped": 0,
        "unrooted_targets": 0,
        "managed_by_matches": 0,
    }

    host_label = census.host or "this machine"
    host_id = graph.node(
        "host",
        f"host|{host_label}",
        host_label,
        verdict="keep",
        verdict_source="root",
        status="running",
        detail=json.dumps(census.machine, sort_keys=True) if census.machine else "",
        synthetic=not bool(census.host),
    )

    item_nodes: list[tuple[CensusItem, str]] = []

    for domain in census.domains:
        domain_id = graph.node(
            "domain",
            f"domain|{domain.name}",
            domain.name,
            summary=domain.summary,
            totals=domain.totals,
            item_count=len(domain.items),
            verdict_source="derived",
        )
        graph.edge(host_id, domain_id, "contains", "EXTRACTED", host_label)

        for item in domain.items:
            stats["items"] += 1
            kind = classify_item(item)
            key = f"{kind}|{domain.name}|{item.label}|{item.path}"
            item_id = graph.node(
                kind,
                key,
                item.label,
                domain=domain.name,
                path=item.path,
                resource=item.resource,
                status=item.status,
                verdict=item.verdict,
                verdict_source="census",
                status_valid=item.status_valid,
                verdict_valid=item.verdict_valid,
                reason=item.reason,
                evidence=item.evidence,
                close_command=item.close_command,
            )
            if not item.status_valid:
                graph.nodes[item_id]["status_valid"] = False
            if not item.verdict_valid:
                graph.nodes[item_id]["verdict_valid"] = False
            graph.edge(domain_id, item_id, "contains", "EXTRACTED", item.label)
            item_nodes.append((item, item_id))

            declared = f"{item.path}\n{item.resource}"
            declared_paths = extract_paths(declared)
            evidence_paths = [p for p in extract_paths(item.evidence) if p not in declared_paths]
            if len(evidence_paths) > MAX_EVIDENCE_PATHS_PER_ITEM:
                stats["evidence_paths_capped"] += 1
                evidence_paths = evidence_paths[:MAX_EVIDENCE_PATHS_PER_ITEM]

            for path in declared_paths:
                _attach_target(
                    graph, item_id, item, path, "EXTRACTED", stats,
                    is_own_path=(path == normalize_path(item.path)),
                )
            for path in evidence_paths:
                _attach_target(graph, item_id, item, path, "INFERRED", stats, is_own_path=False)

            declared_endpoints = extract_endpoints(f"{item.label}\n{item.resource}")
            evidence_endpoints = [e for e in extract_endpoints(item.evidence) if e not in declared_endpoints]
            if len(evidence_endpoints) > MAX_EVIDENCE_ENDPOINTS_PER_ITEM:
                stats["evidence_endpoints_capped"] += 1
                evidence_endpoints = evidence_endpoints[:MAX_EVIDENCE_ENDPOINTS_PER_ITEM]
            for host, port in declared_endpoints:
                _attach_port(graph, item_id, item, host, port, "EXTRACTED")
            for host, port in evidence_endpoints:
                _attach_port(graph, item_id, item, host, port, "INFERRED")

    _link_processes_to_jobs(graph, item_nodes, stats)
    _derive_verdicts(graph)

    nodes = sorted(graph.nodes.values(), key=lambda n: (n["kind"], n["label"], n["id"]))
    links = sorted(graph.edges.values(), key=lambda e: (e["relation"], e["source"], e["target"]))

    counts_by_kind: dict[str, int] = {kind: 0 for kind in NODE_KINDS}
    counts_by_verdict: dict[str, int] = {}
    counts_by_status: dict[str, int] = {}
    for node in nodes:
        counts_by_kind[node["kind"]] = counts_by_kind.get(node["kind"], 0) + 1
        verdict = node.get("verdict", "unknown")
        counts_by_verdict[verdict] = counts_by_verdict.get(verdict, 0) + 1
        if node["kind"] in ("job", "process", "listener", "unit"):
            status = node.get("status", "unknown")
            counts_by_status[status] = counts_by_status.get(status, 0) + 1
    counts_by_relation: dict[str, int] = {}
    for link in links:
        counts_by_relation[link["relation"]] = counts_by_relation.get(link["relation"], 0) + 1

    return {
        "schema_version": SCHEMA_VERSION,
        "census": {
            "schema_version": census.schema_version,
            "generated_at": census.generated_at,
            "host": census.host,
            "machine": census.machine,
            "totals": census.totals,
            "shape": census.shape,
            "source_path": source_path,
            "source_sha256": source_sha256,
            "domain_count": len(census.domains),
            "item_count": stats["items"],
        },
        "nodes": nodes,
        "links": links,
        "stats": {
            "node_count": len(nodes),
            "edge_count": len(links),
            "counts_by_kind": {k: v for k, v in sorted(counts_by_kind.items()) if v},
            "counts_by_verdict": dict(sorted(counts_by_verdict.items())),
            "counts_by_status": dict(sorted(counts_by_status.items())),
            "counts_by_relation": dict(sorted(counts_by_relation.items())),
            "extraction": dict(sorted(stats.items())),
        },
        "warnings": census.warnings,
    }


def _attach_target(
    graph: _Graph,
    item_id: str,
    item: CensusItem,
    path: str,
    confidence: str,
    stats: dict[str, int],
    *,
    is_own_path: bool,
) -> None:
    """Wire item -> target -> repo/area. The item's own plist is a manifest, not a target."""
    container = container_for(path)
    if is_own_path and path.endswith(".plist"):
        # The plist IS the item's manifest; graph the tree it lives in, not itself.
        if container:
            graph.edge(item_id, _container_node(graph, container), "lives_in", confidence, item.label)
        else:
            stats["unrooted_targets"] += 1
        return

    relation = target_relation(path)
    target_id = graph.node(
        "target",
        f"target|{path}",
        path.rsplit("/", 1)[-1] or path,
        path=path,
        verdict_source="derived",
    )
    graph.edge(item_id, target_id, relation, confidence, item.label)
    if container:
        graph.edge(target_id, _container_node(graph, container), "lives_in", confidence, item.label)
    else:
        stats["unrooted_targets"] += 1


def _container_node(graph: _Graph, container: tuple[str, str]) -> str:
    """Intern the repo/area node a path belongs to."""
    kind, root = container
    return graph.node(kind, f"{kind}|{root}", root, verdict_source="derived")


def _attach_port(
    graph: _Graph, item_id: str, item: CensusItem, host: str, port: int, confidence: str
) -> None:
    key = f"{host}:{port}"
    port_id = graph.node(
        "port",
        f"port|{key}",
        key,
        host=host,
        port=port,
        exposure=exposure_for(host),
        verdict_source="derived",
    )
    graph.edge(item_id, port_id, "listens_on", confidence, item.label)


def _link_processes_to_jobs(
    graph: _Graph, item_nodes: list[tuple[CensusItem, str]], stats: dict[str, int]
) -> None:
    """Correlate live processes / listeners back to the launchd job that owns them.

    Deterministic substring match on distinctive launchd labels (dotted, >= 6
    chars) inside the item's own text. No fuzzy matching, no guessing.
    """
    job_labels: list[tuple[str, str]] = []
    for item, nid in item_nodes:
        if graph.nodes[nid]["kind"] != "job":
            continue
        label = item.label
        if len(label) >= 6 and "." in label and _LAUNCHD_LABEL_RE.match(label):
            job_labels.append((label, nid))
    job_labels.sort(key=lambda pair: (-len(pair[0]), pair[0]))

    for item, nid in item_nodes:
        if graph.nodes[nid]["kind"] == "job":
            continue
        blob = f"{item.label}\n{item.path}\n{item.resource}\n{item.evidence}"
        for label, job_id in job_labels:
            if label in blob:
                graph.edge(nid, job_id, "managed_by", "INFERRED", item.label)
                stats["managed_by_matches"] += 1


#: How an un-rated node inherits a verdict. Ordered so each rule only reads
#: severities the earlier rules already settled — no cycles, no fixpoint loop,
#: and no guilt-by-co-location (a target never inherits from a sibling target
#: that merely shares a repo).
#:
#: (kind, relations, side) where side="source" means "worst of the nodes that
#: point at me", side="target" means "worst of the nodes I point at".
_DERIVE_RULES: tuple[tuple[str, frozenset[str], str], ...] = (
    ("target", frozenset({"launches", "references"}), "source"),
    ("port", frozenset({"listens_on"}), "source"),
    ("repo", frozenset({"lives_in"}), "source"),
    ("area", frozenset({"lives_in"}), "source"),
    ("domain", frozenset({"contains"}), "target"),
)


def _derive_verdicts(graph: _Graph) -> None:
    """Give every un-rated node the worst verdict of the census rows behind it.

    A repo whose only failing job is red reads red — the operator sees where the
    rot is without opening a node. Directional and bounded: verdicts flow the way
    boot flows (job -> target -> repo) and aggregate upward for containers
    (domain <- its items). The host stays neutral; it is the machine, not a
    finding. Provenance stays explicit via ``verdict_source: "derived"``.
    """
    severity: dict[str, int] = {}
    for node in graph.nodes.values():
        if node.get("verdict_source") == "census":
            severity[node["id"]] = VERDICT_SEVERITY.get(node.get("verdict", ""), DEFAULT_SEVERITY)

    edges = list(graph.edges.values())
    for kind, relations, side in _DERIVE_RULES:
        contributions: dict[str, int] = {}
        for edge in edges:
            if edge["relation"] not in relations:
                continue
            near, far = (
                (edge["source"], edge["target"])
                if side == "source"
                else (edge["target"], edge["source"])
            )
            if graph.nodes[far]["kind"] != kind:
                continue
            value = severity.get(near)
            if value is None:
                continue
            contributions[far] = max(contributions.get(far, -1), value)
        for nid, value in contributions.items():
            severity[nid] = max(severity.get(nid, -1), value)

    severity_to_verdict = {v: k for k, v in VERDICT_SEVERITY.items()}
    for nid, node in graph.nodes.items():
        if node.get("verdict_source") == "census" or node["kind"] == "host":
            continue
        value = severity.get(nid)
        node["verdict"] = severity_to_verdict[value] if value is not None else "unknown"
        node["verdict_source"] = "derived"


# --------------------------------------------------------------------------- #
# build entry point
# --------------------------------------------------------------------------- #


def _atomic_write(path: Path, text: str) -> str:
    """Write text atomically; return its sha256."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def resolve_receipt_path(receipt: Path | str | None) -> Path:
    """Resolve the census receipt, fail-closed when absent.

    Explicit path wins. Default is ``latest.json``. There is no bundled-fixture
    fallback: rendering a stand-in as if it were this machine would be a
    fake-green, which is worse than no map.
    """
    if receipt is not None:
        path = Path(receipt).expanduser()
        if not path.exists():
            raise BootMapError(f"NO CENSUS RECEIPT at {path} (--receipt was explicit)")
        return path
    path = DEFAULT_RECEIPT_PATH
    if not path.exists():
        raise BootMapError(
            f"NO CENSUS RECEIPT at {path}. Run the census producer first, or pass "
            f"--receipt <path>. Refusing to render a map of nothing."
        )
    return path


def build(
    receipt: Path | str | None = None,
    out_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Census receipt -> ``graph.json`` + ``index.html`` + ``receipt.json``.

    Returns the build receipt dict. Raises ``BootMapError`` on any refusal.
    """
    receipt_path = resolve_receipt_path(receipt)
    out = Path(out_dir).expanduser() if out_dir is not None else DEFAULT_OUT_DIR

    source_sha = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    census = load_census(receipt_path)
    graph = build_graph(census, source_path=str(receipt_path), source_sha256=source_sha)

    if not graph["nodes"]:  # pragma: no cover - parse_census refuses first
        raise BootMapError("degenerate graph — 0 nodes. Refusing to write an empty render.")

    graph_text = json.dumps(graph, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    html_text = render_html(graph)

    graph_path = out / "graph.json"
    html_path = out / "index.html"
    graph_sha = _atomic_write(graph_path, graph_text)
    html_sha = _atomic_write(html_path, html_text)

    build_receipt = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "tool": "fleet boot-map",
        "source_receipt": {
            "path": str(receipt_path),
            "sha256": source_sha,
            "schema_version": census.schema_version,
            "shape": census.shape,
            "generated_at": census.generated_at,
            "host": census.host,
            "domain_count": len(census.domains),
            "item_count": len(census.items),
        },
        "outputs": [
            {"path": str(graph_path), "sha256": graph_sha, "bytes": len(graph_text.encode("utf-8"))},
            {"path": str(html_path), "sha256": html_sha, "bytes": len(html_text.encode("utf-8"))},
        ],
        "stats": graph["stats"],
        "warnings": graph["warnings"],
        "decision": "PASS",
    }
    _atomic_write(out / "receipt.json", json.dumps(build_receipt, indent=2, sort_keys=True) + "\n")
    return build_receipt


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
