"""Read-only GitHub fleet sitrep.

This module answers one question: *which repositories does GitHub currently
report for this `gh` login, and what default-branch object id did GitHub
supply?*

Hard rules:

* **No clone.** argv never includes ``clone`` / ``repo clone``. Nothing is
  fetched into a working tree.
* **No tokens in this code.** We do not read ``GITHUB_TOKEN`` / ``GH_TOKEN``,
  do not run ``gh auth token``, and refuse ``-t`` / ``--token``. Authentication
  stays inside the operator's ``gh`` credential store.
* **No invented SHA.** A ``sha`` field is a 40- or 64-char hex object id taken
  from GitHub GraphQL ``target.oid``. Anything else becomes ``sha: null`` plus
  ``sha_absent_reason``. Short SHAs, placeholders, and local HEAD values are
  not used.

Network happens only when the operator runs ``fleet sitrep``, and only via the
``gh`` binary. Guard, discover, census, and health stay zero-egress.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "fleet-github-sitrep/v1"
RECEIPT_DIR = Path.home() / ".governance" / "receipts" / "fleet-github-sitrep"
LATEST_NAME = "latest.json"
DEFAULT_LIMIT = 30
MAX_LIMIT = 100
GH_TIMEOUT = 45.0
OWNER_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$")
SHA_RE = re.compile(r"^[0-9a-f]{40}$|^[0-9a-f]{64}$")
TOKEN_FLAG_RE = re.compile(r"^--token(?:=|$)|^--jwt(?:=|$)|^-t$")
SECRET_RE = re.compile(
    r"(gho_|ghp_|ghu_|ghs_|ghr_|github_pat_)[A-Za-z0-9_]+",
    re.IGNORECASE,
)
# Read query only. There is no `mutation` here; tests assert that.
VIEWER_QUERY = """
query($first: Int!) {
  viewer {
    login
    repositories(first: $first, ownerAffiliations: [OWNER], orderBy: {field: PUSHED_AT, direction: DESC}) {
      pageInfo { hasNextPage }
      nodes {
        nameWithOwner
        name
        url
        isPrivate
        isArchived
        isFork
        pushedAt
        visibility
        defaultBranchRef {
          name
          target { oid }
        }
      }
    }
  }
}
""".strip()

OWNER_QUERY = """
query($login: String!, $first: Int!) {
  repositoryOwner(login: $login) {
    login
    repositories(first: $first, orderBy: {field: PUSHED_AT, direction: DESC}) {
      pageInfo { hasNextPage }
      nodes {
        nameWithOwner
        name
        url
        isPrivate
        isArchived
        isFork
        pushedAt
        visibility
        defaultBranchRef {
          name
          target { oid }
        }
      }
    }
  }
}
""".strip()

_REQUIRED_TOP = (
    "schema_version",
    "generated_at",
    "source",
    "query_kind",
    "gh_login",
    "owner",
    "limit",
    "truncated",
    "clone",
    "tokens_used",
    "repo_count",
    "repos",
)
_REQUIRED_REPO = (
    "name_with_owner",
    "name",
    "url",
    "visibility",
    "is_private",
    "is_archived",
    "is_fork",
    "pushed_at",
    "default_branch",
    "sha",
    "sha_absent_reason",
)
_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class SitrepRefusal(Exception):
    """Raised instead of emitting a sitrep that would not testify truthfully."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        Exception.__init__(self, "; ".join(errors))


@dataclass(frozen=True)
class SitrepResult:
    payload: dict[str, Any]
    dated_path: Path | None
    latest_path: Path | None


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def receipt_filename(generated_at: str) -> str:
    compact = generated_at.replace("-", "").replace(":", "")
    return f"sitrep-{compact}.json"


def redact(text: str) -> str:
    return SECRET_RE.sub(lambda m: m.group(1) + "[redacted]", text)


def accept_sha(value: Any) -> str | None:
    """Return a full GitHub object id, or None. Never invent or truncate."""
    if not isinstance(value, str):
        return None
    oid = value.strip().lower()
    if SHA_RE.fullmatch(oid):
        return oid
    return None


def validate(payload: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(payload, dict):
        return [f"payload is {type(payload).__name__}, expected object"]

    for key in _REQUIRED_TOP:
        if key not in payload:
            errors.append(f"missing required key: {key}")
    if errors:
        return errors

    if payload["schema_version"] != SCHEMA_VERSION:
        errors.append(
            f"schema_version is {payload['schema_version']!r}, expected {SCHEMA_VERSION!r}"
        )
    if not isinstance(payload["generated_at"], str) or not _TIMESTAMP_RE.match(
        payload["generated_at"]
    ):
        errors.append("generated_at must be UTC 'YYYY-MM-DDTHH:MM:SSZ'")
    if payload.get("source") != "gh":
        errors.append("source must be 'gh'")
    if payload.get("query_kind") not in {"viewer", "owner"}:
        errors.append("query_kind must be 'viewer' or 'owner'")
    if not isinstance(payload.get("gh_login"), str) or not payload["gh_login"].strip():
        errors.append("gh_login must be a non-empty string")
    owner = payload.get("owner")
    if owner is not None and (not isinstance(owner, str) or not owner.strip()):
        errors.append("owner must be a non-empty string or null")
    if not isinstance(payload.get("limit"), int) or not (1 <= payload["limit"] <= MAX_LIMIT):
        errors.append(f"limit must be an int 1..{MAX_LIMIT}")
    if not isinstance(payload.get("truncated"), bool):
        errors.append("truncated must be a boolean")
    if payload.get("clone") is not False:
        errors.append("clone must be false")
    if payload.get("tokens_used") is not False:
        errors.append("tokens_used must be false")

    repos = payload.get("repos")
    if not isinstance(repos, list):
        errors.append("repos must be an array")
        repos = []
    if payload.get("repo_count") != len(repos):
        errors.append("repo_count must equal len(repos)")

    for index, repo in enumerate(repos):
        where = f"repos[{index}]"
        if not isinstance(repo, dict):
            errors.append(f"{where} must be an object")
            continue
        for key in _REQUIRED_REPO:
            if key not in repo:
                errors.append(f"{where} missing {key}")
        sha = repo.get("sha")
        reason = repo.get("sha_absent_reason")
        if sha is None:
            if not isinstance(reason, str) or not reason.strip():
                errors.append(f"{where}.sha_absent_reason must explain a null sha")
        else:
            if accept_sha(sha) != sha:
                errors.append(f"{where}.sha is not a full hex object id")
            if reason is not None:
                errors.append(f"{where}.sha_absent_reason must be null when sha is present")
            # Defense: never allow a short SHA through a receipt.
            if isinstance(sha, str) and 0 < len(sha) < 40:
                errors.append(f"{where}.sha is abbreviated; sitrep refuses short SHAs")
    return errors


def build_repo(node: Any) -> dict[str, Any]:
    if not isinstance(node, dict):
        return {
            "name_with_owner": "",
            "name": "",
            "url": "",
            "visibility": "",
            "is_private": False,
            "is_archived": False,
            "is_fork": False,
            "pushed_at": None,
            "default_branch": None,
            "sha": None,
            "sha_absent_reason": "malformed_repository_node",
        }

    ref = node.get("defaultBranchRef")
    branch = None
    if isinstance(ref, dict):
        branch_val = ref.get("name")
        if isinstance(branch_val, str) and branch_val.strip():
            branch = branch_val
    sha, sha_reason = _sha_from_ref(ref)

    visibility = node.get("visibility")
    if not isinstance(visibility, str) or not visibility.strip():
        visibility = "PRIVATE" if node.get("isPrivate") is True else "UNKNOWN"

    nwo = node.get("nameWithOwner")
    name = node.get("name")
    url = node.get("url")
    pushed = node.get("pushedAt")

    return {
        "name_with_owner": nwo if isinstance(nwo, str) else "",
        "name": name if isinstance(name, str) else "",
        "url": url if isinstance(url, str) else "",
        "visibility": visibility,
        "is_private": bool(node.get("isPrivate") is True),
        "is_archived": bool(node.get("isArchived") is True),
        "is_fork": bool(node.get("isFork") is True),
        "pushed_at": pushed if isinstance(pushed, str) else None,
        "default_branch": branch,
        "sha": sha,
        "sha_absent_reason": sha_reason,
    }


def _sha_from_ref(ref: Any) -> tuple[str | None, str | None]:
    if ref is None:
        return None, "empty_repository"
    if not isinstance(ref, dict):
        return None, "sha_unavailable"
    target = ref.get("target")
    oid = target.get("oid") if isinstance(target, dict) else None
    sha = accept_sha(oid)
    if sha is not None:
        return sha, None
    if oid is None:
        return None, "sha_unavailable"
    return None, "sha_rejected_not_full_oid"


def payload_from_graphql(
    body: dict[str, Any],
    *,
    owner: str | None,
    limit: int,
    generated_at: str,
) -> dict[str, Any]:
    if body.get("errors"):
        messages = []
        for err in body["errors"]:
            if isinstance(err, dict) and isinstance(err.get("message"), str):
                messages.append(redact(err["message"]))
            else:
                messages.append("github graphql error")
        raise SitrepRefusal(messages or ["github graphql returned errors"])
    data = body.get("data")
    if not isinstance(data, dict):
        raise SitrepRefusal(["github graphql returned no data"])

    if owner:
        block = data.get("repositoryOwner")
        if block is None:
            raise SitrepRefusal(
                [f"GitHub returned no owner named {owner!r}; not an empty fleet"]
            )
        query_kind = "owner"
    else:
        block = data.get("viewer")
        query_kind = "viewer"
    if not isinstance(block, dict):
        raise SitrepRefusal(["github graphql owner/viewer block is missing"])

    login = block.get("login")
    if not isinstance(login, str) or not login.strip():
        raise SitrepRefusal(["github graphql returned no login"])

    repos_block = block.get("repositories")
    if not isinstance(repos_block, dict):
        raise SitrepRefusal(["github graphql returned no repositories block"])
    nodes = repos_block.get("nodes")
    if nodes is None:
        nodes = []
    if not isinstance(nodes, list):
        raise SitrepRefusal(["github graphql repositories.nodes is not an array"])

    page_info = repos_block.get("pageInfo") if isinstance(repos_block.get("pageInfo"), dict) else {}
    truncated = page_info.get("hasNextPage") is True

    repos = [build_repo(node) for node in nodes]
    # Drop nodes GitHub sent with no identity — a blank name is not a repo.
    repos = [r for r in repos if r["name_with_owner"].strip()]

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "source": "gh",
        "query_kind": query_kind,
        "gh_login": login,
        "owner": owner,
        "limit": limit,
        "truncated": truncated,
        "clone": False,
        "tokens_used": False,
        "repo_count": len(repos),
        "repos": repos,
    }


def _forbidden_argv(args: list[str]) -> str | None:
    for a in args:
        if TOKEN_FLAG_RE.match(a):
            return f"token flag forbidden: {a}"
    if args[:2] != ["api", "graphql"]:
        return "sitrep may only invoke `gh api graphql`"
    blob = " ".join(args).lower()
    if "mutation" in blob:
        return "graphql mutation is forbidden"
    if re.search(r"\bclone\b", blob):
        return "clone is forbidden in sitrep"
    if "auth token" in blob:
        return "gh auth token is forbidden"
    return None


def run_gh(args: list[str], timeout: float = GH_TIMEOUT) -> str:
    """Run a read-only ``gh`` invocation. Never clone, never pass a token flag."""
    forbidden = _forbidden_argv(args)
    if forbidden:
        raise SitrepRefusal([forbidden])

    gh = shutil.which("gh")
    if not gh:
        raise SitrepRefusal(
            [
                "gh is not on PATH. fleet sitrep does not clone, does not call "
                "GitHub itself, and does not invent a fleet from local checkouts."
            ]
        )

    env = os.environ.copy()
    env["GH_PROMPT_DISABLED"] = "1"
    env["GH_NO_UPDATE_NOTIFIER"] = "1"
    # Do not read or inject GITHUB_TOKEN / GH_TOKEN. If they are already in the
    # operator environment, gh may use them; this process still never inspects
    # their values or writes them into a receipt.
    try:
        proc = subprocess.run(
            [gh, *args],
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            check=False,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:
        raise SitrepRefusal([f"gh timed out after {timeout:g}s"]) from exc
    except OSError as exc:
        raise SitrepRefusal([f"gh could not be executed: {exc}"]) from exc

    if proc.returncode != 0:
        err = redact((proc.stderr or proc.stdout or "").strip() or f"exit {proc.returncode}")
        raise SitrepRefusal([f"gh failed: {err[:500]}"])
    return proc.stdout


def fetch_graphql(owner: str | None, limit: int) -> dict[str, Any]:
    if owner:
        if not OWNER_RE.fullmatch(owner):
            raise SitrepRefusal([f"owner {owner!r} is not a GitHub login"])
        args = [
            "api",
            "graphql",
            "-f",
            f"query={OWNER_QUERY}",
            "-F",
            f"login={owner}",
            "-F",
            f"first={limit}",
        ]
    else:
        args = [
            "api",
            "graphql",
            "-f",
            f"query={VIEWER_QUERY}",
            "-F",
            f"first={limit}",
        ]
    raw = run_gh(args)
    try:
        body = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SitrepRefusal([f"gh graphql output was not JSON: {exc}"]) from exc
    if not isinstance(body, dict):
        raise SitrepRefusal(["gh graphql output was not a JSON object"])
    return body


def _atomic_write(path: Path, text: str) -> None:
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    )
    tmp_path = Path(handle.name)
    try:
        with handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def write_receipt(payload: dict[str, Any], receipt_dir: Path) -> tuple[Path, Path]:
    errors = validate(payload)
    if errors:
        raise SitrepRefusal(errors)

    receipt_dir.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    dated_path = receipt_dir / receipt_filename(payload["generated_at"])
    _atomic_write(dated_path, text)

    try:
        written = json.loads(dated_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SitrepRefusal([f"dated receipt unreadable after write: {exc}"]) from exc
    readback = validate(written)
    if readback:
        raise SitrepRefusal([f"dated receipt failed validation after write: {readback[0]}"])

    latest_path = receipt_dir / LATEST_NAME
    _atomic_write(latest_path, text)
    return dated_path, latest_path


def run_sitrep(
    *,
    owner: str | None = None,
    limit: int = DEFAULT_LIMIT,
    receipt_dir: Path | None = None,
    write: bool = True,
    generated_at: str | None = None,
) -> SitrepResult:
    if not isinstance(limit, int) or not (1 <= limit <= MAX_LIMIT):
        raise SitrepRefusal([f"limit must be an int 1..{MAX_LIMIT}"])
    body = fetch_graphql(owner, limit)
    payload = payload_from_graphql(
        body,
        owner=owner,
        limit=limit,
        generated_at=generated_at or now_iso(),
    )
    errors = validate(payload)
    if errors:
        raise SitrepRefusal(errors)

    dated_path = None
    latest_path = None
    if write:
        dated_path, latest_path = write_receipt(payload, receipt_dir or RECEIPT_DIR)
    return SitrepResult(payload=payload, dated_path=dated_path, latest_path=latest_path)


def render_sitrep(result: SitrepResult) -> list[str]:
    payload = result.payload
    lines = [
        "GitHub fleet sitrep (read-only; no clone; SHAs from GitHub only)",
        f"  login={payload['gh_login']}  owner={payload['owner'] or '(viewer owned)'}  "
        f"repos={payload['repo_count']}  truncated={'yes' if payload['truncated'] else 'no'}",
    ]
    for repo in payload["repos"]:
        sha = repo["sha"] if repo["sha"] else f"<no sha: {repo['sha_absent_reason']}>"
        branch = repo["default_branch"] or "-"
        lines.append(f"  {repo['name_with_owner']:<40} {branch:<16} {sha}")
    if result.dated_path:
        lines.append(f"  Receipt: {result.dated_path}")
        lines.append(f"  Latest:  {result.latest_path}")
    return lines
