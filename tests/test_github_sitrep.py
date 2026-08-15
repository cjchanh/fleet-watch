"""GitHub fleet sitrep: no clone, no tokens, no invented SHA."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from fleet_watch import cli as cli_module
from fleet_watch import github_sitrep as sitrep

FULL_SHA = "9f5ae3386912bd2706c98593c7451152818cc243"
SHORT_SHA = "9f5ae33"
JUNK_SHA = "not-a-real-object-id"


def _node(
    nwo: str = "cjchanh/fleet-watch",
    *,
    oid: str | None = FULL_SHA,
    branch: str | None = "main",
    empty: bool = False,
    **kwargs,
) -> dict:
    if empty:
        ref = None
    elif branch is None and oid is None:
        ref = {"name": None, "target": None}
    else:
        ref = {"name": branch, "target": {"oid": oid} if oid is not None else None}
    node = {
        "nameWithOwner": nwo,
        "name": nwo.split("/", 1)[-1],
        "url": f"https://github.com/{nwo}",
        "isPrivate": False,
        "isArchived": False,
        "isFork": False,
        "pushedAt": "2026-08-09T17:00:00Z",
        "visibility": "PUBLIC",
        "defaultBranchRef": ref,
    }
    node.update(kwargs)
    return node


def _viewer_body(nodes, login: str = "cjchanh", has_next: bool = False) -> dict:
    return {
        "data": {
            "viewer": {
                "login": login,
                "repositories": {
                    "pageInfo": {"hasNextPage": has_next},
                    "nodes": nodes,
                },
            }
        }
    }


def test_accept_sha_takes_full_hex_only():
    assert sitrep.accept_sha(FULL_SHA) == FULL_SHA
    assert sitrep.accept_sha(FULL_SHA.upper()) == FULL_SHA
    assert sitrep.accept_sha(SHORT_SHA) is None
    assert sitrep.accept_sha(JUNK_SHA) is None
    assert sitrep.accept_sha("") is None
    assert sitrep.accept_sha(None) is None
    assert sitrep.accept_sha(0) is None
    assert sitrep.accept_sha("0" * 40) == "0" * 40  # syntactically full, still GitHub-supplied


def test_empty_repo_has_null_sha_and_named_reason():
    repo = sitrep.build_repo(_node(empty=True))
    assert repo["sha"] is None
    assert repo["sha_absent_reason"] == "empty_repository"
    assert repo["default_branch"] is None


def test_short_oid_is_rejected_not_padded_or_kept():
    repo = sitrep.build_repo(_node(oid=SHORT_SHA))
    assert repo["sha"] is None
    assert repo["sha_absent_reason"] == "sha_rejected_not_full_oid"


def test_payload_refuses_graphql_errors_instead_of_partial_fleet():
    body = {
        "data": _viewer_body([]).get("data"),
        "errors": [{"message": "API rate limit exceeded"}],
    }
    with pytest.raises(sitrep.SitrepRefusal) as exc:
        sitrep.payload_from_graphql(
            body, owner=None, limit=30, generated_at="2026-08-15T19:00:00Z"
        )
    assert "rate limit" in str(exc.value)


def test_unknown_owner_is_refusal_not_zero_repos():
    body = {"data": {"repositoryOwner": None}}
    with pytest.raises(sitrep.SitrepRefusal) as exc:
        sitrep.payload_from_graphql(
            body, owner="no-such-user", limit=5, generated_at="2026-08-15T19:00:00Z"
        )
    assert "no owner" in str(exc.value)


def test_zero_owned_repos_is_an_honest_empty_fleet(tmp_path: Path):
    body = _viewer_body([])
    payload = sitrep.payload_from_graphql(
        body, owner=None, limit=30, generated_at="2026-08-15T19:00:00Z"
    )
    assert payload["repo_count"] == 0
    assert payload["repos"] == []
    assert payload["clone"] is False
    assert payload["tokens_used"] is False
    assert sitrep.validate(payload) == []
    dated, latest = sitrep.write_receipt(payload, tmp_path)
    assert dated.exists()
    assert latest.name == "latest.json"
    assert sitrep.validate(json.loads(latest.read_text())) == []


def test_valid_payload_keeps_github_oid_and_marks_truncation():
    payload = sitrep.payload_from_graphql(
        _viewer_body([_node()], has_next=True),
        owner=None,
        limit=30,
        generated_at="2026-08-15T19:00:00Z",
    )
    assert payload["truncated"] is True
    assert payload["repos"][0]["sha"] == FULL_SHA
    assert payload["repos"][0]["sha_absent_reason"] is None
    assert sitrep.validate(payload) == []


def test_validate_rejects_abbreviated_sha_even_if_caller_slips_one_in():
    payload = sitrep.payload_from_graphql(
        _viewer_body([_node()]),
        owner=None,
        limit=30,
        generated_at="2026-08-15T19:00:00Z",
    )
    payload["repos"][0]["sha"] = SHORT_SHA
    payload["repos"][0]["sha_absent_reason"] = None
    errors = sitrep.validate(payload)
    assert errors
    assert any("sha" in e for e in errors)


def test_run_gh_refuses_token_flags_and_clone_without_executing(monkeypatch):
    monkeypatch.setattr(sitrep.shutil, "which", lambda _name: "/usr/bin/gh")

    def boom(*_a, **_k):
        raise AssertionError("subprocess must not run for forbidden argv")

    monkeypatch.setattr(sitrep.subprocess, "run", boom)

    with pytest.raises(sitrep.SitrepRefusal):
        sitrep.run_gh(["api", "graphql", "--token", "gho_secret", "-f", f"query={sitrep.VIEWER_QUERY}"])
    with pytest.raises(sitrep.SitrepRefusal):
        sitrep.run_gh(["repo", "clone", "cjchanh/fleet-watch"])
    with pytest.raises(sitrep.SitrepRefusal):
        sitrep.run_gh(["auth", "token"])


def test_run_gh_refuses_when_gh_is_missing(monkeypatch):
    monkeypatch.setattr(sitrep.shutil, "which", lambda _name: None)
    with pytest.raises(sitrep.SitrepRefusal) as exc:
        sitrep.run_gh(["api", "graphql", "-f", f"query={sitrep.VIEWER_QUERY}"])
    assert "PATH" in str(exc.value)


def test_run_sitrep_uses_mocked_gh_only(monkeypatch, tmp_path: Path):
    captured = []

    def fake_run_gh(args, timeout=sitrep.GH_TIMEOUT):
        captured.append(args)
        assert args[:2] == ["api", "graphql"]
        assert not any(sitrep.TOKEN_FLAG_RE.match(a) for a in args)
        return json.dumps(_viewer_body([_node(), _node("cjchanh/empty", empty=True)]))

    monkeypatch.setattr(sitrep, "run_gh", fake_run_gh)
    result = sitrep.run_sitrep(
        limit=30,
        receipt_dir=tmp_path,
        generated_at="2026-08-15T19:00:00Z",
    )
    assert result.payload["repo_count"] == 2
    by_name = {r["name_with_owner"]: r for r in result.payload["repos"]}
    assert by_name["cjchanh/fleet-watch"]["sha"] == FULL_SHA
    assert by_name["cjchanh/empty"]["sha"] is None
    assert by_name["cjchanh/empty"]["sha_absent_reason"] == "empty_repository"
    assert (tmp_path / "latest.json").exists()
    assert captured and "mutation" not in " ".join(captured[0]).lower()


def test_cli_sitrep_json_smoke(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        sitrep,
        "run_gh",
        lambda *a, **k: json.dumps(_viewer_body([_node()])),
    )
    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        ["sitrep", "--json", "--receipt-dir", str(tmp_path)],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema_version"] == sitrep.SCHEMA_VERSION
    assert payload["repos"][0]["sha"] == FULL_SHA
    assert payload["clone"] is False
    assert payload["tokens_used"] is False


def test_cli_sitrep_refuses_without_gh(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(sitrep.shutil, "which", lambda _name: None)
    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        ["sitrep", "--receipt-dir", str(tmp_path)],
    )
    assert result.exit_code == 1
    assert "REFUSAL" in (result.output + result.stderr)
    assert not (tmp_path / "latest.json").exists()


def test_source_does_not_clone_read_tokens_or_invent_sha_literals():
    src = Path(sitrep.__file__).read_text(encoding="utf-8")
    assert "git clone" not in src
    assert '["repo", "clone"]' not in src
    assert '["clone"' not in src
    assert "os.environ.get(\"GITHUB_TOKEN\")" not in src
    assert "os.environ.get(\"GH_TOKEN\")" not in src
    assert "os.environ[\"GITHUB_TOKEN\"]" not in src
    assert "os.environ[\"GH_TOKEN\"]" not in src
    assert '["auth", "token"]' not in src
    assert "mutation" not in sitrep.VIEWER_QUERY.lower()
    assert "mutation" not in sitrep.OWNER_QUERY.lower()
    # The deadbeef / zero SHA placeholder must not be baked in as a default.
    assert "deadbeef" not in src.lower()
    assert 'sha = "0' not in src
    assert "0000000000000000000000000000000000000000" not in src


def test_queries_are_read_only_graphql():
    assert sitrep.VIEWER_QUERY.strip().startswith("query(")
    assert sitrep.OWNER_QUERY.strip().startswith("query(")
    for query in (sitrep.VIEWER_QUERY, sitrep.OWNER_QUERY):
        assert "target { oid }" in query
        assert "clone" not in query.lower()
