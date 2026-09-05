"""Enforce that AGENTS.md is a byte-identical mirror of CLAUDE.md.

CLAUDE.md is the source of truth. AGENTS.md exists only so OpenAI/opencode
surfaces read the same contract as Claude surfaces.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CLAUDE_MD = REPO_ROOT / "CLAUDE.md"
AGENTS_MD = REPO_ROOT / "AGENTS.md"


def test_agents_md_is_byte_identical_mirror_of_claude_md() -> None:
    claude_bytes = CLAUDE_MD.read_bytes()
    agents_bytes = AGENTS_MD.read_bytes()
    assert agents_bytes == claude_bytes, (
        "AGENTS.md has drifted from CLAUDE.md. "
        "Edit CLAUDE.md (the source of truth), then copy it over AGENTS.md: "
        "`cp CLAUDE.md AGENTS.md`."
    )
