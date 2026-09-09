"""``ps`` must be invoked via the pinned absolute path, never PATH."""

from pathlib import Path

from fleet_watch.constants import PS_BIN

REPO = Path(__file__).resolve().parent.parent
PACKAGE = REPO / "fleet_watch"


def test_ps_bin_is_absolute_system_binary() -> None:
    assert PS_BIN == "/bin/ps"


def test_no_path_resolved_ps_in_package() -> None:
    offenders: list[str] = []
    for path in PACKAGE.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for needle in ('["ps"', "['ps'", ',"ps"', ", 'ps'"):
            if needle in text:
                offenders.append(f"{path.relative_to(REPO)}: {needle}")
    assert offenders == []
