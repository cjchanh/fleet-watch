"""Guard: fleet_watch/ makes no NON-LOOPBACK network calls (CLAUDE.md Invariant #4).

Couples Invariant #4 to the code so it cannot drift silently again. The invariant
is "zero egress", not "zero network": loopback probes to local runtimes
(127.0.0.1/localhost, e.g. the Ollama orphan-runner check) are permitted; any call
that could leave the machine is forbidden.

Two mechanical guards:
  1. Egress HTTP clients (requests/httpx/aiohttp) are forbidden outright — they
     exist only to talk to non-loopback hosts.
  2. For any module importing a low-level network lib (urllib/http.client/socket),
     every http(s):// URL literal must target loopback.

Modules that merely embed a URL *string* without importing a network lib (e.g. the
launchd plist DTD `http://www.apple.com/DTDs/...` in cli.py / boot_coverage.py) are
not network calls and are intentionally not checked.

Origin: E6 graphify finding (commit 496801c2) — the old grep-based invariant was
violated by two loopback urllib probes while egress was genuinely zero, and nothing
tested it. Spec 2616557.
"""
from __future__ import annotations

import re
from pathlib import Path

PKG = Path(__file__).resolve().parent.parent / "fleet_watch"
EGRESS_CLIENTS = ("requests", "httpx", "aiohttp")
NET_LIBS = ("urllib", "http.client", "socket")
LOOPBACK = {"127.0.0.1", "localhost", "::1", "[::1]"}
URL_RE = re.compile(r"""https?://([^/:"'\s}]+)""")


def _py_files() -> list[Path]:
    return sorted(p for p in PKG.rglob("*.py") if "__pycache__" not in p.parts)


def _imports(src: str, module: str) -> bool:
    return re.search(
        rf"^\s*(?:import|from)\s+{re.escape(module)}(?:\s|\.|,|$)", src, re.M
    ) is not None


def test_no_egress_http_clients() -> None:
    offenders = []
    for p in _py_files():
        src = p.read_text(encoding="utf-8", errors="replace")
        for client in EGRESS_CLIENTS:
            if _imports(src, client):
                offenders.append(f"{p.relative_to(PKG.parent)} imports {client}")
    assert not offenders, (
        "Egress HTTP client forbidden in fleet_watch/ (Invariant #4): "
        + "; ".join(offenders)
    )


def test_network_calls_are_loopback_only() -> None:
    offenders = []
    loopback_seen = 0
    net_files = 0
    for p in _py_files():
        src = p.read_text(encoding="utf-8", errors="replace")
        if not any(_imports(src, mod) for mod in NET_LIBS):
            continue  # not a network module — URL string literals are not calls
        net_files += 1
        for m in URL_RE.finditer(src):
            host = m.group(1)
            if host in LOOPBACK:
                loopback_seen += 1
            else:
                line = src[: m.start()].count("\n") + 1
                offenders.append(
                    f"{p.relative_to(PKG.parent)}:{line} non-loopback host {host!r}"
                )
    assert not offenders, (
        "Non-loopback network target in fleet_watch/ (Invariant #4, zero-egress): "
        + "; ".join(offenders)
    )
    # Liveness: if any module networks, the guard must have inspected real URLs —
    # catches silent regex drift that would make this test vacuously pass.
    if net_files:
        assert loopback_seen >= 1, (
            "guard found network-importing modules but no URL literals to verify "
            "(URL_RE drift?)"
        )
