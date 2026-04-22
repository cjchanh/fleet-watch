# AGENTS.md
## Purpose
Fleet Watch is a local process and resource guard for AI workloads sharing one machine.
## Rules
- Do not commit secrets, tokens, private keys, `.env` files, local databases, or generated runtime state.
- Do not add CJ-specific absolute paths or machine-local assumptions to tracked source.
- Keep README and docs aligned with actual runtime behavior in `fleet_watch/` and tests.
- Prefer behavior changes with tests. Do not widen claims beyond what code and tests prove.
## Required validation
Before closing work:
- `python3 -m pytest tests -q`
- `python3 -m fleet_watch.cli guard --json`
Use a temporary `HOME` for smokes when local state could affect results.
## Release gate
Before tagging or publishing:
- git clean for tracked files
- tests passing
- guard smoke passing
- docs match current behavior
