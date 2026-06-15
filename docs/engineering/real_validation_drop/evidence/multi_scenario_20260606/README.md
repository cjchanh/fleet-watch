# Fleet Watch Multi-Scenario Evidence Archive

This directory records the validated multi-scenario Fleet Watch telemetry pack
captured on 2026-06-06.

The raw telemetry bundle is not copied into this repository. The authorized
archive boundary for this lane is README plus manifest only. The manifest keeps
the source bundle path, source file hashes, scenario coverage, receipt verdicts,
and validation commands.

## Source Bundle

- Source path:
  `/Users/cj/tmp/fleet-watch-multi-scenario-telemetry-pack-20260606T235049Z`
- Baseline commit:
  `b460256967fbd0bfd8e6099673e15e19db1f0b6c`
- Claim scope:
  multi-scenario single-machine Fleet Watch production telemetry pack
- Promotion state:
  readiness validated; promotion was not applied

## Scenario Coverage

- Clean allow for the Fleet Watch repository.
- Repo-held deny through a temporary exclusive Fleet session lease.
- Port-conflict deny for occupied port `4242`.
- Safe-port allow for port `8000`.
- GPU-overbudget deny for a deliberately oversized GPU request.
- Memory-pressure warning allow for non-GPU guard work.
- Cleanup allow after closing the temporary Fleet session lease.
- Stale-holder support capture from `fleet stale`.
- Runaway/orphan support capture from `fleet runaway --json` and
  `fleet status --json`.

## Validation Verdict

- Intake receipt: `PASS`.
- Comparator receipt: `PASS`.
- Uncertainty-analysis receipt: `PASS`.
- Promotion-readiness receipt: `PASS`.
- Comparator mismatch count: `0`.
- Promotion applied: `false`.
- Fleet Watch tests: `355 passed`.
- Repo post-session: `PASS`, grade `A`.

## Boundaries

This archive is not a promotion application. It does not push, deploy, signal
processes, mutate Fleet runtime state, or close any broader production
validation gate by itself.
