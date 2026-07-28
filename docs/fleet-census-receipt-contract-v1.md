# fleet-census receipt contract — v1

Status: **SSOT for `schema_version: "fleet-census/v1"`.**
Producer: `fleet census` (`fleet_watch/census/`).
Consumers build against this file.

`fleet census` answers one question: *what boots and runs on this Mac, and
what's stale.* It is deterministic, read-only, LLM-free and network-free. It
never kills anything — `close_command` is advisory text for the operator.

---

## Canonical paths

| What | Path |
| --- | --- |
| Dated receipt | `~/.governance/receipts/fleet-census/census-<UTC yyyymmddTHHMMSSZ>.json` |
| Latest pointer | `~/.governance/receipts/fleet-census/latest.json` |

`latest.json` is written by temp-file + `os.replace` (atomic rename in the same
directory). A receipt that fails schema validation **must not** replace
`latest.json` — never clobber good with bad.

Consumers **must** handle the absent state: no `latest.json` means
**"NO CENSUS RECEIPT"**, never a fake-green or empty-pretty render.

---

## Top-level shape

```json
{
  "schema_version": "fleet-census/v1",
  "generated_at": "2026-07-27T23:59:59Z",
  "host": "example-host",
  "machine": {"os": "Darwin 25.5.0", "cores": 18, "ram_gb": 128},
  "totals": {"items": 168, "keep": 129, "investigate": 39, "close": 0, "remove": 0},
  "domains": [ /* see below */ ],
  "probes":  [ /* see below */ ],
  "drift":   { /* see below */ }
}
```

`generated_at` is always UTC `YYYY-MM-DDTHH:MM:SSZ`.
`totals` is the exact roll-up of every item across every domain; validation
fails if the counts disagree with the domains.

## Domain

```json
{
  "domain_id": "user-launch-agents",
  "domain": "user LaunchAgents (~/Library/LaunchAgents)",
  "summary": "one paragraph naming what was probed and what was found",
  "totals": {"plist_files_on_disk": 91, "items": 96, "keep": 74, "investigate": 21, "close": 0, "remove": 1},
  "items": [ /* see below */ ]
}
```

`domain_id` is the **stable machine key**. `domain` is prose and may be
reworded; bind to `domain_id`. The six domains always appear, in this order,
even when a domain is empty:

| # | `domain_id` | Covers |
| --- | --- | --- |
| 1 | `user-launch-agents` | `~/Library/LaunchAgents` plists, cross-referenced against `launchctl list` and `launchctl print-disabled`, plus both-direction orphan detection |
| 2 | `global-daemons` | `/Library/LaunchDaemons` + `/Library/LaunchAgents` (third-party only) |
| 3 | `processes` | live process census, clustered |
| 4 | `network-listeners` | TCP listeners attributed to owning processes |
| 5 | `cron-login-items` | crontab, login items, brew services |
| 6 | `fleet-layer` | Fleet Watch registry rows + tmux sessions |

Every domain's `totals` always carries `items`, `keep`, `investigate`, `close`
and `remove`; the other keys are domain-specific and may grow.

## Item

```json
{
  "label": "com.example.job",
  "path": "~/Library/LaunchAgents/com.example.job.plist",
  "status": "idle-loaded",
  "evidence": "exact commands run + what they showed",
  "verdict": "keep",
  "reason": "one sentence why",
  "rule": "user-agent/idle-clean",
  "resource": "tcp/8080",
  "close_command": "launchctl bootout gui/$(id -u)/com.example.job"
}
```

Required on every item: `label`, `path`, `status`, `evidence`, `verdict`,
`reason`, `rule`. Optional and **absent when not applicable** (not `null`):
`resource`, `close_command`.

`status` ∈ `running | idle-loaded | dead | failing | stale | orphan | unknown`
`verdict` ∈ `keep | investigate | close | remove`

`rule` names the deterministic heuristic that produced the verdict (e.g.
`user-agent/missing-target`, `listener/stale-unmanaged`), so a receipt
testifies *which* rule fired, not just what it concluded.

**Removal safety.** A `remove` verdict only ever comes from an **absolute**
target path that is absent from disk. Anything the census cannot pin down — a
`python -m` module name, an inline `sh -c` command, a relative argument the
plist does not anchor with `WorkingDirectory` — resolves to
`*/target-unverifiable` (`status: unknown`, `verdict: investigate`). Unverifiable
is neither healthy nor removable; guessing in either direction is how a working
job gets deleted or a dead one gets a clean bill of health.

## Probes

```json
{"command": "sfltool dumpbtm", "ok": false, "lines": 0, "error": "timed out after 10s"}
```

Every external probe the census ran, in execution order. A probe that returned
nothing is recorded with `ok: false` and a reason. Consumers should surface
failed probes — a domain can be empty because the machine is clean *or*
because the probe never ran, and those are different facts.

## Drift

```json
{
  "prior_receipt": "/Users/cj/.governance/receipts/fleet-census/latest.json",
  "prior_status": "ok",
  "excluded_domains": ["processes"],
  "new_items":     [{"key": "user-launch-agents::com.new.job", "domain": "...", "label": "com.new.job", "verdict": "keep"}],
  "disappeared":   [{"key": "user-launch-agents::com.gone.job", "domain": "...", "label": "com.gone.job", "verdict": "investigate"}],
  "verdict_changes": [{"key": "...", "domain": "...", "label": "com.x.job", "from": "keep", "to": "investigate"}]
}
```

`key` is `<domain_id>::<label>`. Arrays are sorted by `key` and are always
present (possibly empty).

`prior_status` ∈ `ok | absent | invalid: <reason>`. An unreadable or
off-contract prior receipt is treated as absent for diffing and says so — it is
never rendered as "no change".

`excluded_domains` lists domains omitted from the diff because their membership
changes by the minute. `processes` is excluded: diffing it would bury a real
boot-surface change under process churn. New / disappeared / verdict-changed
**boot** entries are the testify events.

---

## Fail-closed rules

1. An unparseable plist becomes an item with `status: unknown`,
   `verdict: investigate` — **never silently dropped**.
2. An empty probe produces an explicit evidence line naming the probe and why
   it returned nothing — **never invented items**.
3. Zero items across all domains is a **REFUSAL**, not a receipt: nothing is
   written, `latest.json` is left untouched, and the CLI exits `1`.
4. The dated receipt is read back off disk and re-validated before
   `latest.json` is swapped — what landed is verified, not what was intended.
5. Validation enforces every required item and domain field, and checks each
   **domain's own** totals against that domain's own items — a per-domain
   miscount must not be able to hide inside a correct grand total.
6. The census is read-only. It never loads, unloads, kills or installs
   anything.

---

## Recurring run (operator-gated)

The staged launchd job lives at
`contrib/launchd/io.fleet-watch.census.plist` (daily at 09:00). It is **staged,
not installed** — Fleet Watch never bootstraps a launchd job, because process
control is an operator action.

To install it with the `fleet` path resolved for this machine:

```
fleet census --emit-launchd-plist > ~/Library/LaunchAgents/io.fleet-watch.census.plist && launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/io.fleet-watch.census.plist
```

To remove it:

```
launchctl bootout gui/$(id -u)/io.fleet-watch.census && rm ~/Library/LaunchAgents/io.fleet-watch.census.plist
```

`fleet census --emit-launchd-plist` prints the plist on stdout and these two
lines on stderr, so the redirect above captures only the plist.

The `fleet` on `PATH` may be a separate installation (pipx, for example) that
predates this command — a launchd job pointing at it would fail silently every
morning. `--emit-launchd-plist` probes the resolved binary and warns on stderr
when it does not support `census`; reinstall it before installing the job.

---

## CLI

```
fleet census                      # human summary + receipt
fleet census --json               # full receipt on stdout
fleet census --quiet              # one line: totals + receipt path (used by launchd)
fleet census --no-receipt         # judge only, write nothing
fleet census --receipt-dir DIR    # write elsewhere (tests, dry runs)
fleet census --deep               # wait longer on slow probes (sfltool login items)
fleet census --emit-launchd-plist # print the staged plist; installs nothing
```

Exit `0` on a valid census, `1` on refusal.

`sfltool dumpbtm` (login items) routinely stalls for minutes on a loaded
machine, so the default probe timeout is 10s and the gap is testified in
`probes`. `--deep` raises it to 180s for full coverage.
