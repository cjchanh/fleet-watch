# Fleet Watch Real Validation Drop

This directory documents the required shape for a real Fleet Watch production
telemetry drop. It is a template and contract, not a promoted data package.

Required structure:

```text
real_validation_drop/
  metadata.json
  source_manifest.json
  raw/
    guard_calls.jsonl
    state_snapshots.jsonl
    health_snapshots.jsonl
    registry_events.jsonl
  normalized/
  receipts/
    raw_hash_manifest.json
    normalization_receipt.json
  comparator/
    expected_decisions.jsonl
  ua/
```

Run the intake validator:

```bash
python3 scripts/validate_real_validation_drop.py /path/to/real_validation_drop \
  --receipt-out /path/to/real_validation_drop/receipts/intake_receipt.json
```

The validator can return `PASS` for structural intake while keeping
`promotion_gate_closed=false`. Promotion still requires comparator receipts,
calibrated thresholds, uncertainty analysis, and a separate
operator-authorized promotion gate.

Valid `metadata.json` values for `data_source`:

- `production_telemetry`

Proxy, synthetic, README-example, public benchmark, and analog datasets are not
real-validation closers. They may be useful calibration inputs, but they cannot
close Tier A.

## Raw Stream Requirements

`raw/guard_calls.jsonl` records must include:

- `guard_call_id`
- `timestamp_utc`
- `state_snapshot_id`
- `request`
- `response.allowed`

`raw/state_snapshots.jsonl` records must include:

- `state_snapshot_id`
- `timestamp_utc`
- `state.process_count`
- `state.occupied_ports` or `state.ports_claimed`
- `state.locked_repos` or `state.repos_locked`

`comparator/expected_decisions.jsonl` records must include:

- `guard_call_id`
- `expected_allowed`
- optional `expectation_reason`

Every guard call must link to a state snapshot and a comparator expectation.
Every observed `response.allowed` value must match `expected_allowed`.

## Gate Meaning

Intake `PASS` means the drop is structurally valid enough to use as evidence.
It does not promote Fleet Watch validation. Tier A remains blocked until the
drop also has comparator receipts, uncertainty-analysis receipts, calibrated
thresholds, and an operator-authorized promotion gate.
