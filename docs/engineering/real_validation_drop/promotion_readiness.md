# Fleet Watch Promotion Readiness

This file defines the promotion-readiness gate for a Fleet Watch real telemetry
drop. It does not apply promotion and does not close Tier A by itself.

Promotion readiness requires three receipt inputs:

1. Intake receipt from `scripts/validate_real_validation_drop.py`.
2. Comparator receipt comparing observed guard decisions against expected
   decisions.
3. Uncertainty-analysis receipt documenting error bounds and false-positive /
   false-negative risk.

Run the validator:

```bash
python3 scripts/validate_real_validation_promotion.py \
  --intake-receipt /path/to/real_validation_drop/receipts/intake_receipt.json \
  --comparator /path/to/real_validation_drop/comparator/comparator_receipt.json \
  --ua /path/to/real_validation_drop/ua/uncertainty_receipt.json \
  --receipt-out /path/to/real_validation_drop/receipts/promotion_readiness_receipt.json
```

Default thresholds:

- `post_calibration_error_pct <= 15.0`
- `combined_uncertainty_pct <= 15.0`
- `false_positive_risk_pct <= 15.0`
- `false_negative_risk_pct <= 15.0`
- `mismatch_count == 0`

The validator may emit `tier_a_promotion_ready=true`, but it always emits
`promotion_applied=false`. Applying promotion remains a separate
operator-authorized gate.

## Receipt Semantics

`PASS` means the package is ready for an operator to consider a separate
promotion action. It does not update capability maps, dispatch logs, commercial
readiness, or corpus state.

`BLOCKED` means one or more receipts are missing, over threshold, uncited,
non-passing, or internally inconsistent.

Any promotion claim made before this validator passes must remain
designed/unverified.
