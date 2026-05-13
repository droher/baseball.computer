# Reliability Workflow

Use this checklist before allowing a source field into an imputation or adjustment model.

## Decision Ladder

1. Confirm the row is in the target population.
2. Confirm the target grain exists or should exist.
3. Confirm the source family was acquired for the relevant dimension.
4. Check whether the field is directly observed, deterministically derived, default-coded, unknown-coded, aggregate-filled, or probabilistically estimated.
5. Check known issue ledgers and audits for row-level or field-level exceptions.
6. Check personnel, identity, context, and exposure reliability if the field will constrain possible outcomes.
7. Decide whether the field can be used as truth, a constraint, a weak measurement, a covariate with measurement error, a mask, a weight, or a holdout-only diagnostic.

## Training Eligibility

Use a value as training truth only when:

- The source grain matches the target grain.
- The authority order says this source owns the field at that grain.
- Known issue ledgers do not flag the value as artifact-prone.
- The missingness or observation mechanism is represented in the model or controlled by the evaluation design.
- Conditioning on the value does not leak downstream outcomes into an upstream estimand.

Use a value as a constraint when:

- It is official at an aggregate grain and additive.
- It survives conservation audits.
- It is not an aggregate fill being silently pushed to event grain.

Use a value as a weak measurement when:

- It comes from deterministic inference with known failure modes.
- It is a scorer/source label that may confuse latent contact or geometry.
- It is a context field such as weather, handedness, or park identity with source uncertainty.

## Red Flags

- Complete-case samples are dominated by one era, source family, scorer, park, league, or team.
- Missingness depends on hit/out result, leverage, scorer habit, or event salience.
- Personnel state is inferred from aggregate appearances rather than event substitutions.
- Park identity or game context changed within a season or franchise episode.
- Official scoring conventions changed across the comparison window.
- Box totals disagree with event totals and no issue ledger explains the difference.

When a red flag is present, produce a provenance finding before proposing a statistical model.
