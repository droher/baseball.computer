# Airborne reference sensitivity

This diagnostic measures how much unresolved reference mass remains and how few worst-case reference relabelings can erase the primary model's positive OOF score gains. It uses only the sealed 373-game fitting frame and prior-strength-30 predictions. It does not fit or tune a model, infer which launch angles were measured, or read deferred evaluation or reserve labels.

## Bound inputs

The source is `artifacts/statistical/backtests/geometry_reliability/20260911-air-development-full-v1`, manifest SHA-256 `928d9ee1a5973da5a8a3f8b1de3388228551adb3151d536552110b23c67c3736`. The analysis binds `coverage_frame.parquet` at `cfdf2918180812f6b1333a76a2875f87cd75b581fab07c35bb23dd648c4e3e70` and `oof_predictions.parquet` at `9187b8dc97adff0eac09dd260844b73a5eee2fbde1df68beeba091fd6570beec`.

The diagnostic artifact is `artifacts/statistical/backtests/geometry_reliability/20260911-air-reference-sensitivity-v1`, manifest SHA-256 `512965958e5b51fa61e09238626fa17ca0aba4b47fea202de4fb5994da349a83`. Its toy smoke verifies the tipping algorithm against exhaustive label enumeration for both log loss and Brier score.

## Local-Air class-share bounds

There are 10,768 events with local recorded broad type Air. Of these, 9,869 have a resolved canonical airborne class and 899 remain unresolved: 269 missing-angle events, 572 sub-10-degree Air conflicts, and 58 broad-source conflicts. For each class, the sharp lower bound assigns no unresolved event to that class; the upper bound assigns every unresolved event to it.

These are classwise bounds conditional on preserving local Air and treating resolved reference labels as fixed. The three upper bounds cannot be attained simultaneously. They do not include possible errors in resolved labels; the separate relabeling analysis addresses that risk under fixed predictions.

| Group | Local Air | Resolved | Missing | Angle conflict | Broad conflict | Fly bounds | LineDrive bounds | PopUp bounds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2015 | 2,587 | 2,333 | 55 | 165 | 34 | 36.45–46.27% | 39.85–49.67% | 13.88–23.70% |
| 2019 | 2,858 | 2,496 | 212 | 126 | 24 | 36.14–48.81% | 36.46–49.13% | 14.73–27.40% |
| 2023 | 2,665 | 2,523 | 0 | 142 | 0 | 39.17–44.50% | 40.00–45.33% | 15.50–20.83% |
| 2025 | 2,658 | 2,517 | 2 | 139 | 0 | 39.16–44.47% | 38.49–43.79% | 17.04–22.35% |
| 2015–2019 | 5,445 | 4,829 | 267 | 291 | 58 | 36.29–47.60% | 38.07–49.38% | 14.33–25.64% |
| 2023–2025 | 5,323 | 5,040 | 2 | 281 | 0 | 39.17–44.49% | 39.24–44.56% | 16.27–21.59% |

The separate resolved Statcast-Air population without a local broad type remains excluded from these bounds: eight events in 2015 and 41 in 2019. Their exclusion preserves the stated local-Air target population rather than treating a Statcast field as a prediction input.

## Fixed-prediction adversarial tipping

For each event and comparator, the original log-loss gain is `log(p_candidate[y]) - log(p_reference[y])`. The adversary replaces the reference class with whichever of Fly, LineDrive, or PopUp minimizes that gain. Sorting those event-level changes gives the exact minimum number of relabelings needed to make the average gain nonpositive. Brier uses the analogous reference-score minus candidate-score gain. The selected event and game IDs are preserved in the artifact.

| Split | Comparator | Metric | Original gain | Relabelings | Fraction | Affected games | Residual before | Residual after |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Game | Result | Log loss | 0.353006 | 355 / 9,869 | 3.597% | 109 | 0.000942 | -0.000046 |
| Game | Result | Brier | 0.234466 | 1,060 / 9,869 | 10.741% | 324 | 0.000074 | -0.000143 |
| Game | Recorded | Log loss | 0.043426 | 104 / 9,869 | 1.054% | 18 | 0.000065 | -0.000356 |
| Game | Recorded | Brier | 0.012113 | 185 / 9,869 | 1.875% | 103 | 0.000035 | -0.000030 |
| Park | Result | Log loss | 0.353173 | 357 / 9,869 | 3.617% | 111 | 0.000012 | -0.000964 |
| Park | Result | Brier | 0.234325 | 1,058 / 9,869 | 10.720% | 324 | 0.000074 | -0.000145 |
| Park | Recorded | Log loss | 0.043543 | 106 / 9,869 | 1.074% | 34 | 0.000210 | -0.000202 |
| Park | Recorded | Brier | 0.011920 | 181 / 9,869 | 1.834% | 102 | 0.000004 | -0.000062 |
| Season | Result | Log loss | 0.343214 | 348 / 9,869 | 3.526% | 124 | 0.000360 | -0.000614 |
| Season | Result | Brier | 0.227676 | 1,021 / 9,869 | 10.346% | 273 | 0.000001 | -0.000219 |
| Season | Recorded | Log loss | 0.043613 | 105 / 9,869 | 1.064% | 18 | 0.000236 | -0.000181 |
| Season | Recorded | Brier | 0.011267 | 160 / 9,869 | 1.621% | 110 | 0.000017 | -0.000041 |

The improvement over the result-only model is more resistant than the improvement over the recorded-label model. Even so, these are deliberately worst-case constructions over any resolved event, not estimates that 1–11% of labels are wrong. The affected-game counts describe where the selected event relabelings fall; whole games were not relabeled as blocks.

## Use in the next experiment

Carry the unresolved class-share ranges and fixed-prediction tipping results into the planned measurement-regime analysis. Do not interpret agreement, angle availability, or adversarial selections as observed-versus-estimated provenance. The next model still needs a frozen source/measurement specification, explicit label authorship, selection sensitivity, and independent evaluation.

The bounds and tipping points omit causal origin, contamination prevalence, parameter uncertainty, bootstrap uncertainty, and corrected-model performance. Machine-readable bounds, exact residuals, affected-game counts, and source hashes are in `docs/geometry-air-reference-sensitivity-2026-09-11.json`. Complete game and event selections remain in the bound diagnostic artifact.
