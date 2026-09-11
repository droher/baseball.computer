# Airborne translation development results

The `geometry-air-development-v1` screen failed. The recorded-subtype plus result candidate improved log loss and Brier score over both reference arms in every split family, but it failed the prespecified season calibration and class-share limits. Prior strengths 3 and 300 failed the same screen, so smoothing alone does not resolve the problem. This model should not be promoted or used for historical reconstruction.

## Evidence integrity

The full artifact at `artifacts/statistical/backtests/geometry_reliability/20260911-air-development-full-v1` has manifest SHA-256 `928d9ee1a5973da5a8a3f8b1de3388228551adb3151d536552110b23c67c3736`. All 58 manifest entries match. The archived protocol, runner, frame builder, translation model, target transform, accepted acquisition manifest, and selected games match the hashes bound in `report.json`.

The full frame contains 19,436 events from 373 fitting games. It retains 9,869 eligible recorded-Air events and 956 unresolved events. The target transform assigns 8,562 events the canonical `ground_preserved` status. Separately, 8,540 events have local recorded broad type Ground and therefore receive deterministic Ground predictions; 23 of those have a broad-source conflict, while 45 canonical ground-preserved events lack local recorded Ground. Another 49 resolved Statcast-Air events lack a local recorded broad type and are ineligible for this translation evaluation. The OOF file contains 88,821 rows: every eligible event exactly once in each of three split families and three prior strengths. All probability vectors are finite, nonnegative, and normalized. The 42 archived fits have disjoint training and evaluation games. Replaying those fits verified 88,821 fine-label masks, 88,821 full source-block masks, and 76,860 fold-level local-Ground predictions (`8,540 × 3 × 3`). Per-game score totals conserve every OOF event.

The internal run log is part of the original manifest with SHA-256 `350458564f22e5a8035d9481f0e699c67cf23c892c420c7555eb91df8c57113f`. The external redirect log is empty and is not evidence for the run.

## Prespecified result

At prior strength 30, overall metrics look well calibrated because the opposing season errors cancel:

| Split family | Log loss | Brier | Classwise ECE | Largest absolute class-share bias |
| --- | ---: | ---: | ---: | ---: |
| Game | 0.528501 | 0.306494 | 0.002113 | 0.000485 |
| Park | 0.528330 | 0.306686 | 0.005128 | 0.000975 |
| Leave one season out | 0.538428 | 0.313504 | 0.032528 | 0.002112 |

Every supported season slice exceeds the 0.02 class-share threshold. The leave-one-season-out slices also exceed the 0.05 ECE threshold.

| Split | Season | Events | ECE | Fly bias | LineDrive bias | PopUp bias |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Game | 2015 | 2,333 | 0.043154 | +0.063368 | -0.035631 | -0.027736 |
| Game | 2019 | 2,496 | 0.039826 | +0.053113 | -0.018919 | -0.034194 |
| Game | 2023 | 2,523 | 0.041339 | -0.056394 | +0.028065 | +0.028330 |
| Game | 2025 | 2,517 | 0.037924 | -0.055787 | +0.022666 | +0.033120 |
| Park | 2015 | 2,333 | 0.045103 | +0.063427 | -0.036050 | -0.027377 |
| Park | 2019 | 2,496 | 0.042242 | +0.053078 | -0.019348 | -0.033731 |
| Park | 2023 | 2,523 | 0.041459 | -0.056719 | +0.027738 | +0.028981 |
| Park | 2025 | 2,517 | 0.038063 | -0.055978 | +0.022379 | +0.033599 |
| Season | 2015 | 2,333 | 0.058459 | +0.086245 | -0.047266 | -0.038979 |
| Season | 2019 | 2,496 | 0.055140 | +0.074586 | -0.026546 | -0.048040 |
| Season | 2023 | 2,523 | 0.053466 | -0.072037 | +0.036426 | +0.035611 |
| Season | 2025 | 2,517 | 0.050035 | -0.073416 | +0.029430 | +0.043986 |

The paired 500-replicate game-cluster intervals are positive against both references. Against the result reference, log-loss gain intervals are `[0.335467, 0.373951]`, `[0.336327, 0.372967]`, and `[0.323993, 0.363602]` for game, park, and season splits; corresponding Brier intervals are `[0.221726, 0.250038]`, `[0.221999, 0.249009]`, and `[0.214408, 0.242763]`. Against the recorded-label reference, log-loss intervals are `[0.038570, 0.048360]`, `[0.038503, 0.048143]`, and `[0.038360, 0.048133]`; Brier intervals are `[0.010274, 0.013915]`, `[0.010219, 0.013521]`, and `[0.009322, 0.012898]`. These are development intervals conditional on fixed fits.

## Era diagnosis

The angle-defined target mix is nearly stable while the local recorded subtype mix changes sharply.

| Cohort | Eligible events | Recorded Fly | Recorded LineDrive | Recorded PopUp | Target Fly | Target LineDrive | Target PopUp |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2015 and 2019 | 4,829 | 66.10% | 32.45% | 1.45% | 40.92% | 42.93% | 16.15% |
| 2023 and 2025 | 5,040 | 47.88% | 38.63% | 13.49% | 41.37% | 41.45% | 17.18% |

Within local recorded Fly, the canonical Fly share is 59.61% in 2015 and 60.11% in 2019, then 80.08% in 2023 and 78.97% in 2025. The largest common recorded-subtype by result-family cells move in the same direction: recorded-Fly outs in play increase by 20.44 percentage points, hits by 18.26 points, and sacrifices by 16.98 points. Recorded LineDrive remains much more stable: its canonical LineDrive share is 97.28%, 94.72%, 95.10%, and 94.82% by season.

Nine cells appear in both era groups and cover 99.90% of early eligible events and 99.94% of late eligible events. A symmetric decomposition within those cells gives a +0.51-point Fly-target change: cell composition contributes -11.51 points and conditional mapping contributes +12.02 points. The two large components nearly cancel in the aggregate. A pooled translation therefore learns an average relationship that overpredicts Fly in 2015/2019 and underpredicts it in 2023/2025.

Exact local subtype agreement with the archived Statcast `bb_type` is 74.02% in 2015, 77.28% in 2019, and 100% in both 2023 and 2025 among eligible events. This is evidence that the relationship between the two recorded category fields changes by era. It does not establish a cause or make Statcast `bb_type` an independent reference in the later seasons.

The numeric launch-angle target is also not guaranteed to be an independent instrument. [Baseball Savant's CSV documentation](https://baseballsavant.mlb.com/csv-docs) says that launch angle and exit velocity include estimates for a limited subset of balls not tracked directly. Its linked [2017 Statcast processing explanation](https://tangotiger.com/index.php/site/article/statcast-lab-no-nulls-in-batted-balls-launch-parameters) says these estimates can use stringer descriptions and actual outcomes, with different assignments when the tracking system was unavailable versus operating but missed a play. Recorded subtype and result may therefore overlap inputs used to impute the target. The accepted frame has no row-level observed-versus-estimated flag, so this analysis cannot isolate directly tracked angles or quantify that dependence.

[Sony's 2020 Hawk-Eye announcement](https://sony.mediaroom.com/2020-08-20-Hawk-Eye-Innovations-and-MLB-Introduce-Next-Gen-Baseball-Tracking-and-Analytics-Platform) documents deployment of the optical platform at all MLB parks for the 2020 season. That transition lies between the two era groups and makes measurement regime a necessary part of the next investigation, but temporal coincidence does not identify Hawk-Eye as the cause of the observed mapping shift.

Unresolved events remain in the denominator: 265 in 2015, 407 in 2019, 142 in 2023, and 142 in 2025. Across all seasons these comprise 598 sub-10-degree Air conflicts, 276 missing Air angles, 81 broad-source conflicts, and one unresolved broad type. The diagnostic does not silently condition these away when describing collection coverage.

## Consequence

The next experiment should model source and measurement regime explicitly, distinguish measured from vendor-estimated angles where provenance permits, and establish who authored each recorded classification before assigning scorer effects. A year indicator can describe this fitting collection but cannot establish a measurement mechanism or historical transport. Its specification must be frozen before opening the 121 modern-angle evaluation games. Selection sensitivity must retain unresolved and source-conflict cases, and historical use still requires a separate transport test and uncertainty treatment.

This collection was balanced for development and does not estimate league prevalence. All labels here were already exposed for fitting. The analysis cannot separate tracking hardware, upstream category construction, park, scorer, or other season-linked processes. It reads no deferred evaluation angles, historical reserve, TEST, VALIDATE, or reserve labels.

Machine-readable results are in `docs/geometry-air-development-results-2026-09-11.json`. The bound diagnostic artifact is `artifacts/statistical/backtests/geometry_reliability/20260911-air-era-diagnostic-v1`.
