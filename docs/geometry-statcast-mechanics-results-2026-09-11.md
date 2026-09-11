# Statcast acquisition mechanics results

The amended source counter passes the frozen mechanics gates: all 12 selected fitting games resolve and pair completely, with 633 one-to-one batted-ball matches and 627 available launch angles (99.05%). Six missing measurements remain explicit. No modern evaluation or historical reserve game was acquired.

The first run, `20260911-statcast-mechanics-v1`, failed in two games because Statcast counts unfinished plate appearances ended by a runner making the third out. The [matching amendment](geometry-statcast-matching-amendment.md) repairs that counter using event state, without consulting trajectory labels or angles. The replacement `20260911-statcast-mechanics-v2` reuses the same raw downloads and retains all 12 games.

| Season | Batted balls | Available angles | Recorded Statcast category differs from angle band |
|---|---:|---:|---:|
| 2015 | 149 | 143 | 16 |
| 2019 | 168 | 168 | 21 |
| 2023 | 168 | 168 | 15 |
| 2025 | 148 | 148 | 20 |

These are fitting-sample descriptions, not performance estimates. In this sample, three of nine source-recorded bunts lack angle measurements, and one recorded ground-ball bunt has a 40-degree angle. The earlier Houston bridge included a separate ground-ball bunt at 22 degrees. The user clarified that ground versus air must be preserved and only airborne subtypes standardized. The [current contract](geometry-air-standard-target-contract.md) preserves these ground balls and keeps bunt as a separate attribute. The table above remains an unconditional angle-band diagnostic, not the final target definition.

Reprocessing the 633 events under the current contract yields 299 preserved ground balls and 314 standardized airborne balls. Twenty rows have no calibration target: 15 airborne observations below the lowest air-angle band, three disagreements between sources about ground versus air, and two airborne observations without angles. All remain in the population denominator, with original source values intact.

The v2 manifest is bound by SHA-256 `6f928b4e3dc0af0f7e8efa9ae7efe94ad9f7b68796eb19fd906c449b472e5fd5`. All 60 manifest entries were verified. The expansion gate requires this external binding, mandatory root and per-game evidence, and replay from raw schedule/CSV plus the pinned player crosswalk. It reproduces player identities, plate-appearance keys, inning/frame, angle transformations, and availability instead of trusting a success field in a report. A fabricated self-attested cache was rejected after a focused review exposed and fixed that gate weakness.

This evidence permits acquisition of the remaining 361 fitting games, giving 373 fitting games in total. The 121 modern-angle evaluation games remain unopened until the model and evaluation procedure are frozen. Seven unsupported park-season selection strata remain in the selection report. A mechanically correct bridge does not establish independent physical accuracy or historical transportability; public Statcast numerical values can include estimates, and upstream source-content provenance remains partial.

Implementation validation after the airborne-only clarification: 1,117 statistical tests pass, with 18 slow tests deselected and eight dependency deprecation warnings. Ruff and strict Pyright pass for the new acquisition, matching, canonical-target, and test files. Hypothesis and its sortedcontainers dependency were added for invariant testing; no existing locked package version changed and the running model libraries were not upgraded.
