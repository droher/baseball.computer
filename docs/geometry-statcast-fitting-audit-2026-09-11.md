# Geometry Statcast fitting audit

The fitting acquisition is complete and internally consistent. Its manifest binds 1,504 files; every listed SHA-256 matched, with no missing or unexpected files. All 373 preselected PRIMARY TRAIN fitting games completed one-to-one pairing. The 19,436 local batted-ball events equal the 19,436 paired events. No game failed, no event remained unmatched, the 121 modern-angle evaluation games were not acquired, and overlap with the 6,105-game confirmation reserve was zero.

The audit applies `trajectory-air-standard-v1`, which preserves ground versus air and uses launch angle only to standardize known airborne balls. The acquisition's `angle_standardized_class` remains an unconditional angle-band diagnostic and is not the target. The exact target module and contract are archived with the audit.

| Target outcome | Events |
| --- | ---: |
| Ground preserved | 8,562 |
| Air standardized by angle | 9,918 |
| Air below 10 degrees, unresolved | 598 |
| Air missing angle, unresolved | 276 |
| Local/Statcast broad-source conflict, unresolved | 81 |
| Broad type unresolved | 1 |
| Total | 19,436 |

The operational target resolves 18,480 events (95.08%) and retains all 956 unresolved events in population accounting. Ground balls do not require angle coverage: 321 preserved ground balls lack an angle, and 182 preserved ground balls have angles at or above 10 degrees, including 26 above 25 degrees. The source-broad conflicts split into 58 recorded-air/Statcast-ground and 23 recorded-ground/Statcast-air cases. These remain unresolved rather than being forced into agreement.

Bunt remains a separate attribute. The recorded source has 241 explicit bunts: 215 GroundBallBunt, 23 PopUpBunt, and 3 LineDriveBunt. Of these, 236 receive a resolved target, including all 215 ground bunts without requiring an angle and 21 air bunts standardized by angle. The other five stay explicit as one missing-air-angle event and four broad-source conflicts. The 128 unknown recorded classes have an unknown bunt attribute rather than being treated as non-bunts.

| Season | Parks | Games | Batted balls | Angle available | Resolved target |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2015 | 30 | 90 | 4,834 | 4,637 | 4,569 |
| 2019 | 35 | 99 | 5,161 | 4,782 | 4,754 |
| 2023 | 32 | 93 | 4,742 | 4,734 | 4,600 |
| 2025 | 31 | 91 | 4,699 | 4,679 | 4,557 |

Overall angle availability is 18,832 of 19,436 events (96.89%). The archived `tracking_missingness.parquet` retains denominators by season, result family, and recorded-bunt state. Missing angles are concentrated in 2019 (379) and include 93 explicit bunts across all seasons; that does not imply the target is missing for ground bunts.

Recorded categories and Statcast categories agree on the explicit four-class mapping for 17,995 of 19,308 observed recorded labels: 93.20%, with a 500-repetition game-cluster interval of 92.47% to 93.96%. Their broad ground/air labels agree for 19,227 of 19,308: 99.58%, interval 99.48% to 99.68%. Recorded labels agree with the unconditional angle-band diagnostic on four classes for 15,928 of 18,731 comparable events: 85.04%, interval 84.36% to 85.79%. Ground/air agreement with that diagnostic is 17,937 of 18,731: 95.76%, interval 95.45% to 96.07%.

These are selected-sample agreement summaries, not estimates of physical accuracy. The intervals resample whole games with seed `20260911` for 500 repetitions, but they do not account for shared upstream source information or validate either classification as ground truth. Public Statcast numeric values may be estimated or transformed. The balanced season-by-park selection does not estimate league rates, and this analysis does not establish historical transportability or reliability for naturally missing outcomes.

The 373-game collection is usable for the next fitting stage under the resolved target contract. A new acquisition is not needed. Fitting must require both a non-null trajectory and a resolved `trajectory-air-standard-v1` status for labeled outcomes while retaining broad-source conflicts, air-angle conflicts, missing-air angles, and unresolved broad types in denominator and missingness outputs.

The immutable audit is at `artifacts/statistical/backtests/geometry_reliability/20260911-statcast-fitting-audit-v1`. Its manifest SHA-256 is `ffa072c9b4b1a7a34841acaa1c90fc6b7ad0be0ad18567835a94177bbe593fd8`; the verified acquisition manifest SHA-256 is `fb27ab4c987c587f7e273b63a0b6af8d0bde4d1475594cb9bd1b833fd4a33867`. The matching JSON report is `docs/geometry-statcast-fitting-audit-2026-09-11.json`.
