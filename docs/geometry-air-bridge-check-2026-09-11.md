# Bridging-hitter transport check for the airborne translation

The 2016 to 2018 translation from Statcast launch-angle band to recorded Fly, LineDrive, or PopUp label reproduces the same hitters' recorded label mix in 2009, 2010, and 2011 within 0.02, and in 2012 to 2015 and 2019 within 0.06. Individual reference seasons disagree with each other by 0.03 to 0.06 under the same test, so the 2012 to 2015 and 2019 gaps are the size of season-to-season variation inside the reference, not a different pipeline. The 2020 to 2022 control fails by 0.22, which is the pipeline break already visible in the raw label shares. The conclusion is that 2009 to 2019 is one recording pipeline whose Fly/LineDrive boundary moves by about 0.05 between seasons and whose PopUp label is nearly absent in 2015, 2017, and 2018. No same-pipeline reference exists for 1989 to 2008. Nothing here is promoted, published, or used for historical reconstruction.

## Evidence integrity

The Statcast acquisition is `artifacts/statistical/backtests/geometry_reliability/20260911-statcast-bridge-fitting-2016-2018-v1`, manifest SHA-256 `714fff58b057cdfc2a0adb5738698c85d9ac8576127ef524c45c62fca4ade754`. It ran under `docs/geometry-statcast-bridge-protocol.md` with inputs bound in `docs/geometry-statcast-bridge-inputs.json` (SHA-256 `5aeb3cf1f4c9b54ac2b3923f44d4c4bd4cb06945a712d3fc72167061b5a47ac6`), from the frozen selection `20260911-statcast-bridge-selection-v1` of 4,962 PRIMARY TRAIN single games (1,700 in 2016, 1,651 in 2017, 1,611 in 2018) with the 6,105-game reserve excluded by file and sorted-ID digest. The six-game smoke stage (`20260911-statcast-bridge-smoke-2016-2018-v1`, manifest `1a9043cc…`) passed every declared gate before the full stage ran. Matching used the plate-appearance ordinal rule of the September 11, 2026 amendment and the Chadwick register pinned at `7640314a83d788c63fa7d26fa5ce9a9871053e27`. The acquisition module copied into the artifact has SHA-256 `80493768aa6295f33a50e36badaad1bdf60b0896300242ce2c0db229767f5077`; the module was revised afterwards for resume safety and smoke-gate enforcement without touching the artifact.

The transport check is `20260911-air-bridge-check-v2`, manifest SHA-256 `80d43020ba176f2250807004feaa04ca039696232d6a6769880f89cfc9b13623`. It binds the acquisition manifest and the reserve file (`fa6e8350…`), seed 20260911, 500 bootstrap repetitions, and a minimum of 100 true and 100 recorded events per hitter. Code: `geometry_air_bridge_check.py` SHA-256 `41e2f8dbd85d26fad4a834f2d269cfe10c8301453f73017a8e063f855f74f81b`. A first run of the check folded recorded bunt pop-ups and bunt line drives into the reference while excluding them from the recorded counts; the vocabulary was corrected to the three non-bunt airborne subtypes on both sides and the check was rerun. Every gap moved by less than 0.004 and no conclusion changed. Runtime: NumPy 2.4.6, Polars 1.40.1, SciPy 1.17.1, Pydantic 2.13.3, Python 3.13.5.

## Acquisition outcome

| Quantity | Value |
| --- | ---: |
| Games planned | 4,962 |
| Games paired completely | 4,955 |
| Games paired incompletely | 2 |
| Games that failed resolution | 5 |
| Local batted balls in paired games | 260,941 |
| Matched balls with a launch angle | 255,710 (98.0%) |

The seven games stay in the denominator with their failures recorded, per protocol. `CHA201607240` and `CHN201808290` each return two schedule entries flagged as single games because a suspended game from the day before was completed the same day. `CHA201705260` is flagged as a doubleheader by the MLB schedule while Retrosheet lists a single game. `MIL201709150` and `MIL201709170` are the Marlins series moved to Milwaukee for Hurricane Irma; the MLB schedule lists Miami as the home team and Retrosheet lists Milwaukee, so the home/away match fails. `CHA201607050` (game_pk 448126) and `MIA201807280` (game_pk 530985) each have one Statcast batted-ball row with no Retrosheet plate appearance (ordinals 77 and 74). None of these was repaired by matching on trajectory agreement. Every acquired angle carries `measurement_origin = tracking_or_estimate_not_distinguished`.

Standardization of the 261,058 paired rows under the airborne target contract: 132,728 air-angle standardized, 116,676 ground preserved, 7,815 air-angle conflicts, 2,390 broad source conflicts, 1,329 airborne with no angle, 117 pairing unresolved, 3 broad unresolved. The 131,344 eligible reference events are the standardized rows whose recorded subtype is Fly, LineDrive, or PopUp. Bunts stay separate per the user's standing decision, so 314 recorded PopUpBunt rows, 1 LineDriveBunt row, and 1,069 rows recorded as Unknown are standardized but not eligible.

## Recorded label vocabulary by season

Recorded airborne label shares in PRIMARY TRAIN regular-season games with the reserve anti-joined, all hitters:

| Season | Airborne labels | Fly | LineDrive | PopUp |
| ---: | ---: | ---: | ---: | ---: |
| 2009 | 52,974 | 0.650 | 0.326 | 0.024 |
| 2010 | 50,600 | 0.659 | 0.318 | 0.023 |
| 2011 | 51,066 | 0.634 | 0.342 | 0.025 |
| 2012 | 48,613 | 0.605 | 0.370 | 0.024 |
| 2013 | 49,499 | 0.607 | 0.369 | 0.024 |
| 2014 | 48,632 | 0.609 | 0.369 | 0.022 |
| 2015 | 49,799 | 0.617 | 0.380 | 0.003 |
| 2016 | 48,395 | 0.614 | 0.363 | 0.023 |
| 2017 | 48,600 | 0.635 | 0.361 | 0.003 |
| 2018 | 47,953 | 0.621 | 0.376 | 0.003 |
| 2019 | 48,882 | 0.614 | 0.363 | 0.023 |
| 2020 | 17,223 | 0.369 | 0.505 | 0.126 |
| 2021 | 47,757 | 0.458 | 0.421 | 0.120 |
| 2022 | 48,677 | 0.456 | 0.421 | 0.123 |

In the reference seasons the true band mix is flat while the recorded mix moves:

| Season | Eligible events | True Fly | True LineDrive | True PopUp | Recorded Fly | Recorded LineDrive | Recorded PopUp |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2016 | 43,970 | 0.411 | 0.428 | 0.161 | 0.656 | 0.320 | 0.025 |
| 2017 | 43,932 | 0.411 | 0.427 | 0.162 | 0.679 | 0.317 | 0.004 |
| 2018 | 43,442 | 0.415 | 0.425 | 0.160 | 0.661 | 0.336 | 0.003 |

Pooled 2016 to 2018 translation, rows are the true band and columns the recorded label:

| True band | Fly | LineDrive | PopUp |
| --- | ---: | ---: | ---: |
| Fly | 0.967 | 0.032 | 0.001 |
| LineDrive | 0.272 | 0.728 | 0.000 |
| PopUp | 0.936 | 0.001 | 0.063 |

The 2009 to 2019 pipeline records almost every true pop-up as a fly ball and about a quarter of true line drives as fly balls.

## Transport test

For each target window, the bridging cohort is every hitter with at least 100 eligible reference events in 2016 to 2018 and at least 100 recorded airborne labels in the window. Each hitter's true band profile from the reference is pushed through the reference translation and weighted by the hitter's recorded events in the window; the gap is the cohort's actual recorded share minus that prediction, with a 95% hitter-level bootstrap interval. Held-out rows rebuild the translation and the profiles from the other two reference seasons, so they show how far one reference season sits from the other two under the same test.

| Window | Kind | Hitters | Recorded events | Fly gap | LineDrive gap | PopUp gap | Largest gap |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2016 | held-out reference | 177 | 28,143 | -0.055 [-0.063, -0.047] | +0.035 [+0.028, +0.044] | +0.020 [+0.018, +0.022] | 0.055 |
| 2017 | held-out reference | 216 | 32,719 | -0.021 [-0.028, -0.014] | +0.032 [+0.025, +0.039] | -0.011 [-0.012, -0.010] | 0.032 |
| 2018 | held-out reference | 187 | 28,102 | -0.044 [-0.053, -0.036] | +0.056 [+0.047, +0.064] | -0.011 [-0.012, -0.010] | 0.056 |
| 2009 | transport | 72 | 12,151 | -0.003 [-0.015, +0.010] | -0.008 [-0.021, +0.004] | +0.011 [+0.008, +0.015] | 0.011 |
| 2010 | transport | 80 | 13,004 | -0.001 [-0.013, +0.011] | -0.011 [-0.024, +0.001] | +0.012 [+0.009, +0.015] | 0.012 |
| 2011 | transport | 105 | 17,276 | -0.020 [-0.031, -0.010] | +0.005 [-0.006, +0.016] | +0.015 [+0.013, +0.019] | 0.020 |
| 2012 | transport | 120 | 18,799 | -0.057 [-0.069, -0.046] | +0.042 [+0.032, +0.054] | +0.015 [+0.012, +0.018] | 0.057 |
| 2013 | transport | 145 | 22,901 | -0.058 [-0.066, -0.050] | +0.044 [+0.036, +0.052] | +0.014 [+0.012, +0.017] | 0.058 |
| 2014 | transport | 167 | 26,277 | -0.055 [-0.065, -0.046] | +0.045 [+0.036, +0.055] | +0.010 [+0.008, +0.013] | 0.055 |
| 2009 to 2011 | transport | 126 | 47,175 | -0.010 [-0.017, -0.002] | -0.004 [-0.011, +0.004] | +0.013 [+0.011, +0.015] | 0.013 |
| 2012 to 2014 | transport | 215 | 78,289 | -0.057 [-0.063, -0.051] | +0.044 [+0.038, +0.050] | +0.013 [+0.011, +0.015] | 0.057 |
| 2015 | transport | 198 | 30,892 | -0.045 [-0.054, -0.038] | +0.053 [+0.046, +0.061] | -0.008 [-0.008, -0.007] | 0.053 |
| 2019 | transport | 173 | 26,840 | -0.046 [-0.054, -0.039] | +0.033 [+0.026, +0.041] | +0.012 [+0.010, +0.015] | 0.046 |
| 2020 to 2022 | control | 186 | 53,170 | -0.218 [-0.225, -0.211] | +0.104 [+0.098, +0.110] | +0.114 [+0.108, +0.120] | 0.218 |

Reading the table:

- The reference is not internally uniform. Held out one season at a time, the other two seasons mispredict it by 0.03 to 0.06 on the Fly/LineDrive boundary and by 0.01 to 0.02 on PopUp. That is the noise floor of this test for a single season.
- 2009 to 2011 sits inside that floor by a wide margin. The reference translation predicts those hitters' recorded mix to within 0.02 on every class, even though their true profiles were measured five to nine years later.
- 2012 to 2015 and 2019 are mispredicted by 0.04 to 0.06 in the same direction the held-out reference seasons are: more LineDrive and less Fly recorded than the pooled reference implies. That is the same size as the difference between 2016 or 2018 and the rest of the reference.
- Every 2009 to 2014 and 2019 window records slightly more PopUp than the pooled reference predicts, by 0.010 to 0.015, because those seasons carry the 0.02 PopUp variant while two of the three reference seasons carry the 0.003 variant.
- 2020 to 2022 is mispredicted by 0.22 with intervals nowhere near zero. The test detects the known pipeline break.

## Refitting the translation inside a window is not a usable test

The check also refits a full translation table from each window's hitter-level counts by maximum likelihood, with the same hitter bootstrap. Those refits move 0.14 to 0.30 away from the pooled reference on the Fly and LineDrive rows even in the held-out reference seasons, with bootstrap intervals that exclude zero. The refit only sees between-hitter variation in the true band mix, which is narrow, so it extrapolates far outside the data and the hitter bootstrap does not capture that. The refit tables are recorded in the artifact for completeness. Use the gap test above; do not read the refit differences as evidence for or against transport.

## Limits

- Angles are not distinguished between tracked and provider-estimated. Estimated angles may be functions of the recorded label and outcome, which would make the translation look sharper than it is.
- Bridging hitters are survivors: their true band profiles come from 2016 to 2018 while the 2009 to 2011 labels come from their younger seasons. No ageing adjustment was made. The small 2009 to 2011 gaps could include offsetting ageing and boundary effects; the design cannot separate them.
- The gap test is a cohort aggregate. It can pass through compensating hitter-level errors, and the refit instability above suggests the recorded label depends on more than the true band.
- Recorded counts cover every PRIMARY TRAIN regular-season game with the reserve anti-joined; true profiles cover the 4,955 paired TRAIN games. Both are development data. Nothing from the 121 deferred modern-angle games or the reserve was read.
- 2020 alone has a 0.505 recorded LineDrive share against 0.42 in 2021 and 2022; the short season may have its own variant. The control window pools all three.

## What this changes

The translation from true band to recorded label should be modeled per recording pipeline with a season level inside the 2009 to 2019 pipeline, not as a smooth drift across seasons. Within 2009 to 2019, the 2016 to 2018 reference identifies the translation up to a season-level shift of about 0.05 on the Fly/LineDrive boundary and a PopUp sub-variant (about 0.02 recorded PopUp in most seasons, 0.003 in 2015, 2017, and 2018). Any reconstruction for 2009 to 2015 or 2019 must carry that season-level uncertainty rather than the pooled point translation. The 2020 and later pipeline needs its own reference, which the existing 2023 and 2025 matched games provide. 1989 to 2008 has no same-pipeline reference; it remains partially identified, and the outcome, depth, and fielder-group clues bound the translation there rather than pin it.
