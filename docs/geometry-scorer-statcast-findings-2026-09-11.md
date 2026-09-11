# Historical scoring and the Statcast reference

Subsequent user clarification: preserve ground versus air and standardize only airborne subtypes. The [airborne target contract](geometry-air-standard-target-contract.md) supersedes this investigation's initial unconditional angle-band proposal. Its empirical comparisons remain useful diagnostics.

The next model must estimate two things separately: a consistently defined batted-ball trajectory and the label a historical scorer would have recorded. Improving prediction of old labels alone cannot validate the first output. The user specified current Statcast definitions as the common standard and confirmed that scorer disagreement is mainly within airborne balls. Press-box perspective and individual scorer judgment are plausible measurement effects, distinct from physical park effects.

## Recording selection

In the TRAIN-only 1950s scorer-27 diagnosis, 58,139 events include only 20,934 observed exact trajectory labels. Label coverage is 76.19% on hits and 18.00% on outs. The 54.16% observed line-drive share therefore describes a selected subset. Adding weakly deduced ground balls changes that denominator materially, while 36.67% of all events still lack an exact type. These deductions are clues, not extra ground truth. Dodgers/team/park/source overlap prevents attributing the difference solely to one scorer.

Ground/air candidate clues are direct-label-blind but can share source and scorer errors with the labels used to evaluate them. High agreement supports investigating noisy measurements; it does not establish physical accuracy, fine-air classification, or applicability to unlabeled plays. Both internal folds were inspected during this exploratory work. Row-level Wilson intervals ignore game and scorer dependence and cannot support inferential reliability claims.

Source diagnoses are retained in `artifacts/statistical/backtests/historical_stress/20260911-source-diagnosis-v1/` and `20260911-source-diagnosis-v2/`. Weak-measurement artifacts are under `artifacts/statistical/backtests/geometry_reliability/`; the original `20260911-weak-measurements-v1` remains exploratory and has incomplete reserve/provenance evidence, corrected explicitly in subsequent artifacts.

## Verified Statcast connection

The local source inventory has no native Statcast game or player identifiers and no tracking measurements. A read-only inventory selected three modern primary-TRAIN games, excluding the 6,105 reserved games before external play-label acquisition. The first acquisition used only Houston–San Diego on April 19, 2025: Retrosheet `HOU202504190`, MLB `game_pk=778259`.

All 52 batted balls matched one-to-one using plate-appearance ordinal and passed inning, half-inning, batter, and pitcher checks. Player IDs came from the [Chadwick Register](https://github.com/chadwickbureau/register), pinned to commit `7640314a83d788c63fa7d26fa5ce9a9871053e27`. An initial half-inning check rejected `Bot` versus `Bottom`; an explicit vocabulary mapping fixed the mismatch, with the failed run retained. Event sequence numbers were not assumed to equal plate-appearance numbers.

| Comparison in this one game | Result |
|---|---:|
| Matched batted balls | 52 of 52 |
| Launch angle present | 52 of 52 |
| Raw local type equals Statcast type | 51 of 52 |
| Category agreement after separating bunt status | 52 of 52 |
| Statcast type equals angle-band type | 47 of 52 |
| Angles exactly at 10, 25, or 50 degrees | 0 |

The raw local mismatch is `GroundBallBunt` versus `ground_ball`, an ontology distinction. Its launch angle is 22 degrees, which belongs to the line-drive angle band. Thus even complete agreement between the two recorded category fields does not validate the standardized target. Their matching labels may share upstream lineage and are not independent validation.

The five Statcast-label/angle-band disagreements are a 4-degree line drive, a 22-degree ground ball, a 24-degree fly, a 27-degree line drive, and a 47-degree pop-up. The downloaded category is therefore not simply a deterministic application of the published bands. This does not establish how the category was assigned or which individual measurement was wrong.

Use the [MLB launch-angle definition](https://www.mlb.com/glossary/statcast/launch-angle): ground below 10°, line 10–25°, fly 25–50°, pop above 50°. The overlapping 25° boundary requires a convention; the smoke assigns it to line and flags rounded boundary angles for sensitivity. That choice changes none of this sample's comparisons. Preserve the downloaded category separately. The [CSV documentation](https://baseballsavant.mlb.com/csv-docs) says launch angle and exit velocity can include estimates for untracked balls; a non-null number alone does not establish direct measurement.

Evidence lives in `artifacts/statistical/backtests/geometry_reliability/20260911-statcast-bridge-smoke-v1/`: raw acquisition, versioned crosswalk, executable analysis, local query, paired-event parquet, report, and content manifest. This tests connection mechanics for one selected game, not representative calibration or historical validity.

The selected game hashes to the existing internal evaluation fold. Its labels have now been inspected for target design. Any revised model using that evaluation must call its scores exploratory; changing the split does not erase the exposure. The sealed component-local confirmation reserve remains untouched.

## Modeling consequences and next step

The standardized target has four trajectory categories; bunt status is a separate attribute. The old five-class reference, which combines all bunts, remains a recorded-label control. It cannot silently become the standardized target.

Model the event, historical label, and whether the label was recorded separately. Ground/air agreement supplies a strong but imperfect constraint. Modern matched measurements can inform finer airborne distinctions. Source, era, scorer, and press-box effects describe recording behavior; physical park effects belong in the event model. Confounded source/team/park/scorer effects require combined uncertainty rather than separate causal claims.

Historical labels alone cannot identify standardized classes. For example, an observed airborne mix of 30% lines, 50% flies, and 20% pops permits both perfect classification and a different reality: 50% lines, 30% flies, and 20% pops, with 40% of actual lines called flies. Both examples have identical ground/air agreement. Modern measurements do not remove that historical ambiguity without defensible transfer assumptions.

Next, declare a modern acquisition and game-level evaluation protocol before expanding the sample. Include recording availability, measurement-versus-estimation uncertainty, parks, and source periods. Then fit separate standardized and historical-label layers with transfer and missingness sensitivity. Historical confirmation must account for the intended population, unsupported records, and uncertainty in aggregate totals.

Both full primary-TRAIN extracts match earlier selected rows and class counts and exclude the reserve. The hierarchical prototype supports coherent shared draws for unseen eras and contexts and validates posterior identities. Trajectory and side smoke fits run; their short-chain diagnostics are not acceptance evidence. Development decisions require numerical, overall predictive, and historical-cohort checks together and never claim full reliability from that screen.

No replacement has passed historical reliability validation. No standardized historical output has been published. Production remains unchanged. The [reliability plan](geometry-reliability-plan.md) records the full remaining requirements.

## Validation of this increment

The statistical test suite passes: 1,092 tests, 18 slow tests deselected, eight dependency deprecation warnings. Ruff passes for all seven new Python files. Strict type checks pass for the data exporter, runner, and tests; the hierarchy checks use scoped allowances for missing/unknown third-party PyMC types. The weak-measurement files pass their strict Basedpyright checks. Focused reviews covered sampling/prediction invariants, source dependence, reserve access order, game matching, artifact integrity, and development exposure. These checks validate the implementation, not historical predictive reliability.

The corrected weak-measurement v2 artifact preserves 1,980,625 per-game contingency rows, verifies the accepted reserve file and sorted-ID hashes before label access, and reports zero overlap. Its provenance remains explicitly partial because complete upstream relation content hashes are absent. Its intervals remain descriptive row-IID intervals; game-cluster inference is still outstanding. The Statcast smoke report SHA-256 is `75409ac142f6c9c960eb1cd1253b5f8b6a6e4bb24d3d3fed2b532700d00e7c4f`.
