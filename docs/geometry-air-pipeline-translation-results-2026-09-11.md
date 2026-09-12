# Airborne translation by recording pipeline: results, 2026-09-11

Experiment `geometry-air-pipeline-translation-v1`, run under the frozen [protocol](geometry-air-pipeline-translation-protocol.md). Artifact `artifacts/statistical/backtests/geometry_reliability/20260911-air-pipeline-translation-full-v1`, manifest SHA-256 `8b9fc41c…`, status `development_scoring_complete`. The numbers here are copied from the artifact's `report.json`; the companion [`geometry-air-pipeline-translation-results-2026-09-11.json`](geometry-air-pipeline-translation-results-2026-09-11.json) is an extract of it. A smoke run on 10% of games (`…-smoke-v1`) made no decision.

## Decision

- Pipeline B (2009 to 2019) passes all five screens. The season level carries the observed between-season variation of the referenced seasons, and the candidate arm keyed by recorded subtype and result family beats the subtype-only arm.
- Pipeline C (2020 onward) fails calibration, gain, and the concentration rule. With two referenced seasons, each held-out fit trains on one season, and one season cannot identify the concentration. The failure is in the protocol's held-out design for a two-season pipeline, not evidence about 2020 to 2022; see below.
- The pipeline A bounds assumptions are validated on all seven referenced seasons. Seven pipeline A seasons (2002 to 2008) have a nonempty identified set at the declared slack; 1989 to 2001 are unbounded. The bounds are wide.

Nothing is promoted or published. This is development on inspected PRIMARY TRAIN games. The 121 deferred modern-angle games and the 6,105-game reserve were not opened.

## Bindings

| Item | Value |
| --- | --- |
| Protocol SHA-256 | `5f38d4ac…` |
| 373-game coverage frame SHA-256 | `cfdf2918…` |
| 2016 to 2018 acquisition manifest SHA-256 | `714fff58…` |
| Reserve file SHA-256 / game digest | `fa6e8350…` / `5ac641fe…` (6,105 games) |
| Research database | `geometry-v2-global-side-20260911/bc.db`, 32,242,413,568 bytes, schema `main_models__geometry_v2_research_20260911` |
| Versions | Python 3.13.5, NumPy 2.4.6, Polars 1.40.1, SciPy 1.17.1, DuckDB 1.5.2, Pydantic 2.13.3 |
| Draws / bootstrap replicates / root seed | 4,096 / 500 / 20260911 |

Before scoring, an unscored probe changed two numerical settings and the protocol records both: the concentration grid extends to 10000 because the posterior reached the top of a grid ending at 2000, and the quadrature schedule gained a 512/1024 step because the pooled pipeline B Fly cell missed the 1e-5 moment tolerance at order 512 when the concentration is 10000. After the smoke run and before the full run, the protocol also fixed the pipeline B clue table for pipeline A seasons and the rule that an empty identified set means an unbounded season. No threshold changed after any result was read.

## Reference data

141,192 eligible airborne rows over 5,309 games. Pipeline B: 136,161 rows (2015: 2,326; 2016: 43,970; 2017: 43,932; 2018: 43,442; 2019: 2,491). Pipeline C: 5,031 rows (2023: 2,521; 2025: 2,510). The 2016 to 2018 seasons are full-season acquisitions; the others come from the park- and season-balanced 373-game frame. Five events fell in a `fielders_choice` cell that no training season contained and were predicted from the prior (`prior_only_cell`).

## Pipeline B

Concentration posteriors. The full fits have posterior mean concentration 1,738 (subtype-only arm) and 1,515 (candidate arm) with zero mass on either grid end. The ten held-out fits have means 1,074 to 3,291 with at most 0.051 on the top grid value and none on the bottom. At a concentration near 1,500 the season-level standard deviation of a class share near 0.4 is about 0.013.

Calibration of the point prediction on each held-out season (limit 0.06 on ECE and on absolute bias; every season has more than 500 events):

| Season | Fly ECE / bias | LineDrive ECE / bias | PopUp ECE / bias |
| --- | --- | --- | --- |
| 2015 | 0.008 / +0.007 | 0.014 / −0.014 | 0.010 / +0.007 |
| 2016 | 0.007 / −0.005 | 0.008 / −0.008 | 0.013 / +0.013 |
| 2017 | 0.012 / +0.012 | 0.004 / −0.003 | 0.009 / −0.009 |
| 2018 | 0.007 / −0.007 | 0.015 / +0.015 | 0.008 / −0.007 |
| 2019 | 0.005 / −0.005 | 0.006 / +0.006 | 0.002 / −0.001 |

Class-share coverage: 48 checks (overall plus each recorded subtype with at least 200 held-out events, three classes each). The nominal 95% interval covered 44 of 48 (0.917, limit 0.90) and the nominal 90% interval 42 of 48 (0.875, limit 0.80). All four 95% misses are in 2018, whose LineDrive share sits 0.001 to 0.005 outside the interval overall and inside the recorded Fly and LineDrive cells; the two extra 90% misses are the 2015 recorded LineDrive cell. Whole-game count coverage of the nominal 95% interval: Fly 0.968, LineDrive 0.964, PopUp 0.969 over 5,125 held-out games (limit 0.90).

Gain of the candidate arm over the subtype-only arm, pooled over the five held-out seasons, paired whole-game bootstrap, 500 replicates:

| Metric | Mean per event | 95% interval |
| --- | --- | --- |
| Log loss | 0.064 nats | 0.062 to 0.065 |
| Brier | 0.022 | 0.022 to 0.023 |

New-season predictive of the candidate arm, which applies to 2009 to 2014 (posterior mean and nominal 95% interval of the season composition):

| Recorded subtype, result | Fly | LineDrive | PopUp |
| --- | --- | --- | --- |
| Fly, hit | 0.587 (0.555 to 0.617) | 0.401 (0.370 to 0.432) | 0.013 (0.007 to 0.021) |
| Fly, out in play | 0.597 (0.565 to 0.629) | 0.103 (0.084 to 0.124) | 0.299 (0.271 to 0.329) |
| Fly, sacrifice | 0.807 (0.777 to 0.837) | 0.165 (0.138 to 0.194) | 0.027 (0.016 to 0.041) |
| Fly, reached on error | 0.540 (0.467 to 0.611) | 0.120 (0.076 to 0.172) | 0.340 (0.274 to 0.413) |
| LineDrive, hit | 0.032 (0.022 to 0.045) | 0.967 (0.955 to 0.978) | 0.000 (0.000 to 0.002) |
| LineDrive, out in play | 0.062 (0.047 to 0.079) | 0.936 (0.919 to 0.951) | 0.002 (0.000 to 0.006) |
| PopUp, out in play | 0.038 (0.023 to 0.055) | 0.001 (0.000 to 0.004) | 0.961 (0.944 to 0.976) |

The remaining six cells (fielders' choice, sacrifice and error cells with under 500 events, PopUp hits) are in the JSON with intervals of width 0.4 to 0.9. The referenced-season posteriors per cell are in `pipeline_B.json` under `predictive`.

## Pipeline C

The full fits on 2023 and 2025 have posterior mean concentration about 3,000 to 3,500 with 0.15 to 0.17 of the mass on the top grid value and none on the bottom. Each held-out fit trains on a single season, and its concentration posterior puts 0.30 to 0.47 of its mass on the smallest grid value (posterior means 2 to 12), which fails the 10% rule. With one season the concentration is identified only through the prior's shape: cells whose composition sits near a simplex edge (recorded LineDrive, recorded PopUp outs) favor a small concentration under the uniform composition prior, and a small concentration shrinks the new-season point prediction toward uniform. That is why calibration fails with ECE 0.15 to 0.29 per class while the class-share bias stays within 0.07, why the candidate arm loses to the subtype-only arm (log loss gain −0.019 nats, 95% interval −0.022 to −0.015; Brier −0.015), and why the share and whole-game coverage screens pass trivially at 1.000 with intervals that cover almost the whole simplex.

The two-season full fits do not have this problem, and their new-season predictive (applies to 2020 to 2022 and 2024) is reported without a verdict:

| Recorded subtype, result | Fly | LineDrive | PopUp |
| --- | --- | --- | --- |
| Fly, hit | 0.755 (0.703 to 0.802) | 0.228 (0.177 to 0.278) | 0.018 (0.005 to 0.038) |
| Fly, out in play | 0.802 (0.753 to 0.839) | 0.053 (0.032 to 0.081) | 0.145 (0.111 to 0.186) |
| LineDrive, hit | 0.021 (0.008 to 0.041) | 0.978 (0.957 to 0.991) | 0.001 (0.000 to 0.005) |
| LineDrive, out in play | 0.102 (0.070 to 0.141) | 0.896 (0.855 to 0.929) | 0.002 (0.000 to 0.008) |
| PopUp, out in play | 0.098 (0.064 to 0.136) | 0.002 (0.000 to 0.008) | 0.900 (0.861 to 0.934) |

Pipeline C therefore has no validated season level. The protocol did not anticipate that a two-season pipeline's held-out fit is a one-season fit. The fix belongs in a declared revision, for example a concentration shared across pipelines B and C or a pipeline C concentration prior taken from pipeline B; it is not applied here.

## Pipeline A bounds

Clue tables are P(clue | true band) over the 27 levels of result group by fielder group by depth group, estimated from each referenced pipeline. The slack per level is twice the absolute difference between the pipeline B and C tables with a floor of 0.01: it is 0.01 to 0.045 on 23 levels, 0.08 and 0.11 on out-in-play infield default and shallow, and 0.22 and 0.26 on outfield hits default and shallow, where the two reference pipelines record depth differently.

Validation: for each of the seven referenced seasons, the identified set built from the other pipeline's clue table and the season's own recorded label shares and clue margins contains the season's actual P(band | recorded) in every cell, with zero violation. The sets are wide: the largest cell width is 0.95 to 0.98 in each season. The informative cells are the lower bounds on the diagonal (Fly given recorded Fly at least 0.18 to 0.29, LineDrive given recorded LineDrive at least 0.21 to 0.32, PopUp given recorded PopUp at least 0.02 to 0.05) and the upper bounds on off-diagonal cells (0.42 to 0.98, tightest for the PopUp band given recorded Fly and loosest for the LineDrive band given recorded PopUp).

Pipeline A seasons, pipeline B clue table, 33,243 to 53,990 recorded airborne labels per season:

| Season | Identified set | Smallest slack multiple that makes it nonempty |
| --- | --- | --- |
| 1989 to 1992 | empty | 3.5 to 5.2 |
| 1993 to 1998 | empty | 2.5 to 2.9 |
| 1999 | empty | 1.02 |
| 2000 | empty | 1.24 |
| 2001 | empty | 1.92 |
| 2002 to 2008 | nonempty | 1 |

For 2002 to 2008 the band mix bounds are Fly 0.29 to 0.50, LineDrive 0.24 to 0.60, PopUp 0.07 to 0.30, and the uniform prior over the set has mean band mix near Fly 0.36 to 0.41, LineDrive 0.35 to 0.40, PopUp 0.22 to 0.26. Diagonal lower bounds are Fly 0.29 to 0.36, LineDrive 0.24 to 0.29, PopUp 0.07 to 0.19. The prior mean is a property of the declared uniform prior, not a validated translation. The multiplier column is a diagnostic only: the slack was not widened, and 1989 to 2001 stay unbounded under this protocol. The likely cause is the clue availability itself (fielder and depth recording rates in 1989 to 2001 differ from both reference pipelines by more than the declared slack), which a revision would have to declare separately from the band-conditional clue distribution.

## What this establishes and what it does not

Established: inside 2009 to 2019, a translation keyed by recorded subtype and result family with an inferred season level reproduces held-out seasons within the predeclared limits, and its new-season predictive is the declared translation for 2009 to 2014. The pipeline A clue-transport assumptions hold on every referenced season at the declared slack, and 2002 to 2008 have declared bounds.

Not established: a validated season level for 2020 onward (the protocol's held-out design fails for two seasons); any translation for 1989 to 2001; independent modern confirmation; missing-label validity; reference measurement quality; scorer authorship; publication readiness. Nothing here authorizes publishing historical reconstructed facts.

## Reproduction

```sh
PYTHONPATH=bc uv run --no-sync pytest -q bc/tests/statistical/test_geometry_air_pipeline_model.py bc/tests/statistical/test_geometry_air_pipeline_bounds.py bc/tests/statistical/test_geometry_air_pipeline_development.py

PYTHONPATH=bc uv run --no-sync python -m python_models.statistical.backtests.geometry_air_pipeline_development --output <run root> [--smoke]
```

The full run takes about two minutes; checkpoints under `checkpoints/` are reused when the output root already exists and was produced from the same inputs. A smoke run keeps the games in fold 0 of a ten-fold game hash; in that subsample 2015 has no recorded PopUp rows, so its bounds validation is reported as `missing_recorded_subtype` and the smoke run does not validate the assumptions.
