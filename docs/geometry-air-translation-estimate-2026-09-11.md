# Airborne translation estimate for every season, 2026-09-11

Estimate `geometry-air-translation-estimate-v1`. This is the best available translation from the recorded airborne subtype (Fly, LineDrive, PopUp) and result family to the standardized band, for every season from 1989 to 2025. It is an estimate under stated assumptions, not a validated result. The user's ruling on 2026-09-11: stop trying to prove transport; produce as good an estimate as the data allows and accept that it is imprecise.

The full table is [`geometry-air-translation-estimate-2026-09-11.json`](geometry-air-translation-estimate-2026-09-11.json): per season, per recorded subtype and result family, the posterior mean and nominal 95% interval of each band probability, with the basis of the estimate. Artifact `artifacts/statistical/backtests/geometry_reliability/20260911-air-translation-estimate-v1`, built from the pipeline translation run (manifest `8b9fc41c…`, see [the results](geometry-air-pipeline-translation-results-2026-09-11.md)).

## Basis by season

| Seasons | Basis |
| --- | --- |
| 2015 to 2019, 2023, 2025 | Referenced-season posterior of the pipeline fit (validated for 2009 to 2019). |
| 2009 to 2014 | New-season predictive of the 2009 to 2019 fit (validated by leave-one-season-out). |
| 2020 to 2022, 2024 | New-season predictive of the 2020 onward fit on 2023 and 2025. Its season level is not validated; the two-season fit itself has concentration near 3,000. |
| 1989 to 2008 | Raked from the 2020 onward translation (see below). Not validated; partially identified. |

## The 1989 to 2008 estimate

Assumptions, all declared and none provable from the data:

1. The true band mix in 1989 to 2008 equals the modern mix. Across the seven referenced seasons 2015 to 2025 the true mix is Fly 0.412, LineDrive 0.425, PopUp 0.163 with season standard deviations of 0.003, 0.011, and 0.008. The estimate draws the pre-2009 mix from a Dirichlet with that mean and concentration 2,000, which reproduces that spread.
2. The 2020 onward translation is the closest known vocabulary to 1989 to 2008. Recorded PopUp is 0.15 to 0.17 of airborne labels in 1989 to 2008 and 0.13 in 2020 onward, against 0.01 in 2009 to 2019. Seeded from 2020 onward, the unadjusted translation already predicts a pre-2009 mix of about Fly 0.40, LineDrive 0.41, PopUp 0.19; seeded from 2009 to 2019 it would predict PopUp 0.26 and need large corrections.
3. Each season's translation is the closest one to the seed, in Kullback-Leibler divergence, that reproduces the season's recorded label shares and the assumed mix. That is iterative proportional fitting of the seed translation weighted by label shares to both margins; the solution scales each band column by one factor. The same column factors are applied to the seed's recorded-by-result cells.

Uncertainty combines the seed's new-season predictive draws (4,096, from the 2020 onward fits for both the recorded-only and the recorded-by-result arms) with the mix draws. It does not include the assumption error in items 1 and 2, which cannot be sized from the data.

Representative results (posterior mean, 95% interval):

| Season | Recorded Fly to Fly | Recorded PopUp to PopUp | Recorded LineDrive to LineDrive | Column factors Fly / LineDrive / PopUp |
| --- | --- | --- | --- | --- |
| 1989 | 0.805 (0.736 to 0.852) | 0.822 (0.712 to 0.896) | 0.956 (0.900 to 0.985) | 1.03 / 1.38 / 0.58 |
| 2000 | 0.777 (0.714 to 0.822) | 0.848 (0.731 to 0.922) | 0.961 (0.907 to 0.988) | 0.92 / 1.48 / 0.65 |
| 2008 | 0.759 (0.700 to 0.803) | 0.831 (0.716 to 0.906) | 0.968 (0.918 to 0.991) | 0.88 / 1.71 / 0.54 |

For comparison the 2020 onward seed is Fly to Fly 0.795, PopUp to PopUp 0.901, LineDrive to LineDrive 0.951. The raking mostly moves mass out of the PopUp band (recorded PopUp is more common pre-2009 than the true PopUp share allows) and into LineDrive. The recorded-by-result cells follow the same pattern: for 2000, recorded Fly hits are Fly 0.674, LineDrive 0.314, PopUp 0.011; recorded Fly outs are Fly 0.810, LineDrive 0.086, PopUp 0.103; recorded PopUp outs are PopUp 0.853.

Cells with no reference events in the seed pipeline (fielders' choice, most PopUp cells other than outs) carry the prior-only interval, which spans most of the simplex; they are flagged `prior_only_cell`. In 1989 to 2008 those cells hold well under one percent of airborne events.

## What to do with it

Use the JSON table as the season-by-season translation when standardizing airborne subtypes. Every pre-2009 row is partially identified and should publish with that flag. The 2020 to 2022 and 2024 rows rest on a two-season fit whose season level was not validated. The remaining step is wiring the table into the geometry export; this document does not do that.

## Reproduction

```sh
PYTHONPATH=bc uv run --no-sync pytest -q bc/tests/statistical/test_geometry_air_translation_estimate.py

PYTHONPATH=bc uv run --no-sync python -m python_models.statistical.backtests.geometry_air_translation_estimate --run-root artifacts/statistical/backtests/geometry_reliability/20260911-air-pipeline-translation-full-v1 --output <estimate root>
```
