# Airborne translation by recording pipeline

Experiment: `geometry-air-pipeline-translation-v1`. This specification is frozen before any scoring. It replaces the two-regime structure of `geometry-air-regime-posterior-v1` with one translation per recording pipeline and a season level inside each referenced pipeline, following the vocabulary breaks found on 2026-09-11 and the bridging-hitter transport check (`docs/geometry-air-bridge-check-2026-09-11.md`). It is development on already inspected PRIMARY TRAIN games. The 121 deferred modern-angle games and the 6,105-game historical reserve stay sealed.

## Estimand and pipelines

The target is `trajectory-air-standard-v1`: preserve recorded Ground versus Air, standardize only airborne subtypes, keep bunts separate. The estimand is the distribution of the standardized band (Fly, LineDrive, PopUp) given the recorded airborne subtype, the result family, the recording pipeline, and the season.

Pipelines are declared from recorded label shares alone, before any reference is read:

- Pipeline A: 1989 to 2008. Recorded PopUp is 0.15 to 0.17 of airborne labels. No same-pipeline reference exists.
- Pipeline B: 2009 to 2019. Recorded PopUp is about 0.02, or 0.003 in 2015, 2017, and 2018. Referenced by 2015, 2016, 2017, 2018, and 2019.
- Pipeline C: 2020 onward. Recorded PopUp is about 0.12. Referenced by 2023 and 2025.

A season inside a referenced pipeline but without its own reference (2009 to 2014, 2020 to 2022, 2024) receives the pipeline's new-season predictive, which carries the between-season dispersion estimated from the referenced seasons. Pipeline A receives bounds, not a fitted translation.

## Reference data

Pipeline B: the eligible rows for 2015 and 2019 from the bound 373-game frame (`20260911-air-development-full-v1/coverage_frame.parquet`, SHA-256 `cfdf2918…`) and the eligible rows of the 2016 to 2018 acquisition (`20260911-statcast-bridge-fitting-2016-2018-v1`, manifest SHA-256 `714fff58…`). Pipeline C: the eligible rows for 2023 and 2025 from the same 373-game frame. Eligible means the standardized status is `air_angle_standardized` and the recorded class is exactly Fly, LineDrive, or PopUp. Bunt subtypes and Unknown are not eligible on either side. Result families are the five nonempty strings of `model_input_geometry`. Fielder group and depth are joined from the research database by game and event key for the clue tables only; they are never prediction features.

The 373-game frame is park and season balanced; the 2016 to 2018 acquisition is full-season. Season-level counts therefore differ in size by about an order of magnitude, which the hierarchy absorbs through the season-level Dirichlet, not through reweighting.

## Model for pipelines B and C

Class order is Fly, LineDrive, PopUp. Two arms are fitted on identical rows: the candidate arm with cells keyed by recorded subtype and result family, and the reference arm keyed by recorded subtype alone. Within a pipeline and arm, cells are independent:

`q_cell ~ Dirichlet(1, 1, 1)`

`p_cell,season | q_cell, kappa ~ Dirichlet(kappa * q_cell)`

`counts_cell,season | p ~ Multinomial(n_cell,season, p_cell,season)`

The concentration `kappa` is shared by every cell of a pipeline and arm and is inferred, not fixed. Its prior is uniform over the log-spaced grid 1, 2, 3, 5, 10, 20, 30, 50, 100, 200, 300, 500, 1000, 2000, 3000, 5000, 10000. At 10000 the season-level standard deviation of a class probability is about 0.005, below the multinomial noise of the smallest referenced season, so larger values are indistinguishable from identical seasons and the grid stops there. The posterior over the grid is proportional to the product over cells of the marginal likelihood of the cell's season counts at that concentration, computed from the same simplex quadrature as the regime kernel with the Gamma normalizers restored. This replaces the fixed-concentration sensitivity of the previous protocol, whose verdict depended on the prior strength. If more than 10% of a pipeline-and-arm posterior sits on the smallest grid value, the between-season dispersion exceeds what the grid can express and the fit is a numerical failure requiring a declared revision. Mass on the largest grid value means the referenced seasons are indistinguishable from identical at this resolution; it is reported, and whether that understates the dispersion of a new season is what the held-out coverage screen tests. An unscored probe fit before freezing showed the posterior reaching the top of a grid that ended at 2000, which is why the grid extends to 10000.

Quadrature extends the previous schedule by one step: compare all moments at orders 64 and 128, then 128 and 256, then 256 and 512, then 512 and 1024, at every grid concentration; use the first passing higher order; stop with a numerical failure if none passes. The same unscored probe showed the pooled pipeline B Fly cell, about 90,000 events over five seasons, missing the 1e-5 tolerance at order 512 when the concentration is 10000, which is why the schedule has the fourth step.

Draws: 4,096 per cell with SHA-256 seeds from experiment, fit, arm, and cell identity, root seed 20260911. Each draw samples a concentration from its grid posterior, a global composition from the quadrature posterior at that concentration, then the season composition. A referenced season draws `Dirichlet(counts + kappa * q)`; a new season draws `Dirichlet(kappa * q)`. Events sharing a cell and season share the draw. Cells with no observations in any season are `prior_only_cell` and are reported as such.

## Screens for pipelines B and C

Every referenced season is held out once inside its pipeline and predicted with the new-season predictive from the other seasons. This is the only split; it tests exactly the situation of 2009 to 2014, 2020 to 2022, and 2024. All limits below are sized to the bridge check's measured between-season floor of 0.03 to 0.06 on class shares, not to the earlier 0.02 limit that a point prediction cannot meet when seasons differ by 0.05.

1. Calibration of the point prediction: each class's 15-bin ECE at most 0.06 and absolute class-share bias at most 0.06 on every held-out season with at least 500 eligible events. Report smaller slices without a verdict.
2. Class-share coverage: for every held-out season, and for every recorded subtype cell with at least 200 held-out events plus the overall slice, the nominal 95% equal-tailed posterior predictive interval of the class share must contain the actual share in at least 90% of the checks in each pipeline, and the nominal 90% interval in at least 80%. Report the checks individually.
3. Whole-game count coverage: nominal 95% posterior predictive intervals of per-game class counts must cover at least 90% of held-out games per class and pipeline, using the shared season draw and multinomial counts, as in the previous protocol.
4. Gain: the candidate arm's 95% paired whole-game bootstrap gain over the reference arm in log loss and Brier, pooled over held-out seasons per pipeline, must have a positive lower endpoint. 500 replicates, seed 20260911.
5. Concentration grid: the 10% lower-boundary rule above, for every pipeline and arm fit including the held-out fits.

A pipeline passes when all five hold. Pipeline C has two referenced seasons, so its held-out check is a two-season transfer; report it and do not read it as an estimate of the between-season dispersion for 2020 to 2022.

## Bounds for pipeline A

Pipeline A has no reference. The declared assumptions are:

- The clue distribution given the true band, `P(clue | band)`, transports across pipelines up to a slack `delta` per clue level. The clue is the result family (hit, out in play, other) by fielder group (infield, outfield, none or unknown) by recorded depth (Shallow, Deep or ExtraDeep, Default or unrecorded), 27 levels. `delta` for each level is twice the absolute difference between the pipeline B and pipeline C clue tables for that level, with a floor of 0.01.
- A recorded label is not anti-informative: for every band, the probability of the band given its own recorded label is at least the probability of that band given any other recorded label. This holds in pipeline B by a wide margin.
- Rows of the translation are distributions.

For a season, the identified set is every pair of a translation `T[recorded, band]` and a band mix `pi` such that `sum_recorded P(recorded | season) T[recorded, band] = pi[band]` and `|sum_band pi[band] P(clue | band) - P(clue | season)| <= delta[clue]` for every clue level, with the two constraints above. Pipeline A seasons use the pipeline B clue table, the adjacent pipeline with five referenced seasons. Bounds on each cell of `T` and on `pi` are linear programs. The declared prior is uniform over the identified set, sampled by hit-and-run with 4,096 draws and root seed 20260911 from a feasible interior point. The prior mean and the bounds are both reported; neither is a validated translation. A pipeline A season whose identified set is empty at the declared `delta` is reported as unbounded: the transport assumption fails for that season at the declared slack, and the slack is not widened. For each such season the smallest uniform multiple of `delta` that makes the set nonempty is reported as a diagnostic only.

Validation: apply the same procedure to each referenced season of pipelines B and C using the other pipeline's clue table and the season's own recorded label shares and clue margins. The set must contain the season's actual `P(band | recorded)` in every cell for every one of the seven referenced seasons. Report the width of the bounds. If any cell falls outside its bounds, the assumptions are rejected at the declared `delta` and pipeline A stays unbounded; do not widen `delta` after seeing the result.

## Evidence

Before scoring, bind this protocol, every loaded `python_models` module, the two reference artifacts, the research database schema and size, the reserve digest, package versions, and seeds. Write checkpoints per fit. Retain the reference frame, cell counts, concentration posteriors, quadrature diagnostics, event-level held-out probabilities, per-game scores, predictive count draws, the clue tables, and the bounds. A smoke run on a subsample makes no decision.

Passing these screens establishes that the season level inside a referenced pipeline carries the observed between-season variation and that pipeline A has declared, validated bounds. It does not establish independent modern confirmation, missing-label validity, reference measurement quality, scorer authorship, or publication readiness, and it does not authorize publishing historical reconstructed facts.
