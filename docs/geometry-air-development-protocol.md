# Pooled airborne translation development

Experiment: `geometry-air-development-v1`. This is iterative development on the already inspected 373-game modern fitting collection, not independent confirmation. The 121 deferred modern-angle evaluation games and the historical confirmation reserve remain unopened. The full historical reliability goal is unchanged.

## Question and population

Can a recorded airborne subtype and the recorded play result predict the standardized airborne subtype more usefully than either field alone? The target is `trajectory-air-standard-v1`: preserve local ground versus air; use angle only for airborne subtype. Label translation, reconstruction with a fine label missing, and reconstruction without a broad ground/air observation are different tasks.

Read only the content-bound fitting acquisition. Keep every matched event in the frame and coverage accounting. Fit and score the primary translation experiment only where local recorded broad type is Air and the canonical status is `air_angle_standardized`. Source conflicts, missing angles, sub-10-degree airborne observations, unknown broad type, and ground balls are counted separately. Statcast category and numerical angle never enter predictors. Bunt subtypes normalize to the three airborne labels while the original label and bunt attribute remain in the frame.

Ground preservation receives a separate deterministic check and contributes no correct predictions to airborne accuracy or calibration. No ground/air reconstruction model is claimed by this experiment. A source-block mask that also removes broad type must return unsupported, rather than reuse the hidden reference broad type.

## Models fixed before predictive scoring

Class order is Fly, LineDrive, PopUp. These are categorical count models with explicit smoothing, not a fitted latent scorer model or posterior uncertainty claim.

1. Marginal reference: pooled training class counts plus one per class.
2. Result reference: training counts within result family, shrunk toward the marginal distribution with 30 prior observations.
3. Recorded-label reference: training counts within recorded airborne subtype, shrunk toward the marginal distribution with 30 prior observations.
4. Candidate translation: counts within recorded subtype by result family, shrunk toward the recorded-label reference with 30 prior observations.

All probability denominators use training rows only. Unknown result levels fall back to the corresponding recorded-label reference; with the recorded subtype masked, the candidate uses the result reference. No person, scorer proxy, park, season, player ID, fielder, or Statcast quantity is a predictor. The same game exclusions and training rows apply to every arm. Fixed smoothing strengths 3 and 300 are sensitivity runs; they cannot replace the primary value 30 on the basis of their scores.

## Development splits

For game splits, take the integer SHA-256 of `geometry-air-development-v1:game:` plus game ID modulo five. For park splits use `geometry-air-development-v1:park:` plus park ID modulo five. Produce out-of-fold predictions for every eligible event, fitting each fold on its complement. Leave one season out in each of 2015, 2019, 2023, and 2025. Never split a game across fit and evaluation. The leave-2015-out result is a limited modern backward-transfer check, not evidence for pre-Statcast transport.

Export every fit's class counts, fitted probability specification, training/evaluation game IDs and counts, and event predictions. Export the original all-event coverage frame or its content binding. Null or unsupported cases remain in summaries. The naturally missing fine-label population is not represented by a random mask; a masked-label run is only an information-availability stress check.

## Scoring and decision

Report multiclass log loss, summed multiclass Brier score, classwise 15-bin ECE, and predicted-minus-observed class shares. Resample whole evaluation games for 500 paired score bootstrap replicates with seed 20260911. These intervals describe development score uncertainty conditional on the fitted arms; they are not posterior parameter or future-cohort predictive intervals. Preserve per-game score and class totals so dependence is explicit.

The primary development screen requires the candidate to improve both log loss and Brier over the result and recorded-label references, with both 95% paired gain intervals strictly above zero, in each split family. It also requires ECE at most 0.05 and absolute class-share bias at most 0.02 for the full out-of-fold population and each season supported by at least 30 games and 500 eligible airborne events. Report every smaller slice and its support rather than assigning it a pass. Report park and scorer-proxy slices descriptively; the available small cohorts do not establish separate effects. A sensitivity run that changes the primary pass/fail verdict is a robustness failure requiring investigation, not a substitute winning specification.

The model is not accepted for historical reconstruction even if this screen passes. Remaining gates include a frozen independent modern-angle evaluation, measurement-selection sensitivity for unresolved targets, genuinely missing-label validation, historical transport, source/scorer identification, shared uncertainty draws, and aggregate coverage. Failed runs remain preserved. Any model or acceptance revision becomes a new declared experiment.
