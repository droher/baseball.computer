# Airborne source and measurement bridge

This design follows the failed [pooled development experiment](geometry-air-development-results-2026-09-11.md). It specifies the next scientific questions; it is not an executed model or an independent evaluation protocol. The 121 deferred angle outcomes and the historical confirmation reserve remain sealed.

The next revision now has a separate [frozen development protocol](geometry-air-regime-posterior-protocol.md) and [computational validation](geometry-air-regime-computation-2026-09-11.md). Its hierarchical posterior shares translation uncertainty within cells and between represented regimes. Simulation recovery passes its necessary screen; real-data predictive acceptance remains outstanding.

## Quantities to keep separate

For each documented batted ball, retain the original source classification, its broad Ground/Air grouping, the standardized reference category, and the provenance of the numerical measurement used to construct that reference. The [airborne target contract](geometry-air-standard-target-contract.md) remains authoritative: preserve known Ground/Air, standardize airborne subtypes, and retain bunt separately.

The immediate estimand is the distribution of the standardized **reference label** given an available recorded airborne subtype, recorded result, and supported source regime. A model that predicts this reference well has not necessarily recovered an independently measured flight path. A separate historical-label output estimates what the source would record when that label is missing. These two conditional distributions cannot be obtained by simply renaming the same prediction column.

The observation process has at least three layers:

1. The ball's physical flight and the play outcome.
2. The observer or provider's recorded category, potentially affected by viewing position and convention.
3. The tracking system's numerical output, which may combine measurements and estimates using information from the second layer and the outcome.

The third layer is not conditionally independent of the first two merely because it supplies a number. [Savant's documentation](https://baseballsavant.mlb.com/csv-docs) and its linked [estimation explanation](https://tangotiger.com/index.php/site/article/statcast-lab-no-nulls-in-batted-balls-launch-parameters) establish this risk. The accepted fitting data do not identify which individual angles were directly measured.

A [2009 primary analysis by Harry Pavlidis](https://tht.fangraphs.com/batted-ball-insanity/) combines HITf/x launch measurements with observations supplied by a named Gameday stringer. It provides concrete evidence that a stringer could supply the category field, and that pre-Statcast physical measurements existed for a limited period. It does not establish authorship for this project's historical rows. Availability, licensing, game selection, measurement provenance, and overlap exposure of any such older tracking data must be verified before treating it as a historical anchor; no dataset from that article has been acquired for this work.

## Supported development comparison

Use the same 373 fitting games and local-only prediction inputs. Retain the pooled model as a failed reference. Compare a bridge that permits different recorded-to-reference mappings in the two observed era groups, 2015/2019 and 2023/2025. These groups describe observed data regimes. They do not certify the exact date, provider, hardware, or person responsible for the change.

Keep result-only and recorded-subtype-only references within the same regime structure so an improvement cannot be credited solely to giving one arm era information. Shared effects and regime deviations need partial pooling and uncertainty; the small number of observed seasons does not support a freely estimated historical time trend. Unknown regimes require an explicit unsupported status or a separately reported sensitivity result, not silent assignment to the earliest available group.

Whole-game and park holdouts assess prediction within represented regimes. Leave-one-season-out prediction can use the other observed season in its group; it tests limited within-regime transfer. Leaving out an entire regime is a distinct extrapolation stress test. None establishes transfer to pre-2015 baseball. Year, park, and header scorer identifiers must not be interpreted as separately identified physical or personal effects.

Translation with a recorded fine label, reconstruction with that label masked, and reconstruction without a broad Ground/Air record remain separate evaluation populations. Removing the source block must remove all source-derived classification clues together. Random fine-label masking is an information test, not evidence that naturally missing records follow the same selection process.

## Measurement and selection checks before confirmation

- Determine whether a documented row-level measurement-origin or quality field can be obtained for the existing fitting games. Any added source must be archived and bound to the same game selection. Do not infer origin from angle availability, round numbers, outcome, or agreement between categories.
- If origin remains unknown, assess sensitivity to a declared fraction of potentially estimated or misclassified reference labels, separately by supported regime. Such fractions are assumptions, not estimated tracking-failure rates. Report how much contamination would erase the claimed improvement or calibration result.
- Preserve all unresolved local-Air rows in applicability denominators. Missing angles, sub-10-degree Air conflicts, and broad-source conflicts require separate accounting. Class-share bounds that assign unresolved cases adversarially can show where complete-case conclusions are unsupported.
- Verify historical category authorship where possible. Official-scorer header identity alone does not identify the person or system that supplied a batted-ball category. The user is the available baseball SME; an answer about historical practice informs assumptions, while individual source assertions still need provenance.

Both selection uncertainty and uncertainty in the reference measurement must accompany any physical interpretation. A modern reference-label model may be useful within its supported collection even when the physical estimand remains weakly identified; report that distinction explicitly.

The first [reference sensitivity diagnostic](geometry-air-reference-sensitivity-2026-09-11.md) is complete. It retains all 899 unresolved local-Air cases and finds that about 1.05–1.07% worst-case relabelings erase the pooled model's log-loss advantage over the recorded-label baseline. These assumptions must remain visible in the next experiment; their values do not estimate how often Statcast angles were imputed or incorrect.

## Required uncertainty and acceptance specification

Before executing the next scored comparison, freeze the likelihood or smoothing model, regime pooling, priors, supported fallback behavior, folds, sensitivity grid, and acceptance rules. Preserve the previous failures and thresholds; do not relax season calibration to obtain a pass.

For a probabilistic model, use shared parameter draws across events in the same draw, then generate replicated class counts when testing future aggregate coverage. Independent row draws from a fixed mean vector omit shared estimation uncertainty. Validate computation with a small real-data smoke loop and simulated recovery appropriate to the fitted model before a full run. A score bootstrap conditional on fixed fits remains a score-uncertainty tool, not a substitute posterior.

Artifacts must bind source selection, target contract, code, and experiment specification; retain every event's coverage status, support regime, reference origin status, probability vector, and joint draw identifier where applicable. Modern held-out calibration, historical transport, naturally missing-label validity, and aggregate uncertainty each need their own verdict. Passing one does not clear the others or authorize publication.
