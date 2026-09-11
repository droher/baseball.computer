# Geometry reliability work

Goal: make trajectory and corrected global-side estimates reliable enough for historical reconstruction, including credible uncertainty for missing records. The goal remains active. The completed [stress test](historical-geometry-stress-2026-09-11.md) establishes failures that must be addressed, not a completed reliability claim.

## Required evidence

| Requirement | Current evidence | What remains |
|---|---|---|
| Correct estimand and source semantics | Side corrected; recorded, mapped, and inferred values separated | Explain source-specific observation/label selection before interpreting missing outcomes |
| Sound fitting and reproducibility | Existing references pass numerical and artifact checks | Repeat for each replacement, including simulation recovery and prior sensitivity |
| Useful predictions | Existing overall comparisons improve contextual baselines | Improvements must survive era and whole-scorer blocks |
| Historical calibration | Backward and decade-level failures | Validate class probabilities and aggregate shares in supported historical cohorts |
| Honest uncertainty | Existing checks condition on fitted models | Shared posterior draws, observation variation, game dependence, and held-out aggregate coverage |
| Applicability to naturally unknown records | Context availability mapped; large observed-support gaps | Outcome-dependent recording sensitivity, source connectivity, and explicit accounting for the entire intended population |
| Independent confirmation | 6,105 games sealed for this component family | Lock the model, selection/calibration procedure, metrics, and thresholds before scoring once |
| Safe downstream use | Production unchanged | Versioned probability/draw artifacts and audited consumers after scientific validation |

All requirements matter. A narrower subset passing an average score does not complete this goal. Unsupported rows remain in coverage denominators and are not silently dropped to obtain a pass. Entirely absent games remain outside event-level reconstruction because the event population itself is unavailable; this does not license fabricated plays.

## Agreed baseball interpretation

The user requests two distinct outputs: the original scorer's likely classification and a consistent classification based on the current Statcast definition. Ground versus air is a strong but imperfect agreement assumption. Fine distinctions among line drives, flies, and pop-ups require a separate observation model for scorer judgment and press-box perspective. Physical park effects must remain separate from those measurement effects; the available historical records may not identify them individually.

Statcast is the reference for the standardized output. The user clarified that ground versus air must be preserved, with launch angle standardizing only airborne subtypes. The [current target contract](geometry-air-standard-target-contract.md) implements that distinction, including unresolved angle/source conflicts. Bunt is a separate attribute and never a reason to reclassify a ground ball as a line drive or fly. The [CSV documentation](https://baseballsavant.mlb.com/csv-docs) exposes category separately from angle and exit velocity and states that some numerical values are estimated; matched data confirm that categories are not literal angle bins.

The existing hierarchy is a recorded-label control, not a solution to standardized reconstruction. Its [full side experiment](geometry-side-hierarchical-results-2026-09-11.md) converged but failed historical calibration, including a 12.08-point excess predicted Middle share in backward extrapolation. The trajectory replacement must use the new target and measurement process. New Statcast acquisition selects known TRAIN games before fetching play labels and explicitly excludes the sealed reserve.

## Current implementation step

The exchangeable hierarchical control with persistent result effects is implemented and tested. Its full side results reject the claim that pooling alone fixes historical reconstruction. The [handler-clue provenance diagnostic](geometry-side-clue-provenance-2026-09-11.md) also rejects a refit that simply adds fielder fields: all 4,279,727 derived-side rows already use a known batted-to fielder, while none of the 615,501 naturally unknown rows has one. Only nine unknown rows have any known position elsewhere in a fielding chain. These are correlated outputs of the same play record and must be jointly removed under source-block masking. A new side model requires an independent anchor or an explicit, testable observation model.

For trajectory, use the modern paired fitting data to develop a standardized reference model under the airborne-only contract, with explicit prediction-time broad constraints and realistic feature masking before modern evaluation. Distinguish translation of an available recorded subtype from reconstruction with that subtype absent; success on one task does not validate the other.

The [modern identification audit](geometry-modern-observation-identifiability-2026-09-11.md) finds 119 scorer proxy values but 17 disconnected scorer-park components and no source-category contrast. More fundamentally, the upstream parser conflates official `oscorer` and administrative `scorer`, retaining whichever occurs last. Preserve those fields separately before attempting person-specific corrections, and verify label authorship independently of the header identity. This repair is required follow-up work, not an assumption that current artifacts have been repaired.

The next trajectory experiment should begin with a pooled airborne translation, with no person-specific interpretation. Compare it with a pooled marginal and result-conditioned reference on identical resolved airborne outcomes, retaining all unresolved rows in coverage and missingness summaries. Use whole-game and season/park blocks within the already exposed fitting collection; these are development checks. Score preservation of known ground separately so deterministic ground preservation cannot inflate the airborne model's apparent performance. Freeze the model, feature availability scenarios, uncertainty procedure, metrics, and acceptance rules before acquiring the 121 modern-angle evaluation games. Historical transport and missing-label reconstruction remain separate gates even if modern translation passes.

Use only primary TRAIN for new model development, with game-level internal evaluation and temporal/source blocks. The original 100,000-row TRAIN selection remains the fitting budget before exclusions. Export a narrow full-primary-TRAIN frame for evaluation so older side cohorts are not reduced to tiny samples by the fitting subsample. Verify parity against the original selected TRAIN and full-TRAIN counts. No new TEST or VALIDATE labels enter this development extract. Existing TEST failures may motivate hypotheses and source diagnosis but do not fit or select the new candidate.

Keep the confirmation reserve sealed throughout development. Do not promote or publish based on an intermediate screen. Any later model, prior, calibration, or acceptance-rule revision gets a new declared experiment; failed attempts remain in the evidence.

## Methodological boundaries

The weak ground/air experiment measures cross-field agreement with recorded labels, not physical accuracy: its candidate clues can share scorer and source errors with the labels. Its row-level Wilson intervals are descriptive under an independence assumption, not valid game-cluster uncertainty. Both internal folds were inspected, so any model selected using this experiment must call subsequent scores on those folds exploratory. A new split does not erase that exposure. The frozen hierarchy remains a separate control. Explicit reserve binding and complete upstream provenance remain prerequisites for treating the weak-clue artifact as boundary evidence.

Full TRAIN extracts for both targets match the earlier selected rows and class counts and exclude reserved games. Unscored trajectory and side smoke fits completed. The [Statcast investigation](geometry-scorer-statcast-findings-2026-09-11.md) verifies a 52-ball modern connection and records the distinction between downloaded labels and angle definitions. The [exposure ledger](geometry-development-exposure.md) makes reused TRAIN evaluation explicitly exploratory. Standardized reconstruction, source selection, aggregate uncertainty, and independent confirmation remain unvalidated.

The [modern acquisition protocol](geometry-statcast-development-protocol.md) selects 373 fitting games and 121 deferred modern-angle evaluation games from four seasons and 128 park-season strata. The [completed fitting audit](geometry-statcast-fitting-audit-2026-09-11.md) verifies all 19,436 batted-ball matches and 18,480 resolved canonical targets; 956 unresolved cases remain in the denominators. The user's airborne-only clarification supersedes the acquisition protocol's original unconditional angle target while preserving its game selection. No standardized prediction model or modern-angle evaluation has been fit or scored from this collection.

Simulation-based calibration checks the computation under a generative model; it cannot establish that the model describes historical recording. See the [Stan guidance](https://mc-stan.org/docs/stan-users-guide/simulation-based-calibration.html). Posterior predictive checks compare replicated observations with observed quantities; the [predictive-check guidance](https://mc-stan.org/docs/2_38/stan-users-guide/posterior-predictive-checks.html) motivates separate checks of aggregate variation. The project's thresholds and transfer experiments are substantive choices and must be declared explicitly.
