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

Statcast is the reference for the standardized output. Its [CSV documentation](https://baseballsavant.mlb.com/csv-docs) exposes `bb_type` separately from launch angle and exit velocity, and states that some untracked numerical values are estimated. We must verify the relationship between the published categories and angle bands on matched modern plays before treating either as an independent physical measurement. Bunt status must remain distinct from the standardized four-category trajectory ontology.

The existing hierarchy is a recorded-label control, not a solution to standardized reconstruction. Its full fits are deferred while a small, primary-TRAIN-only Statcast bridge is investigated. The local source inventory has no configured Statcast tracking fields. New acquisition must select known TRAIN games before fetching play labels and explicitly exclude the sealed reserve.

## Current implementation step

Investigate source-specific failures using existing development artifacts. In parallel, build one exchangeable hierarchical candidate with a persistent result-family effect, era effects, and era-by-result deviations. Unknown eras receive shared random-effect draws, preserving result information and representing uncertainty. This is a candidate to test, not an assertion that exchangeable eras explain historical trends.

Use only primary TRAIN for new model development, with game-level internal evaluation and temporal/source blocks. The original 100,000-row TRAIN selection remains the fitting budget before exclusions. Export a narrow full-primary-TRAIN frame for evaluation so older side cohorts are not reduced to tiny samples by the fitting subsample. Verify parity against the original selected TRAIN and full-TRAIN counts. No new TEST or VALIDATE labels enter this development extract. Existing TEST failures may motivate hypotheses and source diagnosis but do not fit or select the new candidate.

Keep the confirmation reserve sealed throughout development. Do not promote or publish based on an intermediate screen. Any later model, prior, calibration, or acceptance-rule revision gets a new declared experiment; failed attempts remain in the evidence.

## Methodological boundaries

The weak ground/air experiment measures cross-field agreement with recorded labels, not physical accuracy: its candidate clues can share scorer and source errors with the labels. Its row-level Wilson intervals are descriptive under an independence assumption, not valid game-cluster uncertainty. Both internal folds were inspected, so any model selected using this experiment must call subsequent scores on those folds exploratory. A new split does not erase that exposure. The frozen hierarchy remains a separate control. Explicit reserve binding and complete upstream provenance remain prerequisites for treating the weak-clue artifact as boundary evidence.

Full TRAIN extracts for both targets match the earlier selected rows and class counts and exclude reserved games. Unscored trajectory and side smoke fits completed. The [Statcast investigation](geometry-scorer-statcast-findings-2026-09-11.md) verifies a 52-ball modern connection and records the distinction between downloaded labels and angle definitions. The [exposure ledger](geometry-development-exposure.md) makes reused TRAIN evaluation explicitly exploratory. Standardized reconstruction, source selection, aggregate uncertainty, and independent confirmation remain unvalidated.

Simulation-based calibration checks the computation under a generative model; it cannot establish that the model describes historical recording. See the [Stan guidance](https://mc-stan.org/docs/stan-users-guide/simulation-based-calibration.html). Posterior predictive checks compare replicated observations with observed quantities; the [predictive-check guidance](https://mc-stan.org/docs/2_38/stan-users-guide/posterior-predictive-checks.html) motivates separate checks of aggregate variation. The project's thresholds and transfer experiments are substantive choices and must be declared explicitly.
