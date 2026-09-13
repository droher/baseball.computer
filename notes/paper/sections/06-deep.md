## Deep-learning supplements

### Intended role

The intended architecture uses a deep classifier as a proposal distribution,
never as recorded truth:

$$\text{deep proposal}\longrightarrow
\text{out-of-fold probabilities}\longrightarrow
\text{Bayesian layer}\longrightarrow
\text{estimated surface}.$$

An argmax class is insufficient because it discards uncertainty. In the legacy
geometry models the proposal entered as
$\gamma\log\widetilde p^{dl}_{i,c}$, with one scalar $\gamma$ shared across
classes and a $\mathrm N(0,0.5)$ prior. Separate `gamma_dl_zero` and
`gamma_dl_shrunk` flavors were meant to show whether the learned proposal added
signal beyond the hierarchical context model.
<!-- src: notes/data-coverage-implementation/04-deep-learning-supplements.md -->
<!-- src: bc/python_models/statistical/models/geometry.py -->

This remains a design, not current publication evidence. The isolated September
13 migration contains four deep geometry pointers and one event-universe pretrain
pointer. All five have overall status `failed`. Each geometry proposal has
numerical evidence marked passed but predictive and calibration evidence marked
failed; transport, identification, and provenance are unsupported. Every evidence
dimension for the pretrain pointer is unsupported. Explicit exploratory reasons
make the compatibility pointers well formed, but do not validate the models.
<!-- src: artifacts/imputation/legacy-publication-candidate-v1/migration_summary.json -->

### Target semantics and missing proposals

The old target named `location_side` was misdescribed. Its six categories are
within-zone angle modifiers, including `Default`, and are now exposed by the
legacy wrapper as `location_angle`. Development scores for that target cannot be
interpreted as global left/center/right field-side reconstruction. In the new PBP
geometry interface, global side is derived from general location and remains
separate from angle, depth, and edge.
<!-- src: docs/modeling-evidence-contract.md -->
<!-- src: artifacts/imputation/legacy-publication-candidate-v1/README.md -->
<!-- src: docs/pbp-imputation.md -->

The legacy location proposal specifications also scored observed rows but did not
supply predictions on the unrecorded production rows they were intended to help
impute. Centering a missing zero vector by the training-class mean then introduced
a constant class-specific logit shift. The repair made a missing proposal
contribute zero and selected deep-free flavors where no production proposal
existed. This is a valid implementation finding. It does not make the repaired
legacy artifacts pass the current evidence contract.
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->
<!-- src: bc/python_models/statistical/bayes/dl_covariate.py -->

### What the retained development reports support

The September 4 review reported modest TEST-partition improvements over
result-family marginals for trajectory, angle, depth, and edge. It also reported
larger held-out gains when the trajectory proposal entered the Bayesian model.
Those figures remain dated development observations. The strongest ablation claims
from the previous draft depended on zero-flavor artifacts and per-cell shift files
that are no longer retained, so their exact log-loss deltas and shares of cells
moving more than 0.25 posterior standard deviations are omitted here.

Gate version 3 now requires aligned model and baseline probabilities on identical
`event_key + partition` rows, declared class truth, improvement in log loss and
Brier score, and classwise calibration evidence. The four migrated deep pointers
fail because the retained artifacts do not provide acceptable baseline and
calibration evidence under that contract. An older numerical pass or a top-1
accuracy improvement cannot substitute for these missing checks.
<!-- src: docs/modeling-evidence-contract.md -->

### Leakage in the legacy stack

Two distinct leakage paths prevent confirmatory interpretation. First, the shared
event-universe pretrain used geometry labels from the whole `TRAIN` partition,
then downstream fold models warm-started embeddings from that fit. A nominally
out-of-fold row could therefore influence its own embedding through pretraining.
The deep partition and Bayesian game-hash holdout were unrelated, so this
contamination also reached part of the Bayesian held-out evaluation.

Second, a trajectory target configured with `fold_count=1` followed a full-fit
fallback while labelling its predictions as out of fold. The later five-fold
repair addressed that direct error, but it did not repair the shared-pretraining
leak or reconstruct complete lineage for the retained artifacts. The dependency
manifest needed to identify exactly which pretrain produced each proposal is also
absent.
<!-- src: notes/data-coverage-implementation/modeling-review-2026-09-03.md -->
<!-- src: docs/modeling-evidence-contract.md -->

The scientific cross-fitting contract is stricter than a prediction-scope label.
Every supervised stage that can encode the target must be fit inside the outer
training games. Predictions must be generated on disjoint games, carry explicit
scope and class labels, and be compared with a baseline on the same rows. Any
calibration stage must also be trained without the evaluation rows. The retained
legacy stack does not demonstrate that complete contract.

### Relation to the September 13 completion layer

The full-history PBP imputation candidate does not require a new deep fit. It uses
recorded values, deterministic derivations, empirical donor distributions,
constrained reconciliation, and explicit broad fallbacks. Legacy probabilities
may be carried as exploratory inputs where configured, but a complete candidate
row does not inherit scientific validation from an old pointer. The builder keeps
the method and artifact identity visible so consumers can distinguish a recorded
fact, a deterministic rule, a transported estimate, and a broad prior.

This separation is deliberate. The coverage objective accepts rough estimates across the acquired historical
PBP population. More deep training would answer a different
question and would require fresh lineage, leakage-free outer folds, retained
baseline predictions, calibration, transport tests, and untouched confirmation
data. None of that work was performed for the September 13 candidate.
<!-- src: docs/pbp-imputation.md -->
<!-- src: notes/full-history-imputation-plan.md -->
