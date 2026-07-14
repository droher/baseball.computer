## Publication policy

Every `main_models.*` table carries one of five publication tiers, and the tier
is a property of the table, assigned centrally rather than inferred by a
consumer from column names. `PublicationTier` enumerates `official`,
`deterministic`, `estimated`, `synthetic`, and `withheld`, and a single
`PUBLICATION_TIERS` registry maps each published model name to its tier.
<!-- src: notes/data-coverage-implementation/phase5-conventions.md --> The
tiers read, in the rollout design's own words: `official` is an authoritative
source value at the target grain; `deterministic` is a rule-based value
computed from canonical inputs; `estimated` is a posterior expected value,
probability, or interval; `synthetic` is a generated row or value drawn from
aggregate-only or synthetic inputs; `withheld` is a quantity that was
specified but did not clear validation or identification and is not published
at all. <!-- src: notes/data-coverage-implementation/06-rollout-and-validation.md -->
Models B, I, and K — discussed in the previous section — sit in `withheld`
today: designed, in two cases prototyped, and not published, because no
amount of additional modeling substitutes for an identifying source of
variation the record does not contain.

The `estimated` tier is the one this paper's results section draws from, and
every row in it carries the same eight-column contract: `artifact_id` (the
fit that produced the row), `model_name`, `model_version`, `source_snapshot_id`,
`method` (`hierarchical_logistic`, `hierarchical_bayes_softmax`, or
`hierarchical_bayes_nb`), `observed_status` (the constant `estimated`, a
namespace marker), `confidence_status` (the fit's validation status as of the
run that produced the row — see below), and `weak_identification_flag` — set
from the fit's own diagnostics when a convergent posterior is nonetheless
weakly identified in some slice: low group-level effective sample size, a
r-hat above the comfort band, any divergence, or a non-finite diagnostic.
<!-- src: notes/data-coverage-implementation/phase5-conventions.md --> <!-- src: docs/estimated-models.md -->
Diagnostic columns such as ESS and r-hat are deliberately absent from the
published rows — they belong in the validation reports that gate publication,
not in the table a downstream consumer joins against.
<!-- src: notes/data-coverage-implementation/phase5-conventions.md -->

`confidence_status` is stamped once, at fit time, from whatever gate suite was
current when the artifact was produced, and it does not move on its own after
that. Every populated table's rows read `exploratory` today because they were
all stamped before the gate suite in §7's Validation subsection existed.
<!-- src: docs/estimated-models.md --> That gate suite is now a runnable
check rather than a manual phase-6 checklist — `just validate-gates` sweeps
every published pointer and reports pass/fail/missing per artifact, and 18 of
23 registered targets pass today. <!-- src: tables/validation_gates.md -->
Passing the sweep is necessary but not sufficient for a row to say `passed`:
the stamp is written by the publish path that produced the row, not
retroactively by a later gate run, so moving a table's `confidence_status`
from `exploratory` to `passed` requires re-publishing it — restating the
`@model` under the current gate suite so the stamp is written fresh. That
re-publication has not happened for any table in this paper; the gate results
in §7 describe what a restated run would be stamped with, not what the
published rows are stamped with today.

Posteriors are published as distributions, and this is enforced structurally
rather than by convention. The event-level imputation tables — geometry,
ball-handler, fielding credit — ship a `class_label` and an `expected_share`
per class, with shares summing to one over an event's class set; none of them
persists a single predicted label. A consumer who wants a top class computes
it at query time, typically with `QUALIFY ROW_NUMBER() OVER (PARTITION BY
event_key ORDER BY expected_share DESC) = 1`, the pattern `docs/estimated-models.md`
documents for `imputed_batted_ball_geometry`.
<!-- src: docs/estimated-models.md --> An argmax convenience column may be
emitted for inspection, but it is never the canonical representation any
downstream consumer is meant to read, and the shipped coverage tables do not
persist one — the full posterior is the product.
<!-- src: notes/data-coverage-implementation/03-hierarchical-models.md -->

Estimated surfaces never overwrite a deterministic legacy table. Where a
legacy point surface already exists — `linear_weights`, `park_factors`,
`run_expectancy_matrix` — its columns are left untouched at the
`deterministic` tier, and the Bayesian counterpart is published as a sibling
model under an explicit `_estimated` suffix, carrying the point estimate and
its uncertainty side by side.
<!-- src: notes/data-coverage-implementation/phase5-conventions.md --> The two
are not merged into one view, because their grains can differ — `park_factor_summary`
carries an `outcome` dimension the deterministic `park_factors` does not — so
the compatibility rule is "keep the legacy table and add a sibling," not
"join and replace." <!-- src: notes/data-coverage-implementation/phase5-conventions.md -->
An official-only consumer reads the legacy table and notices nothing; an
estimated-aware consumer opts into the sibling and its HDI.

The last rule is about what happens to a quantity that is genuinely uncertain
but not unidentifiable. Fielding credit on a no-box-score event is always
published, never withheld for insufficient confidence — the design commits to
a confidence column carrying that uncertainty rather than a hard inclusion
cutoff that would silently drop the sparse, hard cases from the record.
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->
That is the difference between `estimated` and `withheld` in practice:
`estimated` is the tier for "we don't know for certain, and we can say how
much we don't know"; `withheld` is reserved for "no model of this quantity is
identified by the data at hand," and it is used sparingly — three model
letters out of eleven attempted, each with its blocking condition written
down rather than papered over with a wide interval.
