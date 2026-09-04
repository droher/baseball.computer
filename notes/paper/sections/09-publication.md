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
Models B, I, and K — discussed in the previous section and mapped in §4's
table — sit in `withheld` today: designed, in two cases prototyped, and not
published, because no amount of additional modeling substitutes for an
identifying source of variation the record does not contain.

The `estimated` tier is the one this paper's results section draws from, and
every row in it carries the same eight-column contract: `artifact_id` (the
fit that produced the row), `model_name`, `model_version`, `source_snapshot_id`,
`method` (`hierarchical_logistic`, `hierarchical_bayes_softmax`, or
`hierarchical_bayes_nb`; the pitch-summary table stamps the softmax method, since
it is a 12-class multinomial), `observed_status` (the constant `estimated`, a
namespace marker), `confidence_status` (see below), and
`weak_identification_flag`. <!-- src: notes/data-coverage-implementation/phase5-conventions.md --> <!-- src: docs/estimated-models.md -->
The flag is derived from group-level diagnostics: the worst r-hat and smallest
bulk ESS among the posterior variables with at most 512 elements — scalars,
pooling hyperparameters such as the negative-binomial dispersion or the
park-persistence $\rho$, and small group-level effects — not the worst element
among thousands of per-cell or per-scorer parameters. A convergent fit is flagged
when that group-level ESS falls under 400, the group-level r-hat exceeds 1.025,
any divergence was recorded, or a diagnostic is non-finite. The sweep described
below recomputes the flag from the stored posterior, so it reflects the current
thresholds rather than the ones in force when the fit ran.
<!-- src: bc/python_models/statistical/validate.py --> <!-- src: docs/estimated-models.md -->
Diagnostic columns such as ESS and r-hat are deliberately absent from the
published rows — they belong in the validation reports, not in the table a
downstream consumer joins against.
<!-- src: notes/data-coverage-implementation/phase5-conventions.md -->

`confidence_status` is copied onto the rows when the table is materialized, from
the `validation_status` the gate sweep stamped on the fit's manifest. Only the
sweep writes that stamp — `just validate-gates --write` re-derives every
artifact's diagnostics from its stored posterior, grades it, and records the
status together with the gate version that graded it — and a manifest that was
never swept, or was swept under an older gate version, materializes as
`exploratory` no matter how its checks went. <!-- src: bc/python_models/statistical/bayes/manifest_ingest.py --> <!-- src: bc/python_models/statistical/validate.py -->
The version stamp is what makes a `passed` row mean something: adding a gate or
raising a severity bumps the version, and every artifact must be re-swept before
it publishes as `passed` again. The publish path refuses to write a pointer for a
smoke-budget fit, so the smoke thresholds the sweep applies to such fits can no
longer reach a published table. <!-- src: bc/python_models/statistical/cli.py -->
The history of the column is part of the record: the tables were first published
as `exploratory`, restated as `passed` on 2026-07-30 after the first sweep,
reverted to `exploratory` when this revision bumped the gate version, and read
`passed` again on every populated table since the version-2 sweep and the
restate of 2026-09-04 <!-- src: tables/table_inventory.md -->. The gate results
in §7 describe what the current fits are graded, not what a row said on any
earlier date.

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
letters out of eleven, each with its blocking condition written down rather
than papered over with a wide interval.
