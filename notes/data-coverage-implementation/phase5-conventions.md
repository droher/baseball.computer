# Phase 5 conventions (chosen — the design docs leave these open)

The design docs name the five tiers and the estimated metadata contract but do not specify the
assignment mechanism, the contract column sources, or the compat-view naming. These are the choices
made for the build.

## Publication tiers

`PublicationTier` enum in `bc/python_models/statistical/publication_tiers.py`: `official`,
`deterministic`, `estimated`, `synthetic`, `withheld`. A central `PUBLICATION_TIERS:
dict[str, PublicationTier]` maps each published `main_models.*` model name to its tier. The nine
data-coverage coverage tables are `estimated`; the three legacy point surfaces
(`linear_weights`, `park_factors`, `run_expectancy_matrix`) are `deterministic`. The registry is the
declared tier source; the estimated contract is currently stamped in `manifest_ingest` and enforced
by the `estimated_contract_complete` audit directly. Deriving those target sets from the registry
(so it becomes the mechanical source of truth) is a tracked follow-up.

## Estimated metadata contract

Every `estimated` table carries these columns (stamped per artifact, constant within an artifact's
rows). Sources:

| column | source |
| --- | --- |
| `artifact_id` | `manifest.artifact_id` (renamed from the old `bayes_artifact_id`) |
| `model_name` | `manifest.bayes_extras.model_name` |
| `model_version` | `manifest.bayes_extras.model_version` |
| `source_snapshot_id` | `manifest.source_snapshot_id` |
| `method` | per-family constant: `hierarchical_bayes_softmax` (multinomial shares), `hierarchical_bayes_nb` (count summaries), `hierarchical_logistic` (bernoulli propensity / coverage) |
| `observed_status` | constant `estimated` (these surfaces are model estimates, not source values) |
| `confidence_status` | `manifest.validation_status` (`exploratory` / `passed` / `failed`) |
| `weak_identification_flag` | `manifest.bayes_extras.weak_identification_flag` (new field, default `false`; populated at fit time from the publication gate) |

Diagnostic columns (`ess_bulk`, `rhat`) are removed from the published summary surfaces — they belong
in `validation/`, not the published estimated tier. Point + uncertainty columns (mean / sd / HDI or
share) stay.

## Compatibility views

Legacy point surfaces keep their columns unchanged (the `deterministic` tier). The estimated
counterpart is a sibling `main_models.*` model under an explicit `_estimated` suffix where a legacy
name collides, exposing the point estimate **and** its uncertainty. Official-only consumers read the
legacy `deterministic` model; estimated consumers read the `_estimated` sibling. The two are not
merged into one view (the grains differ — e.g. `park_factor_summary` adds an `outcome` dimension the
deterministic `park_factors` lacks), so the compat layer is "keep legacy + add sibling," not "join."

## Out of scope for this pass

- The gated `promote-prod` that writes `bc.db` (the user's call).
- A new BSL `SemanticTable` for estimated outputs (the `bsl` dep group pins `sqlglot < 28`, mutually
  exclusive with the SQLMesh env; conditional per the checklist). LSF schema metadata still surfaces
  the new columns automatically via `generate_llm_context.py`.
- Populating `weak_identification_flag` from the live gate (defaults `false` until wired).
