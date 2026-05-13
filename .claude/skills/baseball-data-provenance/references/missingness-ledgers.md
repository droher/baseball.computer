# Missingness Ledgers

Use this reference when deciding what ledger output should exist before imputation.

## Missingness Classes

| Class | Meaning | Modeling implication |
| --- | --- | --- |
| Structural absence | Target grain does not exist even though coarser data may exist. | Do not create canonical event facts unless a synthetic namespace is accepted. |
| Aggregate-only coverage | Official aggregate exists but event attribution does not. | Fill or constrain only at the aggregate grain. |
| Field-level unknown | Event exists but a parsed field is `Unknown`, `0`, `Default`, null, or otherwise uninformative. | Use deterministic inference first, then constrained probability models. |
| Selection-biased detail | Detail is observed only for non-random event subsets. | Model observation propensity or bias before using complete cases. |
| Cross-source disagreement | Event, box, gamelog, or supplement disagree. | Define authority order and issue-ledger exceptions before reconciliation. |
| State reconstruction gap | Event exists but state or personnel needed to interpret it is missing or derived. | Reconstruct and audit state before downstream modeling. |
| Official scoring convention gap | The desired quantity is an official convention rather than a purely physical fact. | Keep official and reconstructed quantities separate. |
| Taxonomy collapse | Raw codes exist but target categories are coarser or more stable. | Deterministic derivation unless raw inputs are missing. |
| Sparse-context estimation | Data exists, but target conditioning buckets are too sparse. | Use hierarchical pooling, not hard fallback thresholds. |

## Recommended Ledgers

| Ledger | Grain | Purpose |
| --- | --- | --- |
| `source_acquisition_ledger` | `game_id, team_id, dimension` | Source regime, acquisition status, structural absence, source-block absence, usable target flag. |
| `source_artifact_risk_ledger` | source row or modeled row | Known issue flags, suspected parser/source artifacts, and training weights. |
| `event_observation_ledger` | `event_id, dimension` | Observed, unknown-coded, derived, source-block missing, structurally absent, or not applicable. |
| `personnel_reliability_ledger` | `event_id, position` or `game_id, player_id` | Eligibility confidence, ambiguous substitutions, hidden appearance risk, and hard-zero masks. |
| `entity_link_reliability_ledger` | entity id episode | Player, team, park, scorer, and umpire identity confidence. |
| `context_observation_ledger` | `game_id, context_field` | Park, weather, handedness, start time, rule flags, and schedule context reliability. |
| `scoring_regime_ledger` | season, league, source, rule dimension | Official scoring convention regimes and authority order. |
| `exposure_ledger` | game, team-game, inning/frame | Completion, suspension, forfeit, walk-off, innings, and denominator status. |

## Ledger Columns

Prefer narrow, explicit columns:

- `observed_status`: observed, derived, unknown_code, source_block_missing, structural_absence, not_applicable, contradicted.
- `source_family`: event, box, gamelog, supplement, seed, derived, ml_artifact.
- `authority_rank`: integer rank for reconciliation at the target grain.
- `artifact_risk`: none, known_issue, suspected_source_issue, suspected_parser_issue, unresolved.
- `training_weight`: numeric weight for models that should downweight suspect observations.
- `hard_mask`: boolean for impossible values or structurally excluded rows.
- `confidence`: deterministic confidence when a posterior probability table is overkill.

Invariant: ledgers should be joinable by downstream models and auditable without rerunning statistical inference.
