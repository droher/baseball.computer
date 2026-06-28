---
title: Phase 2 Exit EDA - model_input_observation_batted_ball
type: eda-summary
status: closed
audience: humans-and-agents
last-verified: 2026-05-14
---

# Phase 2 Exit EDA — model_input_observation_batted_ball

Captures the first-fitting-family EDA run that closes the Phase 2 exit-gate
checklist item "EDA reports exist for the first model family targeted for
fitting." The raw artifacts live under `artifacts/statistical/` (gitignored);
this file is the tracked record of what they contained.

## Run inputs

- **Dataset**: `model_input_observation_batted_ball` (dataset_version `0.1.0` at the
  time of this run; Phase-3 PR3 bumps the registry to `0.2.0` once the
  DL proposal manifest schema migration lands. Re-run `prepare-dataset` +
  `run-eda` against the new schema before declaring this artifact current
  again — the `dataset_metadata.json::query_hash` will not match after the
  view schema change.)
- **Source DB**: `bc_dev.db` at `BC_LEDGER_SCHEMA=main_models__split_registry`
- **Source snapshot id**: `dev` (literal stamped by the SQLMesh var on this dev env)
- **Dataset artifact id**: `phase2-exit-batted-ball`
- **EDA artifact id**: `phase2-exit-batted-ball-eda`

Reproduce locally:

```sh
BC_LEDGER_SCHEMA=main_models__split_registry \
  PYTHONPATH=bc uv run --group build python -m python_models.statistical.cli \
  prepare-dataset --dataset model_input_observation_batted_ball \
  --artifact-id phase2-exit-batted-ball

PYTHONPATH=bc uv run --group build python -m python_models.statistical.cli \
  run-eda --dataset model_input_observation_batted_ball \
  --dataset-artifact phase2-exit-batted-ball \
  --artifact-id phase2-exit-batted-ball-eda
```

Wall clock on a MacBook Pro M-series: `prepare-dataset` ~18 s,
`run-eda` ~43 s.

## Headline counts

| Metric | Value |
| --- | --- |
| `row_count` | 84,272,874 |
| `target_population_count` | 84,272,874 |
| `observed_truth_count` | 84,272,874 |
| `source_family_block_missing_count` | 0 |
| `data_error_excluded_count` | 0 |
| `weak_identification_flag_count` | 17,965 |
| `blocking_finding_count` | 4 |

`observed_truth_count` equals `row_count` because every row that survives the
view's `target_population_status = 'event_level'` filter also has
`data_error_risk = 'none'` in `event_observation_geometry`, so `training_weight`
is 1.0 across the board. `data_error_excluded_count = 0` confirms the same
from the opposite side — no rows carry `data_error_risk != 'none'`.

## Split-leakage gate

`split_leakage_report.parquet` has **0 rows** — every keying unit (game_id,
scorer, park-season, alignment_regime, source_type+season, season,
game_id+fielding_team_id) maps to a single fold/holdout value. The
deterministic split policy declared in `stress_holdout_registry.sql` and the
inline `HASH(game_id) % 100` in the modeling-dataset view both round-trip
through the gate without violations.

This closes the Phase-2 exit-gate item "Split registry has passed leakage
checks" at the empirical level. Re-run the gate after any future change to
the split policy or stress-holdout SQL.

## Blocking findings (4, all `severity=warn`)

The runner does not raise these as hard failures (their severity is `warn`),
but each one is recorded so Phase 3+ model authors know which knobs the
observation/scorer model owes a policy for.

1. **`dominant_single_scorer_park_team`** — `scorer_park` pair
   `(A. Them/Dozier, STL09)` has `dominant_share = 1.000` over 336 rows.
   This is one of 17,964 collinearity-dominant-share weak-id flags;
   the top pair is the worst offender but the underlying pattern is
   pervasive on pre-modern-era data.
2. **`no_connected_component_for_effect`** — the `source_family_season`
   edge graph has a single-node component for one season/source-family
   pair. The other connectivity edges (`park_park`, `scorer_park`) all
   have multi-node components.
3. **`category_absent_in_train_present_in_test`** (column `park_id`) —
   5 example parks in TEST that never appear in TRAIN, e.g. `CHA01`,
   `CLP01`, `CRL01`, `DAL02`, `DES02`. Pre-modern stadiums with a small
   game count that landed entirely in the TEST hash bucket.
4. **`category_absent_in_train_present_in_test`** (column `scorer`) —
   5 example scorers in TEST that never appear in TRAIN, e.g. composite
   ids like `101,140,266` and `102, 222`. Pre-modern multi-scorer game
   tuples with sparse coverage.

All four are expected pre-modern era artifacts of the
`event_observation_geometry` upstream: the leftmost 1910s tail has few
games, few scorers, and a handful of stadiums that show up only briefly.
The Phase-3+ observation model handles them via a hierarchical scorer-park
prior (`treatment=partial_pool`) and an unseen-park policy
(`treatment=mark_weakly_identified`).

## Weak-identification flags by effect

| Effect | Reason | Count |
| --- | --- | --- |
| `scorer_source_family` | `collinearity_dominant_share` | 10,526 |
| `scorer_park` | `collinearity_dominant_share` | 7,322 |
| `source_family_era` | `collinearity_dominant_share` | 116 |
| `source_family_season` | `single_node_component` | 1 |

Every `scorer_*` pair flag traces to a scorer that only appears in one
park or one source_family — typical of the 1910s through 1940s when a
single scorekeeper covered every home game of a single team. The
observation model addresses this via partial pooling on scorer and
source_family random effects; the `mark_weakly_identified` treatment
applies to the rows where the dominant share is 1.000.

## Where it lives

```
artifacts/statistical/datasets/model_input_observation_batted_ball/
  phase2-exit-batted-ball/
    dataset.parquet                   (~861 MB)
    dataset_metadata.json
    manifest.json

artifacts/statistical/eda/model_input_observation_batted_ball/
  phase2-exit-batted-ball-eda/
    candidate_interactions.parquet
    collinearity_report.parquet       (31,264 rows)
    connectivity_edges.parquet
    data_error_concentration.parquet
    eda.md                            (human-readable summary)
    missingness_by_slice.parquet
    report.json                       (EdaReport JSON)
    source_family_block_missingness.parquet
    split_leakage_report.parquet      (0 rows — gate passes)
    target_distribution.parquet
    weak_identification_flags.parquet (17,965 rows)
    manifest.json
```
