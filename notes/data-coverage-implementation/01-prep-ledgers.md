---
title: Data Coverage Prep Ledgers
type: design-doc
status: draft
audience: humans-and-agents
last-verified: 2026-05-13
---

# Data Coverage Prep Ledgers

## TL;DR

Build deterministic SQLMesh ledgers before fitting any imputation model. These ledgers define the target population, source family and availability, source data-error risk, official aggregate-total availability, personnel eligibility confidence, entity-link confidence, context reliability, exposure status, and field-level observed status for every 1910-2025 target row.

This phase replaces implicit assumptions with joinable tables. A model can then treat a value as truth, constraint, weak measurement, covariate with measurement error, mask, training weight, holdout-only diagnostic, or withheld input without rediscovering that policy inside each fitted model.

## Scope

This doc specifies deterministic SQLMesh prep work. It does not fit probabilistic models, train deep models, or publish imputed values. Its outputs are prerequisites for:

- `02-eda-and-modeling-datasets.md`
- `03-hierarchical-models.md`
- `04-deep-learning-supplements.md`

Invariant: a missing value is not eligible for imputation until the relevant ledger distinguishes source-family block absence, structural absence, field-level unknown, not-applicable, aggregate-only coverage, contradicted evidence, and source/parser data-error risk.

Invariant: season bounds in SQL sketches are placeholders for SQLMesh vars. Production models should use `@VAR('start_season', 1910)` and `@VAR('end_season', 2025)`, not hardcoded literals.

## Ledger Dependency DAG

```mermaid
flowchart TD
  A["game_start_info, season_team_coverage, stg_schedule, stg_gamelog, stg_games"] --> B["source_acquisition_ledger"]
  C["box_score_data_issues, team_game_data_issues, audits, discrepancy views"] --> D["source_data_error_risk_ledger"]
  E["stg_box_score_*, player_position_game_fielding_stats, team_game_fielding_stats"] --> F["official_aggregate_availability"]
  G["event_personnel_lookup, personnel_fielding_states, player_game_appearances"] --> H["personnel_state_reliability"]
  I["people, rosters, teams, parks, scorekeepers, umpires"] --> J["entity_link_reliability"]
  K["game_results, game_line_scores, game_forfeits, game_suspensions"] --> L["game_exposure_ledger"]
  M["game_start_info, game_scorekeeping, weather/context fields"] --> N["game_context_observation_ledger"]
  B --> O1["event_observation_geometry"]
  B --> O2["event_observation_pitch"]
  B --> O3["event_observation_credit"]
  D --> O1
  D --> O2
  D --> O3
  F --> P["official_credit_authority"]
  H --> O1
  H --> O3
  J --> O1
  L --> O1
  L --> O2
  L --> O3
  N --> O1
  O1 --> Q["fielding_credit_gaps"]
  O3 --> Q
  P --> Q
```

The same sequence in prose:

1. Classify which source families exist at each game/team/dimension grain.
2. Mark confirmed and suspected source/parser data errors before those rows can train models.
3. Build official aggregate-total availability by stat and grain.
4. Build reliability ledgers for identities, personnel, context, and exposure.
5. Build three sibling event observation ledgers — geometry, pitch, credit — that preserve field-specific sentinel meanings within tight per-family dimension enums.
6. Build gap tables that translate raw observation status into model-specific target populations.

## Shared Status Seeds

Every ledger that carries a `observed_status` or `reliability_class` column FKs to one of two shared seeds (to be created later in this phase). Each ledger uses a `relationships(...)` audit pointing at the relevant seed instead of restating an inline `accepted_values(... is_in := (...))` list. Inline `is_in` is reserved for ledger-local enums that do not belong in a shared vocabulary (e.g., `target_population_status`, `source_block_status`, `gap_class`).

- `main_seeds.seed_observed_status` — canonical values: `observed`, `derived`, `aggregate_only`, `missing`, `unknown_code`, `default_code`, `not_applicable`, `contradicted`, `data_error_prone`.
- `main_seeds.seed_reliability_class` — canonical values: `direct`, `derived`, `inferred`, `synthetic`, `ambiguous`.

Per-ledger enums that diverged in Batch 1 review (`personnel_state_reliability.eligibility_status`, `entity_link_reliability.link_status`, `game_context_observation_ledger.observed_status`) should be reconciled against these seeds. If a ledger needs ledger-specific status values, it must include the canonical seed values first and document the extensions explicitly; the FK still resolves on the canonical subset and the ledger-local extension lives in a separate column or sub-enum.

Audit pattern (matches the existing `relationships` FK style elsewhere in this repo):

```
audits (
  relationships(column := observed_status, to_model := main_seeds.seed_observed_status, to_column := observed_status),
  relationships(column := reliability_class, to_model := main_seeds.seed_reliability_class, to_column := reliability_class)
)
```

## Ledger Table Contracts

### `source_acquisition_ledger`

| Field | Type | Meaning |
| --- | --- | --- |
| `game_id` | `VARCHAR` | Game key. |
| `team_id` | `TEAM_ID` | Team key for side-dependent dimensions (`box_batting`, `box_pitching`, `box_fielding`, `line_score`). `NULL` for game-wide dimensions (`event`, `pitch_sequence`, `batted_ball`, `gamelog`). |
| `dimension` | `VARCHAR` | `event`, `box_batting`, `box_pitching`, `box_fielding`, `line_score`, `pitch_sequence`, `batted_ball`, `gamelog`, etc. |
| `source_family` | `VARCHAR` | `play_by_play`, `box_score`, `gamelog`, `schedule`, `databank`, `derived`, `absent`. |
| `source_type` | `VARCHAR` | Existing `game_start_info.source_type` where applicable. |
| `target_population_status` | `VARCHAR` | `event_level`, `aggregate_only`, `gamelog_only`, `structural_absence`, `out_of_scope`, `coverage_within_source_sparse`. |
| `source_block_status` | `VARCHAR` | `present_fully_populated`, `present_partial_coverage`, `coverage_within_source_sparse`, `block_missing`, `not_applicable`. Distinguishes "dim is fully populated", "dim is technically possible from source but sparse", and "block missing entirely." |
| `source_availability_status` | `VARCHAR` | `observed`, `not_acquired`, `not_applicable`, `contradicted`, `data_error_prone`. |
| `usable_for_event_imputation` | `BOOLEAN` | True only for event-level rows and dimensions with non-sparse coverage. |
| `usable_as_aggregate_constraint` | `BOOLEAN` | True when the source can constrain aggregate outputs. |
| `authority_rank` | `UTINYINT` | Lower rank wins for target grain and dimension. |

SQL sketch:

```sql
MODEL (
  name main_models.source_acquisition_ledger,
  kind FULL,
  grain (game_id, team_id, dimension),
  columns (
    game_id VARCHAR,
    team_id TEAM_ID,
    dimension VARCHAR,
    source_family VARCHAR,
    source_type VARCHAR,
    target_population_status VARCHAR,
    source_block_status VARCHAR,
    source_availability_status VARCHAR,
    usable_for_event_imputation BOOLEAN,
    usable_as_aggregate_constraint BOOLEAN,
    authority_rank UTINYINT
  ),
  audits (
    not_null(columns := (game_id, dimension)),
    unique_grain(columns := (game_id, team_id, dimension)),
    accepted_values(column := target_population_status, is_in := (
      'event_level',
      'aggregate_only',
      'gamelog_only',
      'structural_absence',
      'out_of_scope',
      'coverage_within_source_sparse'
    ))
  )
);

WITH side_dependent_dimensions AS (
    SELECT *
    FROM (VALUES
        ('box_batting'),
        ('box_pitching'),
        ('box_fielding'),
        ('line_score')
    ) AS t(dimension)
),

game_wide_dimensions AS (
    SELECT *
    FROM (VALUES
        ('event'),
        ('pitch_sequence'),
        ('batted_ball'),
        ('gamelog')
    ) AS t(dimension)
),

team_games AS (
    SELECT
        game_id,
        season,
        team_id,
        source_type
    FROM main_models.team_game_start_info
    WHERE season BETWEEN @VAR('start_season', 1910) AND @VAR('end_season', 2025)
),

game_source AS (
    SELECT
        game_id,
        season,
        MIN(source_type) AS source_type
    FROM team_games
    GROUP BY 1, 2
),

pitch_coverage AS (
    SELECT
        game_id,
        has_pitch_sequence,
        has_pitch_count_data
    FROM main_models.game_data_completeness
),

batted_ball_coverage AS (
    SELECT
        game_id,
        has_offense_batted_ball,
        has_defense_batted_ball
    FROM main_models.game_data_completeness
),

side_dependent_dims AS (
    SELECT
        tg.game_id,
        tg.team_id,
        d.dimension,
        CASE
            WHEN tg.source_type = 'PlayByPlay' THEN 'play_by_play'
            WHEN tg.source_type = 'BoxScore' THEN 'box_score'
            WHEN tg.source_type = 'GameLog' THEN 'gamelog'
            ELSE 'absent'
        END AS source_family,
        tg.source_type,
        CASE
            WHEN tg.source_type = 'PlayByPlay' AND d.dimension IN ('box_batting', 'box_pitching', 'box_fielding') THEN 'aggregate_only'
            WHEN tg.source_type = 'PlayByPlay' AND d.dimension = 'line_score' THEN 'aggregate_only'
            WHEN tg.source_type = 'BoxScore' THEN 'aggregate_only'
            WHEN tg.source_type = 'GameLog' AND d.dimension = 'line_score' THEN 'aggregate_only'
            WHEN tg.source_type = 'GameLog' THEN 'structural_absence'
            ELSE 'structural_absence'
        END AS target_population_status,
        CASE
            WHEN tg.source_type IN ('PlayByPlay', 'BoxScore') THEN 'present_fully_populated'
            WHEN tg.source_type = 'GameLog' AND d.dimension = 'line_score' THEN 'present_fully_populated'
            WHEN tg.source_type IS NULL THEN 'block_missing'
            ELSE 'not_applicable'
        END AS source_block_status,
        CASE
            WHEN tg.source_type IS NULL THEN 'not_acquired'
            WHEN tg.source_type IN ('PlayByPlay', 'BoxScore', 'GameLog') THEN 'observed'
            ELSE 'contradicted'
        END AS source_availability_status
    FROM team_games AS tg
    CROSS JOIN side_dependent_dimensions AS d
),

game_wide_dims AS (
    SELECT
        gs.game_id,
        CAST(NULL AS TEAM_ID) AS team_id,
        d.dimension,
        CASE
            WHEN gs.source_type = 'PlayByPlay' THEN 'play_by_play'
            WHEN gs.source_type = 'BoxScore' THEN 'box_score'
            WHEN gs.source_type = 'GameLog' THEN 'gamelog'
            ELSE 'absent'
        END AS source_family,
        gs.source_type,
        CASE
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'event' THEN 'event_level'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'pitch_sequence' AND COALESCE(pc.has_pitch_sequence, false) THEN 'event_level'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'pitch_sequence' THEN 'coverage_within_source_sparse'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'batted_ball'
                AND (COALESCE(bc.has_offense_batted_ball, false) OR COALESCE(bc.has_defense_batted_ball, false)) THEN 'event_level'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'batted_ball' THEN 'coverage_within_source_sparse'
            WHEN gs.source_type = 'GameLog' AND d.dimension = 'gamelog' THEN 'gamelog_only'
            WHEN gs.source_type = 'BoxScore' AND d.dimension = 'gamelog' THEN 'gamelog_only'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'gamelog' THEN 'gamelog_only'
            ELSE 'structural_absence'
        END AS target_population_status,
        CASE
            WHEN gs.source_type IS NULL THEN 'block_missing'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'event' THEN 'present_fully_populated'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'pitch_sequence' AND COALESCE(pc.has_pitch_sequence, false) THEN 'present_fully_populated'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'pitch_sequence' THEN 'coverage_within_source_sparse'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'batted_ball'
                AND (COALESCE(bc.has_offense_batted_ball, false) OR COALESCE(bc.has_defense_batted_ball, false)) THEN 'present_partial_coverage'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'batted_ball' THEN 'coverage_within_source_sparse'
            WHEN gs.source_type IN ('PlayByPlay', 'BoxScore', 'GameLog') AND d.dimension = 'gamelog' THEN 'present_fully_populated'
            ELSE 'not_applicable'
        END AS source_block_status,
        CASE
            WHEN gs.source_type IS NULL THEN 'not_acquired'
            WHEN gs.source_type IN ('PlayByPlay', 'BoxScore', 'GameLog') THEN 'observed'
            ELSE 'contradicted'
        END AS source_availability_status
    FROM game_source AS gs
    CROSS JOIN game_wide_dimensions AS d
    LEFT JOIN pitch_coverage AS pc USING (game_id)
    LEFT JOIN batted_ball_coverage AS bc USING (game_id)
),

classified AS (
    SELECT * FROM side_dependent_dims
    UNION ALL BY NAME
    SELECT * FROM game_wide_dims
)

SELECT
    *,
    target_population_status = 'event_level'
        AND source_block_status NOT IN ('coverage_within_source_sparse', 'block_missing') AS usable_for_event_imputation,
    target_population_status IN ('event_level', 'aggregate_only') AS usable_as_aggregate_constraint,
    CASE source_family
        WHEN 'play_by_play' THEN 1
        WHEN 'box_score' THEN 2
        WHEN 'gamelog' THEN 3
        ELSE 9
    END AS authority_rank
FROM classified;
```

Validation checks:

- Reconcile the ledger against `season_team_coverage`, `game_start_info.source_type`, `stg_schedule`, `stg_gamelog`, and `stg_games`.
- Verify that 1910 and 1911 are not accidentally filtered out by stale "complete from 1912" metadata.
- Separate source availability by dimension instead of assuming `PlayByPlay` means pitch, batted-ball, and official aggregate totals are all available.
- Confirm `target_population_status = 'event_level'` for `pitch_sequence` and `batted_ball` only when the game-level coverage flags in `game_data_completeness` (or the `event_completeness_pitches` / `event_completeness_batted_balls` rollups) confirm non-sparse coverage.
- Confirm `team_id IS NULL` for game-wide dimensions and non-null for side-dependent dimensions.

### `source_data_error_risk_ledger`

| Field | Type | Meaning |
| --- | --- | --- |
| `data_error_key` | `VARCHAR` | Stable hash or composite key for row/field data-error risk. |
| `source_table` | `VARCHAR` | Table where the value originates. |
| `game_id` | `VARCHAR` | Game key when applicable. |
| `team_id` | `TEAM_ID` | Team key when applicable. |
| `player_id` | `VARCHAR` | Player key when applicable. |
| `field_name` | `VARCHAR` | Affected field or stat. |
| `data_error_class` | `VARCHAR` | `confirmed_issue`, `suspected_source_issue`, `suspected_parser_issue`, `contradiction`, `audit_exception`. |
| `training_action` | `VARCHAR` | `allow`, `downweight`, `exclude`, `constraint_only`, `diagnostic_only`. |
| `training_weight` | `DOUBLE` | Numeric multiplier for fitted models. |
| `issue_source` | `VARCHAR` | Originating audit, issue table, or discrepancy view. |

First implementation sources:

- `main_models.box_score_data_issues`
- `main_models.team_game_data_issues`
- `main_models.box_event_fielding_discrepancies`
- `main_models.unknown_play_no_box`
- `main_models.assists_as_putouts_finder` if materialized or promoted from analysis
- SQLMesh audits that enumerate explicit exceptions

Invariant: data-error risk is not missingness. Non-null wrong values should be masked or down-weighted before they become training truth.

### `official_aggregate_availability`

| Field | Type | Meaning |
| --- | --- | --- |
| `game_id` | `VARCHAR` | Game key. |
| `team_id` | `TEAM_ID` | Team key. |
| `player_id` | `VARCHAR` | Player key when player-grain aggregate total exists. |
| `fielding_position` | `UTINYINT` | Position when stat is position-specific. |
| `stat_name` | `VARCHAR` | `putouts`, `assists`, `errors`, `double_plays`, `plate_appearances`, `earned_runs`, etc. |
| `aggregate_grain` | `VARCHAR` | `team_game`, `player_game`, `player_position_game`. |
| `aggregate_status` | `VARCHAR` | `present_clean`, `present_issue_flagged`, `missing`, `not_applicable`, `negative_residual`, `contradicted`. |
| `aggregate_value` | `DOUBLE` | Official aggregate value when present and numeric. |
| `event_value` | `DOUBLE` | Event-derived comparison value when applicable. |
| `residual_value` | `DOUBLE` | `aggregate_value - event_value` when both exist. |
| `authority_rank` | `UTINYINT` | Rank for target grain/stat. |
| `data_error_risk` | `VARCHAR` | Joined from the data-error risk ledger. |

This ledger fixes a critical ambiguity: `game_data_completeness.has_box_score` identifies primary `BoxScore` source games in the current DB, not whether each `PlayByPlay` game has a usable official aggregate total for each player-game stat.

SQL sketch for fielding aggregate totals:

```sql
MODEL (
  name main_models.official_aggregate_availability,
  kind FULL,
  grain (game_id, team_id, player_id, fielding_position, stat_name),
  columns (
    game_id VARCHAR,
    team_id TEAM_ID,
    player_id VARCHAR,
    fielding_position UTINYINT,
    stat_name VARCHAR,
    aggregate_grain VARCHAR,
    aggregate_status VARCHAR,
    aggregate_value DOUBLE,
    event_value DOUBLE,
    residual_value DOUBLE,
    authority_rank UTINYINT,
    data_error_risk VARCHAR
  )
);

WITH box_agg AS (
    SELECT
        b.game_id,
        CASE WHEN b.side = 'Home' THEN g.home_team_id ELSE g.away_team_id END AS team_id,
        b.fielder_id AS player_id,
        b.fielding_position,
        SUM(b.putouts)::DOUBLE AS putouts,
        SUM(b.assists)::DOUBLE AS assists,
        SUM(b.errors)::DOUBLE AS errors
    FROM main_models.stg_box_score_fielding_lines AS b
    INNER JOIN main_models.stg_games AS g USING (game_id)
    GROUP BY 1, 2, 3, 4
),

event_agg AS (
    SELECT
        game_id,
        team_id,
        player_id,
        fielding_position,
        SUM(putouts)::DOUBLE AS putouts,
        SUM(assists)::DOUBLE AS assists,
        SUM(errors)::DOUBLE AS errors
    FROM main_models.event_player_fielding_stats
    GROUP BY 1, 2, 3, 4
),

personnel_presence AS (
    SELECT DISTINCT
        e.game_id,
        ps.team_id,
        ps.player_id,
        ps.fielding_position
    FROM main_models.personnel_fielding_states AS ps
    INNER JOIN main_models.event_states_full AS e USING (event_key)
    WHERE ps.player_id IS NOT NULL
        AND ps.fielding_position IS NOT NULL
),

wide AS (
    SELECT
        COALESCE(b.game_id, e.game_id, p.game_id) AS game_id,
        COALESCE(b.team_id, e.team_id, p.team_id) AS team_id,
        COALESCE(b.player_id, e.player_id, p.player_id) AS player_id,
        COALESCE(b.fielding_position, e.fielding_position, p.fielding_position) AS fielding_position,
        b.putouts AS box_putouts,
        e.putouts AS event_putouts,
        b.assists AS box_assists,
        e.assists AS event_assists,
        b.errors AS box_errors,
        e.errors AS event_errors,
        (b.game_id IS NOT NULL) AS box_present,
        (e.game_id IS NOT NULL) AS event_present,
        (p.game_id IS NOT NULL) AS personnel_present
    FROM box_agg AS b
    FULL OUTER JOIN event_agg AS e USING (game_id, team_id, player_id, fielding_position)
    FULL OUTER JOIN personnel_presence AS p USING (game_id, team_id, player_id, fielding_position)
),

fielding_long AS (
    SELECT
        game_id,
        team_id,
        player_id,
        fielding_position,
        'putouts' AS stat_name,
        box_putouts AS box_value,
        CASE
            WHEN event_present THEN COALESCE(event_putouts, 0)
            WHEN personnel_present THEN 0
            ELSE NULL
        END AS event_value,
        box_present,
        event_present OR personnel_present AS event_evidence_present
    FROM wide
    UNION ALL BY NAME
    SELECT
        game_id,
        team_id,
        player_id,
        fielding_position,
        'assists' AS stat_name,
        box_assists AS box_value,
        CASE
            WHEN event_present THEN COALESCE(event_assists, 0)
            WHEN personnel_present THEN 0
            ELSE NULL
        END AS event_value,
        box_present,
        event_present OR personnel_present AS event_evidence_present
    FROM wide
    UNION ALL BY NAME
    SELECT
        game_id,
        team_id,
        player_id,
        fielding_position,
        'errors' AS stat_name,
        box_errors AS box_value,
        CASE
            WHEN event_present THEN COALESCE(event_errors, 0)
            WHEN personnel_present THEN 0
            ELSE NULL
        END AS event_value,
        box_present,
        event_present OR personnel_present AS event_evidence_present
    FROM wide
)

SELECT
    game_id,
    team_id,
    player_id,
    fielding_position,
    stat_name,
    'player_position_game' AS aggregate_grain,
    CASE
        WHEN NOT box_present THEN 'missing'
        WHEN NOT event_evidence_present THEN 'present_clean'
        WHEN box_value - event_value < 0 THEN 'negative_residual'
        WHEN box_value = event_value THEN 'present_clean'
        ELSE 'contradicted'
    END AS aggregate_status,
    box_value::DOUBLE AS aggregate_value,
    event_value::DOUBLE AS event_value,
    CASE
        WHEN box_value IS NOT NULL AND event_value IS NOT NULL
            THEN (box_value - event_value)::DOUBLE
        ELSE NULL
    END AS residual_value,
    1 AS authority_rank,
    'none' AS data_error_risk
FROM fielding_long;
```

Notes on the join shape:

- The `box_agg` / `event_agg` join key includes `team_id` so that doubleheader split-squad rosters, mid-game team changes, and historical pre-1900 oddities cannot mis-credit a player to the wrong side.
- The personnel union ensures players who were on the field with zero plays surface as `event_value = 0` instead of `event_value IS NULL`. That distinguishes "no box at all" (`box_present = false`) from "fielder present with no plays" (`event_evidence_present = true, event_value = 0`).

### `official_credit_authority`

This table resolves which source owns each official stat at the output grain after aggregate-total availability and data-error risk are known.

| Field | Meaning |
| --- | --- |
| `game_id`, `team_id`, `player_id`, `fielding_position`, `credit_type` | Target key. |
| `authority_source` | `event`, `box`, `event_box_reconciled`, `estimated_with_aggregate_constraint`, `estimated_no_aggregate_constraint`, `withheld`. |
| `authority_reason` | Short enum explaining the choice. |
| `can_publish_official` | True only when source authority is official at target grain. |
| `can_publish_estimated` | True when the statistical namespace can publish an estimate. |

Decision policy:

- Clean event rows can publish event-derived official counters.
- Clean box-score totals can publish official aggregate counters at aggregate grain.
- Unknown event credit with clean residual constraints can publish estimated counters only in an estimated namespace.
- No-box unknowns can publish only low-confidence estimates unless the project accepts an explicit confidence threshold.
- Contradicted or issue-flagged aggregate totals can be withheld or marked diagnostic-only.

### `personnel_state_reliability`

| Field | Meaning |
| --- | --- |
| `event_key`, `fielding_side`, `fielding_position`, `player_id` | Event-position eligibility key. |
| `reliability_class` | FK to `seed_reliability_class` (`direct`, `derived`, `inferred`, `synthetic`, `ambiguous`). |
| `eligibility_status` | Ledger-specific extension: `direct_event`, `lineup_derived`, `box_derived`, `synthetic`, `missing`, `duplicate_position`, `ambiguous_substitution`. Maps cleanly to `reliability_class` (e.g., `direct_event` -> `direct`; `lineup_derived`, `box_derived` -> `derived`; `synthetic` -> `synthetic`; `duplicate_position`, `ambiguous_substitution` -> `ambiguous`; `missing` -> `inferred` when an event lacks a direct personnel row). |
| `hard_zero_allowed` | True when the model can assign zero probability to players outside this state. |
| `personnel_confidence` | Numeric or enum confidence. |
| `issue_reason` | Ambiguity class. |

Validation checks:

- Count missing fielding positions by season, source type, game type, and team.
- Count duplicate positions inside an event fielding state.
- Compare `event_personnel_lookup`, `personnel_fielding_states`, `game_starting_lineups`, and `player_game_appearances`.
- Flag Ohtani-rule, DH, substitutions, defensive replacements, and multi-position player-games as explicit classes.

Invariant: fielding allocation can use personnel as a hard constraint only when `hard_zero_allowed = true`.

### `entity_link_reliability`

| Field | Meaning |
| --- | --- |
| `entity_type` | `player`, `team`, `park`, `scorer`, `umpire`, `league`. |
| `source_id` | Raw/source ID. |
| `canonical_id` | Project ID. |
| `valid_from`, `valid_to` | Episode bounds when identity can change over time. |
| `reliability_class` | FK to `seed_reliability_class`. |
| `link_status` | Ledger-specific extension: `direct`, `crosswalk`, `alias`, `inferred`, `conflict`, `unresolved`. Maps to `reliability_class` (`direct` -> `direct`; `crosswalk`, `alias` -> `derived`; `inferred` -> `inferred`; `conflict`, `unresolved` -> `ambiguous`). |
| `link_confidence` | Numeric or enum confidence. |
| `conflict_reason` | Why the link is weak. |

Use this ledger before fitting random effects by player, team, park, scorer, or umpire. Park factors need park episode reliability because a single park key can hide renovations, aliases, dimensions, surfaces, or multi-park seasons.

### `game_context_observation_ledger`

| Field | Meaning |
| --- | --- |
| `game_id`, `context_dimension` | Context field key. |
| `raw_value` | Source value as stored or serialized. |
| `normalized_value` | Canonical value if deterministically derived. |
| `observed_status` | FK to `seed_observed_status` (subset used here: `observed`, `derived`, `missing`, `not_applicable`, `contradicted`, `data_error_prone`). |
| `source_family` | Source of the context value. |
| `context_confidence` | Confidence enum or numeric weight. |

Dimensions:

- `park_id`
- `weather`
- `temperature`
- `wind`
- `time_of_day`
- `attendance`
- `dh_rule`
- `extra_inning_runner_rule`
- `game_type`
- `scorer`
- `inputter`
- `translator`
- `umpires`
- `batter_hand`
- `pitcher_hand`

Validation checks:

- Missingness rates by source type, season, league, park, and game type.
- Contradictions between event, box, gamelog, and schedule context.
- Handedness missingness and source confidence before it is used for park or geometry effects.

### `game_exposure_ledger`

| Field | Meaning |
| --- | --- |
| `game_id`, `team_id` | Team-game key. |
| `scheduled_innings` | Scheduled game length where known. |
| `actual_outs_batting`, `actual_outs_fielding` | Observed outs by side. |
| `completion_status` | `complete`, `walk_off`, `shortened`, `suspended`, `forfeit`, `unknown`. |
| `denominator_policy` | `full_game`, `observed_outs`, `official_result_only`, `exclude`, `synthetic_required`. |
| `exposure_confidence` | Confidence enum or numeric weight. |

This table is deterministic after source reconciliation. Missing context values should start with observed-status flags, missing indicators, deterministic fallbacks, and simple tabular imputation baselines. Bayesian context imputation is only justified for high-impact covariates that materially change park, advancement, or run-value estimates. It never overrides official suspension, forfeit, or walk-off facts.

### Event observation ledgers (three siblings)

The central deterministic contract for fitted models is split across three sibling tables at `(event_key, dimension)` grain, each with a tight per-family `dimension` enum. A single combined ledger would be 100-200M rows over 18M events with sentinel-decoding branches that vary by dimension; the per-family split keeps each table's accepted-values surface small, lets per-dimension sentinel logic live next to the dimensions it applies to, and allows the three to be planned and restated independently.

All three share the same column schema:

| Field | Type | Meaning |
| --- | --- | --- |
| `event_key` | `UINTEGER` | Event key. |
| `dimension` | `VARCHAR` | Per-family dimension enum (see each ledger below). |
| `observed_status` | `VARCHAR` | FK to `seed_observed_status`. |
| `sentinel_type` | `VARCHAR` | `null`, `unknown`, `default`, `zero`, `empty_sequence`, `valid_value`, `not_applicable`. Sentinel semantics are per-dimension; see the taxonomy doc. |
| `raw_value` | `VARCHAR` | Source value serialized as text. |
| `deduced_value` | `VARCHAR` | Deterministic derived value when present. |
| `source_acquisition_status` | `VARCHAR` | Joined `source_availability_status` from `source_acquisition_ledger` for the relevant `(game_id, team_id, dimension)` triple. |
| `data_error_risk` | `VARCHAR` | Joined `data_error_class` from `source_data_error_risk_ledger`. |
| `model_input_eligible` | `BOOLEAN` | True when the row can enter a fitted model as truth or evidence (i.e., not `not_applicable`, not `data_error_prone`, and source authority is non-block-missing). |

Each ledger is `kind INCREMENTAL_BY_TIME_RANGE` partitioned by `season`. (No existing model in this repo uses `INCREMENTAL_BY_TIME_RANGE`; `season` as the time column is the user's pick and is encoded as an integer year, not a date — this is a known limitation worth confirming when the first ledger is implemented. The fallback is `kind FULL` with an explicit season filter.) Each body is constructed as `UNION ALL BY NAME` branches keyed by per-family dimension.

#### `event_observation_geometry`

Dimensions: `trajectory`, `location_side`, `location_depth`, `location_edge`, `general_location`, `ball_handler_position`, `pulled_opposite`.

Note on `ball_handler_position`: this is the downstream mirror of the raw `stg_events.batted_to_fielder` column. The raw column name stays as-is; the ledger renames the dimension to make the semantics explicit — this is who fielded or completed the play (handler), not where the ball was hit (location). See the taxonomy doc's §Fielding-Geometry Coupling for why these must not be conflated, especially in shift-heavy eras.

```sql
MODEL (
  name main_models.event_observation_geometry,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column season,
    partition_by_time_column false
  ),
  start @VAR('start_season', 1910),
  end @VAR('end_season', 2025),
  grain (event_key, dimension),
  columns (
    event_key UINTEGER,
    dimension VARCHAR,
    observed_status VARCHAR,
    sentinel_type VARCHAR,
    raw_value VARCHAR,
    deduced_value VARCHAR,
    source_acquisition_status VARCHAR,
    data_error_risk VARCHAR,
    model_input_eligible BOOLEAN
  ),
  audits (
    unique_grain(columns := (event_key, dimension)),
    accepted_values(column := dimension, is_in := (
      'trajectory',
      'location_side',
      'location_depth',
      'location_edge',
      'general_location',
      'ball_handler_position',
      'pulled_opposite'
    )),
    relationships(column := observed_status, to_model := main_seeds.seed_observed_status, to_column := observed_status)
  )
);

WITH events AS (
    SELECT
        e.event_key,
        e.season,
        e.game_id,
        e.batting_team_id,
        e.fielding_team_id
    FROM main_models.event_states_full AS e
    WHERE e.season BETWEEN @start_ds AND @end_ds
),

trajectory AS (
    SELECT
        c.event_key,
        'trajectory' AS dimension,
        CASE
            WHEN c.recorded_trajectory IS NOT NULL AND c.recorded_trajectory != 'Unknown' THEN 'observed'
            WHEN c.trajectory IS NOT NULL AND c.trajectory != 'Unknown' THEN 'derived'
            ELSE 'unknown_code'
        END AS observed_status,
        CASE
            WHEN c.recorded_trajectory IS NULL THEN 'null'
            WHEN c.recorded_trajectory = 'Unknown' THEN 'unknown'
            ELSE 'valid_value'
        END AS sentinel_type,
        c.recorded_trajectory::VARCHAR AS raw_value,
        c.trajectory::VARCHAR AS deduced_value
    FROM main_models.calc_batted_ball_type AS c
    INNER JOIN events USING (event_key)
)

SELECT
    g.event_key,
    g.dimension,
    g.observed_status,
    g.sentinel_type,
    g.raw_value,
    g.deduced_value,
    s.source_availability_status AS source_acquisition_status,
    COALESCE(a.data_error_class, 'none') AS data_error_risk,
    g.observed_status NOT IN ('not_applicable', 'data_error_prone')
        AND s.source_availability_status != 'not_acquired' AS model_input_eligible
FROM trajectory AS g
INNER JOIN events AS e USING (event_key)
INNER JOIN main_models.source_acquisition_ledger AS s
    ON e.game_id = s.game_id
    AND e.batting_team_id = s.team_id
    AND s.dimension = 'batted_ball'
LEFT JOIN main_models.source_data_error_risk_ledger AS a
    ON e.game_id = a.game_id
    AND a.field_name = g.dimension;
```

This sketch shows one dimension; expand with `UNION ALL BY NAME` branches for the other geometry dimensions. Sentinel semantics (e.g., what `Unknown` vs `null` vs `default_code` means) are per-dimension and must match the per-field rules in the taxonomy doc.

#### `event_observation_pitch`

Dimensions: `count_balls`, `count_strikes`, `pitch_sequence`, `pitch_results`, `strike_types`, `pitch_count_total`. Same schema, same incremental kind. Sentinel rules diverge from geometry: `pitch_sequence` uses `empty_sequence` and `default_code` to distinguish "no pitch sequence recorded" from "sequence is structurally empty" from "default-coded entry"; `count_balls` / `count_strikes` use `default_code` for legacy 0/0 defaults that may be unobserved.

```sql
MODEL (
  name main_models.event_observation_pitch,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column season,
    partition_by_time_column false
  ),
  start @VAR('start_season', 1910),
  end @VAR('end_season', 2025),
  grain (event_key, dimension),
  columns (
    event_key UINTEGER,
    dimension VARCHAR,
    observed_status VARCHAR,
    sentinel_type VARCHAR,
    raw_value VARCHAR,
    deduced_value VARCHAR,
    source_acquisition_status VARCHAR,
    data_error_risk VARCHAR,
    model_input_eligible BOOLEAN
  ),
  audits (
    unique_grain(columns := (event_key, dimension)),
    accepted_values(column := dimension, is_in := (
      'count_balls',
      'count_strikes',
      'pitch_sequence',
      'pitch_results',
      'strike_types',
      'pitch_count_total'
    )),
    relationships(column := observed_status, to_model := main_seeds.seed_observed_status, to_column := observed_status)
  )
);

-- Body: UNION ALL BY NAME over the six dimensions, joining
-- source_acquisition_ledger on dimension='pitch_sequence' (or 'event' for the
-- count dimensions) and source_data_error_risk_ledger on field_name.
-- WHERE season BETWEEN @start_ds AND @end_ds applied in the driver CTE.
```

#### `event_observation_credit`

Dimensions: `putout_credit`, `assist_credit`, `error_credit`, `double_play_credit`, `triple_play_credit`, `passed_ball_credit`. Same schema, same incremental kind. Sentinel rules: unknown fielder credit uses `unknown_code` and is the input to `fielding_credit_gaps`; `not_applicable` covers events where the credit type is structurally impossible (e.g., no error credit on a clean play).

```sql
MODEL (
  name main_models.event_observation_credit,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column season,
    partition_by_time_column false
  ),
  start @VAR('start_season', 1910),
  end @VAR('end_season', 2025),
  grain (event_key, dimension),
  columns (
    event_key UINTEGER,
    dimension VARCHAR,
    observed_status VARCHAR,
    sentinel_type VARCHAR,
    raw_value VARCHAR,
    deduced_value VARCHAR,
    source_acquisition_status VARCHAR,
    data_error_risk VARCHAR,
    model_input_eligible BOOLEAN
  ),
  audits (
    unique_grain(columns := (event_key, dimension)),
    accepted_values(column := dimension, is_in := (
      'putout_credit',
      'assist_credit',
      'error_credit',
      'double_play_credit',
      'triple_play_credit',
      'passed_ball_credit'
    )),
    relationships(column := observed_status, to_model := main_seeds.seed_observed_status, to_column := observed_status)
  )
);

-- Body: UNION ALL BY NAME over the six dimensions, joining
-- source_acquisition_ledger on (game_id, fielding_team_id, dimension='box_fielding')
-- for credit attribution and source_data_error_risk_ledger on field_name.
-- WHERE season BETWEEN @start_ds AND @end_ds applied in the driver CTE.
```

Cross-doc reference note: downstream consumers that previously referenced `event_observation_ledger` should be updated to read from the appropriate sibling (`event_observation_geometry` for batted-ball geometry, `event_observation_pitch` for pitch-level evidence, `event_observation_credit` for fielding-credit gaps). Cross-doc reference updates are handled by separate agents on the other docs.

### `event_observation_context`

This view provides one event row with shared covariates for modeling datasets:

- `season`, `league`, `game_type`, `source_type`, `source_family`
- `park_id`, `park_episode_status`
- `scorer`, `inputter`, `translator`, `affiliated_team`
- `inning_start`, `frame_start`, `base_state_start`, `outs_start`
- `score_margin`, `leverage_index`, `runs_on_play`, `hit_or_out`
- `batter_id`, `pitcher_id`, `batter_hand`, `pitcher_hand`
- `fielding_team_id`, `batting_team_id`
- `personnel_confidence`, `context_confidence`, `exposure_status`

Invariant: modeling datasets should select from `event_observation_context` rather than repeatedly reimplementing these joins.

### `fielding_credit_gaps`

| Field | Meaning |
| --- | --- |
| `event_key`, `fielding_team_id` | Event/team key. |
| `fielding_evidence_status` | `complete_with_zero_unknowns`, `complete_with_known_unknowns`, `no_fielding_row`, `incomplete_event_flag`. Distinguishes "fielding row exists with zero unknown credit" from "no fielding row at all (walk, HBP, certain strikeouts)" and from explicit incomplete-event flagging. |
| `gap_class` | `complete`, `unknown_putout`, `unknown_assist_risk`, `box_residual_positive`, `box_residual_negative`, `no_box_unknown`, `data_error_flagged`, `not_applicable`. |
| `unknown_putouts` | Event unknown putouts from `calc_fielding_play_agg`. `NULL` means no fielding row exists; `0` means a fielding row exists with zero unknown credit. The two are not collapsed. |
| `known_putouts`, `known_assists`, `known_errors` | Event-known team values. `NULL` when no fielding row exists. |
| `has_clean_aggregate_total` | Whether a usable official aggregate total exists. |
| `aggregate_residual_putouts`, `aggregate_residual_assists`, `aggregate_residual_errors` | Aggregate residuals when applicable. |
| `personnel_hard_mask_available` | Whether personnel can constrain allocations. |
| `eligible_for_allocation` | True for statistical allocation target rows. |

SQL sketch:

```sql
MODEL (
  name main_models.fielding_credit_gaps,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column season,
    partition_by_time_column false
  ),
  start @VAR('start_season', 1910),
  end @VAR('end_season', 2025),
  grain (event_key, fielding_team_id),
  columns (
    event_key UINTEGER,
    fielding_team_id TEAM_ID,
    fielding_evidence_status VARCHAR,
    gap_class VARCHAR,
    unknown_putouts DOUBLE,
    known_putouts DOUBLE,
    known_assists DOUBLE,
    known_errors DOUBLE,
    has_clean_aggregate_total BOOLEAN,
    aggregate_residual_putouts DOUBLE,
    aggregate_residual_assists DOUBLE,
    aggregate_residual_errors DOUBLE,
    personnel_hard_mask_available BOOLEAN,
    eligible_for_allocation BOOLEAN
  )
);

WITH events AS (
    SELECT
        e.event_key,
        e.season,
        e.game_id,
        e.fielding_team_id,
        e.is_incomplete_event
    FROM main_models.event_states_full AS e
    WHERE e.season BETWEEN @start_ds AND @end_ds
),

fielding_agg AS (
    SELECT
        event_key,
        SUM(unknown_putouts)::DOUBLE AS unknown_putouts,
        BOOL_OR(unknown_putouts IS NOT NULL) AS fielding_row_present
    FROM main_models.calc_fielding_play_agg
    GROUP BY 1
),

player_stat_agg AS (
    SELECT
        event_key,
        team_id,
        SUM(putouts)::DOUBLE AS known_putouts,
        SUM(assists)::DOUBLE AS known_assists,
        SUM(errors)::DOUBLE AS known_errors,
        BOOL_OR(putouts IS NOT NULL OR assists IS NOT NULL OR errors IS NOT NULL)
            AS fielding_stat_row_present
    FROM main_models.event_player_fielding_stats
    GROUP BY 1, 2
),

event_credit AS (
    SELECT
        ev.event_key,
        ev.game_id,
        ev.fielding_team_id,
        ev.is_incomplete_event,
        fa.unknown_putouts,
        ps.known_putouts,
        ps.known_assists,
        ps.known_errors,
        COALESCE(fa.fielding_row_present, false)
            OR COALESCE(ps.fielding_stat_row_present, false) AS fielding_row_present
    FROM events AS ev
    LEFT JOIN fielding_agg AS fa USING (event_key)
    LEFT JOIN player_stat_agg AS ps
        ON ev.event_key = ps.event_key
        AND ev.fielding_team_id = ps.team_id
),

aggregate_totals AS (
    SELECT
        game_id,
        team_id,
        SUM(CASE WHEN stat_name = 'putouts' THEN residual_value ELSE 0 END)::DOUBLE AS aggregate_residual_putouts,
        SUM(CASE WHEN stat_name = 'assists' THEN residual_value ELSE 0 END)::DOUBLE AS aggregate_residual_assists,
        SUM(CASE WHEN stat_name = 'errors' THEN residual_value ELSE 0 END)::DOUBLE AS aggregate_residual_errors,
        BOOL_OR(aggregate_status = 'present_clean') AS has_clean_aggregate_total
    FROM main_models.official_aggregate_availability
    GROUP BY 1, 2
),

personnel AS (
    SELECT
        event_key,
        BOOL_AND(hard_zero_allowed) AS personnel_hard_mask_available
    FROM main_models.personnel_state_reliability
    GROUP BY 1
)

SELECT
    ec.event_key,
    ec.fielding_team_id,
    CASE
        WHEN ec.is_incomplete_event THEN 'incomplete_event_flag'
        WHEN NOT ec.fielding_row_present THEN 'no_fielding_row'
        WHEN COALESCE(ec.unknown_putouts, 0) > 0 THEN 'complete_with_known_unknowns'
        ELSE 'complete_with_zero_unknowns'
    END AS fielding_evidence_status,
    CASE
        WHEN ec.is_incomplete_event THEN 'data_error_flagged'
        WHEN NOT ec.fielding_row_present THEN 'not_applicable'
        WHEN COALESCE(ec.unknown_putouts, 0) > 0 AND COALESCE(a.has_clean_aggregate_total, false) THEN 'unknown_putout'
        WHEN COALESCE(ec.unknown_putouts, 0) > 0 THEN 'no_box_unknown'
        WHEN COALESCE(a.aggregate_residual_putouts, 0) > 0 THEN 'box_residual_positive'
        WHEN COALESCE(a.aggregate_residual_putouts, 0) < 0 THEN 'box_residual_negative'
        ELSE 'complete'
    END AS gap_class,
    ec.unknown_putouts,
    ec.known_putouts,
    ec.known_assists,
    ec.known_errors,
    COALESCE(a.has_clean_aggregate_total, false) AS has_clean_aggregate_total,
    COALESCE(a.aggregate_residual_putouts, 0) AS aggregate_residual_putouts,
    COALESCE(a.aggregate_residual_assists, 0) AS aggregate_residual_assists,
    COALESCE(a.aggregate_residual_errors, 0) AS aggregate_residual_errors,
    COALESCE(p.personnel_hard_mask_available, false) AS personnel_hard_mask_available,
    gap_class IN ('unknown_putout', 'no_box_unknown', 'box_residual_positive')
        AND COALESCE(p.personnel_hard_mask_available, false) AS eligible_for_allocation
FROM event_credit AS ec
LEFT JOIN aggregate_totals AS a
    ON ec.game_id = a.game_id
    AND ec.fielding_team_id = a.team_id
LEFT JOIN personnel AS p USING (event_key);
```

Notes on the join shape:

- The driver is `event_states_full` so events with NO fielding row at all (walks, HBP, certain strikeouts) are represented as rows rather than dropped — they receive `fielding_evidence_status = 'no_fielding_row'` and `unknown_putouts IS NULL`.
- `unknown_putouts = 0` (fielding row exists, zero unknowns) and `unknown_putouts IS NULL` (no fielding row at all) are kept distinct downstream. `gap_class = 'complete'` is reserved for the former; `gap_class = 'not_applicable'` is used for the latter.
- `is_incomplete_event` (or whatever the project surfaces as the event-level incompleteness flag) routes to `fielding_evidence_status = 'incomplete_event_flag'` so partially-recorded events don't masquerade as clean.

## Prep EDA To Run Before Modeling

| Check | Query target | Model decision it informs |
| --- | --- | --- |
| Source-family block matrix by `season, league, team, dimension` | `source_acquisition_ledger` | Whether missingness is source-family block, game-block, or event-level. |
| Data-error concentration by field/source/season | `source_data_error_risk_ledger` | Exclusion/downweight policy. |
| Box residual sign and size by stat/position | `official_aggregate_availability` | Whether residuals are constraints, soft evidence, or source/parser errors. |
| Personnel hard-mask coverage by season/source | `personnel_state_reliability` | Whether fielding allocation can assign hard zero masks. |
| Context missingness by season/park/source | `game_context_observation_ledger` | Park-factor covariates and context imputation. |
| Exposure classes by game type/source | `game_exposure_ledger` | Denominator policy for rates and run values. |
| Observation statuses by dimension/scorer/result | `event_observation_geometry`, `event_observation_pitch`, `event_observation_credit` | Candidate observation model interactions. |
| Fielding gap classes by era/position/scorer | `fielding_credit_gaps` | First allocation target and holdout design. |

Example EDA query:

```sql
SELECT
    e.season,
    e.league,
    e.source_type,
    o.ledger,
    o.dimension,
    o.observed_status,
    COUNT(*) AS events
FROM (
    SELECT 'geometry' AS ledger, event_key, dimension, observed_status
    FROM main_models.event_observation_geometry
    UNION ALL BY NAME
    SELECT 'pitch' AS ledger, event_key, dimension, observed_status
    FROM main_models.event_observation_pitch
    UNION ALL BY NAME
    SELECT 'credit' AS ledger, event_key, dimension, observed_status
    FROM main_models.event_observation_credit
) AS o
INNER JOIN main_models.event_observation_context AS e USING (event_key)
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6;
```

## Audits

Attach SQLMesh audits where possible and add custom audits where built-ins are insufficient:

| Table | Audit |
| --- | --- |
| `source_acquisition_ledger` | `unique_grain((game_id, team_id, dimension))`; accepted `target_population_status` and `source_block_status`; no target `PlayByPlay` event dimension marked aggregate-only; no `pitch_sequence` or `batted_ball` dimension marked `event_level` when game-level coverage flags are absent or false; `team_id IS NULL` exactly for game-wide dimensions. |
| `source_data_error_risk_ledger` | Accepted `training_action`; `training_weight` between 0 and 1; no confirmed issue with `training_action = allow`. |
| `official_aggregate_availability` | `unique_grain((game_id, team_id, player_id, fielding_position, stat_name))`; residual equals aggregate total minus event value when both present; no clean aggregate total with null aggregate value; `team_id` non-null. |
| `personnel_state_reliability` | At most one hard-mask player per event/side/position; `eligibility_status` aligned with `seed_reliability_class` canonical values (`direct`, `derived`, `inferred`, `synthetic`, `ambiguous`) plus any documented ledger-specific extensions; FK via `relationships(column := reliability_class, to_model := main_seeds.seed_reliability_class, ...)`. |
| `event_observation_geometry` | `unique_grain((event_key, dimension))`; `dimension` restricted to the geometry enum (`trajectory`, `location_side`, `location_depth`, `location_edge`, `general_location`, `ball_handler_position`, `pulled_opposite`); `observed_status` FK to `seed_observed_status`; `source_acquisition_status` non-null. |
| `event_observation_pitch` | `unique_grain((event_key, dimension))`; `dimension` restricted to the pitch enum (`count_balls`, `count_strikes`, `pitch_sequence`, `pitch_results`, `strike_types`, `pitch_count_total`); `observed_status` FK to `seed_observed_status`. |
| `event_observation_credit` | `unique_grain((event_key, dimension))`; `dimension` restricted to the credit enum (`putout_credit`, `assist_credit`, `error_credit`, `double_play_credit`, `triple_play_credit`, `passed_ball_credit`); `observed_status` FK to `seed_observed_status`. |
| `fielding_credit_gaps` | `unique_grain((event_key, fielding_team_id))`; accepted `fielding_evidence_status`; unknown putouts nonnegative when present; `unknown_putouts IS NULL` iff `fielding_evidence_status = 'no_fielding_row'`; allocation-eligible rows require personnel hard masks. |

## Acceptance Criteria

This prep phase is done when:

- All ledgers materialize for 1910-2025.
- Existing completeness models can be reproduced from ledger rollups.
- Source counts match the verified DB snapshot unless upstream sources changed and the docs are updated.
- Sentinel meanings are field-specific and not collapsed into a generic null.
- Data-error risk outputs can be joined to every planned modeling dataset.
- Fielding allocation datasets can select `eligible_for_allocation` rows without ad hoc joins to issue tables.
- No fitted model consumes raw event rows directly when a ledgered equivalent exists.
