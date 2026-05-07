MODEL (
  name main_models.box_score_data_issues,
  kind FULL,
  grain (source_table, game_id, player_id, issue_type),
  description 'Every row in the `stg_box_score_*_lines` tables that violates a definitional within-row invariant (e.g. hits exceeding at-bats). Sourced verbatim from the parquet emitted by baseball.computer.rs, so the issues live upstream of this repo. Used by the `bounded_excluding_data_issues` audit to carve out known source-data exceptions on game-grain stat models, so any final-layer violation that is *not* listed here is treated as a real bug introduced by this layer.',
  column_descriptions (
    source_table = 'The `stg_box_score_*_lines` table that carries the bad row.',
    game_id = @doc('game_id'),
    player_id = @doc('player_id'),
    issue_type = 'Stable identifier for the violated invariant. Audits filter on this column to scope which carve-out applies.',
    value_a = 'The larger of the two values that violated the invariant (e.g. `hits` when `hits > at_bats`).',
    value_b = 'The smaller of the two values (e.g. `at_bats` when `hits > at_bats`).'
  ),
  audits (
    not_null(columns := (source_table, game_id, player_id, issue_type)),
    unique_grain(columns := (source_table, game_id, player_id, issue_type))
  ),
);

WITH batting AS (
    SELECT
        'stg_box_score_batting_lines' AS source_table,
        game_id,
        batter_id AS player_id,
        'hits_gt_at_bats' AS issue_type,
        hits::USMALLINT AS value_a,
        at_bats::USMALLINT AS value_b,
    FROM main_models.stg_box_score_batting_lines
    WHERE hits > at_bats
    UNION ALL BY NAME
    SELECT
        'stg_box_score_batting_lines' AS source_table,
        game_id,
        batter_id AS player_id,
        'strikeouts_gt_plate_appearances' AS issue_type,
        strikeouts::USMALLINT AS value_a,
        plate_appearances::USMALLINT AS value_b,
    FROM main_models.stg_box_score_batting_lines
    WHERE plate_appearances IS NOT NULL AND strikeouts > plate_appearances
    UNION ALL BY NAME
    SELECT
        'stg_box_score_batting_lines' AS source_table,
        game_id,
        batter_id AS player_id,
        'extra_base_hits_gt_hits' AS issue_type,
        (COALESCE(doubles, 0) + COALESCE(triples, 0) + COALESCE(home_runs, 0))::USMALLINT AS value_a,
        hits::USMALLINT AS value_b,
    FROM main_models.stg_box_score_batting_lines
    WHERE (COALESCE(doubles, 0) + COALESCE(triples, 0) + COALESCE(home_runs, 0)) > hits
),

pitching AS (
    SELECT
        'stg_box_score_pitching_lines' AS source_table,
        game_id,
        pitcher_id AS player_id,
        'hits_gt_batters_faced' AS issue_type,
        hits::USMALLINT AS value_a,
        batters_faced::USMALLINT AS value_b,
    FROM main_models.stg_box_score_pitching_lines
    WHERE batters_faced IS NOT NULL AND hits > batters_faced
    UNION ALL BY NAME
    SELECT
        'stg_box_score_pitching_lines' AS source_table,
        game_id,
        pitcher_id AS player_id,
        'home_runs_gt_hits' AS issue_type,
        home_runs::USMALLINT AS value_a,
        hits::USMALLINT AS value_b,
    FROM main_models.stg_box_score_pitching_lines
    WHERE home_runs > hits
    UNION ALL BY NAME
    SELECT
        'stg_box_score_pitching_lines' AS source_table,
        game_id,
        pitcher_id AS player_id,
        'strikeouts_gt_batters_faced' AS issue_type,
        strikeouts::USMALLINT AS value_a,
        batters_faced::USMALLINT AS value_b,
    FROM main_models.stg_box_score_pitching_lines
    WHERE batters_faced IS NOT NULL AND strikeouts > batters_faced
    UNION ALL BY NAME
    SELECT
        'stg_box_score_pitching_lines' AS source_table,
        game_id,
        pitcher_id AS player_id,
        'earned_runs_gt_runs' AS issue_type,
        earned_runs::USMALLINT AS value_a,
        runs::USMALLINT AS value_b,
    FROM main_models.stg_box_score_pitching_lines
    WHERE earned_runs > runs
)

SELECT * FROM batting
UNION ALL BY NAME
SELECT * FROM pitching
