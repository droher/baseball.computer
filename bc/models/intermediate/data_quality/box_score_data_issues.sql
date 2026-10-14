MODEL (
  name main_models.box_score_data_issues,
  kind FULL,
  grain (source_table, game_id, player_id, issue_type),
  depends_on (main_models.game_start_info),
  description 'Every row in the `stg_box_score_*_lines` tables and in the Negro Leagues `nlb_batting_lines` and `nlb_pitching_lines` that violates a definitional within-row invariant (e.g. hits exceeding at-bats). The box-score lines come verbatim from the parquet emitted by baseball.computer.rs and the NLB lines from Retrosheet''s negroleagues.zip, so the issues live upstream of this repo. Used by the `bounded_excluding_data_issues` audit to carve out known source-data exceptions on game-grain stat models, so any final-layer violation that is *not* listed here is treated as a real bug introduced by this layer. Extra-base-hit and home-run issues are accepted only from Negro League and pre-1875 box-score accounts; elsewhere they fail the `box_score_data_issue_source_scope` audit.',
  column_descriptions (
    source_table = 'The `stg_box_score_*_lines` or `nlb_*_lines` table that carries the bad row.',
    game_id = @doc('game_id'),
    player_id = @doc('player_id'),
    issue_type = 'Stable identifier for the violated invariant. Audits filter on this column to scope which carve-out applies.',
    value_a = 'The larger of the two values that violated the invariant (e.g. `hits` when `hits > at_bats`).',
    value_b = 'The smaller of the two values (e.g. `at_bats` when `hits > at_bats`).'
  ),
  audits (
    not_null(columns := (source_table, game_id, player_id, issue_type)),
    unique_grain(columns := (source_table, game_id, player_id, issue_type)),
    box_score_data_issue_source_scope(issue_types := ['extra_base_hits_gt_hits', 'home_runs_gt_hits'], excused_population := FALSE),
    box_score_data_issue_source_scope(issue_types := ['extra_base_hits_gt_hits', 'home_runs_gt_hits'], excused_population := TRUE, blocking := FALSE)
  ),
);

WITH batting_lines AS (
    SELECT 'stg_box_score_batting_lines' AS source_table, game_id, batter_id, hits, at_bats, strikeouts, plate_appearances, doubles, triples, home_runs
    FROM main_models.stg_box_score_batting_lines
    UNION ALL
    SELECT 'nlb_batting_lines' AS source_table, game_id, batter_id, hits, at_bats, strikeouts, plate_appearances, doubles, triples, home_runs
    FROM main_models.nlb_batting_lines
),

pitching_lines AS (
    SELECT 'stg_box_score_pitching_lines' AS source_table, game_id, pitcher_id, hits, batters_faced, home_runs, strikeouts, earned_runs, runs
    FROM main_models.stg_box_score_pitching_lines
    UNION ALL
    SELECT 'nlb_pitching_lines' AS source_table, game_id, pitcher_id, hits, batters_faced, home_runs, strikeouts, earned_runs, runs
    FROM main_models.nlb_pitching_lines
),

batting AS (
    SELECT
        source_table,
        game_id,
        batter_id AS player_id,
        'hits_gt_at_bats' AS issue_type,
        hits::USMALLINT AS value_a,
        at_bats::USMALLINT AS value_b,
    FROM batting_lines
    WHERE hits > at_bats
    UNION ALL BY NAME
    SELECT
        source_table,
        game_id,
        batter_id AS player_id,
        'strikeouts_gt_plate_appearances' AS issue_type,
        strikeouts::USMALLINT AS value_a,
        plate_appearances::USMALLINT AS value_b,
    FROM batting_lines
    WHERE plate_appearances IS NOT NULL AND strikeouts > plate_appearances
    UNION ALL BY NAME
    SELECT
        source_table,
        game_id,
        batter_id AS player_id,
        'extra_base_hits_gt_hits' AS issue_type,
        (COALESCE(doubles, 0) + COALESCE(triples, 0) + COALESCE(home_runs, 0))::USMALLINT AS value_a,
        hits::USMALLINT AS value_b,
    FROM batting_lines
    WHERE (COALESCE(doubles, 0) + COALESCE(triples, 0) + COALESCE(home_runs, 0)) > hits
),

pitching AS (
    SELECT
        source_table,
        game_id,
        pitcher_id AS player_id,
        'hits_gt_batters_faced' AS issue_type,
        hits::USMALLINT AS value_a,
        batters_faced::USMALLINT AS value_b,
    FROM pitching_lines
    WHERE batters_faced IS NOT NULL AND hits > batters_faced
    UNION ALL BY NAME
    SELECT
        source_table,
        game_id,
        pitcher_id AS player_id,
        'home_runs_gt_hits' AS issue_type,
        home_runs::USMALLINT AS value_a,
        hits::USMALLINT AS value_b,
    FROM pitching_lines
    WHERE home_runs > hits
    UNION ALL BY NAME
    SELECT
        source_table,
        game_id,
        pitcher_id AS player_id,
        'strikeouts_gt_batters_faced' AS issue_type,
        strikeouts::USMALLINT AS value_a,
        batters_faced::USMALLINT AS value_b,
    FROM pitching_lines
    WHERE batters_faced IS NOT NULL AND strikeouts > batters_faced
    UNION ALL BY NAME
    SELECT
        source_table,
        game_id,
        pitcher_id AS player_id,
        'earned_runs_gt_runs' AS issue_type,
        earned_runs::USMALLINT AS value_a,
        runs::USMALLINT AS value_b,
    FROM pitching_lines
    WHERE earned_runs > runs
)

SELECT * FROM batting
UNION ALL BY NAME
SELECT * FROM pitching
