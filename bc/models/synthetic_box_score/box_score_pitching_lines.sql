MODEL (
  name synthetic_box_score.box_score_pitching_lines,
  kind FULL,
  description 'Synthetic pitching lines for gamelog-only games. One row per team-game with the listed starting pitcher as the only pitcher (nth_pitcher = 1). Stat columns stay NULL — the gamelog gives no per-pitcher detail beyond the starter''s identity.',
  grain (game_id, pitcher_id, nth_pitcher),
  column_descriptions (
    game_id = @doc('game_id'),
    pitcher_id = @doc('pitcher_id'),
    side = @doc('side')
  ),
  audits (
    not_null(columns := (game_id, pitcher_id, nth_pitcher, side)),
    unique_grain(columns := (game_id, pitcher_id, nth_pitcher)),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/synthetic_box_score_box_score_pitching_lines.parquet'
  ),
);









WITH gamelog_games AS (
    SELECT
        g.game_id,
        gl.away_starting_pitcher_id,
        gl.home_starting_pitcher_id
    FROM synthetic_box_score.box_score_games AS g
    INNER JOIN main_models.stg_gamelog AS gl USING (game_id)
),

unioned AS (
    SELECT
        game_id,
        away_starting_pitcher_id AS pitcher_id,
        'Away' AS side
    FROM gamelog_games
    UNION ALL BY NAME
    SELECT
        game_id,
        home_starting_pitcher_id AS pitcher_id,
        'Home' AS side
    FROM gamelog_games
)

SELECT
    game_id::GAME_ID AS game_id,
    pitcher_id::PLAYER_ID AS pitcher_id,
    side::SIDE AS side,
    1::UTINYINT AS nth_pitcher,
    NULL::UTINYINT AS outs_recorded,
    NULL::UTINYINT AS no_out_batters,
    NULL::UTINYINT AS batters_faced,
    NULL::UTINYINT AS hits,
    NULL::UTINYINT AS doubles,
    NULL::UTINYINT AS triples,
    NULL::UTINYINT AS home_runs,
    NULL::UTINYINT AS runs,
    NULL::UTINYINT AS earned_runs,
    NULL::UTINYINT AS walks,
    NULL::UTINYINT AS intentional_walks,
    NULL::UTINYINT AS strikeouts,
    NULL::UTINYINT AS hit_batsmen,
    NULL::UTINYINT AS wild_pitches,
    NULL::UTINYINT AS balks,
    NULL::UTINYINT AS sacrifice_hits,
    NULL::UTINYINT AS sacrifice_flies
FROM unioned
WHERE pitcher_id IS NOT NULL
