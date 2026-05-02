MODEL (
  name main_models.stg_box_score_team_fielding_lines,
  kind FULL,
  description 'Team-level fielding lines from box score accounts. These are generally rare and are only present in certain files.',
  grain (game_id, side),
  columns (
    game_id VARCHAR,
    side VARCHAR,
    outs_played BIGINT,
    putouts UTINYINT,
    assists UTINYINT,
    errors UTINYINT,
    double_plays UTINYINT,
    triple_plays UTINYINT,
    passed_balls UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    side = @doc('side'),
    outs_played = @doc('outs_played'),
    putouts = @doc('putouts'),
    assists = @doc('assists'),
    errors = @doc('errors'),
    double_plays = @doc('double_plays'),
    triple_plays = @doc('triple_plays'),
    passed_balls = @doc('passed_balls')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_box_score_team_fielding_lines.parquet'
  ),
);







WITH source AS (
    SELECT * FROM box_score.box_score_team_fielding_lines
),

renamed AS (
    SELECT
        game_id,
        side,
        outs_played::BIGINT AS outs_played,
        putouts::UTINYINT AS putouts,
        assists::UTINYINT AS assists,
        errors::UTINYINT AS errors,
        double_plays::UTINYINT AS double_plays,
        triple_plays::UTINYINT AS triple_plays,
        passed_balls::UTINYINT AS passed_balls

    FROM source
)

SELECT * FROM renamed
