MODEL (
  name main_models.stg_box_score_fielding_lines,
  kind FULL,
  description 'Aggregate defensive data for each player in each game, derived from Retroshet box score data.',
  columns (
    game_id VARCHAR,
    fielder_id VARCHAR,
    side SIDE,
    fielding_position UTINYINT,
    nth_position_played_by_player UTINYINT,
    outs_played UTINYINT,
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
    fielding_position = @doc('fielding_position'),
    nth_position_played_by_player = 'Number increments on position switch, even if to a previous position',
    outs_played = @doc('outs_played'),
    putouts = @doc('putouts'),
    assists = @doc('assists'),
    errors = @doc('errors'),
    double_plays = @doc('double_plays'),
    triple_plays = @doc('triple_plays'),
    passed_balls = @doc('passed_balls')
  ),
);






WITH source AS (
    SELECT * FROM box_score.box_score_fielding_lines
),

renamed AS (
    SELECT
        game_id,
        fielder_id,
        side,
        fielding_position,
        nth_position_played_by_player,
        outs_played,
        putouts,
        assists,
        errors,
        double_plays,
        triple_plays,
        passed_balls

    FROM source
)

SELECT * FROM renamed
