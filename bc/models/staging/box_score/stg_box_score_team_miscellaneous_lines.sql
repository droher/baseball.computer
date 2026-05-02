MODEL (
  name main_models.stg_box_score_team_miscellaneous_lines,
  kind FULL,
  description 'Team-level miscellaneous lines from box score accounts, featuring stats that either only apply at a team level or were only tracked at a team level.',
  grain (game_id, side),
  columns (
    game_id VARCHAR,
    side VARCHAR,
    left_on_base BIGINT,
    team_earned_runs BIGINT,
    double_plays_turned BIGINT,
    triple_plays_turned BIGINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    side = @doc('side'),
    left_on_base = @doc('left_on_base')
  ),
  audits (
    not_null(columns := (game_id, side)),
    unique_grain(columns := (game_id, side)),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_box_score_team_miscellaneous_lines.parquet'
  ),
);







WITH source AS (
    SELECT * FROM box_score.box_score_team_miscellaneous_lines
),

renamed AS (
    SELECT
        game_id,
        side,
        left_on_base,
        team_earned_runs,
        double_plays_turned,
        triple_plays_turned

    FROM source
)

SELECT * FROM renamed
