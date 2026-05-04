MODEL (
  name main_models.ml_features,
  kind FULL,
  grain (event_key),
  columns (
    event_key UINTEGER,
    outcome_has_batting_bin UTINYINT,
    outcome_is_in_play_bin UTINYINT,
    outcome_batted_trajectory_cat VARCHAR,
    outcome_batted_location_cat VARCHAR,
    outcome_plate_appearance_cat VARCHAR,
    outcome_baserunning_cat VARCHAR,
    outcome_runs_following_num FLOAT,
    outcome_is_win_bin UTINYINT,
    generic_sample_weight FLOAT,
    plate_appearance_sample_weight FLOAT,
    in_play_sample_weight FLOAT,
    trajectory_sample_weight FLOAT,
    location_sample_weight FLOAT,
    baserunning_play_sample_weight FLOAT,
    win_sample_weight FLOAT,
    season_num FLOAT,
    day_of_year_num FLOAT,
    inning_num FLOAT,
    frame_num FLOAT,
    is_night_game_num FLOAT,
    score_batting_team_num FLOAT,
    score_fielding_team_num FLOAT,
    park_cat PARK_ID,
    game_type_cat GAME_TYPE,
    league_cat VARCHAR,
    base_state_cat VARCHAR,
    batter_player VARCHAR,
    runner_first_player VARCHAR,
    runner_second_player VARCHAR,
    runner_third_player VARCHAR,
    pitcher_player VARCHAR,
    meta_train_test_split VARCHAR
  ),
  column_descriptions (
    event_key = @doc('event_key')
  ),
  audits (
    not_null(columns := (event_key)),
    unique_grain(columns := (event_key)),
    relationships(column := event_key, to_model := main_models.ml_event_outcomes, to_column := event_key)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_ml_features.parquet'
  ),
);







WITH final AS (
    SELECT
        o.event_key,
        o.outcome_has_batting_bin,
        o.outcome_is_in_play_bin,
        o.outcome_batted_trajectory_cat,
        o.outcome_batted_location_cat,
        o.outcome_plate_appearance_cat,
        o.outcome_baserunning_cat,
        o.outcome_runs_following_num,
        o.outcome_is_win_bin,
        o.generic_sample_weight,
        o.plate_appearance_sample_weight,
        o.in_play_sample_weight,
        o.trajectory_sample_weight,
        o.location_sample_weight,
        o.baserunning_play_sample_weight,
        o.win_sample_weight,
        e.season::FLOAT AS season_num,
        DATE_PART('dayofyear', e.date)::FLOAT AS day_of_year_num,
        LEAST(e.inning_start, 10)::FLOAT AS inning_num,
        CASE WHEN e.frame_start = 'Top' THEN 0 ELSE 1 END::FLOAT AS frame_num,
        CASE WHEN e.time_of_day = 'Night' THEN 1 ELSE 0 END::FLOAT AS is_night_game_num,
        CASE
            WHEN e.batting_side = 'Home'
                THEN e.score_home_start
            ELSE e.score_away_start
        END::FLOAT AS score_batting_team_num,
        CASE
            WHEN e.batting_side = 'Home'
                THEN e.score_away_start
            ELSE e.score_home_start
        END::FLOAT AS score_fielding_team_num,
        e.park_id AS park_cat,
        e.game_type AS game_type_cat,
        COALESCE(e.league, 'None') AS league_cat,
        e.base_state_start::VARCHAR AS base_state_cat,
        e.batter_id AS batter_player,
        COALESCE(e.runner_first_id_start, 'N/A') AS runner_first_player,
        COALESCE(e.runner_second_id_start, 'N/A') AS runner_second_player,
        COALESCE(e.runner_third_id_start, 'N/A') AS runner_third_player,
        e.pitcher_id AS pitcher_player,
        CASE
            WHEN HASH(e.game_id)::HUGEINT % 100 BETWEEN 0 AND 97 THEN 'TRAIN'
            ELSE 'TEST'
        END AS meta_train_test_split,

    FROM main_models.ml_event_outcomes AS o
    -- This is really an inner join but using o as the base table
    -- preserves the random order
    LEFT JOIN main_models.event_states_full AS e USING (event_key)
)

SELECT * FROM final
