{{
  config(
    materialized="table"
    )
}}
WITH final AS (
    SELECT
        o.event_key,
        o.outcome_has_batting_bin,
        o.outcome_is_in_play_bin,
        o.outcome_batted_contact_cat,
        o.outcome_batted_location_cat,
        o.outcome_plate_appearance_cat,
        o.outcome_baserunning_cat,
        o.outcome_runs_following_num,
        o.outcome_is_win_bin,
        o.generic_sample_weight,
        o.plate_appearance_sample_weight,
        o.in_play_sample_weight,
        o.contact_sample_weight,
        o.location_sample_weight,
        o.baserunning_play_sample_weight,
        o.win_sample_weight,
        e.season AS season_num,
        DATE_PART('dayofyear', e.date) AS day_of_year_num,
        LEAST(e.inning_start, 10) AS inning_num,
        CASE WHEN e.frame_start = 'Top' THEN 0 ELSE 1 END AS frame_num,
        CASE WHEN e.time_of_day = 'Night' THEN 1 ELSE 0 END AS is_night_game_num,
        CASE
            WHEN e.batting_side = 'Home'
                THEN e.score_home_start
            ELSE e.score_away_start
        END AS score_batting_team_num,
        CASE
            WHEN e.batting_side = 'Home'
                THEN e.score_away_start
            ELSE e.score_home_start
        END AS score_fielding_team_num,
        e.park_id AS park_cat,
        e.game_type AS game_type_cat,
        COALESCE(e.league, 'None') AS league_cat,
        e.batting_team_id AS batting_team_cat,
        e.fielding_team_id AS fielding_team_cat,
        e.base_state_start::VARCHAR AS base_state_cat,
        e.batter_lineup_position::VARCHAR AS batter_lineup_position_cat,
        e.batter_id AS batter_player,
        COALESCE(e.runner_first_id_start, 'N/A') AS runner_first_player,
        COALESCE(e.runner_second_id_start, 'N/A') AS runner_second_player,
        COALESCE(e.runner_third_id_start, 'N/A') AS runner_third_player,
        e.pitcher_id AS pitcher_player,
        e.catcher_id AS catcher_player,
        CASE
            WHEN HASH(e.game_id)::HUGEINT % 100 BETWEEN 0 AND 97 THEN 'TRAIN'
            ELSE 'TEST'
        END AS meta_train_test_split,

    FROM {{ ref('ml_event_outcomes') }} AS o
    -- This is really an inner join but using o as the base table
    -- preserves the random order
    LEFT JOIN {{ ref('event_states_full') }} AS e USING (event_key)
)

SELECT * FROM final
