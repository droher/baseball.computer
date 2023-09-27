{{
  config(
    materialized = 'table',
    )
}}
WITH pa_map AS (
    SELECT
        e.game_id,
        e.season,
        e.event_key,
        e.outs_on_play,
        e.batting_side,
        SUM(e.runs_on_play) OVER rest_of_inning::FLOAT AS outcome_runs_following_num,
        result_types.result_category AS pa_result,
        result_types.is_in_play,
        bb.contact,
        bb.location_depth,
        bb.location_side,
    FROM {{ ref('stg_events') }} AS e
    LEFT JOIN {{ ref('seed_plate_appearance_result_types')}} AS result_types USING (plate_appearance_result)
    LEFT JOIN {{ ref('calc_batted_ball_type') }} AS bb USING (event_key)
    WINDOW
        rest_of_inning AS (
            PARTITION BY e.game_id, e.inning, e.frame
            ORDER BY e.event_id
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )
),

baserunning_map AS (
    SELECT
        e.event_key,
        LIST(
            CASE 
                WHEN is_out THEN play_types.result_category_out
                ELSE play_types.result_category_safe
            END
            ORDER BY play_types.priority
        )[1] AS baserunning_result,
    FROM {{ ref('stg_event_baserunners') }} e
    INNER JOIN {{ ref('seed_baserunning_play_types') }} AS play_types USING (baserunning_play_type)
    GROUP BY e.event_key
),

joined AS (
    SELECT
        event_key,
        pa_map.season,
        (pa_map.pa_result IS NOT NULL)::INT AS outcome_has_batting_bin,
        COALESCE(pa_map.is_in_play, FALSE)::INT AS outcome_is_in_play_bin,
        COALESCE(pa_map.pa_result, 'Other') AS outcome_plate_appearance_cat,
        COALESCE(baserunning_map.baserunning_result, 'Other') AS outcome_baserunning_cat,
        COALESCE(pa_map.contact, 'Other') AS outcome_batted_contact_cat,
        COALESCE(pa_map.location_depth || '-' || pa_map.location_side, 'Other') AS outcome_batted_location_cat,
        pa_map.outcome_runs_following_num,
        COALESCE(game_end.winning_side = pa_map.batting_side, FALSE)::INT AS outcome_is_win_bin,
        COALESCE(pa_map.contact != 'Unknown', FALSE)::INT AS has_known_contact,
        COALESCE('Unknown' NOT IN (pa_map.location_depth, pa_map.location_side), FALSE)::INT AS has_known_location,
        (game_end.winning_side IS NOT NULL)::INT AS is_full_game,
        1 / (COUNT(*) OVER (PARTITION BY GREATEST(1913, pa_map.season)) / COUNT(*) OVER ()::FLOAT) AS season_sample_factor,
    FROM pa_map
    LEFT JOIN baserunning_map USING (event_key)
    INNER JOIN {{ ref('game_results') }} AS game_end USING (game_id)
    WHERE pa_map.pa_result IS NOT NULL OR baserunning_map.baserunning_result IS NOT NULL
),

add_weights AS (
    SELECT
        event_key,
        outcome_has_batting_bin,
        outcome_is_in_play_bin,
        outcome_batted_contact_cat,
        outcome_batted_location_cat,
        outcome_plate_appearance_cat,
        outcome_baserunning_cat,
        outcome_runs_following_num,
        outcome_is_win_bin,
        season_sample_factor / AVG(season_sample_factor) OVER ()::FLOAT AS generic_sample_weight,
        generic_sample_weight * outcome_has_batting_bin::FLOAT AS plate_appearance_sample_weight,
        generic_sample_weight * outcome_has_batting_bin::FLOAT AS in_play_sample_weight,
        generic_sample_weight * has_known_contact::FLOAT AS contact_sample_weight,
        generic_sample_weight * has_known_location::FLOAT AS location_sample_weight,
        generic_sample_weight * (1 - outcome_has_batting_bin)::FLOAT AS baserunning_play_sample_weight,
        generic_sample_weight * is_full_game::FLOAT AS win_sample_weight,
    FROM joined

),

final AS (
    SELECT * REPLACE (
        plate_appearance_sample_weight / AVG(plate_appearance_sample_weight) OVER () AS plate_appearance_sample_weight,
        in_play_sample_weight / AVG(in_play_sample_weight) OVER () AS in_play_sample_weight,
        contact_sample_weight / AVG(contact_sample_weight) OVER () AS contact_sample_weight,
        location_sample_weight / AVG(location_sample_weight) OVER () AS location_sample_weight,
        baserunning_play_sample_weight / AVG(baserunning_play_sample_weight) OVER () AS baserunning_play_sample_weight,
        win_sample_weight / AVG(win_sample_weight) OVER () AS win_sample_weight,
    )
    FROM add_weights
)

SELECT *
FROM final
ORDER BY RANDOM()
