WITH final AS (
    SELECT
        cleaned_scorer || COALESCE(inputter, '') || COALESCE(translator, '') AS scorer,
        substring(game_id, 4, 4)::int // 10 * 10 AS decade,
        SUM(game_share) AS plays,
        SUM(batted_location_known * hits) AS hits_location_known,
        SUM(batted_location_unknown * hits) AS hits_location_unknown,
        hits_location_known / SUM(hits) AS location_known_hit_rate,
        SUM(batted_distance_infield * hits)/hits_location_known AS hits_location_infield_rate,
        SUM(batted_distance_outfield * hits)/hits_location_known AS hits_location_outfield_rate,
        SUM(singles * batted_location_known) / SUM(singles) AS location_known_single_rate,
        SUM(doubles * batted_location_known) / SUM(doubles) AS location_known_double_rate,
        SUM(triples * batted_location_known) / SUM(triples) AS location_known_triple_rate,
        SUM((li.win_leverage_index > 1)::int * batted_location_known * hits) / SUM((li.win_leverage_index > 1)::int * hits) AS location_known_high_leverage_rate,
        SUM((e.runs_on_play >= 1)::int * batted_location_known * hits) / SUM((e.runs_on_play >= 1)::int * hits) AS location_known_run_scoring_play_rate,
        SUM((e.base_state > 0)::int * batted_location_known * hits) / SUM((e.base_state > 0)::int * hits) AS location_known_runners_on_base_rate,
        SUM(batted_location_left_infield * hits) / SUM(batted_distance_infield * hits) AS known_location_share_left_infield,
        SUM(batted_location_middle_infield * hits) / SUM(batted_distance_infield * hits) AS known_location_share_middle_infield,
        SUM(batted_location_right_infield * hits) / SUM(batted_distance_infield * hits) AS known_location_share_right_infield,
        SUM(batted_location_left_field * hits) / SUM(batted_distance_outfield * hits) AS known_location_share_left_field,
        SUM(batted_location_center_field * hits) / SUM(batted_distance_outfield * hits) AS known_location_share_center_field,
        SUM(batted_location_right_field * hits) / SUM(batted_distance_outfield * hits) AS known_location_share_right_field,
        SUM((batted_to_fielder = 7)::int * hits) / SUM(fielded_by_outfielder * hits) AS known_fielded_by_left_fielder_rate,
        SUM((batted_to_fielder = 8)::int * hits) / SUM(fielded_by_outfielder * hits) AS known_fielded_by_center_fielder_rate,
        SUM((batted_to_fielder = 9)::int * hits) / SUM(fielded_by_outfielder * hits) AS known_fielded_by_right_fielder_rate,
        SUM(contact_type_ground_ball * batted_location_known * fielded_by_outfielder * hits) / SUM(contact_broad_type_known * batted_location_known * fielded_by_outfielder * hits) AS outfield_hit_ground_ball_rate,
        SUM(batted_balls_pulled * batted_location_known * hits) / SUM(batted_location_known * hits) AS known_location_pulled_rate,
        SUM(batted_balls_opposite_field * batted_location_known * hits) / SUM(batted_location_known * hits) AS known_location_opposite_field_rate,
        SUM(batted_distance_outfield * fielded_by_unknown * hits) / SUM(batted_distance_known * fielded_by_unknown * hits) AS unknown_fielder_hit_in_outfield_rate,
    FROM {{ ref('event_offense_stats') }} s
    LEFT JOIN {{ ref('game_scorekeeping') }} k USING (game_id)
    INNER JOIN {{ ref('event_states_full') }} f USING (event_key)
    LEFT JOIN {{ ref('stg_events') }} AS e USING (event_key)
    LEFT JOIN {{ ref('leverage_index') }} li USING (win_expectancy_start_key)
    WHERE balls_in_play = 1
    GROUP BY 1, 2
)

SELECT location_known_hit_rate * hits_location_outfield_rate AS r, * FROM final
WHERE hits_location_known > 50
