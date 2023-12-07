WITH final AS (
    SELECT
        cleaned_scorer AS scorer,
        substring(game_id, 4, 4)::int // 10 * 10 AS decade,
        SUM(game_share) AS plays,
        SUM(trajectory_broad_known * hits) AS hits_trajectory_known,
        SUM(trajectory_broad_unknown * hits) AS hits_trajectory_unknown,
        hits_trajectory_known / COUNT_IF(hits) AS trajectory_known_hit_rate,
        SUM((scorer_more_common_team_id = batting_team_id)::int * trajectory_broad_known * hits) / SUM((scorer_more_common_team_id = batting_team_id)::int * hits) AS trajectory_known_affiliated_team_hit_rate,
        SUM((scorer_more_common_team_id != batting_team_id)::int * trajectory_broad_known * hits) / SUM((scorer_more_common_team_id != batting_team_id)::int * hits) AS trajectory_known_opposing_team_hit_rate,
        SUM(singles * trajectory_broad_known) / SUM(singles) AS trajectory_known_single_rate,
        SUM(doubles * trajectory_broad_known) / SUM(doubles) AS trajectory_known_double_rate,
        SUM(triples * trajectory_broad_known) / SUM(triples) AS trajectory_known_triple_rate,
        SUM(trajectory_ground_ball * hits * balls_in_play) / SUM(trajectory_broad_known * hits * balls_in_play) AS hit_ground_ball_rate,
        SUM(trajectory_ground_ball * (1 - hits)) / SUM(trajectory_broad_known * (1 - hits)) AS out_ground_ball_rate,
        SUM(trajectory_line_drive * hits * balls_in_play) / SUM(trajectory_broad_air_ball * hits * balls_in_play) AS air_hit_line_drive_rate,
        SUM(trajectory_line_drive * (1 - hits)) / SUM(trajectory_broad_air_ball * (1 - hits)) AS air_out_line_drive_rate,
        SUM(trajectory_pop_fly * hits) / SUM(trajectory_broad_air_ball * hits) AS air_hit_popup_rate,
        SUM(trajectory_pop_fly * (1 - hits)) / SUM(trajectory_broad_air_ball * (1 - hits)) AS air_out_popup_rate,
        SUM(CASE WHEN batted_trajectory = 'Fly' THEN 1 ELSE 0 END * hits * balls_in_play) / SUM(trajectory_broad_air_ball * hits * balls_in_play) AS air_hit_explicit_fly_rate,
        SUM(CASE WHEN batted_trajectory = 'Fly' THEN 1 ELSE 0 END * (1 - hits)) / SUM(trajectory_broad_air_ball * (1 - hits)) AS air_out_explicit_fly_rate,
        SUM(home_runs * trajectory_line_drive) / SUM(home_runs) AS home_run_line_drive_rate,
    FROM {{ ref('event_offense_stats') }} s
    LEFT JOIN {{ ref('stg_events') }} AS e USING (game_id, event_key)
    LEFT JOIN {{ ref('game_scorekeeping') }} k USING (game_id)
    WHERE balls_batted = 1
    GROUP BY 1, 2
)

SELECT * FROM final
