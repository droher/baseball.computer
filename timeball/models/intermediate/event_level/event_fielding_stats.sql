{{
  config(
    materialized = 'table',
    )
}}
WITH fielding_plays_agg AS (
    SELECT
        event_key,
        SUM(putouts) AS putouts,
        SUM(assists) AS assists,
        SUM(errors) AS errors,
        SUM(fielders_choices) AS fielders_choices,
        COUNT(*) AS fielding_plays,
        SUM(unknown_putouts) AS unknown_putouts,
        SUM(unknown_events) AS unknown_events,
    FROM {{ ref('calc_fielding_play_agg') }}
    GROUP BY 1
),

baserunning AS (
    SELECT
        event_key,
        COUNT(*) FILTER (WHERE baserunning_play_type = 'StolenBase') AS stolen_bases,
        COUNT(*) FILTER (WHERE baserunning_play_type LIKE 'CaughtStealing') AS caught_stealing,
        COUNT(*) FILTER (WHERE baserunning_play_type LIKE 'PickedOff%') AS pickoffs,
        BOOL_OR(baserunning_play_type = 'PassedBall')::UTINYINT AS passed_balls,
    FROM {{ ref('stg_event_baserunners') }}
    WHERE baserunning_play_type IS NOT NULL
    GROUP BY 1
),

final AS (
    SELECT
        event_key,
        events.season,
        events.game_id,
        events.fielding_team_id AS team_id,
        lookup.personnel_fielding_key,
        events.outs_on_play AS outs_played,
        (events.plate_appearance_result IS NOT NULL)::UTINYINT AS plate_appearances_in_field,
        events.batted_to_fielder,
        COALESCE(fp.putouts, 0)::UTINYINT AS putouts,
        COALESCE(fp.assists, 0)::UTINYINT AS assists,
        COALESCE(fp.errors, 0)::UTINYINT AS errors,
        COALESCE(fp.fielders_choices, 0)::UTINYINT AS fielders_choices,
        COALESCE(fp.fielding_plays, 0)::UTINYINT AS fielding_plays,
        COALESCE(dp.is_double_play, FALSE)::UTINYINT AS double_plays,
        COALESCE(dp.is_triple_play, FALSE)::UTINYINT AS triple_plays,
        COALESCE(dp.is_ground_ball_double_play, FALSE)::UTINYINT AS ground_ball_double_plays,
        COALESCE(baserunning.stolen_bases, 0)::UTINYINT AS stolen_bases,
        COALESCE(baserunning.caught_stealing, 0)::UTINYINT AS caught_stealing,
        COALESCE(baserunning.pickoffs, 0)::UTINYINT AS pickoffs,
        COALESCE(baserunning.passed_balls, 0)::UTINYINT AS passed_balls,
        CASE WHEN prt.is_in_play THEN 1 ELSE 0 END::UTINYINT AS plate_appearances_in_field_with_ball_in_play,
        CASE WHEN events.plate_appearance_result = 'ReachedOnError' THEN 1 ELSE 0 END::UTINYINT AS reaching_errors,
        COALESCE(fp.unknown_putouts, 0)::UTINYINT AS unknown_putouts,
        COALESCE(fp.unknown_events, 0)::UTINYINT AS unknown_events,
    FROM {{ ref('stg_events') }} AS events
    LEFT JOIN baserunning USING (event_key)
    LEFT JOIN {{ ref('seed_plate_appearance_result_types') }} AS prt USING (plate_appearance_result)
    LEFT JOIN {{ ref('event_double_plays') }} AS dp USING (event_key)
    LEFT JOIN {{ ref('event_personnel_lookup') }} AS lookup USING (event_key)
    LEFT JOIN fielding_plays_agg AS fp USING (event_key)
)

SELECT * FROM final
