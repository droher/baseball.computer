{{
  config(
    materialized = 'table',
    )
}}
WITH sacs AS (
    -- No need for distinct, max one sac per event
    SELECT event_key
    FROM {{ ref('stg_event_flags') }}
    WHERE flag IN ('SacrificeFly', 'SacrificeHit')
),

final AS (
    SELECT
        pa.game_id,
        pa.event_key,
        CASE WHEN result_types.plate_appearance_result = 'StrikeOut'
                THEN COALESCE(pa.strikeout_responsible_batter_id, pa.batter_id)
            ELSE pa.batter_id
        END AS batter_id,
        CASE WHEN result_types.plate_appearance_result IN ('Walk', 'IntentionalWalk')
                THEN COALESCE(pa.walk_responsible_pitcher_id, pa.pitcher_id)
            ELSE pa.pitcher_id
        END AS pitcher_id,
        pa.batting_team_id,
        pa.fielding_team_id,
        pa.batter_lineup_position,
        1 AS plate_appearances,
        (result_types.is_at_bat AND sacs.event_key IS NULL)::INT AS at_bats,
        result_types.is_hit::INT AS hits,
        (result_types.total_bases = 1)::INT AS singles,
        (result_types.total_bases = 2)::INT AS doubles,
        (result_types.total_bases = 3)::INT AS triples,
        (result_types.total_bases = 4)::INT AS home_runs,
        result_types.total_bases,

        (result_types.plate_appearance_result = 'StrikeOut')::INT AS strikeouts,
        (result_types.plate_appearance_result IN ('Walk', 'IntentionalWalk'))::INT AS walks,
        (result_types.plate_appearance_result = 'IntentionalWalk')::INT AS intentional_walks,
        (result_types.plate_appearance_result = 'HitByPitch')::INT AS hit_by_pitches,
        (result_types.plate_appearance_result = 'SacrificeFly')::INT AS sacrifice_flies,
        (result_types.plate_appearance_result = 'SacrificeHit')::INT AS sacrifice_hits,
        (result_types.plate_appearance_result = 'ReachedOnError')::INT AS reached_on_errors,
        (result_types.plate_appearance_result = 'Interference')::INT AS reached_on_interferences,

        result_types.is_on_base_opportunity::INT AS on_base_opportunities,

        result_types.is_on_base_success::INT AS on_base_successes,
        COALESCE(pa.runs_batted_in, 0) AS runs_batted_in,
        COALESCE(double_plays.is_ground_ball_double_play::INT, 0) AS grounded_into_double_plays,

        COALESCE(double_plays.is_double_play::INT, 0) AS double_plays,
        COALESCE(double_plays.is_triple_play::INT, 0) AS triple_plays,
        -- The extra out from GIDPs is attributed to the batter,
        -- but for other types of double plays, the other out
        -- is considered to be a baserunning out (for now)
        result_types.is_batting_out::INT + grounded_into_double_plays AS batting_outs,
        pa.outs_on_play

    FROM {{ ref('stg_events') }} AS pa
    INNER JOIN {{ ref('seed_plate_appearance_result_types') }} AS result_types
        USING (plate_appearance_result)
    LEFT JOIN {{ ref('event_double_plays') }} AS double_plays USING (event_key)
    LEFT JOIN sacs USING (event_key)
    WHERE pa.plate_appearance_result IS NOT NULL
)

SELECT * FROM final