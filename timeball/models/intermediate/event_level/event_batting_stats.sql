{{
  config(
    materialized = 'table',
    )
}}
WITH rbi AS (
    SELECT
        event_key,
        COUNT(*) AS runs_batted_in
    FROM {{ ref('stg_event_runs') }}
    WHERE rbi_flag
    GROUP BY 1
),

add_ids AS (
    SELECT
        plate_appearances.*,
        events.game_id
    FROM {{ ref('stg_event_plate_appearances') }} AS plate_appearances
    INNER JOIN {{ ref('stg_events') }} AS events USING (event_key)
),

final AS (
    SELECT
        add_ids.event_key,
        1 AS plate_appearances,
        result_types.is_at_bat::INT AS at_bats,
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
        (result_types.plate_appearance_result = 'FieldersChoice')::INT AS fielders_choices,
        (result_types.plate_appearance_result = 'ReachedOnError')::INT AS reached_on_errors,
        (result_types.plate_appearance_result = 'Interference')::INT AS reached_on_interferences,

        result_types.is_on_base_opportunity::INT AS on_base_opportunities,

        result_types.is_on_base_success::INT AS on_base_successes,
        COALESCE(rbi.runs_batted_in, 0) AS runs_batted_in,
        COALESCE(double_plays.is_ground_ball_double_play::INT, 0) AS ground_ball_double_plays,

        COALESCE(double_plays.is_double_play::INT, 0) AS double_plays,
        COALESCE(double_plays.is_triple_play::INT, 0) AS triple_plays,
        -- The extra out from GIDPs is attributed to the batter,
        -- but for other types of double plays, the other out
        -- is considered to be a baserunning out.
        result_types.is_batting_out::INT + ground_ball_double_plays AS batting_outs

    FROM add_ids
    INNER JOIN {{ ref('seed_plate_appearance_result_types') }} AS result_types
        USING (plate_appearance_result)
    LEFT JOIN {{ ref('event_double_plays') }} AS double_plays USING (event_key)
    LEFT JOIN rbi USING (event_key)
)

SELECT * FROM final
