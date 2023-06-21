{{
  config(
    materialized = 'table',
    )
}}
WITH base_state AS (
    SELECT
        event_key,
        BIT_OR(baserunner_bit) AS base_state,
    FROM {{ ref('stg_event_base_states') }}
    WHERE base_state_type = 'Starting'
    GROUP BY 1
),

outs_agg AS (
    SELECT
        event_key,
        COUNT(*) AS outs
    FROM {{ ref('stg_event_outs') }}
    GROUP BY 1
),

add_outs AS (
    SELECT
        event_key,
        events.inning,
        events.frame,
        -- Next two cols are transformed to reduce size of partition key in next step
        event_key // 255 AS game_key,
        CASE events.frame WHEN 'Top' THEN 0 ELSE 1 END AS frame_key,
        events.outs AS outs_start,
        COALESCE(outs_agg.outs, 0) AS outs_on_play,
        events.outs + COALESCE(outs_agg.outs, 0) AS outs_end,
        base_state.base_state,
    FROM {{ ref('stg_events') }} AS events
    LEFT JOIN base_state USING (event_key)
    LEFT JOIN outs_agg USING (event_key)
    WHERE events.event_key NOT IN (SELECT event_key FROM {{ ref('event_no_plays') }})
),

final AS (
    SELECT
        event_key,
        inning AS inning_start,
        LEAD(inning) OVER wide AS inning_end,
        frame AS frame_start,
        LEAD(frame) OVER wide AS frame_end,
        outs_start,
        outs_end,
        outs_on_play,
        base_state AS base_state_start,
        LEAD(base_state) OVER narrow AS base_state_end,
        LAG(event_key) OVER narrow IS NULL AS frame_start_flag,
        LEAD(event_key) OVER narrow IS NULL AS frame_end_flag,
        LEAD(event_key) OVER narrow IS NULL AND outs_end != 3 AS truncated_frame_flag,
        LAG(event_key) OVER wide IS NULL AS game_start_flag,
        LEAD(event_key) OVER wide IS NULL AS game_end_flag,
    FROM add_outs
    WINDOW
        wide AS (
            PARTITION BY game_key
            ORDER BY event_key
        ),
        narrow AS (
            PARTITION BY game_key, inning, frame_key
            ORDER BY event_key
        )
)

SELECT * FROM final
