{{
  config(
    materialized = 'table',
    )
}}
WITH base_states_players AS (
    SELECT
        base_states.event_key,
        base_states.baserunner,
        lineups.player_id
    FROM {{ ref('stg_event_starting_base_states') }} AS base_states
    INNER JOIN {{ ref('event_lineup_states') }} AS lineups
        ON base_states.event_key = lineups.event_key
            AND base_states.runner_lineup_position = lineups.lineup_position
),

pivoter AS (
    SELECT
        event_key,
        FIRST(player_id) FILTER (WHERE baserunner = 'First') AS first_base_runner_id,
        FIRST(player_id) FILTER (WHERE baserunner = 'Second') AS second_base_runner_id,
        FIRST(player_id) FILTER (WHERE baserunner = 'Third') AS third_base_runner_id
    FROM base_states_players
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
        -- Next two cols are transformed to reduce size of partition key in next step
        event_key // 255 AS game_key,
        CASE events.frame WHEN 'Top' THEN 0 ELSE 1 END AS frame,
        events.outs AS outs_start,
        COALESCE(outs_agg.outs, 0) AS outs_on_play,
        events.outs + COALESCE(outs_agg.outs, 0) AS outs_end,
        pivoter.first_base_runner_id,
        pivoter.second_base_runner_id,
        pivoter.third_base_runner_id,
        pivoter.first_base_runner_id IS NOT NULL AS is_runner_on_first,
        pivoter.second_base_runner_id IS NOT NULL AS is_runner_on_second,
        pivoter.third_base_runner_id IS NOT NULL AS is_runner_on_third,
    FROM {{ ref('stg_events') }} AS events
    LEFT JOIN pivoter USING (event_key)
    LEFT JOIN outs_agg USING (event_key)
    WHERE events.event_key NOT IN (SELECT event_key FROM {{ ref('event_no_plays') }})
),

add_state_ref AS (
    SELECT
        add_outs.*,
        base_state_ref.base_state,
        base_state_ref.base_state_string
    FROM add_outs
    INNER JOIN {{ ref('seed_base_state_info') }} AS base_state_ref
        USING (is_runner_on_first, is_runner_on_second, is_runner_on_third)
),

final AS (
    SELECT
        event_key,
        outs_start,
        outs_end,
        outs_on_play,
        first_base_runner_id AS first_base_runner_id_start,
        second_base_runner_id AS second_base_runner_id_start,
        third_base_runner_id AS third_base_runner_id_start,
        LEAD(first_base_runner_id) OVER w AS first_base_runner_id_end,
        LEAD(second_base_runner_id) OVER w AS second_base_runner_id_end,
        LEAD(third_base_runner_id) OVER w AS third_base_runner_id_end,
        is_runner_on_first AS is_runner_on_first_start,
        is_runner_on_second AS is_runner_on_second_start,
        is_runner_on_third AS is_runner_on_third_start,
        LEAD(is_runner_on_first) OVER w AS is_runner_on_first_end,
        LEAD(is_runner_on_second) OVER w AS is_runner_on_second_end,
        LEAD(is_runner_on_third) OVER w AS is_runner_on_third_end,
        base_state AS base_state_start,
        base_state_string AS base_state_string_start,
        LEAD(base_state) OVER w AS base_state_end,
        LEAD(base_state_string) OVER w AS base_state_string_end,
        LAG(event_key) OVER w IS NULL AS frame_start_flag,
        LEAD(event_key) OVER w IS NULL AS frame_end_flag,
        LEAD(event_key) OVER w IS NULL AND outs_end != 3 AS truncated_frame_flag,
    FROM add_state_ref
    WINDOW w AS (
        PARTITION BY game_key, inning, frame
        ORDER BY event_key
    )
)

SELECT * FROM final
