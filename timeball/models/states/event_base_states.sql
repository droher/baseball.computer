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
    PIVOT base_states_players
    ON baserunner IN ('First', 'Second', 'Third')
    USING FIRST(player_id) AS base_runner_id
    GROUP BY event_key
),

add_flags AS (
    SELECT
        *,
        first_base_runner_id IS NOT NULL AS is_runner_on_first,
        second_base_runner_id IS NOT NULL AS is_runner_on_second,
        third_base_runner_id IS NOT NULL AS is_runner_on_third
    FROM pivoter
),

final AS (
    SELECT
        add_flags.*,
        base_state_ref.base_state,
        base_state_ref.base_state_string
    FROM add_flags
    INNER JOIN {{ ref('seed_base_state_info') }} AS base_state_ref
        USING (is_runner_on_first, is_runner_on_second, is_runner_on_third)
)

SELECT * FROM final
