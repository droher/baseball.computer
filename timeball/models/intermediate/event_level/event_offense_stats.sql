{{
  config(
    materialized = 'table',
    )
}}
WITH batter_baserunning AS (
    SELECT *
    FROM {{ ref('event_baserunning_stats') }}
    WHERE baserunner = 'Batter'
),

batter_stats AS (
    SELECT
        states.game_id,
        states.batting_team_id,
        states.batter_id AS player_id,
        event_key,
        'Batter' AS baserunner,
        hit.* EXCLUDE (event_key),
        bat.* EXCLUDE (event_key),
        batter_baserunning.* EXCLUDE (event_key, baserunner),
    FROM {{ ref('event_batting_stats') }} AS hit
    LEFT JOIN {{ ref('event_batted_ball_stats') }} AS bat USING (event_key)
    FULL OUTER JOIN batter_baserunning USING (event_key)
    LEFT JOIN {{ ref('event_states_batter_pitcher') }} states USING (event_key)
),

other_baserunner_stats AS (
    SELECT
        states.game_id,
        states.team_id AS batting_team_id,
        states.player_id,
        run.*
    FROM {{ ref('event_baserunning_stats') }} AS run
    INNER JOIN {{ ref('event_lineup_states') }} AS states
        ON run.event_key = states.event_key
            AND run.runner_lineup_position = states.lineup_position
    WHERE run.baserunner != 'Batter'
),

unioned AS (
    SELECT * FROM batter_stats
    UNION ALL BY NAME
    SELECT * FROM other_baserunner_stats

)

SELECT
    game_id,
    event_key,
    batting_team_id,
    baserunner,
    player_id,
    {% for stat in var('offense_stats') -%}
        COALESCE({{ stat }}, 0) AS {{ stat }},
    {% endfor %}
FROM unioned
