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
        hit.batter_id AS player_id,
        event_key,
        'Batter' AS baserunner,
        hit.* EXCLUDE (event_key, batter_id),
        bat.* EXCLUDE (event_key),
        batter_baserunning.* EXCLUDE (event_key, baserunner),
    FROM {{ ref('event_batting_stats') }} AS hit
    LEFT JOIN {{ ref('event_batted_ball_stats') }} AS bat USING (event_key)
    FULL OUTER JOIN batter_baserunning USING (event_key)
),

unioned AS (
    SELECT * FROM batter_stats
    UNION ALL BY NAME
    SELECT * FROM {{ ref('event_baserunning_stats') }}
    WHERE baserunner != 'Batter'
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
