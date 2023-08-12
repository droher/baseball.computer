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
        hit.batting_team_id AS team_id,
        event_key,
        'Batter' AS baserunner,
        hit.* EXCLUDE (event_key),
        bat.* EXCLUDE (event_key),
        batter_baserunning.* EXCLUDE (event_key, baserunner),
        pitch.* EXCLUDE (event_key),
    FROM {{ ref('event_batting_stats') }} AS hit
    LEFT JOIN {{ ref('event_batted_ball_stats') }} AS bat USING (event_key)
    LEFT JOIN {{ ref('event_pitch_sequence_stats') }} AS pitch USING (event_key)
    FULL OUTER JOIN batter_baserunning USING (event_key)
),

unioned AS (
    SELECT * FROM batter_stats
    UNION ALL BY NAME
    SELECT * FROM {{ ref('event_baserunning_stats') }}
    WHERE baserunner != 'Batter'
),

final AS (
    SELECT
        game_id,
        event_key,
        team_id,
        baserunner,
        player_id,
        {% for stat in event_level_offense_stats() -%}
            COALESCE({{ stat }}, 0)::INT2 AS {{ stat }},
        {% endfor %}
    FROM unioned
)

SELECT * FROM final
