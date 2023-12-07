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
        COALESCE(hit.batter_id, batter_baserunning.runner_id) AS player_id,
        COALESCE(hit.batting_team_id, batter_baserunning.batting_team_id) AS team_id,
        event_key,
        'Batter'::BASERUNNER AS baserunner,
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
        game_id::GAME_ID AS game_id,
        event_key,
        COALESCE(team_id, batting_team_id)::TEAM_ID AS team_id,
        COALESCE(player_id, runner_id)::PLAYER_ID AS player_id,
        baserunner,
        {% for stat in event_level_offense_stats() -%}
            COALESCE({{ stat }}, 0)::INT1 AS {{ stat }},
        {% endfor %},
        hits + walks + hit_by_pitches - grounded_into_double_plays)
    FROM unioned
)

SELECT * FROM final
