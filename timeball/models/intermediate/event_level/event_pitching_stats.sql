-- SELECT game_id,
--     pitcher_id,
--     team_id,
--     opponent_id,
--     wins,
--     losses,
--     --games,
--     games_started,
--     games_finished,
--     complete_games,
--     shutouts,
--     saves,
--     innings_pitched,
--     --hits,
--     runs,
--     earned_runs,
--     --home_runs,
--     --walks,
--     --intentional_walks,
--     --strikeouts,
--     --hit_by_pitch,
--     balks,
--     wild_pitches,
--     batters_faced,
--     wins_in_games_started,
--     losses_in_games_started,
--     team_wins_in_games_started,
--     team_losses_in_games_started,
--     no_decisions,
--     quality_starts,
--     cheap_wins,
--     tough_losses,
--     losses_in_save_situations,
--     game_score,
--     bequeathed_runners,
--     bequeathed_runners_scored,
--     days_rest,
--     run_support,

--     games_relieved,
--     wins_in_games_relieved,
--     losses_in_games_relieved,
--     save_opportunities,
--     blown_saves,
--     save_situations,
--     holds,
--     inherited_runners,
--     inherited_runners_scored
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
        hit.pitcher_id AS player_id,
        event_key,
        'Batter' AS baserunner,
        hit.* EXCLUDE (event_key, batter_id),
        bat.* EXCLUDE (event_key),
        batter_baserunning.* EXCLUDE (event_key, baserunner),
    FROM {{ ref('event_batting_stats') }} AS hit
    LEFT JOIN {{ ref('event_batted_ball_stats') }} AS bat USING (event_key)
    FULL OUTER JOIN batter_baserunning USING (event_key)
),

non_batter_baserunning AS (
    SELECT
        baserunning.*,
        lineup.game_id,
        lineup.team_id AS batting_team_id,
        lineup.player_id AS baserunner_id,
    FROM {{ ref('event_baserunning_stats') }} AS baserunning
    INNER JOIN {{ ref('event_lineup_states') }} AS lineup
        ON lineup.event_key = baserunning.event_key
            AND lineup.lineup_position = baserunning.runner_lineup_position
    WHERE baserunning.baserunner != 'Batter'
),

unioned AS (
    SELECT * FROM batter_stats
    UNION ALL BY NAME
    SELECT * FROM non_batter_baserunning
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
