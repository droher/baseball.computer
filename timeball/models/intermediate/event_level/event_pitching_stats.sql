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
{% set baserunning_stats_cols = dbt_utils.get_filtered_columns_in_relation(from=ref('event_baserunning_stats')) %}
WITH baserunning_agg AS (
    -- Runs are populated separately to charge to the right pitcher
    SELECT
        event_key,
        {% for col in baserunning_stats_cols if col in pitching_stats() -%}
            SUM({{ col }}) AS {{ col }},
        {% endfor %}
    FROM {{ ref('event_baserunning_stats') }}
    GROUP BY 1
),

outs_agg AS (
    SELECT
        event_key,
        COUNT(*) AS outs_recorded,
    FROM {{ ref('stg_event_outs') }}
    GROUP BY 1
),

joined_stats AS (
    SELECT
        event_key,
        states.pitcher_id AS player_id,
        states.game_id,
        states.fielding_team_id AS team_id,
        hit.* EXCLUDE (event_key),
        bat.* EXCLUDE (event_key),
        -- Populate runs with the CTE below
        baserunning_agg.* EXCLUDE (event_key, runs),
        pitch.* EXCLUDE (event_key),
        hit.plate_appearances AS batters_faced,
        outs_agg.outs_recorded
    FROM {{ ref('event_states_batter_pitcher') }} AS states
    LEFT JOIN {{ ref('event_batting_stats') }} AS hit USING (event_key)
    LEFT JOIN {{ ref('event_batted_ball_stats') }} AS bat USING (event_key)
    LEFT JOIN {{ ref('event_pitch_sequence_stats') }} AS pitch USING (event_key)
    LEFT JOIN baserunning_agg USING (event_key)
    LEFT JOIN outs_agg USING (event_key)
),

add_current_pitcher_runs AS (
    SELECT
        joined_stats.*,
        runs.runs,
        runs.inherited_runners_scored,
    FROM joined_stats
    LEFT JOIN {{ ref('event_run_assignment_stats') }} AS runs
        ON joined_stats.event_key = runs.event_key
            AND joined_stats.player_id = runs.pitcher_id
),

-- This gets unioned instead of joined as these rows are supplemental
insert_non_current_pitcher_runs AS (
    SELECT
        game_id,
        event_key,
        team_id,
        pitcher_id AS player_id,
        runs,
        bequeathed_runners_scored,
    FROM {{ ref('event_run_assignment_stats') }}
    -- Meaning they are not currently in the game
    WHERE bequeathed_runners_scored > 0
),

unioned AS (
    SELECT * FROM add_current_pitcher_runs
    UNION ALL BY NAME
    SELECT * FROM insert_non_current_pitcher_runs
),

final AS (
    SELECT
        game_id,
        event_key,
        team_id,
        player_id,
        {% for stat in pitching_stats() -%}
            COALESCE({{ stat }}, 0) AS {{ stat }},
        {% endfor %}
    FROM unioned
)

SELECT
    SUM(outs_recorded) AS outs_recorded,
    SUM(runs) AS runs,
    SUM(hits) AS hits,
    SUM(walks) AS walks,
    SUM(intentional_walks) AS intentional_walks,
    SUM(strikeouts) AS strikeouts,
    SUM(home_runs) AS home_runs,
    SUM(batters_faced) AS batters_faced,
    SUM(batted_broad_type_ground_ball) AS batted_type_ground_ball,
    SUM(batted_type_fly_ball) AS batted_type_fly_ball,
    SUM(batted_type_line_drive) AS batted_type_line_drive,
    SUM(batted_subtype_pop_fly) AS batted_subtype_pop_fly,
    SUM(batted_type_unknown) AS batted_type_unknown,
FROM final
WHERE game_id = 'SLN197107210'
AND team_id = 'SLN'
