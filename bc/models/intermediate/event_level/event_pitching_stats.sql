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
        MIN(game_id) AS game_id,
        MIN(current_pitcher_id) AS player_id,
        MIN(fielding_team_id) AS team_id,
        {% for col in baserunning_stats_cols if col in event_level_pitching_stats() -%}
            SUM({{ col }})::TINYINT AS {{ col }},
        {% endfor %}
    FROM {{ ref('event_baserunning_stats') }}
    GROUP BY 1
),

joined_stats AS (
    SELECT
        event_key,
        COALESCE(baserunning_agg.game_id, hit.game_id) AS game_id,
        COALESCE(baserunning_agg.player_id, hit.pitcher_id) AS player_id,
        COALESCE(baserunning_agg.team_id, hit.fielding_team_id) AS team_id,
        hit.* EXCLUDE (event_key),
        bat.* EXCLUDE (event_key),
        -- Populate runs with the CTE below
        baserunning_agg.* EXCLUDE (event_key, runs),
        pitch.* EXCLUDE (event_key),
        hit.plate_appearances AS batters_faced,
        COALESCE(hit.outs_on_play, baserunning_agg.outs_on_basepaths) AS outs_recorded,
    FROM {{ ref('event_batting_stats') }} AS hit
    FULL OUTER JOIN baserunning_agg USING (event_key)
    LEFT JOIN {{ ref('event_batted_ball_stats') }} AS bat USING (event_key)
    LEFT JOIN {{ ref('event_pitch_sequence_stats') }} AS pitch USING (event_key)
),

add_current_pitcher_runs AS (
    SELECT
        joined_stats.*,
        runs.runs,
        runs.team_unearned_runs,
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
        team_unearned_runs,
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
        {% for stat in event_level_pitching_stats() -%}
            COALESCE({{ stat }}, 0)::INT1 AS {{ stat }},
        {% endfor %}
    FROM unioned
)

SELECT * FROM final
