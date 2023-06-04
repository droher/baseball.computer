{# SELECT
    game_id,
    team_id,
    event_key,
    sequence_id,
    -- Situation info
    frame,
    inning,
    base_state_start,
    away_team_runs_start,
    home_team_runs_start,
    outs_start,
    -- Baserunning, hitting, pitching, defense?
    -- Differentiate between SB/CS2 vs 3, H
    event_category,
    -- Results
    event_type,
    base_state_end,
    runs_scored,
    away_team_runs_end,
    home_team_runs_end,
    outs_end, #}
WITH final AS (
    SELECT
        season,
        league,
        base_state,
        outs,
        SUM(runs_on_play) OVER rest_of_inning AS runs_scored,
    FROM {{ ref('event_states_full') }}
    WHERE game_type = 'RegularSeason'
    WINDOW rest_of_inning AS (
        PARTITION BY game_id, frame, inning
        ORDER BY event_id
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    )
)

SELECT
    season,
    league,
    base_state,
    outs,
    COUNT(*) AS num_plays,
    AVG(runs_scored) AS run_expectancy
FROM final
