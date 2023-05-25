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
SELECT 1
