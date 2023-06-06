WITH game_full AS (
    SELECT
        games.*,
        franchises.league,
        away.team_id AS away_team_id,
        home.team_id AS home_team_id,
    FROM {{ ref('stg_games') }} AS games
    INNER JOIN {{ ref('stg_game_teams') }} AS away
        ON games.game_id = away.game_id
            AND away.side = 'Away'
    INNER JOIN {{ ref('stg_game_teams') }} AS home
        ON games.game_id = home.game_id
            AND home.side = 'Home'
    INNER JOIN {{ ref('stg_franchises') }} AS franchises
        ON home.team_id = franchises.team_id
            AND games.season BETWEEN franchises.season_start AND franchises.season_end
),

final AS (
    SELECT
        -- IDs
        game_id,
        e.event_id,
        e.event_key,
        -- Basic state
        g.season,
        g.league,
        g.game_type,
        g.date,
        g.park_id,
        g.bat_first_side,
        base_out.inning_start,
        base_out.frame_start,
        base_out.outs_start,
        add_bio.batting_side,
        add_bio.fielding_side,
        runs.score_home_start,
        runs.score_away_start,
        runs.score_home_start::INT - runs.score_away_start AS home_margin,
        -- Perform upstream for consistency
        GREATEST(LEAST(home_margin, 10), -10) AS truncated_home_margin,
        add_bio.batter_lineup_position,
        -- Player/Team IDs and info
        add_bio.batter_hand,
        add_bio.pitcher_hand,
        g.away_team_id,
        g.home_team_id,
        add_bio.batting_team_id,
        add_bio.fielding_team_id,
        add_bio.batter_id,
        add_bio.pitcher_id,
        base_out.base_state_start,
        base_out.first_base_runner_id_start,
        base_out.second_base_runner_id_start,
        base_out.third_base_runner_id_start,
        base_out.frame_start_flag,
        -- Future state
        -- TODO: Enforce clearer separation
        e.count_balls,
        e.count_strikes,
        base_out.inning_end,
        base_out.frame_end,
        base_out.outs_on_play,
        base_out.outs_end,
        base_out.base_state_end,
        runs.runs_on_play,
        runs.score_home_end,
        runs.score_away_end,
        base_out.frame_end_flag,
        base_out.truncated_frame_flag,
        base_out.game_end_flag
    FROM {{ ref('stg_events') }} AS e
    INNER JOIN game_full AS g USING (game_id)
    INNER JOIN {{ ref('event_base_out_states') }} AS base_out USING (event_key)
    INNER JOIN {{ ref('event_score_states') }} AS runs USING (event_key)
    INNER JOIN {{ ref('event_states_batter_pitcher') }} AS add_bio USING (event_key)
)

SELECT * FROM final
