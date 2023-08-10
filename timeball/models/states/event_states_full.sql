WITH final AS (
    SELECT
        -- IDs
        game_id,
        e.event_id,
        e.event_key,
        -- Basic state
        g.season,
        g.home_league AS league,
        g.game_type,
        g.date,
        g.park_id,
        g.bat_first_side,
        -- Useful for determining save situations
        CASE WHEN e.batting_side = 'Home'
                THEN g.away_starting_pitcher_id
            ELSE g.home_starting_pitcher_id
        END AS pitching_team_starting_pitcher_id,
        base_out.inning_start,
        base_out.frame_start,
        base_out.outs_start,
        base_out.inning_in_outs_start,
        add_bio.batting_side,
        add_bio.fielding_side,
        runs.score_home_start,
        runs.score_away_start,
        runs.score_home_start::INT - runs.score_away_start AS home_margin_start,
        CASE WHEN e.batting_side = 'Home'
                THEN home_margin_start
            ELSE -home_margin_start
        END AS batting_team_margin_start,
        -- Perform upstream for consistency
        GREATEST(LEAST(home_margin_start, 10), -10) AS truncated_home_margin_start,
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
        base_out.runners_count_start,
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
        runs.score_home_end::INT - runs.score_away_end AS home_margin_end,
        CASE WHEN e.batting_side = 'Home'
                THEN home_margin_end
            ELSE -home_margin_end
        END AS batting_team_margin_end,
        -- Perform upstream for consistency
        GREATEST(LEAST(home_margin_end, 10), -10) AS truncated_home_margin_end,
        base_out.frame_end_flag,
        base_out.truncated_frame_flag,
        base_out.game_end_flag
    FROM {{ ref('stg_events') }} AS e
    INNER JOIN {{ ref('game_start_info') }} AS g USING (game_id)
    INNER JOIN {{ ref('event_base_out_states') }} AS base_out USING (event_key)
    INNER JOIN {{ ref('event_score_states') }} AS runs USING (event_key)
    INNER JOIN {{ ref('event_states_batter_pitcher') }} AS add_bio USING (event_key)
)

SELECT * FROM final
