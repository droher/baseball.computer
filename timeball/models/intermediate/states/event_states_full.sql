{{
  config(
    materialized = 'table',
    )
}}
WITH final AS (
    SELECT
        -- IDs
        game_id,
        e.event_id,
        e.event_key,
        -- Basic state
        g.season::SMALLINT AS season,
        g.home_league AS league,
        g.is_interleague,
        g.game_type,
        g.date,
        g.park_id,
        g.bat_first_side,
        g.time_of_day,
        -- Useful for determining save situations
        CASE WHEN e.batting_side = 'Home'::SIDE
                THEN g.away_starting_pitcher_id
            ELSE g.home_starting_pitcher_id
        END AS pitching_team_starting_pitcher_id,
        base_out.inning_start,
        base_out.frame_start,
        base_out.outs_start,
        base_out.inning_in_outs_start,
        base_out.is_gidp_eligible,
        players.batting_side,
        players.fielding_side,
        base_out.score_home_start,
        base_out.score_away_start,
        (base_out.score_home_start::INT - base_out.score_away_start)::INT1 AS home_margin_start,
        CASE WHEN e.batting_side = 'Home'
                THEN home_margin_start
            ELSE -home_margin_start
        END::INT1 AS batting_team_margin_start,
        players.batter_lineup_position,
        players.batter_fielding_position,
        -- Player/Team IDs and info
        players.batter_hand,
        players.pitcher_hand,
        g.away_team_id,
        g.home_team_id,
        players.batting_team_id,
        players.fielding_team_id,
        players.batter_id,
        players.pitcher_id,
        -- These are too memory-intensive to include
        -- at the moment - can put them back in later
        {# players.catcher_id, 
        players.first_base_id,
        players.second_base_id,
        players.third_base_id,
        players.shortstop_id,
        players.left_field_id,
        players.center_field_id,
        players.right_field_id, #}
        base_out.base_state_start,
        base_out.runners_count_start,
        base_out.frame_start_flag,
        base_out.runner_first_id_start,
        base_out.runner_second_id_start,
        base_out.runner_third_id_start,
        -- Future state
        -- TODO: Enforce clearer separation
        e.count_balls,
        e.count_strikes,
        base_out.inning_end,
        base_out.frame_end,
        base_out.outs_on_play,
        base_out.outs_end,
        base_out.base_state_end,
        base_out.runs_on_play,
        base_out.score_home_end,
        base_out.score_away_end,
        (base_out.score_home_end::INT1 - base_out.score_away_end)::INT1 AS home_margin_end,
        CASE WHEN e.batting_side = 'Home'::SIDE
                THEN home_margin_end
            ELSE -home_margin_end
        END::INT1 AS batting_team_margin_end,
        base_out.frame_end_flag,
        base_out.truncated_frame_flag,
        base_out.game_end_flag,
        -- IDs for calculating expectancy_values
        CASE WHEN g.home_league NOT IN ('AL', 'NL', 'FL')
                THEN 'Other'
            ELSE g.home_league
        END AS league_group,
        GREATEST(g.season, 1914) AS season_group,
        CASE WHEN base_out.inning_start < 10
                THEN base_out.inning_start::VARCHAR
            WHEN g.season >= 2020 AND g.game_type = 'RegularSeason'
                THEN 11
            ELSE 10
        END AS inning_group,
        GREATEST(LEAST(home_margin_start, 10), -10)::INT1 AS truncated_home_margin_start,
        GREATEST(LEAST(home_margin_end, 10), -10)::INT1 AS truncated_home_margin_end,
        CONCAT_WS(
            '_', season_group, league_group,
            base_out.outs_start, base_out.base_state_start
        ) AS run_expectancy_start_key,
        CONCAT_WS(
            '_', season_group, league_group,
            base_out.outs_end, COALESCE(base_out.base_state_end, 0)
        ) AS run_expectancy_end_key,
        CONCAT_WS(
            '_', inning_group, base_out.frame_start, truncated_home_margin_start,
            base_out.outs_start, base_out.base_state_start
        ) AS win_expectancy_start_key,
        CONCAT_WS(
            '_', inning_group, base_out.frame_end, truncated_home_margin_end,
            base_out.outs_end % 3, COALESCE(base_out.base_state_end, 0)
        ) AS win_expectancy_end_key,
    FROM {{ ref('stg_events') }} AS e
    INNER JOIN {{ ref('game_start_info') }} AS g USING (game_id)
    INNER JOIN {{ ref('event_states_batter_pitcher') }} AS players USING (event_key)
    INNER JOIN {{ ref('event_base_out_states') }} AS base_out USING (event_key)

)

SELECT * FROM final
