MODEL (
  name main_models.event_states_full,
  kind FULL,
  description 'A catch-all table for information describing the state of the event both before and after it occurred. Includes information about the score, the base-out state, and the players involved in the event.',
  grain (event_key),
  columns (
    game_id VARCHAR,
    event_id UTINYINT,
    event_key UINTEGER,
    season SMALLINT,
    league VARCHAR,
    is_interleague BOOLEAN,
    game_type GAME_TYPE,
    date DATE,
    park_id PARK_ID,
    bat_first_side SIDE,
    time_of_day TIME_OF_DAY,
    pitching_team_starting_pitcher_id VARCHAR,
    inning_start UTINYINT,
    frame_start FRAME,
    outs_start UTINYINT,
    inning_in_outs_start UTINYINT,
    is_gidp_eligible BOOLEAN,
    batting_side SIDE,
    fielding_side SIDE,
    score_home_start UTINYINT,
    score_away_start UTINYINT,
    home_margin_start TINYINT,
    batting_team_margin_start TINYINT,
    batter_lineup_position UTINYINT,
    batter_fielding_position UTINYINT,
    batter_hand HAND,
    pitcher_hand HAND,
    away_team_id TEAM_ID,
    home_team_id TEAM_ID,
    batting_team_id TEAM_ID,
    fielding_team_id TEAM_ID,
    batter_id VARCHAR,
    pitcher_id VARCHAR,
    base_state_start UTINYINT,
    runners_count_start UTINYINT,
    frame_start_flag BOOLEAN,
    runner_first_id_start VARCHAR,
    runner_second_id_start VARCHAR,
    runner_third_id_start VARCHAR,
    count_balls UTINYINT,
    count_strikes UTINYINT,
    inning_end UTINYINT,
    frame_end FRAME,
    outs_on_play UTINYINT,
    outs_end UTINYINT,
    base_state_end UTINYINT,
    runs_on_play UTINYINT,
    score_home_end UTINYINT,
    score_away_end UTINYINT,
    home_margin_end TINYINT,
    batting_team_margin_end TINYINT,
    frame_end_flag BOOLEAN,
    truncated_frame_flag BOOLEAN,
    game_end_flag BOOLEAN,
    league_group VARCHAR,
    season_group SMALLINT,
    inning_group_start VARCHAR,
    inning_group_end VARCHAR,
    truncated_home_margin_start TINYINT,
    truncated_home_margin_end TINYINT,
    run_expectancy_start_key VARCHAR,
    run_expectancy_end_key VARCHAR,
    win_expectancy_start_key VARCHAR,
    win_expectancy_end_key VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_id = @doc('event_id'),
    event_key = @doc('event_key'),
    season = @doc('season'),
    league = @doc('league'),
    is_interleague = @doc('is_interleague'),
    game_type = @doc('game_type'),
    date = @doc('date'),
    park_id = @doc('park_id'),
    bat_first_side = @doc('bat_first_side'),
    time_of_day = @doc('time_of_day'),
    inning_start = @doc('inning_start'),
    frame_start = @doc('frame_start'),
    outs_start = @doc('outs_start'),
    inning_in_outs_start = @doc('inning_in_outs_start'),
    is_gidp_eligible = @doc('is_gidp_eligible'),
    batting_side = @doc('batting_side'),
    score_home_start = @doc('score_home_start'),
    score_away_start = @doc('score_away_start'),
    away_team_id = @doc('away_team_id'),
    home_team_id = @doc('home_team_id'),
    batter_id = @doc('batter_id'),
    pitcher_id = @doc('pitcher_id'),
    base_state_start = @doc('base_state_start'),
    runners_count_start = @doc('runners_count_start'),
    frame_start_flag = @doc('frame_start_flag'),
    runner_first_id_start = @doc('runner_first_id_start'),
    runner_second_id_start = @doc('runner_second_id_start'),
    runner_third_id_start = @doc('runner_third_id_start'),
    inning_end = @doc('inning_end'),
    frame_end = @doc('frame_end'),
    outs_on_play = @doc('outs_on_play'),
    outs_end = @doc('outs_end'),
    base_state_end = @doc('base_state_end'),
    runs_on_play = @doc('runs_on_play'),
    score_home_end = @doc('score_home_end'),
    score_away_end = @doc('score_away_end'),
    frame_end_flag = @doc('frame_end_flag'),
    truncated_frame_flag = @doc('truncated_frame_flag'),
    game_end_flag = @doc('game_end_flag')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_states_full.parquet'
  ),
);







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
        CASE WHEN g.home_league NOT IN ('AL', 'NL', 'FL') OR g.home_league IS NULL
                THEN 'Other'
            ELSE g.home_league
        END AS league_group,
        GREATEST(g.season, 1914) AS season_group,
        CASE WHEN base_out.inning_start < 10
                THEN base_out.inning_start::VARCHAR
            WHEN g.season >= 2020 AND g.game_type = 'RegularSeason'
                THEN '11'
            ELSE '10'
        END AS inning_group_start,
        CASE WHEN base_out.inning_start < 10
                THEN base_out.inning_end::VARCHAR
            WHEN g.season >= 2020 AND g.game_type = 'RegularSeason'
                THEN '11'
            ELSE '10'
        END AS inning_group_end,
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
            '_', inning_group_start, base_out.frame_start, truncated_home_margin_start,
            base_out.outs_start, base_out.base_state_start
        ) AS win_expectancy_start_key,
        CONCAT_WS(
            '_', inning_group_end, base_out.frame_end, truncated_home_margin_end,
            base_out.outs_end % 3, COALESCE(base_out.base_state_end, 0)
        ) AS win_expectancy_end_key,
    FROM main_models.stg_events AS e
    INNER JOIN main_models.game_start_info AS g USING (game_id)
    INNER JOIN main_models.event_states_batter_pitcher AS players USING (event_key)
    INNER JOIN main_models.event_base_out_states AS base_out USING (event_key)

)

SELECT * FROM final
