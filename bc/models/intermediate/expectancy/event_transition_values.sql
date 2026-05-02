MODEL (
  name main_models.event_transition_values,
  kind FULL,
  grain (event_key),
  columns (
    event_key UINTEGER,
    run_expectancy_start_key VARCHAR,
    run_expectancy_end_key VARCHAR,
    win_expectancy_start_key VARCHAR,
    win_expectancy_end_key VARCHAR,
    season SMALLINT,
    league VARCHAR,
    game_type GAME_TYPE,
    inning_start UTINYINT,
    frame_start FRAME,
    truncated_home_margin_start TINYINT,
    batting_side SIDE,
    base_state_start UTINYINT,
    outs_start UTINYINT,
    inning_end UTINYINT,
    frame_end FRAME,
    truncated_home_margin_end TINYINT,
    base_state_end UTINYINT,
    outs_end UTINYINT,
    runs_on_play UTINYINT,
    game_end_flag BOOLEAN,
    expected_runs_change DECIMAL(18,3),
    expected_home_win_change DECIMAL(14,3),
    expected_batting_win_change DECIMAL(14,3)
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    season = @doc('season'),
    league = @doc('league'),
    game_type = @doc('game_type'),
    inning_start = @doc('inning_start'),
    frame_start = @doc('frame_start'),
    batting_side = @doc('batting_side'),
    base_state_start = @doc('base_state_start'),
    outs_start = @doc('outs_start'),
    inning_end = @doc('inning_end'),
    frame_end = @doc('frame_end'),
    base_state_end = @doc('base_state_end'),
    outs_end = @doc('outs_end'),
    runs_on_play = @doc('runs_on_play'),
    game_end_flag = @doc('game_end_flag')
  ),
  audits (
    not_null_proportion(column := expected_runs_change, threshold := 0.999),
    not_null_proportion(column := expected_home_win_change, threshold := 0.999),
    not_null_proportion(column := expected_batting_win_change, threshold := 0.999)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_transition_values.parquet'
  ),
);







WITH final AS (
    SELECT
        states.event_key,
        states.run_expectancy_start_key,
        states.run_expectancy_end_key,
        states.win_expectancy_start_key,
        states.win_expectancy_end_key,
        states.season,
        states.league,
        states.game_type,
        states.inning_start,
        states.frame_start,
        states.truncated_home_margin_start,
        states.batting_side,
        states.base_state_start,
        states.outs_start,
        states.inning_end,
        states.frame_end,
        states.truncated_home_margin_end,
        states.base_state_end,
        states.outs_end,
        states.runs_on_play,
        states.game_end_flag,
        ROUND(
            states.runs_on_play
            + COALESCE(runs_end.avg_runs_scored, 0)
            - runs_start.avg_runs_scored,
            3
        ) AS expected_runs_change,
        ROUND(
            CASE
                WHEN states.game_end_flag AND states.truncated_home_margin_end > 0
                    THEN 1 - wins_start.home_win_rate
                WHEN states.game_end_flag AND states.truncated_home_margin_end < 0
                    THEN 0 - wins_start.home_win_rate
                WHEN states.game_end_flag
                    THEN NULL
                ELSE wins_end.home_win_rate - wins_start.home_win_rate
            END,
            3
        ) AS expected_home_win_change,
        CASE WHEN states.batting_side = 'Home' THEN expected_home_win_change
            ELSE -expected_home_win_change
        END AS expected_batting_win_change
    FROM main_models.event_states_full AS states
    LEFT JOIN main_models.run_expectancy_matrix AS runs_start
        ON runs_start.run_expectancy_key = states.run_expectancy_start_key
    LEFT JOIN main_models.run_expectancy_matrix AS runs_end
        ON runs_end.run_expectancy_key = states.run_expectancy_end_key
    LEFT JOIN main_models.win_expectancy_matrix AS wins_start
        ON wins_start.win_expectancy_key = states.win_expectancy_start_key
    LEFT JOIN main_models.win_expectancy_matrix AS wins_end
        ON wins_end.win_expectancy_key = states.win_expectancy_end_key
    WHERE states.game_type = 'RegularSeason'
        -- We can include plays from called games, but not the very last one
        AND NOT (states.game_end_flag AND states.truncated_home_margin_end = 0)
)

SELECT * FROM final
