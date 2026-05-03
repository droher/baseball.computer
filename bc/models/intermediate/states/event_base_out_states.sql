MODEL (
  name main_models.event_base_out_states,
  kind FULL,
  description 'This model provides event-level information about the state of the game in terms of bases and outs. It combines data from the ''stg_events'' and ''stg_event_baserunners'' tables to calculate various metrics such as the number of outs at the start and end of each event, the number of runs scored on each play, the base state at the start and end of each event, and the IDs of the baserunners at the start and end of each event. Additionally, it includes flags to indicate the start and end of innings, frames, and games, as well as flags for truncated frames.',
  grain (event_key),
  columns (
    event_key UINTEGER,
    inning_start UTINYINT,
    inning_end UTINYINT,
    frame_start FRAME,
    frame_end FRAME,
    inning_in_outs_start UTINYINT,
    outs_start UTINYINT,
    outs_end UTINYINT,
    outs_on_play UTINYINT,
    is_gidp_eligible BOOLEAN,
    base_state_start UTINYINT,
    runner_first_id_start VARCHAR,
    runner_second_id_start VARCHAR,
    runner_third_id_start VARCHAR,
    runners_count_start UTINYINT,
    base_state_end UTINYINT,
    runners_count_end UTINYINT,
    runner_first_id_end VARCHAR,
    runner_second_id_end VARCHAR,
    runner_third_id_end VARCHAR,
    score_home_start UTINYINT,
    score_away_start UTINYINT,
    score_home_end UTINYINT,
    score_away_end UTINYINT,
    runs_on_play UTINYINT,
    frame_start_flag BOOLEAN,
    frame_end_flag BOOLEAN,
    truncated_frame_flag BOOLEAN,
    game_start_flag BOOLEAN,
    game_end_flag BOOLEAN
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    inning_start = @doc('inning_start'),
    inning_end = @doc('inning_end'),
    frame_start = @doc('frame_start'),
    frame_end = @doc('frame_end'),
    inning_in_outs_start = @doc('inning_in_outs_start'),
    outs_start = @doc('outs_start'),
    outs_end = @doc('outs_end'),
    outs_on_play = @doc('outs_on_play'),
    is_gidp_eligible = @doc('is_gidp_eligible'),
    base_state_start = @doc('base_state_start'),
    runner_first_id_start = @doc('runner_first_id_start'),
    runner_second_id_start = @doc('runner_second_id_start'),
    runner_third_id_start = @doc('runner_third_id_start'),
    runners_count_start = @doc('runners_count_start'),
    base_state_end = @doc('base_state_end'),
    runners_count_end = @doc('runners_count_end'),
    runner_first_id_end = @doc('runner_first_id_end'),
    runner_second_id_end = @doc('runner_second_id_end'),
    runner_third_id_end = @doc('runner_third_id_end'),
    score_home_start = @doc('score_home_start'),
    score_away_start = @doc('score_away_start'),
    score_home_end = @doc('score_home_end'),
    score_away_end = @doc('score_away_end'),
    runs_on_play = @doc('runs_on_play'),
    frame_start_flag = @doc('frame_start_flag'),
    frame_end_flag = @doc('frame_end_flag'),
    truncated_frame_flag = @doc('truncated_frame_flag'),
    game_start_flag = @doc('game_start_flag'),
    game_end_flag = @doc('game_end_flag')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_base_out_states.parquet'
  ),
);







WITH runners AS (
    SELECT
        event_key,
        ANY_VALUE(CASE WHEN baserunner_bit = 1 THEN runner_id END)::PLAYER_ID AS runner_first_id,
        ANY_VALUE(CASE WHEN baserunner_bit = 2 THEN runner_id END)::PLAYER_ID AS runner_second_id,
        ANY_VALUE(CASE WHEN baserunner_bit = 4 THEN runner_id END)::PLAYER_ID AS runner_third_id,
    FROM main_models.stg_event_baserunners
    GROUP BY 1
),

add_outs AS (
    SELECT
        event_key,
        events.batting_side,
        events.inning,
        events.frame,
        -- Next two cols are transformed to reduce size of partition key in next step
        event_key // 255 AS game_key,
        CASE events.frame WHEN 'Top' THEN 0 ELSE 1 END AS frame_key,
        events.outs AS outs_start,
        events.outs_on_play,
        events.runs_on_play,
        events.outs + events.outs_on_play AS outs_end,
        runners.runner_first_id,
        runners.runner_second_id,
        runners.runner_third_id,
        events.base_state AS base_state,
        COALESCE(info.is_force_on_second, FALSE) AND outs_start < 2 AS is_gidp_eligible,
    FROM main_models.stg_events AS events
    LEFT JOIN runners USING (event_key)
    LEFT JOIN main_seeds.seed_base_state_info AS info USING (base_state)
),

scored AS (
    SELECT
        event_key,
        inning AS inning_start,
        LEAD(inning) OVER end_event AS inning_end,
        frame AS frame_start,
        LEAD(frame) OVER end_event AS frame_end,
        ((inning - 1) * 3 + outs_start)::UTINYINT AS inning_in_outs_start,
        -- TODO: Add inning_in_outs_end
        -- (tricky since not sure whether it should be +1 or +4 or null)
        outs_start,
        outs_end,
        outs_on_play,
        batting_side,
        is_gidp_eligible,
        base_state AS base_state_start,
        runner_first_id AS runner_first_id_start,
        runner_second_id AS runner_second_id_start,
        runner_third_id AS runner_third_id_start,
        BIT_COUNT(base_state)::UTINYINT AS runners_count_start,
        LEAD(base_state) OVER narrow AS base_state_end,
        BIT_COUNT(LEAD(base_state) OVER narrow)::UTINYINT AS runners_count_end,
        LEAD(runner_first_id) OVER narrow AS runner_first_id_end,
        LEAD(runner_second_id) OVER narrow AS runner_second_id_end,
        LEAD(runner_third_id) OVER narrow AS runner_third_id_end,
        COALESCE(SUM(runs_on_play) FILTER (WHERE batting_side = 'Home') OVER end_event, 0)::UTINYINT AS score_home_end,
        COALESCE(SUM(runs_on_play) FILTER (WHERE batting_side = 'Away') OVER end_event, 0)::UTINYINT AS score_away_end,
        runs_on_play,
        LAG(event_key) OVER narrow IS NULL AS frame_start_flag,
        LEAD(event_key) OVER narrow IS NULL AS frame_end_flag,
        LEAD(event_key) OVER narrow IS NULL AND outs_end != 3 AS truncated_frame_flag,
        LAG(event_key) OVER end_event IS NULL AS game_start_flag,
        LEAD(event_key) OVER end_event IS NULL AS game_end_flag,
    FROM add_outs
    WINDOW
        narrow AS (
            PARTITION BY game_key, inning, frame_key
            ORDER BY event_key
        ),
        -- One game_key-partitioned window covers all the running aggregates
        -- and the LAG/LEAD checks (frame is ignored for LAG/LEAD), instead
        -- of running a separate ...AND 1 PRECEDING window for score_*_start.
        end_event AS (
            PARTITION BY game_key
            ORDER BY event_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
),

final AS (
    SELECT
        * EXCLUDE (batting_side),
        -- score_*_end reflects runs through the current row; subtracting
        -- the runs scored ON the current row (which can only be home or
        -- away) yields the start-of-row score. Saves two windowed SUMs.
        (score_home_end - CASE WHEN batting_side = 'Home' THEN runs_on_play ELSE 0 END)::UTINYINT AS score_home_start,
        (score_away_end - CASE WHEN batting_side = 'Away' THEN runs_on_play ELSE 0 END)::UTINYINT AS score_away_start,
    FROM scored
)

SELECT * FROM final
