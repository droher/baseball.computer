
WITH runners AS (
    SELECT
        event_key,
        ANY_VALUE(CASE WHEN baserunner_bit = 1 THEN runner_id END)::PLAYER_ID AS runner_first_id,
        ANY_VALUE(CASE WHEN baserunner_bit = 2 THEN runner_id END)::PLAYER_ID AS runner_second_id,
        ANY_VALUE(CASE WHEN baserunner_bit = 4 THEN runner_id END)::PLAYER_ID AS runner_third_id,
    FROM "timeball"."main_models"."stg_event_baserunners"
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
    FROM "timeball"."main_models"."stg_events" AS events
    LEFT JOIN runners USING (event_key)
    LEFT JOIN "timeball"."main_seeds"."seed_base_state_info" AS info USING (base_state)
),

final AS (
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
        COALESCE(SUM(runs_on_play) FILTER (WHERE batting_side = 'Home') OVER start_event, 0)::UTINYINT AS score_home_start,
        COALESCE(SUM(runs_on_play) FILTER (WHERE batting_side = 'Away') OVER start_event, 0)::UTINYINT AS score_away_start,
        COALESCE(SUM(runs_on_play) FILTER (WHERE batting_side = 'Home') OVER end_event, 0)::UTINYINT AS score_home_end,
        COALESCE(SUM(runs_on_play) FILTER (WHERE batting_side = 'Away') OVER end_event, 0)::UTINYINT AS score_away_end,
        runs_on_play,
        LAG(event_key) OVER narrow IS NULL AS frame_start_flag,
        LEAD(event_key) OVER narrow IS NULL AS frame_end_flag,
        LEAD(event_key) OVER narrow IS NULL AND outs_end != 3 AS truncated_frame_flag,
        LAG(event_key) OVER start_event IS NULL AS game_start_flag,
        LEAD(event_key) OVER end_event IS NULL AS game_end_flag,
    FROM add_outs
    WINDOW
        narrow AS (
            PARTITION BY game_key, inning, frame_key
            ORDER BY event_key
        ),
        start_event AS (
            PARTITION BY game_key
            ORDER BY event_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        end_event AS (
            PARTITION BY game_key
            ORDER BY event_key
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
)

SELECT * FROM final