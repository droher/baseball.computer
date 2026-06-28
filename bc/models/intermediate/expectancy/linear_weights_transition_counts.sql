MODEL (
  name main_models.linear_weights_transition_counts,
  kind FULL,
  grain (season, league, play, run_expectancy_start_key, run_expectancy_end_key, runs_on_play),
  column_descriptions (
    season = @doc('season'),
    league = @doc('league'),
    runs_on_play = @doc('runs_on_play')
  ),
  audits (
    not_null(columns := (season, play, play_category, run_expectancy_start_key, run_expectancy_end_key, runs_on_play, n)),
    unique_grain(columns := (season, league, play, run_expectancy_start_key, run_expectancy_end_key, runs_on_play))
  ),
);

WITH union_plays AS (
    SELECT
        e.event_key,
        CASE WHEN cat.result_category = 'InPlayOut' AND e.outs_on_play > 1
                THEN 'DoublePlay'
            ELSE cat.result_category
        END AS play,
        'BATTING' AS play_category,
    FROM main_models.stg_events AS e
    INNER JOIN main_seeds.seed_plate_appearance_result_types AS cat USING (plate_appearance_result)
    WHERE e.event_key NOT IN (
        SELECT event_key FROM main_models.stg_event_baserunners WHERE baserunning_play_type IS NOT NULL
    )
    UNION ALL BY NAME
    SELECT
        e.event_key,
        FIRST(CASE WHEN e.is_out THEN cat.result_category_out ELSE cat.result_category_safe END) AS play,
        FIRST('BASERUNNING') AS play_category,
    FROM main_models.stg_event_baserunners AS e
    INNER JOIN main_seeds.seed_baserunning_play_types AS cat USING (baserunning_play_type)
    WHERE e.event_key NOT IN (
            SELECT event_key FROM main_models.stg_events WHERE plate_appearance_result IS NOT NULL
        )
    GROUP BY 1
    HAVING COUNT(*) = 1
),

joined AS (
    SELECT
        states.season,
        states.league,
        union_plays.play,
        union_plays.play_category,
        states.run_expectancy_start_key,
        states.run_expectancy_end_key,
        states.runs_on_play
    FROM union_plays
    INNER JOIN main_models.event_states_full AS states USING (event_key)
    WHERE states.game_type = 'RegularSeason'
        AND NOT (states.game_end_flag AND states.truncated_home_margin_end = 0)
),

final AS (
    SELECT
        season,
        COALESCE(league, 'N/A') AS league,
        play,
        play_category,
        run_expectancy_start_key,
        run_expectancy_end_key,
        runs_on_play,
        COUNT(*) AS n
    FROM joined
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT * FROM final
