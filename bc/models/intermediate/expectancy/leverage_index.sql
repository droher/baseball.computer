MODEL (
  name main_models.leverage_index,
  kind FULL,
  grain (win_expectancy_start_key),
  columns (
    win_expectancy_start_key VARCHAR,
    win_leverage_unscaled DOUBLE,
    run_leverage_unscaled DOUBLE,
    win_leverage_index DOUBLE,
    run_leverage_index DOUBLE,
    agg_sample_size HUGEINT
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_leverage_index.parquet'
  ),
);







WITH trans AS (
    SELECT
        win_expectancy_start_key,
        win_expectancy_end_key,
        COUNT(*) AS sample_size,
        -- We use the win key instead of the run key because we aren't
        -- including league/time in the leverage calc
        AVG(ABS(expected_home_win_change)) AS absolute_expected_home_win_change,
        AVG(ABS(expected_runs_change)) AS absolute_expected_runs_change,
    FROM main_models.event_transition_values
    GROUP BY 1, 2
),

weighted AS (
    SELECT
        win_expectancy_start_key,
        SUM(sample_size) AS agg_sample_size,
        -- Weighted averages
        SUM(absolute_expected_home_win_change * sample_size)
        / SUM(sample_size) AS win_leverage_unscaled,
        SUM(absolute_expected_runs_change * sample_size)
        / SUM(sample_size) AS run_leverage_unscaled,
        -- Note that this is a window function applied after the aggregation
        SUM(win_leverage_unscaled * agg_sample_size) OVER ()
        / SUM(agg_sample_size) OVER () AS avg_win_leverage_unscaled,
        SUM(run_leverage_unscaled * agg_sample_size) OVER ()
        / SUM(agg_sample_size) OVER () AS avg_run_leverage_unscaled,
    FROM trans
    GROUP BY 1
),

final AS (
    SELECT
        win_expectancy_start_key,
        ROUND(win_leverage_unscaled, 3) AS win_leverage_unscaled,
        ROUND(run_leverage_unscaled, 3) AS run_leverage_unscaled,
        ROUND(win_leverage_unscaled / avg_win_leverage_unscaled, 2) AS win_leverage_index,
        ROUND(run_leverage_unscaled / avg_run_leverage_unscaled, 2) AS run_leverage_index,
        agg_sample_size
    FROM weighted
)

SELECT * FROM final
