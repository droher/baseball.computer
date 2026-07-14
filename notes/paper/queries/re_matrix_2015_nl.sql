SELECT
    state,
    base_state,
    outs,
    ROUND(re_value_mean, 3) AS re_value_mean,
    ROUND(re_value_hdi_lower, 3) AS re_value_hdi_lower,
    ROUND(re_value_hdi_upper, 3) AS re_value_hdi_upper
FROM main_models.run_expectancy_summary
WHERE season = 2015
  AND league = 'NL'
  AND outcome = 'runs_to_end'
ORDER BY outs, base_state;
