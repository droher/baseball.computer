-- linear_weights_estimated (Bayesian) vs linear_weights (deterministic), season 2015, joined on (season, league, play)
SELECT
    le.league,
    le.play,
    le.play_category,
    ROUND(l.average_run_value, 3) AS deterministic_value,
    ROUND(le.run_value_mean, 3) AS estimated_mean,
    ROUND(le.run_value_hdi_lower, 3) AS hdi_low,
    ROUND(le.run_value_hdi_upper, 3) AS hdi_high,
    ROUND(le.run_value_mean - l.average_run_value, 3) AS diff
FROM main_models.linear_weights_estimated le
JOIN main_models.linear_weights l
    ON le.season = l.season
   AND le.league = l.league
   AND le.play = l.play
WHERE le.season = 2015
  AND le.league = 'NL'
ORDER BY le.run_value_mean DESC;
