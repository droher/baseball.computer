SELECT
    end_class,
    ROUND(prob_mean, 4) AS prob_mean,
    ROUND(prob_hdi_lower, 4) AS prob_hdi_lower,
    ROUND(prob_hdi_upper, 4) AS prob_hdi_upper
FROM main_models.state_transition_summary
WHERE start_state = '1_1'
  AND season = 2015
  AND league = 'NL'
ORDER BY prob_mean DESC;
