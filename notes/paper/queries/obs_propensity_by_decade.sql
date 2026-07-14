SELECT
    (e.season // 10) * 10 AS decade,
    ROUND(AVG(p.p_observed_mean) FILTER (WHERE p.dimension = 'trajectory'), 3) AS trajectory,
    ROUND(AVG(p.p_observed_mean) FILTER (WHERE p.dimension = 'location_side'), 3) AS location_side,
    ROUND(AVG(p.p_observed_mean) FILTER (WHERE p.dimension = 'location_depth'), 3) AS location_depth,
    ROUND(AVG(p.p_observed_mean) FILTER (WHERE p.dimension = 'location_edge'), 3) AS location_edge,
    ROUND(AVG(p.p_observed_mean) FILTER (WHERE p.dimension = 'general_location'), 3) AS general_location,
    ROUND(AVG(p.p_observed_mean) FILTER (WHERE p.dimension = 'ball_handler_position'), 3) AS ball_handler_position
FROM main_models.scorer_observation_propensities p
JOIN main_models.event_states_full e USING (event_key)
GROUP BY 1
ORDER BY 1;
