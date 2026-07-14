SELECT
    CASE
        WHEN e.season < 1950 THEN 'pre-1950'
        WHEN e.season < 1988 THEN '1950-1987'
        ELSE '1988+'
    END AS era_bucket,
    g.class_label,
    ROUND(AVG(g.expected_share), 3) AS avg_expected_share,
    COUNT(*) AS n_rows
FROM main_models.imputed_batted_ball_geometry g
JOIN main_models.event_states_full e USING (event_key)
WHERE g.geometry_dimension = 'trajectory'
GROUP BY 1, 2
ORDER BY 1, 2;
