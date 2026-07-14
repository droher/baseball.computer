WITH classified AS (
    SELECT
        season,
        observed_status,
        CASE
            WHEN observed_status = 'observed' THEN raw_value
            WHEN observed_status = 'derived' THEN deduced_value
        END AS trajectory_class
    FROM main_models.model_input_geometry
    WHERE geometry_dimension = 'trajectory'
      AND observed_status IN ('observed', 'derived')
),
broad AS (
    SELECT
        season,
        observed_status,
        CASE
            WHEN trajectory_class IN ('GroundBall', 'GroundBallBunt') THEN 'ground'
            WHEN trajectory_class IN ('Fly', 'LineDrive', 'PopUp', 'AirBall', 'PopUpBunt', 'LineDriveBunt') THEN 'air'
            ELSE NULL
        END AS broad_class
    FROM classified
),
era_rows AS (
    SELECT
        CASE
            WHEN season < 1950 THEN 'pre-1950'
            WHEN season < 1988 THEN '1950-1987'
            ELSE '1988+'
        END AS era,
        observed_status,
        broad_class
    FROM broad
)
SELECT
    era,
    COUNT(*) FILTER (WHERE observed_status = 'observed') AS n_observed,
    COUNT(*) FILTER (WHERE observed_status = 'derived') AS n_derived,
    ROUND(
        COUNT(*) FILTER (WHERE observed_status = 'observed' AND broad_class = 'ground')::DOUBLE
        / NULLIF(COUNT(*) FILTER (WHERE observed_status = 'observed' AND broad_class IS NOT NULL), 0),
        4
    ) AS ground_share_observed,
    ROUND(
        COUNT(*) FILTER (WHERE broad_class = 'ground')::DOUBLE
        / NULLIF(COUNT(*) FILTER (WHERE broad_class IS NOT NULL), 0),
        4
    ) AS ground_share_obs_plus_derived,
    ROUND(
        (COUNT(*) FILTER (WHERE broad_class = 'ground')::DOUBLE
            / NULLIF(COUNT(*) FILTER (WHERE broad_class IS NOT NULL), 0))
        -
        (COUNT(*) FILTER (WHERE observed_status = 'observed' AND broad_class = 'ground')::DOUBLE
            / NULLIF(COUNT(*) FILTER (WHERE observed_status = 'observed' AND broad_class IS NOT NULL), 0)),
        4
    ) AS gap
FROM era_rows
GROUP BY era
ORDER BY era;
