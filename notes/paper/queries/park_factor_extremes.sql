-- (a) Top 8 and bottom 8 park-seasons by park_factor_mean (outcome = 'team_runs', the only outcome present)
WITH ranked AS (
    SELECT
        park_id,
        season,
        league,
        park_factor_mean,
        theta_hdi_lower,
        theta_hdi_upper
    FROM main_models.park_factor_summary
    WHERE outcome = 'team_runs'
),
top8 AS (
    SELECT 'top' AS rank_group, *
    FROM ranked
    ORDER BY park_factor_mean DESC
    LIMIT 8
),
bottom8 AS (
    SELECT 'bottom' AS rank_group, *
    FROM ranked
    ORDER BY park_factor_mean ASC
    LIMIT 8
)
SELECT
    rank_group,
    park_id,
    season,
    league,
    ROUND(park_factor_mean, 3) AS park_factor_mean,
    ROUND(EXP(theta_hdi_lower), 3) AS park_factor_hdi_lower,
    ROUND(EXP(theta_hdi_upper), 3) AS park_factor_hdi_upper
FROM top8
UNION ALL
SELECT
    rank_group,
    park_id,
    season,
    league,
    ROUND(park_factor_mean, 3) AS park_factor_mean,
    ROUND(EXP(theta_hdi_lower), 3) AS park_factor_hdi_lower,
    ROUND(EXP(theta_hdi_upper), 3) AS park_factor_hdi_upper
FROM bottom8
ORDER BY rank_group DESC, park_factor_mean DESC;

-- (b) Average HDI width (on the park_factor_mean scale) by league, with row counts to show sparsity
SELECT
    league,
    COUNT(*) AS n_park_seasons,
    ROUND(AVG(EXP(theta_hdi_upper) - EXP(theta_hdi_lower)), 4) AS avg_hdi_width
FROM main_models.park_factor_summary
WHERE outcome = 'team_runs'
GROUP BY league
ORDER BY avg_hdi_width DESC;
