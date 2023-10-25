WITH base_data AS (
    SELECT
        e.season,
        e.event_key,
        e.outs,
        b.baserunner,
        b.runner_id,
        e.base_state,
        e.batted_to_fielder,
        COALESCE(bbt.contact_broad_classification, 'Unknown') AS contact,
        b.bases_advanced,
        b.outs_on_basepaths,
        -- We don't want to distinguish between out types
        -- because that selects on the dependent variable
        -- (e.g. a sac fly is always a successful advance)
        CASE WHEN part.is_batting_out
                THEN 'InPlayOut'
        ELSE part.plate_appearance_result
        END AS plate_appearance_result,
    FROM {{ ref('event_baserunning_stats') }} AS b
    INNER JOIN {{ ref('stg_events') }} AS e USING (event_key)
    INNER JOIN {{ ref('seed_plate_appearance_result_types') }} AS part USING (plate_appearance_result)
    LEFT JOIN {{ ref('calc_batted_ball_type') }} AS bbt USING (event_key)
    WHERE part.is_in_play AND e.plate_appearance_result != 'GroundRuleDouble'
),

averages AS (
    SELECT
        base_state,
        outs,
        baserunner,
        batted_to_fielder,
        plate_appearance_result,
        contact,
        AVG(bases_advanced) AS average_bases_advanced,
        AVG(outs_on_basepaths) AS average_outs_on_basepaths,
    FROM base_data
    GROUP BY 1, 2, 3, 4, 5, 6
),

expectations AS (
    SELECT
        bd.*,
        a.average_bases_advanced,
        a.average_outs_on_basepaths,
        bd.bases_advanced - a.average_bases_advanced AS bases_advanced_above_average,
        bd.outs_on_basepaths - a.average_outs_on_basepaths AS outs_on_basepaths_above_average,
        AVG(bases_advanced_above_average) OVER baseline AS season_adjustment_bases,
        AVG(outs_on_basepaths_above_average) OVER baseline AS season_adjustment_outs,
        bases_advanced_above_average - season_adjustment_bases AS adjusted_bases_advanced,
        outs_on_basepaths_above_average - season_adjustment_outs AS adjusted_outs_on_basepaths
    FROM base_data AS bd
    INNER JOIN averages AS a
        USING (base_state, outs, baserunner, batted_to_fielder, plate_appearance_result, contact)
    WINDOW baseline AS (PARTITION BY season, base_state, outs, baserunner, plate_appearance_result, batted_to_fielder)
),

leaders AS (
    SELECT
        runner_id,
        COUNT(DISTINCT event_key) AS plate_appearances,
        SUM(adjusted_bases_advanced) AS bases_aa,
        SUM(adjusted_outs_on_basepaths) AS outs_aa,
        bases_aa * .2 + outs_aa * -0.42 AS run_value
    FROM expectations
    GROUP BY 1
)

SELECT *,
    run_value / plate_appearances * 100 AS run_value_per_100
FROM leaders
ORDER BY run_value DESC