WITH base_data AS (
    SELECT
        e.season,
        e.event_key,
        e.outs,
        b.baserunner,
        e.base_state,
        e.batting_side,
        e.batted_to_fielder,
        g.park_id,
        f.player_id AS fielder_id,
        COALESCE(bbt.contact_broad_classification, 'Unknown') AS contact,
        b.bases_advanced,
        b.unforced_outs_on_basepaths,
        -- We don't want to distinguish between out types because that selects on the dependent variable
        -- (e.g. a sac fly is always a successful advance). We also want to lump ROEs and failed fielders choices
        -- in with hits here so we can assign the reaching-base penalty consistently separately.
        CASE WHEN part.plate_appearance_result = 'ReachedOnError'
            OR (part.plate_appearance_result = 'FieldersChoice' AND e.outs_on_play = 0)
                THEN FALSE
            ELSE part.is_batting_out
        END AS is_out,
    FROM {{ ref('event_baserunning_stats') }} AS b
    INNER JOIN {{ ref('stg_events') }} AS e USING (event_key)
    INNER JOIN {{ ref('seed_plate_appearance_result_types') }} AS part USING (plate_appearance_result)
    INNER JOIN {{ ref('game_start_info') }} AS g ON g.game_id = e.game_id
    LEFT JOIN {{ ref('calc_batted_ball_type') }} AS bbt USING (event_key)
    LEFT JOIN {{ ref('event_player_fielding_stats') }} AS f
        ON e.event_key = f.event_key
        AND e.batted_to_fielder = f.fielding_position
    WHERE part.is_in_play
),

-- TODO: add park
averages AS (
    SELECT
        base_state,
        outs,
        baserunner,
        batted_to_fielder,
        is_out,
        contact,
        AVG(bases_advanced) AS average_bases_advanced,
        AVG(unforced_outs_on_basepaths) AS average_outs_on_basepaths,
    FROM base_data
    GROUP BY 1, 2, 3, 4, 5, 6
),

expectations AS (
    SELECT
        bd.*,
        a.average_bases_advanced,
        a.average_outs_on_basepaths,
        bd.bases_advanced - a.average_bases_advanced AS bases_advanced_above_average,
        bd.unforced_outs_on_basepaths - a.average_outs_on_basepaths AS outs_on_basepaths_above_average,
        AVG(bases_advanced_above_average) OVER season_baseline AS season_adjustment_bases,
        AVG(outs_on_basepaths_above_average) OVER season_baseline AS season_adjustment_outs,
        bases_advanced_above_average - season_adjustment_bases AS adjusted_bases_advanced,
        outs_on_basepaths_above_average - season_adjustment_outs AS adjusted_outs_on_basepaths
    FROM base_data AS bd
    INNER JOIN averages AS a
        USING (base_state, outs, baserunner, is_out, contact, batted_to_fielder)
    WINDOW season_baseline AS (PARTITION BY season, base_state, outs, baserunner, is_out, contact, batted_to_fielder)
),

leaders AS (
    SELECT
        fielder_id,
        COUNT(DISTINCT event_key) AS plate_appearances,
        SUM(adjusted_bases_advanced) AS bases_aa,
        SUM(adjusted_outs_on_basepaths) AS outs_aa,
        bases_aa * .2 + outs_aa * -0.42 AS run_value
    FROM expectations
    GROUP BY 1
)

SELECT *,
    bases_aa / plate_appearances AS bases_aa_per_pa,
    outs_aa / plate_appearances AS outs_aa_per_pa,
    run_value / plate_appearances AS run_value_per_pa
FROM leaders
WHERE plate_appearances > 100
ORDER BY run_value_per_pa
