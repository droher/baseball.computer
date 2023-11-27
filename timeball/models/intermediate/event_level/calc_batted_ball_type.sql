{{
  config(
    materialized = 'table',
    )
}}
-- Contact type inference rules (only valid for batted balls without contact type)
-- 1. Unassisted putouts are air balls (unassisted GB putouts should already be explicit grounders)
-- 2. Balls with an outfield locaiton are air balls
-- 2. Home runs are air balls
-- 4. Balls fielded by infielders with an assisted putout are ground balls
-- Many exceptions are possible, but they are some combination
-- of rare and likely to be explicitly noted when they do occur.

-- Location inference rules (applies to batted balls without location)
-- 1. Ground balls fielded by outfielders have infield depth
-- 2. All other depth/side cases are dictated by the fielder's position
--    via the `seed_batted_to_fielder_categories` table
-- 3. If there is no fielder, we go by the explicit location
-- (which is rarely available, especially when there is no fielder).
-- The explicit location data is better than the fielder data in a vacuum,
-- but fielder-based location is far more consistently available.
-- Choosing it as the default makes the data more precise (in the sense of self-consistency).
WITH putouts AS (
    SELECT
        event_key,
        SUM(putouts - assisted_putouts) AS unassisted_putouts,
        SUM(assisted_putouts) AS assisted_putouts,
    FROM {{ ref('calc_fielding_play_agg') }}
    -- When the putout is from an unknown fielder, that often means
    -- that there is a missing assist on the play as well, so we can't
    -- infer anything from it.
    WHERE fielding_position != 0
    GROUP BY 1

),

inference AS (
    SELECT
        batted_ball.game_id,
        batted_ball.event_key,
        batted_ball.plate_appearance_result,
        -- Null out batted_to_fielder on homers to distinguish between "no fielder" and "unknown fielder"
        -- TODO: Handle upstream
        CASE WHEN batted_ball.plate_appearance_result NOT IN ('HomeRun', 'GroundRuleDouble')
                THEN batted_ball.batted_to_fielder
        END AS batted_to_fielder,
        batted_ball.batted_contact_type AS recorded_contact,
        batted_ball.batted_location_general AS recorded_location,
        batted_ball.batted_location_depth AS recorded_location_depth,
        batted_ball.batted_location_angle AS recorded_location_angle,
        location_info.category_depth,
        location_info.category_side,
        location_info.category_edge,
        (
            (recorded_contact != 'GroundBall' AND putouts.unassisted_putouts > 0)
            OR batted_ball.plate_appearance_result = 'HomeRun'
            -- 2000-2002 seasons have a lot of shallow outfield flies that are actually
            -- ground balls, and no metadata to isolate faulty sources.
            OR (location_info.category_depth = 'Outfield' AND batted_ball.season NOT BETWEEN 2000 AND 2002)
        ) AS is_inferred_air_ball,
        CASE
            WHEN batted_ball.batted_to_fielder BETWEEN 1 AND 6 AND putouts.assisted_putouts > 0
                THEN 'GroundBall'
            ELSE 'Unknown'
        END AS inferred_contact,
        CASE WHEN recorded_contact = 'Unknown' THEN inferred_contact
            ELSE recorded_contact
        END AS batted_contact_type,
    FROM {{ ref('stg_events') }} AS batted_ball
    LEFT JOIN putouts USING (event_key)
    LEFT JOIN {{ ref('seed_hit_location_categories') }} AS location_info USING (batted_location_general)
    WHERE batted_ball.batted_contact_type IS NOT NULL
),

final AS (
    SELECT
        inference.game_id,
        inference.event_key,
        inference.plate_appearance_result,
        inference.batted_to_fielder,
        inference.batted_contact_type AS contact,
        inference.recorded_contact,
        inference.batted_contact_type != inference.recorded_contact AS is_contact_inferred,
        CASE WHEN inference.is_inferred_air_ball
                THEN 'AirBall'
            ELSE contact_info.broad_classification
        END AS contact_broad_classification,
        inference.recorded_location,
        inference.recorded_location_depth,
        inference.recorded_location_angle,
        CASE
            WHEN inference.batted_contact_type = 'GroundBall' AND inference.batted_to_fielder BETWEEN 7 AND 9
                THEN 'Infield'
            WHEN inference.batted_contact_type = 'Unknown' AND inference.batted_to_fielder BETWEEN 7 AND 9
                THEN 'Unknown'
            WHEN fielder.category_depth IS NOT NULL
                THEN fielder.category_depth
            WHEN inference.category_depth IS NOT NULL
                THEN inference.category_depth
            ELSE 'Unknown'
        END AS location_depth,
        COALESCE(fielder.category_side, inference.category_side, 'Unknown') AS location_side,
        COALESCE(inference.category_edge, 'Unknown') AS location_edge,
    FROM inference
    LEFT JOIN {{ ref('seed_plate_appearance_contact_types') }} AS contact_info USING (batted_contact_type)
    LEFT JOIN {{ ref('seed_hit_to_fielder_categories') }} AS fielder USING (batted_to_fielder)
)

SELECT * FROM final
