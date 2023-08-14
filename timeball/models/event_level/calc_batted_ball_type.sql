-- Contact type inference rules (only valid for batted balls without contact type)
-- 1. Putouts by outfielders are fly balls
-- 2. Putouts by infielders are pop flies (unassisted GB putouts should already be explicit grounders)
-- 2. Non-inside-the-park home runs are fly balls
-- 3. Balls fielded by infielders where there is anything other than
--    an unassisted putout are ground balls
-- 4. Any outs in foul territory are popups if caught by an infielder
--    and fly balls if caught by an outfielder
-- 5. If the location is explicitly recorded and specifies the outfield,
--    it is a fly ball, unless it is the shallow outfield, in which case
--    it is a popup
-- Many exceptions are possible, but they are some combination
-- of rare and likely to be explicitly noted when they do occur.
--
-- Location inference rules (applies to batted balls without location)
-- 1. Ground balls fielded by outfielders have infield depth
-- 2. All other depth/side cases are dictated by the fielder's position
--    via the `seed_hit_to_fielder_categories` table
WITH unassisted_putouts AS (
    SELECT
        event_key,
        fielding_position,
    FROM {{ ref('stg_event_fielding_plays') }}
    WHERE sequence_id = 1
        AND fielding_play = 'Putout'
        -- When the putout is from an unknown fielder, that often means
        -- that there is a missing assist on the play as well, so we can't
        -- infer anything from it.
        AND fielding_position != 0
),

home_runs AS (
    SELECT event_key
    FROM {{ ref('stg_event_plate_appearances') }}
    WHERE plate_appearance_result = 'HomeRun'
),

inference AS (
    SELECT
        batted_ball.event_key,
        -- Null out hit_to_fielder on homers to distinguish between "no fielder" and "unknown fielder"
        -- TODO: Handle upstream
        CASE WHEN home_runs.event_key IS NULL
                THEN batted_ball.hit_to_fielder
        END AS hit_to_fielder,
        batted_ball.contact AS recorded_contact,
        batted_ball.general_location AS recorded_location,
        location_info.category_depth,
        location_info.category_side,
        location_info.category_edge,
        CASE
            WHEN unassisted_putouts.fielding_position BETWEEN 7 AND 9 THEN 'Fly'
            WHEN unassisted_putouts.fielding_position BETWEEN 1 AND 6 THEN 'PopFly'
            WHEN home_runs.event_key IS NOT NULL THEN 'Fly'
            WHEN location_info.category_depth = 'Outfield' AND batted_ball.depth = 'Shallow' THEN 'PopFly'
            WHEN location_info.category_depth = 'Outfield' THEN 'Fly'
            WHEN batted_ball.hit_to_fielder BETWEEN 1 AND 6 THEN 'GroundBall'
            ELSE 'Unknown'
        END AS inferred_contact,
        CASE WHEN recorded_contact = 'Unknown' THEN inferred_contact
            ELSE recorded_contact
        END AS contact,
    FROM {{ ref('stg_event_batted_ball_info') }} AS batted_ball
    LEFT JOIN unassisted_putouts USING (event_key)
    LEFT JOIN home_runs USING (event_key)
    LEFT JOIN {{ ref('seed_hit_location_categories') }} AS location_info USING (general_location)


),

final AS (
    SELECT
        inference.event_key,
        inference.hit_to_fielder,
        inference.contact,
        inference.recorded_contact,
        inference.contact != inference.recorded_contact AS is_contact_inferred,
        contact_info.broad_classification AS contact_broad_classification,
        contact_info.is_bunt,
        inference.recorded_location,
        fielder.category_side,
        CASE
            WHEN inference.category_depth IS NOT NULL THEN inference.category_depth
            WHEN inference.contact = 'GroundBall' AND inference.hit_to_fielder BETWEEN 7 AND 9 THEN 'Infield'
            WHEN inference.contact = 'Unknown' AND inference.hit_to_fielder BETWEEN 7 AND 9 THEN 'Unknown'
            WHEN fielder.category_depth IS NOT NULL THEN fielder.category_depth
            ELSE 'Unknown'
        END AS location_depth,
        COALESCE(inference.category_side, fielder.category_side, 'Unknown') AS location_side,
        COALESCE(inference.category_edge, 'Unknown') AS location_edge,
    FROM inference
    LEFT JOIN {{ ref('seed_plate_appearance_contact_types') }} AS contact_info USING (contact)
    LEFT JOIN {{ ref('seed_hit_to_fielder_categories') }} AS fielder USING (hit_to_fielder)
)

SELECT * FROM final