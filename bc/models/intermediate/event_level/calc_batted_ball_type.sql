MODEL (
  name main_models.calc_batted_ball_type,
  kind FULL,
  description 'This table contains one row for each event that ended in a batted ball. It supplements the raw trajectory and location data with additional information based on inference and additional metadata. For millions of plays in the database without batted ball info, at least part of the information can be deduced from other data points, particularly fielding data. For example, if a plate appearance is recorded as a putout to the center fielder, we can deduce that the trajectory of the batted ball was an air ball, and the location was to center field. Both of these deductions are less precise and less accurate than explicit batted ball information, but they are much better than nothing. They also provide support for additional inference in statistical or deep learning models. In addition to these deductions, we also have additional ontologies for batted ball data (see `seeds`) that allow us to make other useful classifications, particularly for location. The raw data divides the field into a few dozen separate zones (http://www.retrosheet.org/location.htm) but we can categorize each of those zones according to angle, depth, etc. Trajectory inference rules (only valid for batted balls without trajectory type): 1. Unassisted putouts are air balls (unassisted GB putouts should already be explicit grounders) 2. Balls with an outfield location are air balls 2. Home runs are air balls 4. Balls fielded by infielders with an assisted putout are ground balls Many exceptions are possible, but they are some combination of rare and likely to be explicitly noted when they do occur. Location inference rules (applies to batted balls without location): 1. Ground balls fielded by outfielders have infield depth 2. All other depth/side cases are dictated by the fielder''s position via the `seed_batted_to_fielder_categories` table 3. If there is no fielder, we go by the explicit location (which is rarely available, especially when there is no fielder). The explicit location data is better than the fielder data in a vacuum, but fielder-based location is far more consistently available. Choosing it as the default makes the data more precise (in the sense of self-consistency).',
  grain (event_key),
  columns (
    game_id VARCHAR,
    event_key UINTEGER,
    plate_appearance_result PLATE_APPEARANCE_RESULT,
    batted_to_fielder UTINYINT,
    trajectory TRAJECTORY,
    recorded_trajectory TRAJECTORY,
    is_trajectory_deduced BOOLEAN,
    trajectory_broad_classification VARCHAR,
    recorded_location LOCATION_GENERAL,
    recorded_location_depth LOCATION_DEPTH,
    recorded_location_angle LOCATION_ANGLE,
    location_depth VARCHAR,
    location_side VARCHAR,
    location_edge VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_key = @doc('event_key'),
    plate_appearance_result = @doc('plate_appearance_result'),
    batted_to_fielder = @doc('batted_to_fielder'),
    trajectory = 'The trajectory of the batted ball, either as recorded or deduced.',
    recorded_trajectory = 'The trajectory of the batted ball as recorded.',
    is_trajectory_deduced = 'Whether the trajectory was deduced from other data. This is false if the trajectory was recorded or if it remains unknown after attempting deduction.',
    trajectory_broad_classification = 'Deduced trajectory classification that groups fly balls, pop-ups, and line drives together as air balls. Generally speaking, it is much easier to deduce that a batted ball was an air ball than to deduce a particular kind of air ball.',
    recorded_location = 'The recorded general location of the batted ball. See `batted_location_general` in `stg_events` for more information.',
    recorded_location_depth = 'The recorded depth of the batted ball. See `batted_location_depth` in `stg_events` for more information.',
    recorded_location_angle = 'The recorded angle of the batted ball. See `batted_location_angle` in `stg_events` for more information.',
    location_depth = 'The *overall* depth category of the batted ball. This is either the plate, the infield, or the outfield.',
    location_side = 'The side of the field that the batted ball was hit to. This is either left, center, or right.',
    location_edge = 'The edge of the area that the batted ball was hit to, relative to `location_side`. This is currently not deduced and only appears on rows with recorded locations.'
  ),
  audits (
    not_null(columns := (event_key)),
    unique_values(columns := (event_key)),
    accepted_values(column := location_edge, is_in := ('Left', 'Middle', 'Right', 'All', 'Unknown')),
    relationships(column := event_key, to_model := main_models.stg_events, to_column := event_key),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_calc_batted_ball_type.parquet'
  ),
);







-- trajectory type inference rules (only valid for batted balls without trajectory type)
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
    FROM main_models.calc_fielding_play_agg
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
        batted_ball.batted_trajectory AS recorded_trajectory,
        batted_ball.batted_location_general AS recorded_location,
        batted_ball.batted_location_depth AS recorded_location_depth,
        batted_ball.batted_location_angle AS recorded_location_angle,
        location_info.category_depth,
        location_info.category_side,
        location_info.category_edge,
        (
            (recorded_trajectory != 'GroundBall' AND putouts.unassisted_putouts > 0)
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
        CASE WHEN recorded_trajectory = 'Unknown' THEN inferred_contact
            ELSE recorded_trajectory
        END AS batted_trajectory,
    FROM main_models.stg_events AS batted_ball
    LEFT JOIN putouts USING (event_key)
    LEFT JOIN main_seeds.seed_hit_location_categories AS location_info USING (batted_location_general)
    WHERE batted_ball.batted_trajectory IS NOT NULL
),

final AS (
    SELECT
        inference.game_id,
        inference.event_key,
        inference.plate_appearance_result,
        inference.batted_to_fielder,
        inference.batted_trajectory::trajectory AS trajectory,
        inference.recorded_trajectory,
        inference.batted_trajectory != inference.recorded_trajectory AS is_trajectory_deduced,
        CASE WHEN inference.is_inferred_air_ball
                THEN 'AirBall'
            ELSE trajectory_info.broad_classification
        END AS trajectory_broad_classification,
        inference.recorded_location,
        inference.recorded_location_depth,
        inference.recorded_location_angle,
        CASE
            WHEN inference.batted_trajectory = 'GroundBall' AND inference.batted_to_fielder BETWEEN 7 AND 9
                THEN 'Infield'
            WHEN inference.batted_trajectory = 'Unknown' AND inference.batted_to_fielder BETWEEN 7 AND 9
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
    LEFT JOIN main_seeds.seed_plate_appearance_trajectories AS trajectory_info USING (batted_trajectory)
    LEFT JOIN main_seeds.seed_hit_to_fielder_categories AS fielder USING (batted_to_fielder)
)

SELECT * FROM final
