WITH base AS (
    SELECT DISTINCT ON (s.batter_hand, is_shift_era, base_state, under_two_outs, c.recorded_location, c.recorded_location_angle, c.batted_to_fielder)
        s.batter_hand,
        s.season >= 2010 AS is_shift_era,
        s.base_state_start AS base_state,
        s.outs_start < 2 AS under_two_outs,
        c.recorded_location,
        c.recorded_location_angle,
        c.batted_to_fielder,
        -- Number of balls fielded by this fielder at this location
        COUNT(*) OVER fielder_at_location AS ground_balls,
        -- E.g 64% of the time an infielder fields a ball at ThirdShortstop, it's to the third baseman
        ground_balls / COUNT(*) OVER fielder_group_at_location AS share_within_group,
        -- E.g. 65% of all ground balls fielded by the left fielder come through ThirdShortstop
        ground_balls / COUNT(*) OVER fielder_all AS share_within_fielder,
        SUM(e.hits) OVER location_all / COUNT(*) OVER location_all AS batting_average_at_location,
        SUM(e.hits) OVER fielder_at_location / ground_balls AS batting_average,
        ground_balls / COUNT(*) OVER location_all AS batting_average_sample_weight,
    FROM "timeball"."main_models"."event_offense_stats" AS e
    INNER JOIN "timeball"."main_models"."calc_batted_ball_type" AS c USING (event_key)
    INNER JOIN "timeball"."main_models"."event_states_full" AS s USING (event_key)
    WHERE c.trajectory = 'GroundBall'
        AND e.plate_appearances = 1
        AND c.recorded_location != 'Unknown'
        -- These are the only seasons in the current data where location/trajectory data
        -- is widely available and unaffected by selection bias.
        -- 2000-2019 coverage should improve substantially in a future Retrosheet release.
        AND (
            season BETWEEN 1989 AND 1999
            OR season >= 2020
        )
        -- Remove balls without fielder info and balls fielded by catchers,
        -- since a catcher would never plausibly have a chance at a ball that
        -- would go to the outfield. 
        AND c.batted_to_fielder NOT IN (0, 2)
        -- Remove any shallow locations, since those balls shouldn't be fielded by outfielders
        -- (Presence in the data is rare, and likely either
        -- a miscode or an extremely unusual fielding configuration).
        -- We keep 'Pitcher' (the mound) in the data, since it may include deflections
        AND c.recorded_location_depth != 'Shallow'
        AND c.recorded_location NOT IN ('Catcher', 'CatcherFirst', 'CatcherThird', 'PitcherFirst', 'PitcherThird')
        -- Exclude infield hits for a couple reasons: they are disproportionately soft ground balls
        -- that would not have reached the outfield, and the fielder picking up the ball is often not the one
        -- who had a real chance (e.g. shortstop covering behind third baseman).
    WINDOW
        fielder_at_location AS (
            PARTITION BY s.batter_hand, is_shift_era, base_state, under_two_outs,
            c.recorded_location, c.recorded_location_angle, c.batted_to_fielder
        ),
        fielder_group_at_location AS (
            PARTITION BY s.batter_hand, is_shift_era, base_state, under_two_outs,
            c.recorded_location, c.recorded_location_angle, e.fielded_by_outfielder
        ),
        fielder_all AS (
            PARTITION BY s.batter_hand, is_shift_era, base_state, under_two_outs,
            c.batted_to_fielder
        ),
        location_all AS (
            PARTITION BY s.batter_hand, is_shift_era, base_state, under_two_outs,
            c.recorded_location, c.recorded_location_angle
        )
),

joined AS (
    SELECT
        batter_hand,
        is_shift_era,
        base_state,
        under_two_outs,
        recorded_location,
        recorded_location_angle,
        outfield.batting_average_sample_weight,
        outfield.batting_average_at_location,
        infield.batting_average AS infield_hit_rate,
        outfield.batted_to_fielder AS outfield_position,
        infield.batted_to_fielder AS infield_position,
        -- If 65% of balls to the left fielder come from the ThirdShortstop location,
        -- and 64% of infielder-fielded balls in ThirdShortstop are handled by the third baseman,
        -- then we infer that 65 * 64 = 41.6% ground balls to the left fielder are balls
        -- that came through ThirdShortstop that "should" have been fielded by the third baseman.
        -- Summing up all outfield-infield-location combinations, as we do in the final subquery,
        -- gives each outfielder a full 100%, divided among the infielders accordingly.
        outfield.share_within_fielder * infield.share_within_group AS share,
        -- We can use the same method to determine the share of balls lost by a given infielder
        -- that were fielded by a given outfielder.
        infield.share_within_fielder * outfield.share_within_group AS inverse_share,
    FROM base AS outfield
    INNER JOIN base AS infield USING (batter_hand, is_shift_era, base_state, under_two_outs, recorded_location, recorded_location_angle)
    WHERE outfield.batted_to_fielder > 6
        AND infield.batted_to_fielder BETWEEN 1 AND 6
),

final AS (
    SELECT
        batter_hand,
        is_shift_era,
        base_state,
        under_two_outs,
        outfield_position,
        infield_position,
        SUM(share) AS share,
        SUM(inverse_share) AS inverse_share,
        SUM(batting_average_at_location * batting_average_sample_weight) / SUM(batting_average_sample_weight) AS batting_average,
    FROM joined
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT * FROM final