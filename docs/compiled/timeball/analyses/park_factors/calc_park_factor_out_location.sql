






    WITH unique_park_seasons AS (
        SELECT
            park_id,
            season,
            home_league AS league
        FROM "timeball"."main_models"."game_start_info"
        WHERE game_type = 'RegularSeason'
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 25
    ),

    lines AS (
        SELECT *
        FROM "timeball"."main_models"."event_offense_stats"
        WHERE batted_location_known = 1 AND batting_outs > 0
    ),

    lines_agg AS (
        SELECT
            states.park_id,
            states.season,
            states.league,
            states.batter_id,
            states.pitcher_id,
            ANY_VALUE(states.batter_hand) AS batter_hand,
                SUM(lines.plate_appearances)::INT AS plate_appearances,
                SUM(lines.batted_distance_plate)::INT AS batted_distance_plate,
                SUM(lines.batted_distance_infield)::INT AS batted_distance_infield,
                SUM(lines.batted_distance_outfield)::INT AS batted_distance_outfield,
                SUM(lines.fielded_by_battery)::INT AS fielded_by_battery,
                SUM(lines.fielded_by_infielder)::INT AS fielded_by_infielder,
                SUM(lines.fielded_by_outfielder)::INT AS fielded_by_outfielder,
                SUM(lines.batted_angle_left)::INT AS batted_angle_left,
                SUM(lines.batted_angle_right)::INT AS batted_angle_right,
                SUM(lines.batted_angle_middle)::INT AS batted_angle_middle,
                SUM(lines.batted_location_plate)::INT AS batted_location_plate,
                SUM(lines.batted_location_right_infield)::INT AS batted_location_right_infield,
                SUM(lines.batted_location_middle_infield)::INT AS batted_location_middle_infield,
                SUM(lines.batted_location_left_infield)::INT AS batted_location_left_infield,
                SUM(lines.batted_location_left_field)::INT AS batted_location_left_field,
                SUM(lines.batted_location_center_field)::INT AS batted_location_center_field,
                SUM(lines.batted_location_right_field)::INT AS batted_location_right_field,
        FROM "timeball"."main_models"."event_states_full" AS states
        INNER JOIN lines USING (event_key)
        -- Restrict to parks with decent sample
        INNER JOIN unique_park_seasons USING (season, league, park_id)
        WHERE states.game_type = 'RegularSeason'
            AND NOT states.is_interleague
            
                AND states.batter_hand IN ('L', 'R')
            
        GROUP BY 1, 2, 3, 4, 5
    ),

    multi_year_range AS MATERIALIZED (
        SELECT
            la.park_id,
            ups.season,
            la.league,
            la.batter_id,
            la.pitcher_id,
            batter_hand,
                SUM(la.plate_appearances)::INT AS plate_appearances,
                SUM(la.batted_distance_plate)::INT AS batted_distance_plate,
                SUM(la.batted_distance_infield)::INT AS batted_distance_infield,
                SUM(la.batted_distance_outfield)::INT AS batted_distance_outfield,
                SUM(la.fielded_by_battery)::INT AS fielded_by_battery,
                SUM(la.fielded_by_infielder)::INT AS fielded_by_infielder,
                SUM(la.fielded_by_outfielder)::INT AS fielded_by_outfielder,
                SUM(la.batted_angle_left)::INT AS batted_angle_left,
                SUM(la.batted_angle_right)::INT AS batted_angle_right,
                SUM(la.batted_angle_middle)::INT AS batted_angle_middle,
                SUM(la.batted_location_plate)::INT AS batted_location_plate,
                SUM(la.batted_location_right_infield)::INT AS batted_location_right_infield,
                SUM(la.batted_location_middle_infield)::INT AS batted_location_middle_infield,
                SUM(la.batted_location_left_infield)::INT AS batted_location_left_infield,
                SUM(la.batted_location_left_field)::INT AS batted_location_left_field,
                SUM(la.batted_location_center_field)::INT AS batted_location_center_field,
                SUM(la.batted_location_right_field)::INT AS batted_location_right_field,
        FROM lines_agg AS la
        INNER JOIN unique_park_seasons AS ups
            ON la.park_id = ups.park_id
                AND la.league = ups.league
                AND la.season BETWEEN ups.season - 2 AND ups.season
        GROUP BY 1, 2, 3, 4, 5, 6
    ),

    averages AS MATERIALIZED (
        SELECT
            season,
            league,
            batter_hand,
                SUM(batted_distance_plate) / SUM(plate_appearances) AS batted_distance_plate_rate,
                SUM(batted_distance_infield) / SUM(plate_appearances) AS batted_distance_infield_rate,
                SUM(batted_distance_outfield) / SUM(plate_appearances) AS batted_distance_outfield_rate,
                SUM(fielded_by_battery) / SUM(plate_appearances) AS fielded_by_battery_rate,
                SUM(fielded_by_infielder) / SUM(plate_appearances) AS fielded_by_infielder_rate,
                SUM(fielded_by_outfielder) / SUM(plate_appearances) AS fielded_by_outfielder_rate,
                SUM(batted_angle_left) / SUM(plate_appearances) AS batted_angle_left_rate,
                SUM(batted_angle_right) / SUM(plate_appearances) AS batted_angle_right_rate,
                SUM(batted_angle_middle) / SUM(plate_appearances) AS batted_angle_middle_rate,
                SUM(batted_location_plate) / SUM(plate_appearances) AS batted_location_plate_rate,
                SUM(batted_location_right_infield) / SUM(plate_appearances) AS batted_location_right_infield_rate,
                SUM(batted_location_middle_infield) / SUM(plate_appearances) AS batted_location_middle_infield_rate,
                SUM(batted_location_left_infield) / SUM(plate_appearances) AS batted_location_left_infield_rate,
                SUM(batted_location_left_field) / SUM(plate_appearances) AS batted_location_left_field_rate,
                SUM(batted_location_center_field) / SUM(plate_appearances) AS batted_location_center_field_rate,
                SUM(batted_location_right_field) / SUM(plate_appearances) AS batted_location_right_field_rate,
        FROM multi_year_range
        GROUP BY 1, 2, 3
    ),

    -- Give each park pair a batter-pitcher matchup at the league average
    -- with 1000 PA per park
    with_priors AS (
        SELECT *
        FROM multi_year_range
        UNION ALL BY NAME
        SELECT
            unique_park_seasons.park_id,
            season,
            league,
            'MARK' AS batter_id,
            'PRIOR' AS pitcher_id,
            batter_hand,
            1000 AS plate_appearances,
                averages.batted_distance_plate_rate * 1000 AS batted_distance_plate,
                averages.batted_distance_infield_rate * 1000 AS batted_distance_infield,
                averages.batted_distance_outfield_rate * 1000 AS batted_distance_outfield,
                averages.fielded_by_battery_rate * 1000 AS fielded_by_battery,
                averages.fielded_by_infielder_rate * 1000 AS fielded_by_infielder,
                averages.fielded_by_outfielder_rate * 1000 AS fielded_by_outfielder,
                averages.batted_angle_left_rate * 1000 AS batted_angle_left,
                averages.batted_angle_right_rate * 1000 AS batted_angle_right,
                averages.batted_angle_middle_rate * 1000 AS batted_angle_middle,
                averages.batted_location_plate_rate * 1000 AS batted_location_plate,
                averages.batted_location_right_infield_rate * 1000 AS batted_location_right_infield,
                averages.batted_location_middle_infield_rate * 1000 AS batted_location_middle_infield,
                averages.batted_location_left_infield_rate * 1000 AS batted_location_left_infield,
                averages.batted_location_left_field_rate * 1000 AS batted_location_left_field,
                averages.batted_location_center_field_rate * 1000 AS batted_location_center_field,
                averages.batted_location_right_field_rate * 1000 AS batted_location_right_field,
        FROM averages
        INNER JOIN unique_park_seasons USING (season, league)
    ),

    self_joined AS (
        SELECT
            this.park_id AS this_park_id,
            other.park_id AS other_park_id,
            this.season,
            this.league,  
            this.batter_id,
            this.pitcher_id,
            this.batter_hand,
                this.plate_appearances AS this_plate_appearances,
                other.plate_appearances AS other_plate_appearances,
                this.batted_distance_plate AS this_batted_distance_plate,
                other.batted_distance_plate AS other_batted_distance_plate,
                this.batted_distance_infield AS this_batted_distance_infield,
                other.batted_distance_infield AS other_batted_distance_infield,
                this.batted_distance_outfield AS this_batted_distance_outfield,
                other.batted_distance_outfield AS other_batted_distance_outfield,
                this.fielded_by_battery AS this_fielded_by_battery,
                other.fielded_by_battery AS other_fielded_by_battery,
                this.fielded_by_infielder AS this_fielded_by_infielder,
                other.fielded_by_infielder AS other_fielded_by_infielder,
                this.fielded_by_outfielder AS this_fielded_by_outfielder,
                other.fielded_by_outfielder AS other_fielded_by_outfielder,
                this.batted_angle_left AS this_batted_angle_left,
                other.batted_angle_left AS other_batted_angle_left,
                this.batted_angle_right AS this_batted_angle_right,
                other.batted_angle_right AS other_batted_angle_right,
                this.batted_angle_middle AS this_batted_angle_middle,
                other.batted_angle_middle AS other_batted_angle_middle,
                this.batted_location_plate AS this_batted_location_plate,
                other.batted_location_plate AS other_batted_location_plate,
                this.batted_location_right_infield AS this_batted_location_right_infield,
                other.batted_location_right_infield AS other_batted_location_right_infield,
                this.batted_location_middle_infield AS this_batted_location_middle_infield,
                other.batted_location_middle_infield AS other_batted_location_middle_infield,
                this.batted_location_left_infield AS this_batted_location_left_infield,
                other.batted_location_left_infield AS other_batted_location_left_infield,
                this.batted_location_left_field AS this_batted_location_left_field,
                other.batted_location_left_field AS other_batted_location_left_field,
                this.batted_location_center_field AS this_batted_location_center_field,
                other.batted_location_center_field AS other_batted_location_center_field,
                this.batted_location_right_field AS this_batted_location_right_field,
                other.batted_location_right_field AS other_batted_location_right_field,
            SQRT(LEAST(this_plate_appearances, other_plate_appearances)) AS sample_size,
            SUM(sample_size) OVER (PARTITION BY this.park_id, other.park_id, this.season, this.league) AS sum_sample_size,
        FROM with_priors AS this
        INNER JOIN with_priors AS other
            ON this.park_id != other.park_id
                AND this.season = other.season
                AND this.batter_id = other.batter_id
                AND this.pitcher_id = other.pitcher_id
                
                    AND this.batter_hand = other.batter_hand
                
    ),

    rate_calculation AS (
        SELECT
            *,
                this_batted_distance_plate / this_plate_appearances AS this_batted_distance_plate_rate,
                other_batted_distance_plate / other_plate_appearances AS other_batted_distance_plate_rate,
                this_batted_distance_infield / this_plate_appearances AS this_batted_distance_infield_rate,
                other_batted_distance_infield / other_plate_appearances AS other_batted_distance_infield_rate,
                this_batted_distance_outfield / this_plate_appearances AS this_batted_distance_outfield_rate,
                other_batted_distance_outfield / other_plate_appearances AS other_batted_distance_outfield_rate,
                this_fielded_by_battery / this_plate_appearances AS this_fielded_by_battery_rate,
                other_fielded_by_battery / other_plate_appearances AS other_fielded_by_battery_rate,
                this_fielded_by_infielder / this_plate_appearances AS this_fielded_by_infielder_rate,
                other_fielded_by_infielder / other_plate_appearances AS other_fielded_by_infielder_rate,
                this_fielded_by_outfielder / this_plate_appearances AS this_fielded_by_outfielder_rate,
                other_fielded_by_outfielder / other_plate_appearances AS other_fielded_by_outfielder_rate,
                this_batted_angle_left / this_plate_appearances AS this_batted_angle_left_rate,
                other_batted_angle_left / other_plate_appearances AS other_batted_angle_left_rate,
                this_batted_angle_right / this_plate_appearances AS this_batted_angle_right_rate,
                other_batted_angle_right / other_plate_appearances AS other_batted_angle_right_rate,
                this_batted_angle_middle / this_plate_appearances AS this_batted_angle_middle_rate,
                other_batted_angle_middle / other_plate_appearances AS other_batted_angle_middle_rate,
                this_batted_location_plate / this_plate_appearances AS this_batted_location_plate_rate,
                other_batted_location_plate / other_plate_appearances AS other_batted_location_plate_rate,
                this_batted_location_right_infield / this_plate_appearances AS this_batted_location_right_infield_rate,
                other_batted_location_right_infield / other_plate_appearances AS other_batted_location_right_infield_rate,
                this_batted_location_middle_infield / this_plate_appearances AS this_batted_location_middle_infield_rate,
                other_batted_location_middle_infield / other_plate_appearances AS other_batted_location_middle_infield_rate,
                this_batted_location_left_infield / this_plate_appearances AS this_batted_location_left_infield_rate,
                other_batted_location_left_infield / other_plate_appearances AS other_batted_location_left_infield_rate,
                this_batted_location_left_field / this_plate_appearances AS this_batted_location_left_field_rate,
                other_batted_location_left_field / other_plate_appearances AS other_batted_location_left_field_rate,
                this_batted_location_center_field / this_plate_appearances AS this_batted_location_center_field_rate,
                other_batted_location_center_field / other_plate_appearances AS other_batted_location_center_field_rate,
                this_batted_location_right_field / this_plate_appearances AS this_batted_location_right_field_rate,
                other_batted_location_right_field / other_plate_appearances AS other_batted_location_right_field_rate,
            -- Find the park pair with the highest sample size, and upweight all other pairs to match
            MAX(sum_sample_size) OVER (PARTITION BY this_park_id, season, league) AS scaling_factor,
            sample_size * (scaling_factor / sum_sample_size) AS sample_weight
        FROM self_joined
    ),

    weighted_average AS (
        SELECT
            this_park_id AS park_id,
            season,
            league,
            batter_hand,
            SUM(sample_size) AS sqrt_sample_size,
                SUM(this_batted_distance_plate_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_distance_plate_rate,
                SUM(other_batted_distance_plate_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_distance_plate_rate,
                avg_this_batted_distance_plate_rate
                / (1 - avg_this_batted_distance_plate_rate) AS this_batted_distance_plate_odds,
                avg_other_batted_distance_plate_rate
                / (1 - avg_other_batted_distance_plate_rate) AS other_batted_distance_plate_odds,
                this_batted_distance_plate_odds
                / other_batted_distance_plate_odds AS batted_distance_plate_odds_park_factor,
                avg_this_batted_distance_plate_rate / avg_other_batted_distance_plate_rate AS batted_distance_plate_rate_park_factor,
                SUM(this_batted_distance_infield_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_distance_infield_rate,
                SUM(other_batted_distance_infield_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_distance_infield_rate,
                avg_this_batted_distance_infield_rate
                / (1 - avg_this_batted_distance_infield_rate) AS this_batted_distance_infield_odds,
                avg_other_batted_distance_infield_rate
                / (1 - avg_other_batted_distance_infield_rate) AS other_batted_distance_infield_odds,
                this_batted_distance_infield_odds
                / other_batted_distance_infield_odds AS batted_distance_infield_odds_park_factor,
                avg_this_batted_distance_infield_rate / avg_other_batted_distance_infield_rate AS batted_distance_infield_rate_park_factor,
                SUM(this_batted_distance_outfield_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_distance_outfield_rate,
                SUM(other_batted_distance_outfield_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_distance_outfield_rate,
                avg_this_batted_distance_outfield_rate
                / (1 - avg_this_batted_distance_outfield_rate) AS this_batted_distance_outfield_odds,
                avg_other_batted_distance_outfield_rate
                / (1 - avg_other_batted_distance_outfield_rate) AS other_batted_distance_outfield_odds,
                this_batted_distance_outfield_odds
                / other_batted_distance_outfield_odds AS batted_distance_outfield_odds_park_factor,
                avg_this_batted_distance_outfield_rate / avg_other_batted_distance_outfield_rate AS batted_distance_outfield_rate_park_factor,
                SUM(this_fielded_by_battery_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_fielded_by_battery_rate,
                SUM(other_fielded_by_battery_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_fielded_by_battery_rate,
                avg_this_fielded_by_battery_rate
                / (1 - avg_this_fielded_by_battery_rate) AS this_fielded_by_battery_odds,
                avg_other_fielded_by_battery_rate
                / (1 - avg_other_fielded_by_battery_rate) AS other_fielded_by_battery_odds,
                this_fielded_by_battery_odds
                / other_fielded_by_battery_odds AS fielded_by_battery_odds_park_factor,
                avg_this_fielded_by_battery_rate / avg_other_fielded_by_battery_rate AS fielded_by_battery_rate_park_factor,
                SUM(this_fielded_by_infielder_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_fielded_by_infielder_rate,
                SUM(other_fielded_by_infielder_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_fielded_by_infielder_rate,
                avg_this_fielded_by_infielder_rate
                / (1 - avg_this_fielded_by_infielder_rate) AS this_fielded_by_infielder_odds,
                avg_other_fielded_by_infielder_rate
                / (1 - avg_other_fielded_by_infielder_rate) AS other_fielded_by_infielder_odds,
                this_fielded_by_infielder_odds
                / other_fielded_by_infielder_odds AS fielded_by_infielder_odds_park_factor,
                avg_this_fielded_by_infielder_rate / avg_other_fielded_by_infielder_rate AS fielded_by_infielder_rate_park_factor,
                SUM(this_fielded_by_outfielder_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_fielded_by_outfielder_rate,
                SUM(other_fielded_by_outfielder_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_fielded_by_outfielder_rate,
                avg_this_fielded_by_outfielder_rate
                / (1 - avg_this_fielded_by_outfielder_rate) AS this_fielded_by_outfielder_odds,
                avg_other_fielded_by_outfielder_rate
                / (1 - avg_other_fielded_by_outfielder_rate) AS other_fielded_by_outfielder_odds,
                this_fielded_by_outfielder_odds
                / other_fielded_by_outfielder_odds AS fielded_by_outfielder_odds_park_factor,
                avg_this_fielded_by_outfielder_rate / avg_other_fielded_by_outfielder_rate AS fielded_by_outfielder_rate_park_factor,
                SUM(this_batted_angle_left_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_angle_left_rate,
                SUM(other_batted_angle_left_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_angle_left_rate,
                avg_this_batted_angle_left_rate
                / (1 - avg_this_batted_angle_left_rate) AS this_batted_angle_left_odds,
                avg_other_batted_angle_left_rate
                / (1 - avg_other_batted_angle_left_rate) AS other_batted_angle_left_odds,
                this_batted_angle_left_odds
                / other_batted_angle_left_odds AS batted_angle_left_odds_park_factor,
                avg_this_batted_angle_left_rate / avg_other_batted_angle_left_rate AS batted_angle_left_rate_park_factor,
                SUM(this_batted_angle_right_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_angle_right_rate,
                SUM(other_batted_angle_right_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_angle_right_rate,
                avg_this_batted_angle_right_rate
                / (1 - avg_this_batted_angle_right_rate) AS this_batted_angle_right_odds,
                avg_other_batted_angle_right_rate
                / (1 - avg_other_batted_angle_right_rate) AS other_batted_angle_right_odds,
                this_batted_angle_right_odds
                / other_batted_angle_right_odds AS batted_angle_right_odds_park_factor,
                avg_this_batted_angle_right_rate / avg_other_batted_angle_right_rate AS batted_angle_right_rate_park_factor,
                SUM(this_batted_angle_middle_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_angle_middle_rate,
                SUM(other_batted_angle_middle_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_angle_middle_rate,
                avg_this_batted_angle_middle_rate
                / (1 - avg_this_batted_angle_middle_rate) AS this_batted_angle_middle_odds,
                avg_other_batted_angle_middle_rate
                / (1 - avg_other_batted_angle_middle_rate) AS other_batted_angle_middle_odds,
                this_batted_angle_middle_odds
                / other_batted_angle_middle_odds AS batted_angle_middle_odds_park_factor,
                avg_this_batted_angle_middle_rate / avg_other_batted_angle_middle_rate AS batted_angle_middle_rate_park_factor,
                SUM(this_batted_location_plate_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_location_plate_rate,
                SUM(other_batted_location_plate_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_location_plate_rate,
                avg_this_batted_location_plate_rate
                / (1 - avg_this_batted_location_plate_rate) AS this_batted_location_plate_odds,
                avg_other_batted_location_plate_rate
                / (1 - avg_other_batted_location_plate_rate) AS other_batted_location_plate_odds,
                this_batted_location_plate_odds
                / other_batted_location_plate_odds AS batted_location_plate_odds_park_factor,
                avg_this_batted_location_plate_rate / avg_other_batted_location_plate_rate AS batted_location_plate_rate_park_factor,
                SUM(this_batted_location_right_infield_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_location_right_infield_rate,
                SUM(other_batted_location_right_infield_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_location_right_infield_rate,
                avg_this_batted_location_right_infield_rate
                / (1 - avg_this_batted_location_right_infield_rate) AS this_batted_location_right_infield_odds,
                avg_other_batted_location_right_infield_rate
                / (1 - avg_other_batted_location_right_infield_rate) AS other_batted_location_right_infield_odds,
                this_batted_location_right_infield_odds
                / other_batted_location_right_infield_odds AS batted_location_right_infield_odds_park_factor,
                avg_this_batted_location_right_infield_rate / avg_other_batted_location_right_infield_rate AS batted_location_right_infield_rate_park_factor,
                SUM(this_batted_location_middle_infield_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_location_middle_infield_rate,
                SUM(other_batted_location_middle_infield_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_location_middle_infield_rate,
                avg_this_batted_location_middle_infield_rate
                / (1 - avg_this_batted_location_middle_infield_rate) AS this_batted_location_middle_infield_odds,
                avg_other_batted_location_middle_infield_rate
                / (1 - avg_other_batted_location_middle_infield_rate) AS other_batted_location_middle_infield_odds,
                this_batted_location_middle_infield_odds
                / other_batted_location_middle_infield_odds AS batted_location_middle_infield_odds_park_factor,
                avg_this_batted_location_middle_infield_rate / avg_other_batted_location_middle_infield_rate AS batted_location_middle_infield_rate_park_factor,
                SUM(this_batted_location_left_infield_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_location_left_infield_rate,
                SUM(other_batted_location_left_infield_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_location_left_infield_rate,
                avg_this_batted_location_left_infield_rate
                / (1 - avg_this_batted_location_left_infield_rate) AS this_batted_location_left_infield_odds,
                avg_other_batted_location_left_infield_rate
                / (1 - avg_other_batted_location_left_infield_rate) AS other_batted_location_left_infield_odds,
                this_batted_location_left_infield_odds
                / other_batted_location_left_infield_odds AS batted_location_left_infield_odds_park_factor,
                avg_this_batted_location_left_infield_rate / avg_other_batted_location_left_infield_rate AS batted_location_left_infield_rate_park_factor,
                SUM(this_batted_location_left_field_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_location_left_field_rate,
                SUM(other_batted_location_left_field_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_location_left_field_rate,
                avg_this_batted_location_left_field_rate
                / (1 - avg_this_batted_location_left_field_rate) AS this_batted_location_left_field_odds,
                avg_other_batted_location_left_field_rate
                / (1 - avg_other_batted_location_left_field_rate) AS other_batted_location_left_field_odds,
                this_batted_location_left_field_odds
                / other_batted_location_left_field_odds AS batted_location_left_field_odds_park_factor,
                avg_this_batted_location_left_field_rate / avg_other_batted_location_left_field_rate AS batted_location_left_field_rate_park_factor,
                SUM(this_batted_location_center_field_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_location_center_field_rate,
                SUM(other_batted_location_center_field_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_location_center_field_rate,
                avg_this_batted_location_center_field_rate
                / (1 - avg_this_batted_location_center_field_rate) AS this_batted_location_center_field_odds,
                avg_other_batted_location_center_field_rate
                / (1 - avg_other_batted_location_center_field_rate) AS other_batted_location_center_field_odds,
                this_batted_location_center_field_odds
                / other_batted_location_center_field_odds AS batted_location_center_field_odds_park_factor,
                avg_this_batted_location_center_field_rate / avg_other_batted_location_center_field_rate AS batted_location_center_field_rate_park_factor,
                SUM(this_batted_location_right_field_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batted_location_right_field_rate,
                SUM(other_batted_location_right_field_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batted_location_right_field_rate,
                avg_this_batted_location_right_field_rate
                / (1 - avg_this_batted_location_right_field_rate) AS this_batted_location_right_field_odds,
                avg_other_batted_location_right_field_rate
                / (1 - avg_other_batted_location_right_field_rate) AS other_batted_location_right_field_odds,
                this_batted_location_right_field_odds
                / other_batted_location_right_field_odds AS batted_location_right_field_odds_park_factor,
                avg_this_batted_location_right_field_rate / avg_other_batted_location_right_field_rate AS batted_location_right_field_rate_park_factor,
        FROM rate_calculation
        GROUP BY 1, 2, 3, 4
    ),

    final AS (
        SELECT
            park_id,
            season,
            league,
            batter_hand,
            ROUND(sqrt_sample_size, 0) AS sqrt_sample_size,
                
                    ROUND(batted_distance_plate_odds_park_factor, 2) AS batted_distance_plate_park_factor,
                
                
                    ROUND(batted_distance_infield_odds_park_factor, 2) AS batted_distance_infield_park_factor,
                
                
                    ROUND(batted_distance_outfield_odds_park_factor, 2) AS batted_distance_outfield_park_factor,
                
                
                    ROUND(fielded_by_battery_odds_park_factor, 2) AS fielded_by_battery_park_factor,
                
                
                    ROUND(fielded_by_infielder_odds_park_factor, 2) AS fielded_by_infielder_park_factor,
                
                
                    ROUND(fielded_by_outfielder_odds_park_factor, 2) AS fielded_by_outfielder_park_factor,
                
                
                    ROUND(batted_angle_left_odds_park_factor, 2) AS batted_angle_left_park_factor,
                
                
                    ROUND(batted_angle_right_odds_park_factor, 2) AS batted_angle_right_park_factor,
                
                
                    ROUND(batted_angle_middle_odds_park_factor, 2) AS batted_angle_middle_park_factor,
                
                
                    ROUND(batted_location_plate_odds_park_factor, 2) AS batted_location_plate_park_factor,
                
                
                    ROUND(batted_location_right_infield_odds_park_factor, 2) AS batted_location_right_infield_park_factor,
                
                
                    ROUND(batted_location_middle_infield_odds_park_factor, 2) AS batted_location_middle_infield_park_factor,
                
                
                    ROUND(batted_location_left_infield_odds_park_factor, 2) AS batted_location_left_infield_park_factor,
                
                
                    ROUND(batted_location_left_field_odds_park_factor, 2) AS batted_location_left_field_park_factor,
                
                
                    ROUND(batted_location_center_field_odds_park_factor, 2) AS batted_location_center_field_park_factor,
                
                
                    ROUND(batted_location_right_field_odds_park_factor, 2) AS batted_location_right_field_park_factor,
                
        FROM weighted_average
    )

    SELECT * FROM final
