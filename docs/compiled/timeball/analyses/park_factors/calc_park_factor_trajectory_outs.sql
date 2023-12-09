






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
        WHERE trajectory_known = 1 AND batting_outs > 0
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
                SUM(lines.trajectory_broad_air_ball)::INT AS trajectory_broad_air_ball,
                SUM(lines.trajectory_ground_ball)::INT AS trajectory_ground_ball,
                SUM(lines.trajectory_fly_ball)::INT AS trajectory_fly_ball,
                SUM(lines.trajectory_line_drive)::INT AS trajectory_line_drive,
                SUM(lines.trajectory_pop_up)::INT AS trajectory_pop_up,
        FROM "timeball"."main_models"."event_states_full" AS states
        INNER JOIN lines USING (event_key)
        -- Restrict to parks with decent sample
        INNER JOIN unique_park_seasons USING (season, league, park_id)
        WHERE states.game_type = 'RegularSeason'
            AND NOT states.is_interleague
            
        GROUP BY 1, 2, 3, 4, 5
    ),

    multi_year_range AS MATERIALIZED (
        SELECT
            la.park_id,
            ups.season,
            la.league,
            la.batter_id,
            la.pitcher_id,
            
                SUM(la.plate_appearances)::INT AS plate_appearances,
                SUM(la.trajectory_broad_air_ball)::INT AS trajectory_broad_air_ball,
                SUM(la.trajectory_ground_ball)::INT AS trajectory_ground_ball,
                SUM(la.trajectory_fly_ball)::INT AS trajectory_fly_ball,
                SUM(la.trajectory_line_drive)::INT AS trajectory_line_drive,
                SUM(la.trajectory_pop_up)::INT AS trajectory_pop_up,
        FROM lines_agg AS la
        INNER JOIN unique_park_seasons AS ups
            ON la.park_id = ups.park_id
                AND la.league = ups.league
                AND la.season BETWEEN ups.season - 2 AND ups.season
        GROUP BY 1, 2, 3, 4, 5
    ),

    averages AS MATERIALIZED (
        SELECT
            season,
            league,
            
                SUM(trajectory_broad_air_ball) / SUM(plate_appearances) AS trajectory_broad_air_ball_rate,
                SUM(trajectory_ground_ball) / SUM(plate_appearances) AS trajectory_ground_ball_rate,
                SUM(trajectory_fly_ball) / SUM(plate_appearances) AS trajectory_fly_ball_rate,
                SUM(trajectory_line_drive) / SUM(plate_appearances) AS trajectory_line_drive_rate,
                SUM(trajectory_pop_up) / SUM(plate_appearances) AS trajectory_pop_up_rate,
        FROM multi_year_range
        GROUP BY 1, 2
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
            
            1000 AS plate_appearances,
                averages.trajectory_broad_air_ball_rate * 1000 AS trajectory_broad_air_ball,
                averages.trajectory_ground_ball_rate * 1000 AS trajectory_ground_ball,
                averages.trajectory_fly_ball_rate * 1000 AS trajectory_fly_ball,
                averages.trajectory_line_drive_rate * 1000 AS trajectory_line_drive,
                averages.trajectory_pop_up_rate * 1000 AS trajectory_pop_up,
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
            
                this.plate_appearances AS this_plate_appearances,
                other.plate_appearances AS other_plate_appearances,
                this.trajectory_broad_air_ball AS this_trajectory_broad_air_ball,
                other.trajectory_broad_air_ball AS other_trajectory_broad_air_ball,
                this.trajectory_ground_ball AS this_trajectory_ground_ball,
                other.trajectory_ground_ball AS other_trajectory_ground_ball,
                this.trajectory_fly_ball AS this_trajectory_fly_ball,
                other.trajectory_fly_ball AS other_trajectory_fly_ball,
                this.trajectory_line_drive AS this_trajectory_line_drive,
                other.trajectory_line_drive AS other_trajectory_line_drive,
                this.trajectory_pop_up AS this_trajectory_pop_up,
                other.trajectory_pop_up AS other_trajectory_pop_up,
            SQRT(LEAST(this_plate_appearances, other_plate_appearances)) AS sample_size,
            SUM(sample_size) OVER (PARTITION BY this.park_id, other.park_id, this.season, this.league) AS sum_sample_size,
        FROM with_priors AS this
        INNER JOIN with_priors AS other
            ON this.park_id != other.park_id
                AND this.season = other.season
                AND this.batter_id = other.batter_id
                AND this.pitcher_id = other.pitcher_id
                
    ),

    rate_calculation AS (
        SELECT
            *,
                this_trajectory_broad_air_ball / this_plate_appearances AS this_trajectory_broad_air_ball_rate,
                other_trajectory_broad_air_ball / other_plate_appearances AS other_trajectory_broad_air_ball_rate,
                this_trajectory_ground_ball / this_plate_appearances AS this_trajectory_ground_ball_rate,
                other_trajectory_ground_ball / other_plate_appearances AS other_trajectory_ground_ball_rate,
                this_trajectory_fly_ball / this_plate_appearances AS this_trajectory_fly_ball_rate,
                other_trajectory_fly_ball / other_plate_appearances AS other_trajectory_fly_ball_rate,
                this_trajectory_line_drive / this_plate_appearances AS this_trajectory_line_drive_rate,
                other_trajectory_line_drive / other_plate_appearances AS other_trajectory_line_drive_rate,
                this_trajectory_pop_up / this_plate_appearances AS this_trajectory_pop_up_rate,
                other_trajectory_pop_up / other_plate_appearances AS other_trajectory_pop_up_rate,
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
            
            SUM(sample_size) AS sqrt_sample_size,
                SUM(this_trajectory_broad_air_ball_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_trajectory_broad_air_ball_rate,
                SUM(other_trajectory_broad_air_ball_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_trajectory_broad_air_ball_rate,
                avg_this_trajectory_broad_air_ball_rate
                / (1 - avg_this_trajectory_broad_air_ball_rate) AS this_trajectory_broad_air_ball_odds,
                avg_other_trajectory_broad_air_ball_rate
                / (1 - avg_other_trajectory_broad_air_ball_rate) AS other_trajectory_broad_air_ball_odds,
                this_trajectory_broad_air_ball_odds
                / other_trajectory_broad_air_ball_odds AS trajectory_broad_air_ball_odds_park_factor,
                avg_this_trajectory_broad_air_ball_rate / avg_other_trajectory_broad_air_ball_rate AS trajectory_broad_air_ball_rate_park_factor,
                SUM(this_trajectory_ground_ball_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_trajectory_ground_ball_rate,
                SUM(other_trajectory_ground_ball_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_trajectory_ground_ball_rate,
                avg_this_trajectory_ground_ball_rate
                / (1 - avg_this_trajectory_ground_ball_rate) AS this_trajectory_ground_ball_odds,
                avg_other_trajectory_ground_ball_rate
                / (1 - avg_other_trajectory_ground_ball_rate) AS other_trajectory_ground_ball_odds,
                this_trajectory_ground_ball_odds
                / other_trajectory_ground_ball_odds AS trajectory_ground_ball_odds_park_factor,
                avg_this_trajectory_ground_ball_rate / avg_other_trajectory_ground_ball_rate AS trajectory_ground_ball_rate_park_factor,
                SUM(this_trajectory_fly_ball_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_trajectory_fly_ball_rate,
                SUM(other_trajectory_fly_ball_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_trajectory_fly_ball_rate,
                avg_this_trajectory_fly_ball_rate
                / (1 - avg_this_trajectory_fly_ball_rate) AS this_trajectory_fly_ball_odds,
                avg_other_trajectory_fly_ball_rate
                / (1 - avg_other_trajectory_fly_ball_rate) AS other_trajectory_fly_ball_odds,
                this_trajectory_fly_ball_odds
                / other_trajectory_fly_ball_odds AS trajectory_fly_ball_odds_park_factor,
                avg_this_trajectory_fly_ball_rate / avg_other_trajectory_fly_ball_rate AS trajectory_fly_ball_rate_park_factor,
                SUM(this_trajectory_line_drive_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_trajectory_line_drive_rate,
                SUM(other_trajectory_line_drive_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_trajectory_line_drive_rate,
                avg_this_trajectory_line_drive_rate
                / (1 - avg_this_trajectory_line_drive_rate) AS this_trajectory_line_drive_odds,
                avg_other_trajectory_line_drive_rate
                / (1 - avg_other_trajectory_line_drive_rate) AS other_trajectory_line_drive_odds,
                this_trajectory_line_drive_odds
                / other_trajectory_line_drive_odds AS trajectory_line_drive_odds_park_factor,
                avg_this_trajectory_line_drive_rate / avg_other_trajectory_line_drive_rate AS trajectory_line_drive_rate_park_factor,
                SUM(this_trajectory_pop_up_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_trajectory_pop_up_rate,
                SUM(other_trajectory_pop_up_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_trajectory_pop_up_rate,
                avg_this_trajectory_pop_up_rate
                / (1 - avg_this_trajectory_pop_up_rate) AS this_trajectory_pop_up_odds,
                avg_other_trajectory_pop_up_rate
                / (1 - avg_other_trajectory_pop_up_rate) AS other_trajectory_pop_up_odds,
                this_trajectory_pop_up_odds
                / other_trajectory_pop_up_odds AS trajectory_pop_up_odds_park_factor,
                avg_this_trajectory_pop_up_rate / avg_other_trajectory_pop_up_rate AS trajectory_pop_up_rate_park_factor,
        FROM rate_calculation
        GROUP BY 1, 2, 3
    ),

    final AS (
        SELECT
            park_id,
            season,
            league,
            
            ROUND(sqrt_sample_size, 0) AS sqrt_sample_size,
                
                    ROUND(trajectory_broad_air_ball_odds_park_factor, 2) AS trajectory_broad_air_ball_park_factor,
                
                
                    ROUND(trajectory_ground_ball_odds_park_factor, 2) AS trajectory_ground_ball_park_factor,
                
                
                    ROUND(trajectory_fly_ball_odds_park_factor, 2) AS trajectory_fly_ball_park_factor,
                
                
                    ROUND(trajectory_line_drive_odds_park_factor, 2) AS trajectory_line_drive_park_factor,
                
                
                    ROUND(trajectory_pop_up_odds_park_factor, 2) AS trajectory_pop_up_park_factor,
                
        FROM weighted_average
    )

    SELECT * FROM final
