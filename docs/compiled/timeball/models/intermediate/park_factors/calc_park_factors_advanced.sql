




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

batting_agg AS (
    SELECT
        states.park_id,
        states.season,
        states.league,
        states.batter_id,
        states.pitcher_id,
            SUM(batting.plate_appearances)::INT AS plate_appearances,
            SUM(batting.singles)::INT AS singles,
            SUM(batting.doubles)::INT AS doubles,
            SUM(batting.triples)::INT AS triples,
            SUM(batting.home_runs)::INT AS home_runs,
            SUM(batting.strikeouts)::INT AS strikeouts,
            SUM(batting.walks)::INT AS walks,
            SUM(batting.batting_outs)::INT AS batting_outs,
            SUM(batting.runs)::INT AS runs,
            SUM(batting.balls_in_play)::INT AS balls_in_play,
            SUM(batting.trajectory_fly_ball)::INT AS trajectory_fly_ball,
            SUM(batting.trajectory_ground_ball)::INT AS trajectory_ground_ball,
            SUM(batting.trajectory_line_drive)::INT AS trajectory_line_drive,
            SUM(batting.trajectory_pop_up)::INT AS trajectory_pop_up,
            SUM(batting.trajectory_unknown)::INT AS trajectory_unknown,
            SUM(batting.batted_distance_infield)::INT AS batted_distance_infield,
            SUM(batting.batted_distance_outfield)::INT AS batted_distance_outfield,
            SUM(batting.batted_distance_unknown)::INT AS batted_distance_unknown,
            SUM(batting.batted_angle_left)::INT AS batted_angle_left,
            SUM(batting.batted_angle_right)::INT AS batted_angle_right,
            SUM(batting.batted_angle_middle)::INT AS batted_angle_middle,
    FROM "timeball"."main_models"."event_states_full" AS states
    INNER JOIN "timeball"."main_models"."event_offense_stats" AS batting USING (event_key)
    -- Restrict to parks with decent sample
    INNER JOIN unique_park_seasons USING (season, league, park_id)
    WHERE states.game_type = 'RegularSeason'
        AND NOT states.is_interleague
    GROUP BY 1, 2, 3, 4, 5
),

multi_year_range AS MATERIALIZED (
    SELECT
        park_id,
        season,
        league,
        batter_id,
        pitcher_id,
            SUM(plate_appearances)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS plate_appearances,
            SUM(singles)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS singles,
            SUM(doubles)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS doubles,
            SUM(triples)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS triples,
            SUM(home_runs)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS home_runs,
            SUM(strikeouts)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS strikeouts,
            SUM(walks)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS walks,
            SUM(batting_outs)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS batting_outs,
            SUM(runs)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS runs,
            SUM(balls_in_play)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS balls_in_play,
            SUM(trajectory_fly_ball)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS trajectory_fly_ball,
            SUM(trajectory_ground_ball)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS trajectory_ground_ball,
            SUM(trajectory_line_drive)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS trajectory_line_drive,
            SUM(trajectory_pop_up)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS trajectory_pop_up,
            SUM(trajectory_unknown)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS trajectory_unknown,
            SUM(batted_distance_infield)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS batted_distance_infield,
            SUM(batted_distance_outfield)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS batted_distance_outfield,
            SUM(batted_distance_unknown)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS batted_distance_unknown,
            SUM(batted_angle_left)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS batted_angle_left,
            SUM(batted_angle_right)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS batted_angle_right,
            SUM(batted_angle_middle)
                OVER (
                    PARTITION BY park_id, batter_id, pitcher_id, league
                    ORDER BY season
                    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
            )::INT
            AS batted_angle_middle,
    FROM batting_agg
),

averages AS MATERIALIZED (
    SELECT
        season,
        league,
            SUM(singles) / SUM(plate_appearances) AS avg_singles_per_pa,
            SUM(doubles) / SUM(plate_appearances) AS avg_doubles_per_pa,
            SUM(triples) / SUM(plate_appearances) AS avg_triples_per_pa,
            SUM(home_runs) / SUM(plate_appearances) AS avg_home_runs_per_pa,
            SUM(strikeouts) / SUM(plate_appearances) AS avg_strikeouts_per_pa,
            SUM(walks) / SUM(plate_appearances) AS avg_walks_per_pa,
            SUM(batting_outs) / SUM(plate_appearances) AS avg_batting_outs_per_pa,
            SUM(runs) / SUM(plate_appearances) AS avg_runs_per_pa,
            SUM(balls_in_play) / SUM(plate_appearances) AS avg_balls_in_play_per_pa,
            SUM(trajectory_fly_ball) / SUM(plate_appearances) AS avg_trajectory_fly_ball_per_pa,
            SUM(trajectory_ground_ball) / SUM(plate_appearances) AS avg_trajectory_ground_ball_per_pa,
            SUM(trajectory_line_drive) / SUM(plate_appearances) AS avg_trajectory_line_drive_per_pa,
            SUM(trajectory_pop_up) / SUM(plate_appearances) AS avg_trajectory_pop_up_per_pa,
            SUM(trajectory_unknown) / SUM(plate_appearances) AS avg_trajectory_unknown_per_pa,
            SUM(batted_distance_infield) / SUM(plate_appearances) AS avg_batted_distance_infield_per_pa,
            SUM(batted_distance_outfield) / SUM(plate_appearances) AS avg_batted_distance_outfield_per_pa,
            SUM(batted_distance_unknown) / SUM(plate_appearances) AS avg_batted_distance_unknown_per_pa,
            SUM(batted_angle_left) / SUM(plate_appearances) AS avg_batted_angle_left_per_pa,
            SUM(batted_angle_right) / SUM(plate_appearances) AS avg_batted_angle_right_per_pa,
            SUM(batted_angle_middle) / SUM(plate_appearances) AS avg_batted_angle_middle_per_pa,
    FROM multi_year_range
    GROUP BY 1, 2
),

-- Give each park pair a batter-pitcher matchup at the league average
-- with 1000::SMALLINT PA per park
with_priors AS (
    SELECT *
    FROM multi_year_range
    UNION ALL
    SELECT
        unique_park_seasons.park_id,
        season,
        league,
        'MARK' AS batter_id,
        'PRIOR' AS pitcher_id,
        1000::SMALLINT AS plate_appearances,
            averages.avg_singles_per_pa * 1000::SMALLINT AS singles,
            averages.avg_doubles_per_pa * 1000::SMALLINT AS doubles,
            averages.avg_triples_per_pa * 1000::SMALLINT AS triples,
            averages.avg_home_runs_per_pa * 1000::SMALLINT AS home_runs,
            averages.avg_strikeouts_per_pa * 1000::SMALLINT AS strikeouts,
            averages.avg_walks_per_pa * 1000::SMALLINT AS walks,
            averages.avg_batting_outs_per_pa * 1000::SMALLINT AS batting_outs,
            averages.avg_runs_per_pa * 1000::SMALLINT AS runs,
            averages.avg_balls_in_play_per_pa * 1000::SMALLINT AS balls_in_play,
            averages.avg_trajectory_fly_ball_per_pa * 1000::SMALLINT AS trajectory_fly_ball,
            averages.avg_trajectory_ground_ball_per_pa * 1000::SMALLINT AS trajectory_ground_ball,
            averages.avg_trajectory_line_drive_per_pa * 1000::SMALLINT AS trajectory_line_drive,
            averages.avg_trajectory_pop_up_per_pa * 1000::SMALLINT AS trajectory_pop_up,
            averages.avg_trajectory_unknown_per_pa * 1000::SMALLINT AS trajectory_unknown,
            averages.avg_batted_distance_infield_per_pa * 1000::SMALLINT AS batted_distance_infield,
            averages.avg_batted_distance_outfield_per_pa * 1000::SMALLINT AS batted_distance_outfield,
            averages.avg_batted_distance_unknown_per_pa * 1000::SMALLINT AS batted_distance_unknown,
            averages.avg_batted_angle_left_per_pa * 1000::SMALLINT AS batted_angle_left,
            averages.avg_batted_angle_right_per_pa * 1000::SMALLINT AS batted_angle_right,
            averages.avg_batted_angle_middle_per_pa * 1000::SMALLINT AS batted_angle_middle,
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
            this.singles AS this_singles,
            other.singles AS other_singles,
            this.doubles AS this_doubles,
            other.doubles AS other_doubles,
            this.triples AS this_triples,
            other.triples AS other_triples,
            this.home_runs AS this_home_runs,
            other.home_runs AS other_home_runs,
            this.strikeouts AS this_strikeouts,
            other.strikeouts AS other_strikeouts,
            this.walks AS this_walks,
            other.walks AS other_walks,
            this.batting_outs AS this_batting_outs,
            other.batting_outs AS other_batting_outs,
            this.runs AS this_runs,
            other.runs AS other_runs,
            this.balls_in_play AS this_balls_in_play,
            other.balls_in_play AS other_balls_in_play,
            this.trajectory_fly_ball AS this_trajectory_fly_ball,
            other.trajectory_fly_ball AS other_trajectory_fly_ball,
            this.trajectory_ground_ball AS this_trajectory_ground_ball,
            other.trajectory_ground_ball AS other_trajectory_ground_ball,
            this.trajectory_line_drive AS this_trajectory_line_drive,
            other.trajectory_line_drive AS other_trajectory_line_drive,
            this.trajectory_pop_up AS this_trajectory_pop_up,
            other.trajectory_pop_up AS other_trajectory_pop_up,
            this.trajectory_unknown AS this_trajectory_unknown,
            other.trajectory_unknown AS other_trajectory_unknown,
            this.batted_distance_infield AS this_batted_distance_infield,
            other.batted_distance_infield AS other_batted_distance_infield,
            this.batted_distance_outfield AS this_batted_distance_outfield,
            other.batted_distance_outfield AS other_batted_distance_outfield,
            this.batted_distance_unknown AS this_batted_distance_unknown,
            other.batted_distance_unknown AS other_batted_distance_unknown,
            this.batted_angle_left AS this_batted_angle_left,
            other.batted_angle_left AS other_batted_angle_left,
            this.batted_angle_right AS this_batted_angle_right,
            other.batted_angle_right AS other_batted_angle_right,
            this.batted_angle_middle AS this_batted_angle_middle,
            other.batted_angle_middle AS other_batted_angle_middle,
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
            this_singles / this_plate_appearances AS this_singles_per_pa,
            other_singles / other_plate_appearances AS other_singles_per_pa,
            this_doubles / this_plate_appearances AS this_doubles_per_pa,
            other_doubles / other_plate_appearances AS other_doubles_per_pa,
            this_triples / this_plate_appearances AS this_triples_per_pa,
            other_triples / other_plate_appearances AS other_triples_per_pa,
            this_home_runs / this_plate_appearances AS this_home_runs_per_pa,
            other_home_runs / other_plate_appearances AS other_home_runs_per_pa,
            this_strikeouts / this_plate_appearances AS this_strikeouts_per_pa,
            other_strikeouts / other_plate_appearances AS other_strikeouts_per_pa,
            this_walks / this_plate_appearances AS this_walks_per_pa,
            other_walks / other_plate_appearances AS other_walks_per_pa,
            this_batting_outs / this_plate_appearances AS this_batting_outs_per_pa,
            other_batting_outs / other_plate_appearances AS other_batting_outs_per_pa,
            this_runs / this_plate_appearances AS this_runs_per_pa,
            other_runs / other_plate_appearances AS other_runs_per_pa,
            this_balls_in_play / this_plate_appearances AS this_balls_in_play_per_pa,
            other_balls_in_play / other_plate_appearances AS other_balls_in_play_per_pa,
            this_trajectory_fly_ball / this_plate_appearances AS this_trajectory_fly_ball_per_pa,
            other_trajectory_fly_ball / other_plate_appearances AS other_trajectory_fly_ball_per_pa,
            this_trajectory_ground_ball / this_plate_appearances AS this_trajectory_ground_ball_per_pa,
            other_trajectory_ground_ball / other_plate_appearances AS other_trajectory_ground_ball_per_pa,
            this_trajectory_line_drive / this_plate_appearances AS this_trajectory_line_drive_per_pa,
            other_trajectory_line_drive / other_plate_appearances AS other_trajectory_line_drive_per_pa,
            this_trajectory_pop_up / this_plate_appearances AS this_trajectory_pop_up_per_pa,
            other_trajectory_pop_up / other_plate_appearances AS other_trajectory_pop_up_per_pa,
            this_trajectory_unknown / this_plate_appearances AS this_trajectory_unknown_per_pa,
            other_trajectory_unknown / other_plate_appearances AS other_trajectory_unknown_per_pa,
            this_batted_distance_infield / this_plate_appearances AS this_batted_distance_infield_per_pa,
            other_batted_distance_infield / other_plate_appearances AS other_batted_distance_infield_per_pa,
            this_batted_distance_outfield / this_plate_appearances AS this_batted_distance_outfield_per_pa,
            other_batted_distance_outfield / other_plate_appearances AS other_batted_distance_outfield_per_pa,
            this_batted_distance_unknown / this_plate_appearances AS this_batted_distance_unknown_per_pa,
            other_batted_distance_unknown / other_plate_appearances AS other_batted_distance_unknown_per_pa,
            this_batted_angle_left / this_plate_appearances AS this_batted_angle_left_per_pa,
            other_batted_angle_left / other_plate_appearances AS other_batted_angle_left_per_pa,
            this_batted_angle_right / this_plate_appearances AS this_batted_angle_right_per_pa,
            other_batted_angle_right / other_plate_appearances AS other_batted_angle_right_per_pa,
            this_batted_angle_middle / this_plate_appearances AS this_batted_angle_middle_per_pa,
            other_batted_angle_middle / other_plate_appearances AS other_batted_angle_middle_per_pa,
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
            SUM(this_singles_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_singles_per_pa,
            SUM(other_singles_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_singles_per_pa,
            avg_this_singles_per_pa
            / (1 - avg_this_singles_per_pa) AS this_singles_odds,
            avg_other_singles_per_pa
            / (1 - avg_other_singles_per_pa) AS other_singles_odds,
            this_singles_odds
            / other_singles_odds AS singles_park_factor,
            SUM(this_doubles_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_doubles_per_pa,
            SUM(other_doubles_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_doubles_per_pa,
            avg_this_doubles_per_pa
            / (1 - avg_this_doubles_per_pa) AS this_doubles_odds,
            avg_other_doubles_per_pa
            / (1 - avg_other_doubles_per_pa) AS other_doubles_odds,
            this_doubles_odds
            / other_doubles_odds AS doubles_park_factor,
            SUM(this_triples_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_triples_per_pa,
            SUM(other_triples_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_triples_per_pa,
            avg_this_triples_per_pa
            / (1 - avg_this_triples_per_pa) AS this_triples_odds,
            avg_other_triples_per_pa
            / (1 - avg_other_triples_per_pa) AS other_triples_odds,
            this_triples_odds
            / other_triples_odds AS triples_park_factor,
            SUM(this_home_runs_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_home_runs_per_pa,
            SUM(other_home_runs_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_home_runs_per_pa,
            avg_this_home_runs_per_pa
            / (1 - avg_this_home_runs_per_pa) AS this_home_runs_odds,
            avg_other_home_runs_per_pa
            / (1 - avg_other_home_runs_per_pa) AS other_home_runs_odds,
            this_home_runs_odds
            / other_home_runs_odds AS home_runs_park_factor,
            SUM(this_strikeouts_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_strikeouts_per_pa,
            SUM(other_strikeouts_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_strikeouts_per_pa,
            avg_this_strikeouts_per_pa
            / (1 - avg_this_strikeouts_per_pa) AS this_strikeouts_odds,
            avg_other_strikeouts_per_pa
            / (1 - avg_other_strikeouts_per_pa) AS other_strikeouts_odds,
            this_strikeouts_odds
            / other_strikeouts_odds AS strikeouts_park_factor,
            SUM(this_walks_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_walks_per_pa,
            SUM(other_walks_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_walks_per_pa,
            avg_this_walks_per_pa
            / (1 - avg_this_walks_per_pa) AS this_walks_odds,
            avg_other_walks_per_pa
            / (1 - avg_other_walks_per_pa) AS other_walks_odds,
            this_walks_odds
            / other_walks_odds AS walks_park_factor,
            SUM(this_batting_outs_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_batting_outs_per_pa,
            SUM(other_batting_outs_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_batting_outs_per_pa,
            avg_this_batting_outs_per_pa
            / (1 - avg_this_batting_outs_per_pa) AS this_batting_outs_odds,
            avg_other_batting_outs_per_pa
            / (1 - avg_other_batting_outs_per_pa) AS other_batting_outs_odds,
            this_batting_outs_odds
            / other_batting_outs_odds AS batting_outs_park_factor,
            SUM(this_runs_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_runs_per_pa,
            SUM(other_runs_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_runs_per_pa,
            avg_this_runs_per_pa
            / (1 - avg_this_runs_per_pa) AS this_runs_odds,
            avg_other_runs_per_pa
            / (1 - avg_other_runs_per_pa) AS other_runs_odds,
            this_runs_odds
            / other_runs_odds AS runs_park_factor,
            SUM(this_balls_in_play_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_balls_in_play_per_pa,
            SUM(other_balls_in_play_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_balls_in_play_per_pa,
            avg_this_balls_in_play_per_pa
            / (1 - avg_this_balls_in_play_per_pa) AS this_balls_in_play_odds,
            avg_other_balls_in_play_per_pa
            / (1 - avg_other_balls_in_play_per_pa) AS other_balls_in_play_odds,
            this_balls_in_play_odds
            / other_balls_in_play_odds AS balls_in_play_park_factor,
            SUM(this_trajectory_fly_ball_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_trajectory_fly_ball_per_pa,
            SUM(other_trajectory_fly_ball_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_trajectory_fly_ball_per_pa,
            avg_this_trajectory_fly_ball_per_pa
            / (1 - avg_this_trajectory_fly_ball_per_pa) AS this_trajectory_fly_ball_odds,
            avg_other_trajectory_fly_ball_per_pa
            / (1 - avg_other_trajectory_fly_ball_per_pa) AS other_trajectory_fly_ball_odds,
            this_trajectory_fly_ball_odds
            / other_trajectory_fly_ball_odds AS trajectory_fly_ball_park_factor,
            SUM(this_trajectory_ground_ball_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_trajectory_ground_ball_per_pa,
            SUM(other_trajectory_ground_ball_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_trajectory_ground_ball_per_pa,
            avg_this_trajectory_ground_ball_per_pa
            / (1 - avg_this_trajectory_ground_ball_per_pa) AS this_trajectory_ground_ball_odds,
            avg_other_trajectory_ground_ball_per_pa
            / (1 - avg_other_trajectory_ground_ball_per_pa) AS other_trajectory_ground_ball_odds,
            this_trajectory_ground_ball_odds
            / other_trajectory_ground_ball_odds AS trajectory_ground_ball_park_factor,
            SUM(this_trajectory_line_drive_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_trajectory_line_drive_per_pa,
            SUM(other_trajectory_line_drive_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_trajectory_line_drive_per_pa,
            avg_this_trajectory_line_drive_per_pa
            / (1 - avg_this_trajectory_line_drive_per_pa) AS this_trajectory_line_drive_odds,
            avg_other_trajectory_line_drive_per_pa
            / (1 - avg_other_trajectory_line_drive_per_pa) AS other_trajectory_line_drive_odds,
            this_trajectory_line_drive_odds
            / other_trajectory_line_drive_odds AS trajectory_line_drive_park_factor,
            SUM(this_trajectory_pop_up_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_trajectory_pop_up_per_pa,
            SUM(other_trajectory_pop_up_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_trajectory_pop_up_per_pa,
            avg_this_trajectory_pop_up_per_pa
            / (1 - avg_this_trajectory_pop_up_per_pa) AS this_trajectory_pop_up_odds,
            avg_other_trajectory_pop_up_per_pa
            / (1 - avg_other_trajectory_pop_up_per_pa) AS other_trajectory_pop_up_odds,
            this_trajectory_pop_up_odds
            / other_trajectory_pop_up_odds AS trajectory_pop_up_park_factor,
            SUM(this_trajectory_unknown_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_trajectory_unknown_per_pa,
            SUM(other_trajectory_unknown_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_trajectory_unknown_per_pa,
            avg_this_trajectory_unknown_per_pa
            / (1 - avg_this_trajectory_unknown_per_pa) AS this_trajectory_unknown_odds,
            avg_other_trajectory_unknown_per_pa
            / (1 - avg_other_trajectory_unknown_per_pa) AS other_trajectory_unknown_odds,
            this_trajectory_unknown_odds
            / other_trajectory_unknown_odds AS trajectory_unknown_park_factor,
            SUM(this_batted_distance_infield_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_batted_distance_infield_per_pa,
            SUM(other_batted_distance_infield_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_batted_distance_infield_per_pa,
            avg_this_batted_distance_infield_per_pa
            / (1 - avg_this_batted_distance_infield_per_pa) AS this_batted_distance_infield_odds,
            avg_other_batted_distance_infield_per_pa
            / (1 - avg_other_batted_distance_infield_per_pa) AS other_batted_distance_infield_odds,
            this_batted_distance_infield_odds
            / other_batted_distance_infield_odds AS batted_distance_infield_park_factor,
            SUM(this_batted_distance_outfield_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_batted_distance_outfield_per_pa,
            SUM(other_batted_distance_outfield_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_batted_distance_outfield_per_pa,
            avg_this_batted_distance_outfield_per_pa
            / (1 - avg_this_batted_distance_outfield_per_pa) AS this_batted_distance_outfield_odds,
            avg_other_batted_distance_outfield_per_pa
            / (1 - avg_other_batted_distance_outfield_per_pa) AS other_batted_distance_outfield_odds,
            this_batted_distance_outfield_odds
            / other_batted_distance_outfield_odds AS batted_distance_outfield_park_factor,
            SUM(this_batted_distance_unknown_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_batted_distance_unknown_per_pa,
            SUM(other_batted_distance_unknown_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_batted_distance_unknown_per_pa,
            avg_this_batted_distance_unknown_per_pa
            / (1 - avg_this_batted_distance_unknown_per_pa) AS this_batted_distance_unknown_odds,
            avg_other_batted_distance_unknown_per_pa
            / (1 - avg_other_batted_distance_unknown_per_pa) AS other_batted_distance_unknown_odds,
            this_batted_distance_unknown_odds
            / other_batted_distance_unknown_odds AS batted_distance_unknown_park_factor,
            SUM(this_batted_angle_left_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_batted_angle_left_per_pa,
            SUM(other_batted_angle_left_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_batted_angle_left_per_pa,
            avg_this_batted_angle_left_per_pa
            / (1 - avg_this_batted_angle_left_per_pa) AS this_batted_angle_left_odds,
            avg_other_batted_angle_left_per_pa
            / (1 - avg_other_batted_angle_left_per_pa) AS other_batted_angle_left_odds,
            this_batted_angle_left_odds
            / other_batted_angle_left_odds AS batted_angle_left_park_factor,
            SUM(this_batted_angle_right_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_batted_angle_right_per_pa,
            SUM(other_batted_angle_right_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_batted_angle_right_per_pa,
            avg_this_batted_angle_right_per_pa
            / (1 - avg_this_batted_angle_right_per_pa) AS this_batted_angle_right_odds,
            avg_other_batted_angle_right_per_pa
            / (1 - avg_other_batted_angle_right_per_pa) AS other_batted_angle_right_odds,
            this_batted_angle_right_odds
            / other_batted_angle_right_odds AS batted_angle_right_park_factor,
            SUM(this_batted_angle_middle_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_this_batted_angle_middle_per_pa,
            SUM(other_batted_angle_middle_per_pa * sample_weight)
            / SUM(sample_weight) AS avg_other_batted_angle_middle_per_pa,
            avg_this_batted_angle_middle_per_pa
            / (1 - avg_this_batted_angle_middle_per_pa) AS this_batted_angle_middle_odds,
            avg_other_batted_angle_middle_per_pa
            / (1 - avg_other_batted_angle_middle_per_pa) AS other_batted_angle_middle_odds,
            this_batted_angle_middle_odds
            / other_batted_angle_middle_odds AS batted_angle_middle_park_factor,
    FROM rate_calculation
    GROUP BY 1, 2, 3
),

final AS (
    SELECT
        park_id,
        season,
        league,
        ROUND(sqrt_sample_size, 0) AS sqrt_sample_size,
            ROUND(singles_park_factor, 2) AS singles_park_factor,
            ROUND(doubles_park_factor, 2) AS doubles_park_factor,
            ROUND(triples_park_factor, 2) AS triples_park_factor,
            ROUND(home_runs_park_factor, 2) AS home_runs_park_factor,
            ROUND(strikeouts_park_factor, 2) AS strikeouts_park_factor,
            ROUND(walks_park_factor, 2) AS walks_park_factor,
            ROUND(batting_outs_park_factor, 2) AS batting_outs_park_factor,
            ROUND(runs_park_factor, 2) AS runs_park_factor,
            ROUND(balls_in_play_park_factor, 2) AS balls_in_play_park_factor,
            ROUND(trajectory_fly_ball_park_factor, 2) AS trajectory_fly_ball_park_factor,
            ROUND(trajectory_ground_ball_park_factor, 2) AS trajectory_ground_ball_park_factor,
            ROUND(trajectory_line_drive_park_factor, 2) AS trajectory_line_drive_park_factor,
            ROUND(trajectory_pop_up_park_factor, 2) AS trajectory_pop_up_park_factor,
            ROUND(trajectory_unknown_park_factor, 2) AS trajectory_unknown_park_factor,
            ROUND(batted_distance_infield_park_factor, 2) AS batted_distance_infield_park_factor,
            ROUND(batted_distance_outfield_park_factor, 2) AS batted_distance_outfield_park_factor,
            ROUND(batted_distance_unknown_park_factor, 2) AS batted_distance_unknown_park_factor,
            ROUND(batted_angle_left_park_factor, 2) AS batted_angle_left_park_factor,
            ROUND(batted_angle_right_park_factor, 2) AS batted_angle_right_park_factor,
            ROUND(batted_angle_middle_park_factor, 2) AS batted_angle_middle_park_factor,
    FROM weighted_average
)

SELECT * FROM final