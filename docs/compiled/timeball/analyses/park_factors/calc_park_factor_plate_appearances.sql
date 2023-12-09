






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
        WHERE 1=1
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
                SUM(lines.singles)::INT AS singles,
                SUM(lines.doubles)::INT AS doubles,
                SUM(lines.triples)::INT AS triples,
                SUM(lines.home_runs)::INT AS home_runs,
                SUM(lines.strikeouts)::INT AS strikeouts,
                SUM(lines.reached_on_errors)::INT AS reached_on_errors,
                SUM(lines.walks)::INT AS walks,
                SUM(lines.batting_outs)::INT AS batting_outs,
                SUM(lines.runs)::INT AS runs,
                SUM(lines.balls_in_play)::INT AS balls_in_play,
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
                SUM(la.singles)::INT AS singles,
                SUM(la.doubles)::INT AS doubles,
                SUM(la.triples)::INT AS triples,
                SUM(la.home_runs)::INT AS home_runs,
                SUM(la.strikeouts)::INT AS strikeouts,
                SUM(la.reached_on_errors)::INT AS reached_on_errors,
                SUM(la.walks)::INT AS walks,
                SUM(la.batting_outs)::INT AS batting_outs,
                SUM(la.runs)::INT AS runs,
                SUM(la.balls_in_play)::INT AS balls_in_play,
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
            
                SUM(singles) / SUM(plate_appearances) AS singles_rate,
                SUM(doubles) / SUM(plate_appearances) AS doubles_rate,
                SUM(triples) / SUM(plate_appearances) AS triples_rate,
                SUM(home_runs) / SUM(plate_appearances) AS home_runs_rate,
                SUM(strikeouts) / SUM(plate_appearances) AS strikeouts_rate,
                SUM(reached_on_errors) / SUM(plate_appearances) AS reached_on_errors_rate,
                SUM(walks) / SUM(plate_appearances) AS walks_rate,
                SUM(batting_outs) / SUM(plate_appearances) AS batting_outs_rate,
                SUM(runs) / SUM(plate_appearances) AS runs_rate,
                SUM(balls_in_play) / SUM(plate_appearances) AS balls_in_play_rate,
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
                averages.singles_rate * 1000 AS singles,
                averages.doubles_rate * 1000 AS doubles,
                averages.triples_rate * 1000 AS triples,
                averages.home_runs_rate * 1000 AS home_runs,
                averages.strikeouts_rate * 1000 AS strikeouts,
                averages.reached_on_errors_rate * 1000 AS reached_on_errors,
                averages.walks_rate * 1000 AS walks,
                averages.batting_outs_rate * 1000 AS batting_outs,
                averages.runs_rate * 1000 AS runs,
                averages.balls_in_play_rate * 1000 AS balls_in_play,
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
                this.reached_on_errors AS this_reached_on_errors,
                other.reached_on_errors AS other_reached_on_errors,
                this.walks AS this_walks,
                other.walks AS other_walks,
                this.batting_outs AS this_batting_outs,
                other.batting_outs AS other_batting_outs,
                this.runs AS this_runs,
                other.runs AS other_runs,
                this.balls_in_play AS this_balls_in_play,
                other.balls_in_play AS other_balls_in_play,
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
                this_singles / this_plate_appearances AS this_singles_rate,
                other_singles / other_plate_appearances AS other_singles_rate,
                this_doubles / this_plate_appearances AS this_doubles_rate,
                other_doubles / other_plate_appearances AS other_doubles_rate,
                this_triples / this_plate_appearances AS this_triples_rate,
                other_triples / other_plate_appearances AS other_triples_rate,
                this_home_runs / this_plate_appearances AS this_home_runs_rate,
                other_home_runs / other_plate_appearances AS other_home_runs_rate,
                this_strikeouts / this_plate_appearances AS this_strikeouts_rate,
                other_strikeouts / other_plate_appearances AS other_strikeouts_rate,
                this_reached_on_errors / this_plate_appearances AS this_reached_on_errors_rate,
                other_reached_on_errors / other_plate_appearances AS other_reached_on_errors_rate,
                this_walks / this_plate_appearances AS this_walks_rate,
                other_walks / other_plate_appearances AS other_walks_rate,
                this_batting_outs / this_plate_appearances AS this_batting_outs_rate,
                other_batting_outs / other_plate_appearances AS other_batting_outs_rate,
                this_runs / this_plate_appearances AS this_runs_rate,
                other_runs / other_plate_appearances AS other_runs_rate,
                this_balls_in_play / this_plate_appearances AS this_balls_in_play_rate,
                other_balls_in_play / other_plate_appearances AS other_balls_in_play_rate,
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
                SUM(this_singles_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_singles_rate,
                SUM(other_singles_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_singles_rate,
                avg_this_singles_rate
                / (1 - avg_this_singles_rate) AS this_singles_odds,
                avg_other_singles_rate
                / (1 - avg_other_singles_rate) AS other_singles_odds,
                this_singles_odds
                / other_singles_odds AS singles_odds_park_factor,
                avg_this_singles_rate / avg_other_singles_rate AS singles_rate_park_factor,
                SUM(this_doubles_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_doubles_rate,
                SUM(other_doubles_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_doubles_rate,
                avg_this_doubles_rate
                / (1 - avg_this_doubles_rate) AS this_doubles_odds,
                avg_other_doubles_rate
                / (1 - avg_other_doubles_rate) AS other_doubles_odds,
                this_doubles_odds
                / other_doubles_odds AS doubles_odds_park_factor,
                avg_this_doubles_rate / avg_other_doubles_rate AS doubles_rate_park_factor,
                SUM(this_triples_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_triples_rate,
                SUM(other_triples_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_triples_rate,
                avg_this_triples_rate
                / (1 - avg_this_triples_rate) AS this_triples_odds,
                avg_other_triples_rate
                / (1 - avg_other_triples_rate) AS other_triples_odds,
                this_triples_odds
                / other_triples_odds AS triples_odds_park_factor,
                avg_this_triples_rate / avg_other_triples_rate AS triples_rate_park_factor,
                SUM(this_home_runs_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_home_runs_rate,
                SUM(other_home_runs_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_home_runs_rate,
                avg_this_home_runs_rate
                / (1 - avg_this_home_runs_rate) AS this_home_runs_odds,
                avg_other_home_runs_rate
                / (1 - avg_other_home_runs_rate) AS other_home_runs_odds,
                this_home_runs_odds
                / other_home_runs_odds AS home_runs_odds_park_factor,
                avg_this_home_runs_rate / avg_other_home_runs_rate AS home_runs_rate_park_factor,
                SUM(this_strikeouts_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_strikeouts_rate,
                SUM(other_strikeouts_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_strikeouts_rate,
                avg_this_strikeouts_rate
                / (1 - avg_this_strikeouts_rate) AS this_strikeouts_odds,
                avg_other_strikeouts_rate
                / (1 - avg_other_strikeouts_rate) AS other_strikeouts_odds,
                this_strikeouts_odds
                / other_strikeouts_odds AS strikeouts_odds_park_factor,
                avg_this_strikeouts_rate / avg_other_strikeouts_rate AS strikeouts_rate_park_factor,
                SUM(this_reached_on_errors_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_reached_on_errors_rate,
                SUM(other_reached_on_errors_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_reached_on_errors_rate,
                avg_this_reached_on_errors_rate
                / (1 - avg_this_reached_on_errors_rate) AS this_reached_on_errors_odds,
                avg_other_reached_on_errors_rate
                / (1 - avg_other_reached_on_errors_rate) AS other_reached_on_errors_odds,
                this_reached_on_errors_odds
                / other_reached_on_errors_odds AS reached_on_errors_odds_park_factor,
                avg_this_reached_on_errors_rate / avg_other_reached_on_errors_rate AS reached_on_errors_rate_park_factor,
                SUM(this_walks_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_walks_rate,
                SUM(other_walks_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_walks_rate,
                avg_this_walks_rate
                / (1 - avg_this_walks_rate) AS this_walks_odds,
                avg_other_walks_rate
                / (1 - avg_other_walks_rate) AS other_walks_odds,
                this_walks_odds
                / other_walks_odds AS walks_odds_park_factor,
                avg_this_walks_rate / avg_other_walks_rate AS walks_rate_park_factor,
                SUM(this_batting_outs_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_batting_outs_rate,
                SUM(other_batting_outs_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_batting_outs_rate,
                avg_this_batting_outs_rate
                / (1 - avg_this_batting_outs_rate) AS this_batting_outs_odds,
                avg_other_batting_outs_rate
                / (1 - avg_other_batting_outs_rate) AS other_batting_outs_odds,
                this_batting_outs_odds
                / other_batting_outs_odds AS batting_outs_odds_park_factor,
                avg_this_batting_outs_rate / avg_other_batting_outs_rate AS batting_outs_rate_park_factor,
                SUM(this_runs_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_runs_rate,
                SUM(other_runs_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_runs_rate,
                avg_this_runs_rate
                / (1 - avg_this_runs_rate) AS this_runs_odds,
                avg_other_runs_rate
                / (1 - avg_other_runs_rate) AS other_runs_odds,
                this_runs_odds
                / other_runs_odds AS runs_odds_park_factor,
                avg_this_runs_rate / avg_other_runs_rate AS runs_rate_park_factor,
                SUM(this_balls_in_play_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_balls_in_play_rate,
                SUM(other_balls_in_play_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_balls_in_play_rate,
                avg_this_balls_in_play_rate
                / (1 - avg_this_balls_in_play_rate) AS this_balls_in_play_odds,
                avg_other_balls_in_play_rate
                / (1 - avg_other_balls_in_play_rate) AS other_balls_in_play_odds,
                this_balls_in_play_odds
                / other_balls_in_play_odds AS balls_in_play_odds_park_factor,
                avg_this_balls_in_play_rate / avg_other_balls_in_play_rate AS balls_in_play_rate_park_factor,
        FROM rate_calculation
        GROUP BY 1, 2, 3
    ),

    final AS (
        SELECT
            park_id,
            season,
            league,
            
            ROUND(sqrt_sample_size, 0) AS sqrt_sample_size,
                
                    ROUND(singles_odds_park_factor, 2) AS singles_park_factor,
                
                
                    ROUND(doubles_odds_park_factor, 2) AS doubles_park_factor,
                
                
                    ROUND(triples_odds_park_factor, 2) AS triples_park_factor,
                
                
                    ROUND(home_runs_odds_park_factor, 2) AS home_runs_park_factor,
                
                
                    ROUND(strikeouts_odds_park_factor, 2) AS strikeouts_park_factor,
                
                
                    ROUND(reached_on_errors_odds_park_factor, 2) AS reached_on_errors_park_factor,
                
                
                    ROUND(walks_odds_park_factor, 2) AS walks_park_factor,
                
                
                    ROUND(batting_outs_odds_park_factor, 2) AS batting_outs_park_factor,
                
                
                    ROUND(runs_odds_park_factor, 2) AS runs_park_factor,
                
                
                    ROUND(balls_in_play_odds_park_factor, 2) AS balls_in_play_park_factor,
                
        FROM weighted_average
    )

    SELECT * FROM final
