






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
        WHERE balls_in_play = 1
    ),

    lines_agg AS (
        SELECT
            states.park_id,
            states.season,
            states.league,
            states.batter_id,
            states.pitcher_id,
            ANY_VALUE(states.batter_hand) AS batter_hand,
                SUM(lines.balls_in_play)::INT AS balls_in_play,
                SUM(lines.hits)::INT AS hits,
                SUM(lines.singles)::INT AS singles,
                SUM(lines.doubles)::INT AS doubles,
                SUM(lines.triples)::INT AS triples,
                SUM(lines.reached_on_errors)::INT AS reached_on_errors,
                SUM(lines.batting_outs)::INT AS batting_outs,
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
            
                SUM(la.balls_in_play)::INT AS balls_in_play,
                SUM(la.hits)::INT AS hits,
                SUM(la.singles)::INT AS singles,
                SUM(la.doubles)::INT AS doubles,
                SUM(la.triples)::INT AS triples,
                SUM(la.reached_on_errors)::INT AS reached_on_errors,
                SUM(la.batting_outs)::INT AS batting_outs,
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
            
                SUM(hits) / SUM(balls_in_play) AS hits_rate,
                SUM(singles) / SUM(balls_in_play) AS singles_rate,
                SUM(doubles) / SUM(balls_in_play) AS doubles_rate,
                SUM(triples) / SUM(balls_in_play) AS triples_rate,
                SUM(reached_on_errors) / SUM(balls_in_play) AS reached_on_errors_rate,
                SUM(batting_outs) / SUM(balls_in_play) AS batting_outs_rate,
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
            
            1000 AS balls_in_play,
                averages.hits_rate * 1000 AS hits,
                averages.singles_rate * 1000 AS singles,
                averages.doubles_rate * 1000 AS doubles,
                averages.triples_rate * 1000 AS triples,
                averages.reached_on_errors_rate * 1000 AS reached_on_errors,
                averages.batting_outs_rate * 1000 AS batting_outs,
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
            
                this.balls_in_play AS this_balls_in_play,
                other.balls_in_play AS other_balls_in_play,
                this.hits AS this_hits,
                other.hits AS other_hits,
                this.singles AS this_singles,
                other.singles AS other_singles,
                this.doubles AS this_doubles,
                other.doubles AS other_doubles,
                this.triples AS this_triples,
                other.triples AS other_triples,
                this.reached_on_errors AS this_reached_on_errors,
                other.reached_on_errors AS other_reached_on_errors,
                this.batting_outs AS this_batting_outs,
                other.batting_outs AS other_batting_outs,
            SQRT(LEAST(this_balls_in_play, other_balls_in_play)) AS sample_size,
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
                this_hits / this_balls_in_play AS this_hits_rate,
                other_hits / other_balls_in_play AS other_hits_rate,
                this_singles / this_balls_in_play AS this_singles_rate,
                other_singles / other_balls_in_play AS other_singles_rate,
                this_doubles / this_balls_in_play AS this_doubles_rate,
                other_doubles / other_balls_in_play AS other_doubles_rate,
                this_triples / this_balls_in_play AS this_triples_rate,
                other_triples / other_balls_in_play AS other_triples_rate,
                this_reached_on_errors / this_balls_in_play AS this_reached_on_errors_rate,
                other_reached_on_errors / other_balls_in_play AS other_reached_on_errors_rate,
                this_batting_outs / this_balls_in_play AS this_batting_outs_rate,
                other_batting_outs / other_balls_in_play AS other_batting_outs_rate,
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
                SUM(this_hits_rate * sample_weight)
                / SUM(sample_weight) AS avg_this_hits_rate,
                SUM(other_hits_rate * sample_weight)
                / SUM(sample_weight) AS avg_other_hits_rate,
                avg_this_hits_rate
                / (1 - avg_this_hits_rate) AS this_hits_odds,
                avg_other_hits_rate
                / (1 - avg_other_hits_rate) AS other_hits_odds,
                this_hits_odds
                / other_hits_odds AS hits_odds_park_factor,
                avg_this_hits_rate / avg_other_hits_rate AS hits_rate_park_factor,
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
        FROM rate_calculation
        GROUP BY 1, 2, 3
    ),

    final AS (
        SELECT
            park_id,
            season,
            league,
            
            ROUND(sqrt_sample_size, 0) AS sqrt_sample_size,
                
                    ROUND(hits_odds_park_factor, 2) AS hits_park_factor,
                
                
                    ROUND(singles_odds_park_factor, 2) AS singles_park_factor,
                
                
                    ROUND(doubles_odds_park_factor, 2) AS doubles_park_factor,
                
                
                    ROUND(triples_odds_park_factor, 2) AS triples_park_factor,
                
                
                    ROUND(reached_on_errors_odds_park_factor, 2) AS reached_on_errors_park_factor,
                
                
                    ROUND(batting_outs_odds_park_factor, 2) AS batting_outs_park_factor,
                
        FROM weighted_average
    )

    SELECT * FROM final
