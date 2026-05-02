MODEL (
  name main_models.calc_park_factors_advanced,
  kind FULL,
  grain (park_id, season, league),
  columns (
    park_id PARK_ID,
    season SMALLINT,
    league VARCHAR,
    sqrt_sample_size DOUBLE,
    singles_park_factor DOUBLE,
    doubles_park_factor DOUBLE,
    triples_park_factor DOUBLE,
    home_runs_park_factor DOUBLE,
    strikeouts_park_factor DOUBLE,
    walks_park_factor DOUBLE,
    batting_outs_park_factor DOUBLE,
    runs_park_factor DOUBLE,
    balls_in_play_park_factor DOUBLE,
    trajectory_fly_ball_park_factor DOUBLE,
    trajectory_ground_ball_park_factor DOUBLE,
    trajectory_line_drive_park_factor DOUBLE,
    trajectory_pop_up_park_factor DOUBLE,
    trajectory_unknown_park_factor DOUBLE,
    batted_distance_infield_park_factor DOUBLE,
    batted_distance_outfield_park_factor DOUBLE,
    batted_distance_unknown_park_factor DOUBLE,
    batted_angle_left_park_factor DOUBLE,
    batted_angle_right_park_factor DOUBLE,
    batted_angle_middle_park_factor DOUBLE
  ),
  column_descriptions (
    park_id = @doc('park_id'),
    season = @doc('season'),
    league = @doc('league')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_calc_park_factors_advanced.parquet'
  ),
);







WITH unique_park_seasons AS (
    SELECT
        park_id,
        season,
        home_league AS league
    FROM main_models.game_start_info
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
        @EACH(@advanced_park_factor_stats(), s -> SUM(batting.@{s})::INT AS @s)
    FROM main_models.event_states_full AS states
    INNER JOIN main_models.event_offense_stats AS batting USING (event_key)
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
        @EACH(@advanced_park_factor_stats(), s -> SUM(@s) OVER (PARTITION BY park_id, batter_id, pitcher_id, league ORDER BY season RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)::INT AS @s)
    FROM batting_agg
),

averages AS MATERIALIZED (
    SELECT
        season,
        league,
        @EACH(@advanced_park_factor_rate_stats(), s -> SUM(@s) / SUM(plate_appearances) AS avg_@{s}_per_pa)
    FROM multi_year_range
    GROUP BY 1, 2
),

-- Give each park pair a batter-pitcher matchup at the league average
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
        @EACH(@advanced_park_factor_rate_stats(), s -> averages.avg_@{s}_per_pa * 1000::SMALLINT AS @s)
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
        @EACH(@advanced_park_factor_stats(), s -> this.@{s} AS this_@s),
        @EACH(@advanced_park_factor_stats(), s -> other.@{s} AS other_@s),
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
        @EACH(@advanced_park_factor_rate_stats(), s -> this_@s / this_plate_appearances AS this_@{s}_per_pa),
        @EACH(@advanced_park_factor_rate_stats(), s -> other_@s / other_plate_appearances AS other_@{s}_per_pa),
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
        @EACH(@advanced_park_factor_rate_stats(), s -> SUM(this_@{s}_per_pa * sample_weight) / SUM(sample_weight) AS avg_this_@{s}_per_pa),
        @EACH(@advanced_park_factor_rate_stats(), s -> SUM(other_@{s}_per_pa * sample_weight) / SUM(sample_weight) AS avg_other_@{s}_per_pa),
        @EACH(@advanced_park_factor_rate_stats(), s -> avg_this_@{s}_per_pa / (1 - avg_this_@{s}_per_pa) AS this_@{s}_odds),
        @EACH(@advanced_park_factor_rate_stats(), s -> avg_other_@{s}_per_pa / (1 - avg_other_@{s}_per_pa) AS other_@{s}_odds),
        @EACH(@advanced_park_factor_rate_stats(), s -> this_@{s}_odds / other_@{s}_odds AS @{s}_park_factor)
    FROM rate_calculation
    GROUP BY 1, 2, 3
),

final AS (
    SELECT
        park_id,
        season,
        league,
        ROUND(sqrt_sample_size, 0) AS sqrt_sample_size,
        @EACH(@advanced_park_factor_rate_stats(), s -> ROUND(@{s}_park_factor, 2) AS @{s}_park_factor)
    FROM weighted_average
)

SELECT * FROM final
