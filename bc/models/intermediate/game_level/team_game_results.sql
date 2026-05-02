MODEL (
  name main_models.team_game_results,
  kind FULL,
  description 'A version of `game_results` that includes one row for each team in each game. Also includes additional statistics (traditional box score stats) for games that have that information available.',
  grain (game_id, team_id),
  columns (
    season SMALLINT,
    game_id VARCHAR,
    game_finish_date DATE,
    team_id TEAM_ID,
    game_type GAME_TYPE,
    team_side SIDE,
    league VARCHAR,
    division VARCHAR,
    opponent_league VARCHAR,
    opponent_division VARCHAR,
    season_game_number BIGINT,
    is_interleague BOOLEAN,
    wins INTEGER,
    losses INTEGER,
    runs_scored UTINYINT,
    runs_allowed UTINYINT,
    hits USMALLINT,
    errors UTINYINT,
    left_on_base USMALLINT,
    at_bats USMALLINT,
    doubles USMALLINT,
    triples USMALLINT,
    home_runs USMALLINT,
    runs_batted_in USMALLINT,
    sacrifice_hits USMALLINT,
    sacrifice_flies USMALLINT,
    hit_by_pitches USMALLINT,
    walks USMALLINT,
    intentional_walks USMALLINT,
    strikeouts USMALLINT,
    stolen_bases USMALLINT,
    caught_stealing USMALLINT,
    grounded_into_double_plays USMALLINT,
    reached_on_interferences USMALLINT,
    innings_pitched DECIMAL(6,4),
    individual_earned_runs_allowed USMALLINT,
    earned_runs_allowed UTINYINT,
    wild_pitches USMALLINT,
    balks USMALLINT,
    putouts UTINYINT,
    assists UTINYINT,
    passed_balls UTINYINT,
    double_plays_turned UTINYINT,
    triple_plays_turned UTINYINT,
    opponent_team_id TEAM_ID,
    opponent_runs USMALLINT,
    opponent_hits USMALLINT,
    opponent_errors UTINYINT,
    opponent_left_on_base USMALLINT,
    opponent_at_bats USMALLINT,
    opponent_doubles USMALLINT,
    opponent_triples USMALLINT,
    opponent_home_runs USMALLINT,
    opponent_runs_batted_in USMALLINT,
    opponent_sacrifice_hits USMALLINT,
    opponent_sacrifice_flies USMALLINT,
    opponent_hit_by_pitches USMALLINT,
    opponent_walks USMALLINT,
    opponent_intentional_walks USMALLINT,
    opponent_strikeouts USMALLINT,
    opponent_stolen_bases USMALLINT,
    opponent_caught_stealing USMALLINT,
    opponent_grounded_into_double_plays USMALLINT,
    opponent_reached_on_interferences USMALLINT,
    opponent_innings_pitched DECIMAL(6,4),
    opponent_individual_earned_runs_allowed USMALLINT,
    opponent_earned_runs_allowed UTINYINT,
    opponent_wild_pitches USMALLINT,
    opponent_balks USMALLINT,
    opponent_putouts UTINYINT,
    opponent_assists UTINYINT,
    opponent_passed_balls UTINYINT,
    opponent_double_plays UTINYINT,
    opponent_triple_plays UTINYINT,
    home_wins INTEGER,
    home_losses INTEGER,
    away_wins INTEGER,
    away_losses INTEGER,
    interleague_wins INTEGER,
    interleague_losses INTEGER,
    east_wins INTEGER,
    east_losses INTEGER,
    central_wins INTEGER,
    central_losses INTEGER,
    west_wins INTEGER,
    west_losses INTEGER,
    one_run_wins INTEGER,
    one_run_losses INTEGER,
    win_streak_id BIGINT,
    loss_streak_id BIGINT,
    win_streak_length BIGINT,
    loss_streak_length BIGINT
  ),
  column_descriptions (
    season = @doc('season'),
    game_id = @doc('game_id'),
    team_id = @doc('team_id'),
    game_type = @doc('game_type'),
    league = @doc('league'),
    division = @doc('division'),
    is_interleague = @doc('is_interleague'),
    wins = @doc('wins'),
    losses = @doc('losses'),
    hits = @doc('hits'),
    errors = @doc('errors'),
    left_on_base = @doc('left_on_base'),
    at_bats = @doc('at_bats'),
    doubles = @doc('doubles'),
    triples = @doc('triples'),
    home_runs = @doc('home_runs'),
    runs_batted_in = @doc('runs_batted_in'),
    sacrifice_hits = @doc('sacrifice_hits'),
    sacrifice_flies = @doc('sacrifice_flies'),
    hit_by_pitches = @doc('hit_by_pitches'),
    walks = @doc('walks'),
    intentional_walks = @doc('intentional_walks'),
    strikeouts = @doc('strikeouts'),
    stolen_bases = @doc('stolen_bases'),
    caught_stealing = @doc('caught_stealing'),
    grounded_into_double_plays = @doc('grounded_into_double_plays'),
    reached_on_interferences = @doc('reached_on_interferences'),
    innings_pitched = @doc('innings_pitched'),
    wild_pitches = @doc('wild_pitches'),
    balks = @doc('balks'),
    putouts = @doc('putouts'),
    assists = @doc('assists'),
    passed_balls = @doc('passed_balls')
  ),
  audits (
    not_null(columns := (game_id, team_id)),
    unique_grain(columns := (game_id, team_id)),
    valid_baseball_season(column := season),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id),
    relationships(column := team_id, to_model := main_seeds.seed_franchises, to_column := team_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_team_game_results.parquet'
  ),
);







WITH joined AS (
    SELECT
        s.season,
        s.game_id,
        r.game_finish_date,
        s.team_id,
        s.game_type,
        s.team_side,
        s.league,
        s.division,
        s.opponent_league,
        s.opponent_division,
        s.season_game_number,
        s.is_interleague,
        -- Traditional top-level stats
        CASE WHEN s.team_id = r.winning_team_id THEN 1 ELSE 0 END AS wins,
        CASE WHEN s.team_id = r.losing_team_id THEN 1 ELSE 0 END AS losses,
        CASE WHEN s.team_side = 'Home' THEN r.home_runs_scored ELSE r.away_runs_scored END AS runs_scored,
        CASE WHEN s.team_side = 'Home' THEN r.away_runs_scored ELSE r.home_runs_scored END AS runs_allowed,
        o.hits,
        f.errors,
        o.left_on_base,
        -- Traditional box score stats
        o.at_bats,
        o.doubles,
        o.triples,
        o.home_runs,
        o.runs_batted_in,
        o.sacrifice_hits,
        o.sacrifice_flies,
        o.hit_by_pitches,
        o.walks,
        o.intentional_walks,
        o.strikeouts,
        o.stolen_bases,
        o.caught_stealing,
        o.grounded_into_double_plays,
        o.reached_on_interferences,
        p.innings_pitched,
        p.individual_earned_runs AS individual_earned_runs_allowed,
        p.earned_runs AS earned_runs_allowed,
        p.wild_pitches,
        p.balks,
        f.putouts,
        f.assists,
        f.passed_balls,
        f.double_plays AS double_plays_turned,
        f.triple_plays AS triple_plays_turned,
        -- Same for opponent
        o_opp.team_id AS opponent_team_id,
        o_opp.runs AS opponent_runs,
        o_opp.hits AS opponent_hits,
        f_opp.errors AS opponent_errors,
        o_opp.left_on_base AS opponent_left_on_base,
        o_opp.at_bats AS opponent_at_bats,
        o_opp.doubles AS opponent_doubles,
        o_opp.triples AS opponent_triples,
        o_opp.home_runs AS opponent_home_runs,
        o_opp.runs_batted_in AS opponent_runs_batted_in,
        o_opp.sacrifice_hits AS opponent_sacrifice_hits,
        o_opp.sacrifice_flies AS opponent_sacrifice_flies,
        o_opp.hit_by_pitches AS opponent_hit_by_pitches,
        o_opp.walks AS opponent_walks,
        o_opp.intentional_walks AS opponent_intentional_walks,
        o_opp.strikeouts AS opponent_strikeouts,
        o_opp.stolen_bases AS opponent_stolen_bases,
        o_opp.caught_stealing AS opponent_caught_stealing,
        o_opp.grounded_into_double_plays AS opponent_grounded_into_double_plays,
        o_opp.reached_on_interferences AS opponent_reached_on_interferences,
        p_opp.innings_pitched AS opponent_innings_pitched,
        p_opp.individual_earned_runs AS opponent_individual_earned_runs_allowed,
        p_opp.earned_runs AS opponent_earned_runs_allowed,
        p_opp.wild_pitches AS opponent_wild_pitches,
        p_opp.balks AS opponent_balks,
        f_opp.putouts AS opponent_putouts,
        f_opp.assists AS opponent_assists,
        f_opp.passed_balls AS opponent_passed_balls,
        f_opp.double_plays AS opponent_double_plays,
        f_opp.triple_plays AS opponent_triple_plays,
    FROM main_models.team_game_start_info AS s
    INNER JOIN main_models.game_results AS r USING (game_id)
    LEFT JOIN main_models.team_game_offense_stats AS o USING (game_id, team_id)
    LEFT JOIN main_models.team_game_fielding_stats AS f USING (game_id, team_id)
    LEFT JOIN main_models.team_game_pitching_stats AS p USING (game_id, team_id)
    LEFT JOIN main_models.team_game_offense_stats AS o_opp
        ON o_opp.game_id = o.game_id AND o_opp.team_id != o.team_id
    LEFT JOIN main_models.team_game_fielding_stats AS f_opp
        ON f_opp.game_id = f.game_id AND f_opp.team_id != f.team_id
    LEFT JOIN main_models.team_game_pitching_stats AS p_opp
        ON p_opp.game_id = p.game_id AND p_opp.team_id != p.team_id
),

enriched AS (
    SELECT
        *,
        CASE WHEN team_side = 'Home' THEN wins ELSE 0 END AS home_wins,
        CASE WHEN team_side = 'Home' THEN losses ELSE 0 END AS home_losses,
        CASE WHEN team_side = 'Away' THEN wins ELSE 0 END AS away_wins,
        CASE WHEN team_side = 'Away' THEN losses ELSE 0 END AS away_losses,
        CASE WHEN is_interleague THEN wins ELSE 0 END AS interleague_wins,
        CASE WHEN is_interleague THEN losses ELSE 0 END AS interleague_losses,
        CASE WHEN NOT is_interleague AND opponent_division = 'E' THEN wins ELSE 0 END AS east_wins,
        CASE WHEN NOT is_interleague AND opponent_division = 'E' THEN losses ELSE 0 END AS east_losses,
        CASE WHEN NOT is_interleague AND opponent_division = 'C' THEN wins ELSE 0 END AS central_wins,
        CASE WHEN NOT is_interleague AND opponent_division = 'C' THEN losses ELSE 0 END AS central_losses,
        CASE WHEN NOT is_interleague AND opponent_division = 'W' THEN wins ELSE 0 END AS west_wins,
        CASE WHEN NOT is_interleague AND opponent_division = 'W' THEN losses ELSE 0 END AS west_losses,
        CASE WHEN ABS(runs_scored::INT - runs_allowed) = 1 THEN wins ELSE 0 END AS one_run_wins,
        CASE WHEN ABS(runs_scored::INT - runs_allowed) = 1 THEN losses ELSE 0 END AS one_run_losses,
        CASE WHEN wins = 1 AND NOT LAG(wins = 1, 1, FALSE) OVER streak_window
                THEN season_game_number
        END AS win_streak_id,
        CASE WHEN losses = 1 AND NOT LAG(losses = 1, 1, FALSE) OVER streak_window
                THEN season_game_number
        END AS loss_streak_id,
    FROM joined
    WINDOW streak_window AS (
        PARTITION BY season, team_id, game_type
        ORDER BY game_finish_date, season_game_number
    )
),

streak_calc_continued AS (
    SELECT
        * REPLACE (
            CASE WHEN wins = 1 AND win_streak_id IS NULL
                    THEN LAG(win_streak_id IGNORE NULLS) OVER streak_window
                ELSE win_streak_id
            END AS win_streak_id,
            CASE WHEN losses = 1 AND loss_streak_id IS NULL
                    THEN LAG(loss_streak_id IGNORE NULLS) OVER streak_window
                ELSE loss_streak_id
            END AS loss_streak_id
        )
    FROM enriched
    WINDOW streak_window AS (
        PARTITION BY season, team_id, game_type
        ORDER BY game_finish_date, season_game_number
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),

final AS (
    SELECT
        *,
        CASE WHEN wins = 1 THEN COUNT(*) OVER win_streak_window ELSE 0 END AS win_streak_length,
        CASE WHEN losses = 1 THEN COUNT(*) OVER loss_streak_window ELSE 0 END AS loss_streak_length,
    FROM streak_calc_continued
    WINDOW
        win_streak_window AS (
            PARTITION BY season, team_id, game_type, win_streak_id
            ORDER BY game_finish_date, season_game_number
        ),
        loss_streak_window AS (
            PARTITION BY season, team_id, game_type, loss_streak_id
            ORDER BY game_finish_date, season_game_number
        )
)

SELECT * FROM final
