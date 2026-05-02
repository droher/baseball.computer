MODEL (
  name main_models.standings,
  kind FULL,
  description 'Daily snapshots of team standings.',
  grain (season, date, team_id),
  columns (
    season BIGINT,
    date DATE,
    league VARCHAR,
    division VARCHAR,
    team_id TEAM_ID,
    team_name VARCHAR,
    wins HUGEINT,
    losses HUGEINT,
    win_percentage DOUBLE,
    games_behind DOUBLE,
    pythagorean_win_percentage DOUBLE,
    average_run_differential DOUBLE,
    win_streak_length BIGINT,
    loss_streak_length BIGINT,
    runs_scored HUGEINT,
    runs_allowed HUGEINT,
    home_wins HUGEINT,
    home_losses HUGEINT,
    away_wins HUGEINT,
    away_losses HUGEINT,
    interleague_wins HUGEINT,
    interleague_losses HUGEINT,
    east_wins HUGEINT,
    east_losses HUGEINT,
    central_wins HUGEINT,
    central_losses HUGEINT,
    west_wins HUGEINT,
    west_losses HUGEINT,
    one_run_wins HUGEINT,
    one_run_losses HUGEINT,
    last_10_wins HUGEINT,
    last_10_losses HUGEINT
  ),
  column_descriptions (
    season = @doc('season'),
    date = @doc('date'),
    league = @doc('league'),
    division = @doc('division'),
    team_id = @doc('team_id'),
    team_name = @doc('team_name'),
    wins = 'Number of games won by the team to date.',
    losses = 'Number of games lost by the team to date.',
    win_percentage = 'Percentage of games won by the team to date.',
    games_behind = 'Number of games the team is behind the division leader to date.',
    pythagorean_win_percentage = 'Expected winning percentage based on runs scored and runs allowed to date. Invented by Bill James, pythagorean expectation is probably the most fundamental sabermetric concept because of how clearly it connects the process of scoring runs to the outcome of winning games. Ironically, ''pythagorean'' ended up applying much better as a descriptor of its centrality to the discipline than it did as a means of describing the formula itself, which has nothing to do with the Pythagorean theorem.',
    average_run_differential = 'Average per-game difference in runs scored and runs allowed.',
    win_streak_length = 'Number of consecutive games won by the team to date.',
    loss_streak_length = 'Number of consecutive games lost by the team to date.',
    runs_scored = 'Number of runs scored by the team to date.',
    runs_allowed = 'Number of runs allowed by the team to date.',
    home_wins = 'Number of games won by the team at home to date.',
    home_losses = 'Number of games lost by the team at home to date.',
    away_wins = 'Number of games won by the team on the road to date.',
    away_losses = 'Number of games lost by the team on the road to date.',
    interleague_wins = 'Number of games won by the team in interleague play to date.',
    interleague_losses = 'Number of games lost by the team in interleague play to date.',
    east_wins = 'Number of games won by the team against eastern division opponents within their league to date.',
    east_losses = 'Number of games lost by the team against eastern division opponents within their league to date.',
    central_wins = 'Number of games won by the team against central division opponents within their league to date.',
    central_losses = 'Number of games lost by the team against central division opponents within their league to date.',
    west_wins = 'Number of games won by the team against western division opponents within their league to date.',
    west_losses = 'Number of games lost by the team against western division opponents within their league to date.',
    one_run_wins = 'Number of games won by the team by one run to date.',
    one_run_losses = 'Number of games lost by the team by one run to date.',
    last_10_wins = 'Number of games won by the team in their last 10 games.',
    last_10_losses = 'Number of games lost by the team in their last 10 games.'
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_standings.parquet'
  ),
);







WITH date_spine AS MATERIALIZED (
    SELECT UNNEST(GENERATE_SERIES(
        (SELECT MIN(date) FROM main_models.game_start_info),
        (SELECT MAX(date) FROM main_models.game_start_info) + 1,
        INTERVAL '1 day'
    ))::DATE AS date,
    EXTRACT(YEAR FROM date) AS season
),

season_date_bounds AS (
    SELECT
        season,
        MIN(game_finish_date) - 1 AS season_start_date,
        MAX(game_finish_date) + 1 AS season_end_date
    FROM main_models.game_results
    WHERE game_type = 'RegularSeason'
    GROUP BY 1
),

team_spine AS (
    SELECT
        season,
        team_id,
        -- TODO: Test one league per team per regular season
        ANY_VALUE(league) AS league,
        ANY_VALUE(team_name) AS team_name,
        ANY_VALUE(division) AS division,
    FROM main_models.team_game_start_info
    WHERE game_type = 'RegularSeason'
    GROUP BY 1, 2
),

standings_spine AS (
    SELECT
        d.season,
        d.date,
        t.* EXCLUDE (season)
    FROM date_spine AS d
    INNER JOIN season_date_bounds AS b USING (season)
    LEFT JOIN team_spine AS t USING (season)
    WHERE d.date BETWEEN b.season_start_date AND b.season_end_date
),

crossed AS (
    SELECT DISTINCT ON (s.date, s.season, s.league, s.team_id)
        s.date,
        s.season,
        s.league,
        s.division,
        s.team_id,
        s.team_name,
        COALESCE(SUM(r.wins) OVER team_window, 0) AS wins,
        COALESCE(SUM(r.losses) OVER team_window, 0) AS losses,
        -- Take the streak count from the last game of the day
        COALESCE(LAST(r.win_streak_length IGNORE NULLS) OVER team_window, 0) AS win_streak_length,
        COALESCE(LAST(r.loss_streak_length IGNORE NULLS) OVER team_window, 0) AS loss_streak_length,
        COALESCE(SUM(r.runs_scored) OVER team_window, 0) AS runs_scored,
        COALESCE(SUM(r.runs_allowed) OVER team_window, 0) AS runs_allowed,
        COALESCE(SUM(r.home_wins) OVER team_window, 0) AS home_wins,
        COALESCE(SUM(r.home_losses) OVER team_window, 0) AS home_losses,
        COALESCE(SUM(r.away_wins) OVER team_window, 0) AS away_wins,
        COALESCE(SUM(r.away_losses) OVER team_window, 0) AS away_losses,
        COALESCE(SUM(r.interleague_wins) OVER team_window, 0) AS interleague_wins,
        COALESCE(SUM(r.interleague_losses) OVER team_window, 0) AS interleague_losses,
        COALESCE(SUM(r.east_wins) OVER team_window, 0) AS east_wins,
        COALESCE(SUM(r.east_losses) OVER team_window, 0) AS east_losses,
        COALESCE(SUM(r.central_wins) OVER team_window, 0) AS central_wins,
        COALESCE(SUM(r.central_losses) OVER team_window, 0) AS central_losses,
        COALESCE(SUM(r.west_wins) OVER team_window, 0) AS west_wins,
        COALESCE(SUM(r.west_losses) OVER team_window, 0) AS west_losses,
        COALESCE(SUM(r.one_run_wins) OVER team_window, 0) AS one_run_wins,
        COALESCE(SUM(r.one_run_losses) OVER team_window, 0) AS one_run_losses,
        COALESCE(SUM(r.wins) OVER last_10_window, 0) AS last_10_wins,
        COALESCE(SUM(r.losses) OVER last_10_window, 0) AS last_10_losses,
    FROM standings_spine AS s
    LEFT JOIN main_models.team_game_results AS r
        ON r.season = s.season
            AND r.team_id = s.team_id
            AND r.game_finish_date = s.date
            AND r.game_type = 'RegularSeason'
    WINDOW
        team_window AS (
            PARTITION BY s.season, s.team_id
            ORDER BY s.date, r.season_game_number
        ),
        last_10_window AS (
            PARTITION BY s.season, s.team_id
            ORDER BY r.season_game_number
            RANGE BETWEEN 9 PRECEDING AND CURRENT ROW
        )
),

final AS (
    SELECT
        -- Put the calc cols further up
        season,
        date,
        league,
        division,
        team_id,
        team_name,
        wins,
        losses,
        wins / (wins + losses) AS win_percentage,
        GREATEST(
            0,
            (FIRST(wins) OVER division_snapshot - FIRST(losses) OVER division_snapshot - (wins - losses)) / 2
        ) AS games_behind,
        runs_scored ^ 1.85 / (runs_scored ^ 1.85 + runs_allowed ^ 1.85) AS pythagorean_win_percentage,
        (runs_scored - runs_allowed) / (wins + losses) AS average_run_differential,
        * EXCLUDE (date, season, league, division, team_id, team_name, wins, losses)
    FROM crossed
    WINDOW
        division_snapshot AS (
            PARTITION BY season, league, division, date
            ORDER BY wins - losses DESC
        )
)

SELECT * FROM final
