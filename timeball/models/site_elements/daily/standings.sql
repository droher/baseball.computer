{{
  config(
    materialized = 'table',
    )
}}
WITH date_spine AS MATERIALIZED (
    SELECT UNNEST(GENERATE_SERIES(
        (SELECT MIN(date) FROM {{ ref('game_start_info') }}),
        (SELECT MAX(date) FROM {{ ref('game_start_info') }}) + 1,
        INTERVAL '1 day'
    ))::DATE AS date,
    EXTRACT(YEAR FROM date) AS season
),

season_date_bounds AS (
    SELECT
        season,
        MIN(game_finish_date) - 1 AS season_start_date,
        MAX(game_finish_date) + 1 AS season_end_date
    FROM {{ ref('game_results') }}
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
    FROM {{ ref('team_game_start_info') }}
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

results AS (
    SELECT
        r.season,
        -- Add one so that data is exclusive of the game that day
        r.game_finish_date + 1 AS date,
        r.team_id,
        tsi.season_game_number,
        r.wins,
        r.losses,
        r.runs_scored,
        r.runs_allowed,
        CASE WHEN tsi.team_side = 'Home' THEN r.wins ELSE 0 END AS home_wins,
        CASE WHEN tsi.team_side = 'Home' THEN r.losses ELSE 0 END AS home_losses,
        CASE WHEN tsi.team_side = 'Away' THEN r.wins ELSE 0 END AS away_wins,
        CASE WHEN tsi.team_side = 'Away' THEN r.losses ELSE 0 END AS away_losses,
        CASE WHEN tsi.is_interleague THEN r.wins ELSE 0 END AS interleague_wins,
        CASE WHEN tsi.is_interleague THEN r.losses ELSE 0 END AS interleague_losses,
        CASE WHEN NOT tsi.is_interleague AND tsi.opponent_division = 'E' THEN r.wins ELSE 0 END AS east_wins,
        CASE WHEN NOT tsi.is_interleague AND tsi.opponent_division = 'E' THEN r.losses ELSE 0 END AS east_losses,
        CASE WHEN NOT tsi.is_interleague AND tsi.opponent_division = 'C' THEN r.wins ELSE 0 END AS central_wins,
        CASE WHEN NOT tsi.is_interleague AND tsi.opponent_division = 'C' THEN r.losses ELSE 0 END AS central_losses,
        CASE WHEN NOT tsi.is_interleague AND tsi.opponent_division = 'W' THEN r.wins ELSE 0 END AS west_wins,
        CASE WHEN NOT tsi.is_interleague AND tsi.opponent_division = 'W' THEN r.losses ELSE 0 END AS west_losses,
        CASE WHEN ABS(r.runs_scored::INT - r.runs_allowed) = 1 THEN r.wins ELSE 0 END AS one_run_wins,
        CASE WHEN ABS(r.runs_scored::INT - r.runs_allowed) = 1 THEN r.losses ELSE 0 END AS one_run_losses,
    FROM {{ ref('team_game_results') }} AS r
    INNER JOIN {{ ref('team_game_start_info') }} AS tsi USING (game_id, team_id)
    WHERE tsi.game_type = 'RegularSeason'
),

crossed AS (
    SELECT
        s.date,
        s.season,
        s.league,
        s.division,
        team_id,
        s.team_name,
        COALESCE(SUM(r.wins) OVER team_window, 0) AS wins,
        COALESCE(SUM(r.losses) OVER team_window, 0) AS losses,
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
    LEFT JOIN results AS r USING (season, date, team_id)
    WINDOW
        team_window AS (
            PARTITION BY s.season, s.team_id
            ORDER BY s.date
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
        * EXCLUDE (date, season, league, division, team_name, wins, losses)
    FROM crossed
    WINDOW division_snapshot AS (
        PARTITION BY season, league, division, date
        ORDER BY wins - losses DESC
    )
)

SELECT * FROM final
WHERE season = 1998
AND league = 'AL'
AND division = 'W'
ORDER BY date, win_percentage DESC