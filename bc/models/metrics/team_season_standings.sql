MODEL (
  name main_models.team_season_standings,
  kind FULL,
  description 'Final regular-season standings, one row per team and season. Retrosheet rows total the team''s regular-season games in team_game_results, as the last daily row of standings does. In Negro Leagues seasons that nlb_season_sources assigns to Lahman, Negro Leagues teams come from Lahman''s teams table instead, since Retrosheet has only a few of their games; teams Retrosheet does not have keep their Lahman team id (see stg_databank_teams). For those seasons this table, not standings, is the season-level record. A Retrosheet row with no league takes Lahman''s league for the team-season when Lahman gives exactly one.',
  grain (season, team_id),
  columns (
    season SMALLINT,
    team_id VARCHAR,
    league VARCHAR,
    is_major_league BOOLEAN,
    division VARCHAR,
    games USMALLINT,
    wins USMALLINT,
    losses USMALLINT,
    ties USMALLINT,
    win_percentage DOUBLE,
    league_rank UTINYINT,
    won_pennant BOOLEAN,
    standings_source VARCHAR
  ),
  column_descriptions (
    season = @doc('season'),
    team_id = @doc('team_id'),
    league = @doc('league'),
    is_major_league = 'Whether the league counts as a major league that season, per seed_leagues. FALSE for teams with no league.',
    division = 'Division, when the league had divisions.',
    games = 'Regular-season games.',
    wins = 'Regular-season wins.',
    losses = 'Regular-season losses.',
    ties = 'Regular-season games that were neither won nor lost.',
    win_percentage = 'wins / (wins + losses).',
    league_rank = 'Final place within the league, or within the division when the league had divisions. Retrosheet rows rank by win percentage, with equal percentages sharing a place; Lahman rows keep Lahman''s rank.',
    won_pennant = 'Lahman''s pennant flag (lg_win), which can differ from first place. NULL for Retrosheet rows.',
    standings_source = 'Retrosheet when the row totals Retrosheet games, Databank when it comes from Lahman''s teams table.'
  ),
  audits (
    not_null(columns := (season, team_id, games, wins, losses, standings_source, is_major_league)),
    relationships(column := league, to_model := main_seeds.seed_leagues, to_column := league_id),
    unique_grain(columns := (season, team_id)),
    valid_baseball_season(column := season),
    relationships(column := team_id, to_model := main_seeds.seed_franchises, to_column := team_id),
    accepted_values(column := standings_source, is_in := ('Retrosheet', 'Databank')),
    bounded_range(column := wins + losses, min_v := 0, max_v := games)
  ),
);

WITH databank_sourced_nlb_seasons AS (
    SELECT season
    FROM main_models.nlb_season_sources
    WHERE season_source = 'Databank'
),

team_leagues AS (
    SELECT
        season,
        team_id,
        ANY_VALUE(league) AS league,
        ANY_VALUE(division) AS division,
        BOOL_OR(is_negro_leagues) AS is_negro_leagues
    FROM main_models.team_game_start_info
    WHERE game_type = 'RegularSeason'
    GROUP BY 1, 2
),

databank_team_leagues AS (
    SELECT
        season,
        team_id,
        ANY_VALUE(league_id) AS league
    FROM main_models.stg_databank_teams
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT league_id) = 1
),

retrosheet AS (
    SELECT
        results.season::SMALLINT AS season,
        results.team_id::VARCHAR AS team_id,
        COUNT(*)::USMALLINT AS games,
        SUM(results.wins)::USMALLINT AS wins,
        SUM(results.losses)::USMALLINT AS losses
    FROM main_models.team_game_results AS results
    WHERE results.game_type = 'RegularSeason'
        AND results.game_finish_date IS NOT NULL
    GROUP BY 1, 2
),

retrosheet_standings AS (
    SELECT
        retrosheet.*,
        COALESCE(team_leagues.league, databank_team_leagues.league) AS league,
        team_leagues.division,
        NULL::UTINYINT AS lahman_rank,
        NULL::BOOLEAN AS won_pennant,
        'Retrosheet' AS standings_source
    FROM retrosheet
    INNER JOIN team_leagues
        ON retrosheet.season = team_leagues.season
            AND retrosheet.team_id = team_leagues.team_id::VARCHAR
    LEFT JOIN databank_team_leagues
        ON retrosheet.season = databank_team_leagues.season
            AND retrosheet.team_id = databank_team_leagues.team_id
    WHERE NOT (
        team_leagues.is_negro_leagues
        AND retrosheet.season IN (SELECT season FROM databank_sourced_nlb_seasons)
    )
),

databank_standings AS (
    SELECT
        season,
        team_id,
        games,
        wins,
        losses,
        league_id AS league,
        NULL::VARCHAR AS division,
        league_rank AS lahman_rank,
        won_pennant,
        'Databank' AS standings_source
    FROM main_models.stg_databank_teams
    WHERE is_negro_leagues
        AND season IN (SELECT season FROM databank_sourced_nlb_seasons)
),

unioned AS (
    SELECT * FROM retrosheet_standings
    UNION ALL BY NAME
    SELECT * FROM databank_standings
),

final AS (
    SELECT
        unioned.season,
        unioned.team_id,
        unioned.league,
        COALESCE(
            unioned.season BETWEEN leagues.major_league_first_season
                AND COALESCE(leagues.major_league_last_season, unioned.season),
            FALSE
        ) AS is_major_league,
        unioned.division,
        games,
        wins,
        losses,
        (games - wins - losses)::USMALLINT AS ties,
        wins / NULLIF(wins + losses, 0) AS win_percentage,
        COALESCE(
            lahman_rank,
            RANK() OVER (
                PARTITION BY season, league, division, standings_source
                ORDER BY wins / NULLIF(wins + losses, 0) DESC NULLS LAST
            )
        )::UTINYINT AS league_rank,
        won_pennant,
        standings_source
    FROM unioned
    LEFT JOIN main_seeds.seed_leagues AS leagues
        ON unioned.league = leagues.league_id
)

SELECT * FROM final
