MODEL (
  name main_models.ngl_stats_only_games,
  kind FULL,
  description 'Negro Leagues games that Retrosheet records only in negroleagues.zip, with no event or box file: one row per game, in the shape game_start_info and game_results take from stg_gamelog. Starting pitchers come from the pitching lines (p_gs). Line scores are NULL; the runs by inning are in stg_ngl_teams.',
  grain (game_id),
  audits (
    not_null(columns := (game_id, date, home_team_id, away_team_id)),
    unique_values(columns := (game_id))
  ),
);

WITH starters AS (
    SELECT
        game_id,
        ANY_VALUE(player_id) FILTER (WHERE is_home) AS home_starting_pitcher_id,
        ANY_VALUE(player_id) FILTER (WHERE NOT is_home) AS away_starting_pitcher_id,
    FROM main_models.stg_ngl_pitching
    WHERE p_gs = 1
    GROUP BY game_id
),

games AS (
    SELECT
        g.game_id,
        g.date,
        EXTRACT(YEAR FROM g.date)::INT2 AS season,
        g.hometeam::TEAM_ID AS home_team_id,
        g.visteam::TEAM_ID AS away_team_id,
        CASE g.number
            WHEN 0 THEN 'SingleGame'
            WHEN 1 THEN 'DoubleHeaderGame1'
            WHEN 2 THEN 'DoubleHeaderGame2'
            WHEN 3 THEN 'DoubleHeaderGame3'
        END::DOUBLEHEADER_STATUS AS doubleheader_status,
        CASE g.daynight WHEN 'day' THEN 'Day' WHEN 'night' THEN 'Night' ELSE 'Unknown' END::TIME_OF_DAY
            AS time_of_day,
        CASE g.gametype
            WHEN 'regular' THEN 'RegularSeason'
            WHEN 'exhibition' THEN 'Exhibition'
            WHEN 'allstar' THEN 'AllStarGame'
            WHEN 'championship' THEN 'OtherChampionship'
            WHEN 'lcs' THEN 'LeagueChampionshipSeries'
            WHEN 'divisionseries' THEN 'DivisionSeries'
            WHEN 'playoff' THEN 'TiebreakerPlayoff'
        END::GAME_TYPE AS game_type,
        CASE WHEN g.htbf = 1 THEN 'Home' ELSE 'Away' END::SIDE AS bat_first_side,
        g.site::PARK_ID AS park_id,
        TRY_CAST(g.attendance AS UINTEGER) AS attendance,
        COALESCE(g.usedh, FALSE) AS use_dh,
        g.umphome AS umpire_home_id,
        g.ump1b AS umpire_first_id,
        g.ump2b AS umpire_second_id,
        g.ump3b AS umpire_third_id,
        starters.home_starting_pitcher_id::PLAYER_ID AS home_starting_pitcher_id,
        starters.away_starting_pitcher_id::PLAYER_ID AS away_starting_pitcher_id,
        CASE WHEN g.timeofgame > 0 THEN g.timeofgame END::INT2 AS duration_minutes,
        g.hruns::UTINYINT AS home_runs_scored,
        g.vruns::UTINYINT AS away_runs_scored,
        NULL::VARCHAR AS home_line_score,
        NULL::VARCHAR AS away_line_score,
        g.wp AS winning_pitcher_id,
        g.lp AS losing_pitcher_id,
        g.save AS save_pitcher_id,
        'NegroLeaguesStats' AS source_type,
    FROM main_models.stg_ngl_games AS g
    LEFT JOIN starters USING (game_id)
    WHERE g.game_id NOT IN (SELECT game_id FROM main_models.stg_games)
        AND g.game_id NOT IN (SELECT game_id FROM main_models.stg_gamelog)
)

SELECT * FROM games
