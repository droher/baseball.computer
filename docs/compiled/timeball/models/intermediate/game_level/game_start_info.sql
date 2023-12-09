
WITH games AS (
    SELECT
        g.game_id,
        g.date,
        g.start_time,
        g.season,
        g.home_team_id,
        g.away_team_id,
        g.doubleheader_status,
        g.time_of_day,
        g.game_type,
        g.bat_first_side,
        g.sky,
        g.field_condition,
        g.precipitation,
        g.wind_direction,
        g.park_id,
        g.temperature_fahrenheit,
        g.attendance::UINTEGER AS attendance,
        g.wind_speed_mph::UTINYINT AS wind_speed_mph,
        g.use_dh,
        g.scorer,
        g.scoring_method,
        g.source_type,
        g.umpire_home_id,
        g.umpire_first_id,
        g.umpire_second_id,
        g.umpire_third_id,
        g.umpire_left_id,
        g.umpire_right_id,
        g.filename,
    FROM "timeball"."main_models"."stg_games" AS g
),

add_gamelog AS (
    SELECT *
    FROM games
    UNION ALL BY NAME
    -- Gamelogs from non-acquired games
    -- have a small subset of info
    SELECT
        game_id,
        date,
        season,
        home_team_id,
        away_team_id,
        doubleheader_status,
        time_of_day,
        game_type,
        bat_first_side,
        park_id,
        attendance::UINTEGER AS attendance,
        use_dh,
        umpire_home_id,
        umpire_first_id,
        umpire_second_id,
        umpire_third_id,
        source_type,
        home_starting_pitcher_id,
        away_starting_pitcher_id,
        'Unknown'::SKY AS sky,
        'Unknown'::FIELD_CONDITION AS field_condition,
        'Unknown'::PRECIPITATION AS precipitation,
        'Unknown'::WIND_DIRECTION AS wind_direction,
    FROM "timeball"."main_models"."stg_gamelog"
    WHERE game_id NOT IN (SELECT game_id FROM games)
),

add_rest AS (
    SELECT
        add_gamelog.* REPLACE (
            COALESCE(add_gamelog.away_starting_pitcher_id, lineups.fielding_map_away[1][1])
            AS away_starting_pitcher_id,
            COALESCE(add_gamelog.home_starting_pitcher_id, lineups.fielding_map_home[1][1])
            AS home_starting_pitcher_id,
            COALESCE(add_gamelog.park_id, missing_parks.park_id) AS park_id,
        ),
        game_types.is_regular_season,
        game_types.is_postseason,
        COALESCE(
            add_gamelog.season >= 1947 AND add_gamelog.filename NOT LIKE '%.E%R', FALSE
        ) AS is_integrated,
        COALESCE(add_gamelog.filename LIKE '%.E%R', FALSE) AS is_negro_leagues,
        (NOT is_integrated AND NOT is_negro_leagues) AS is_segregated_white,
        franchise_a.franchise_id::TEAM_ID AS away_franchise_id,
        franchise_h.franchise_id::TEAM_ID AS home_franchise_id,
        franchise_a.league AS away_league,
        franchise_h.league AS home_league,
        franchise_a.division AS away_division,
        franchise_h.division AS home_division,
        franchise_a.location || ' ' || franchise_a.nickname AS away_team_name,
        franchise_h.location || ' ' || franchise_h.nickname AS home_team_name,
        COALESCE(franchise_a.league != franchise_h.league, FALSE) AS is_interleague,
        lineups.lineup_map_away,
        lineups.lineup_map_home,
        lineups.fielding_map_away,
        lineups.fielding_map_home,
    FROM add_gamelog
    -- It's an extra join, but we need to join after denormalizing
    -- in order to get the gamelog-only games
    LEFT JOIN "timeball"."main_seeds"."seed_franchises" AS franchise_a
        ON add_gamelog.away_team_id = franchise_a.team_id
            AND add_gamelog.date BETWEEN
            franchise_a.date_start AND COALESCE(franchise_a.date_end, '9999-12-31')
    LEFT JOIN "timeball"."main_seeds"."seed_franchises" AS franchise_h
        ON add_gamelog.home_team_id = franchise_h.team_id
            AND add_gamelog.date BETWEEN
            franchise_h.date_start AND COALESCE(franchise_h.date_end, '9999-12-31')
    LEFT JOIN "timeball"."main_models"."game_starting_lineups" AS lineups USING (game_id)
    LEFT JOIN "timeball"."main_seeds"."seed_game_types" AS game_types USING (game_type)
    -- Some parks are missing from early box files, so we supplement with the gamelog
    -- Otherwise, gamelog games are mutually exclusive with box/event games in this data
    LEFT JOIN "timeball"."main_models"."stg_gamelog" AS missing_parks USING (game_id)
)

SELECT * FROM add_rest