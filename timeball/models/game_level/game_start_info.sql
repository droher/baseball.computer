{{
  config(
    materialized = 'table',
    )
}}
WITH teams_flat AS (
    SELECT
        game_id,
        FIRST(team_id) FILTER (WHERE side = 'Home') AS home_team_id,
        FIRST(team_id) FILTER (WHERE side = 'Away') AS away_team_id
    FROM {{ ref('stg_game_teams') }}
    GROUP BY 1
),

umps_flat AS (
    SELECT
        game_id,
        FIRST(umpire_id) FILTER (WHERE position = 'Home') AS umpire_home_id,
        FIRST(umpire_id) FILTER (WHERE position = 'First') AS umpire_first_id,
        FIRST(umpire_id) FILTER (WHERE position = 'Second') AS umpire_second_id,
        FIRST(umpire_id) FILTER (WHERE position = 'Third') AS umpire_third_id,
        FIRST(umpire_id) FILTER (WHERE position = 'LeftField') AS umpire_left_field_id,
        FIRST(umpire_id) FILTER (WHERE position = 'RightField') AS umpire_right_field_id,
    FROM {{ ref('stg_game_umpires') }}
    GROUP BY 1
),

game_flat AS (
    SELECT
        g.game_id,
        g.date,
        g.start_time,
        g.season,
        teams_flat.home_team_id,
        teams_flat.away_team_id,
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
        g.attendance,
        g.wind_speed_mph,
        g.use_dh,
        g.scorer,
        g.scoring_method,
        g.source_type,
        umps_flat.umpire_home_id,
        umps_flat.umpire_first_id,
        umps_flat.umpire_second_id,
        umps_flat.umpire_third_id,
        umps_flat.umpire_left_field_id,
        umps_flat.umpire_right_field_id,
    FROM {{ ref('stg_games') }} AS g
    LEFT JOIN teams_flat USING (game_id)
    LEFT JOIN umps_flat USING (game_id)
),

add_gamelog AS (
    SELECT *
    FROM game_flat
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
        attendance,
        use_dh,
        umpire_home_id,
        umpire_first_id,
        umpire_second_id,
        umpire_third_id,
        source_type,
        'Unknown' AS sky,
        'Unknown' AS field_condition,
        'Unknown' AS precipitation,
        'Unknown' AS wind_direction,
    FROM {{ ref('stg_gamelog') }}
),

add_rest AS (
    SELECT
        add_gamelog.*,
        game_types.is_regular_season,
        game_types.is_postseason,
        franchise_a.franchise_id AS away_franchise_id,
        franchise_h.franchise_id AS home_franchise_id,
        franchise_a.league AS away_league,
        franchise_h.league AS home_league,
        franchise_a.division AS away_division,
        franchise_h.division AS home_division,
        franchise_a.location || ' ' || franchise_a.nickname AS away_team_name,
        franchise_h.location || ' ' || franchise_h.nickname AS home_team_name,
        franchise_a.league != franchise_h.league AS is_interleague,
        lineups.fielding_map_away[1][1] AS away_starting_pitcher_id,
        lineups.fielding_map_home[1][1] AS home_starting_pitcher_id,
        lineups.lineup_map_away,
        lineups.lineup_map_home,
        lineups.fielding_map_away,
        lineups.fielding_map_home,
    FROM add_gamelog
    -- It's an extra join, but we need to join after denormalizing
    -- in order to get the gamelog-only games
    LEFT JOIN {{ ref('seed_franchises') }} AS franchise_a
        ON add_gamelog.away_team_id = franchise_a.team_id
            AND add_gamelog.date BETWEEN
            franchise_a.date_start AND COALESCE(franchise_a.date_end, '9999-12-31')
    LEFT JOIN {{ ref('seed_franchises') }} AS franchise_h
        ON add_gamelog.home_team_id = franchise_h.team_id
            AND add_gamelog.date BETWEEN
            franchise_h.date_start AND COALESCE(franchise_h.date_end, '9999-12-31')
    LEFT JOIN {{ ref('game_starting_lineups') }} AS lineups USING (game_id)
    LEFT JOIN {{ ref('seed_game_types') }} AS game_types USING (game_type)
)

SELECT * FROM add_rest
