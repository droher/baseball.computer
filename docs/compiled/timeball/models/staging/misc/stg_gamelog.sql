WITH source AS (
    SELECT * FROM "timeball"."misc"."gamelog"
),

renamed AS (
    SELECT
        EXTRACT(YEAR FROM date)::INT2 AS season,
        date::DATE AS date, -- noqa: RF04
        CASE double_header
            WHEN 0 THEN 'SingleGame'
            WHEN 1 THEN 'DoubleHeaderGame1'
            WHEN 2 THEN 'DoubleHeaderGame2'
            WHEN 3 THEN 'DoubleHeaderGame3'
        END::DOUBLEHEADER_STATUS AS doubleheader_status,
        home_team || STRFTIME(date, '%Y%m%d') || double_header AS game_id,
        visiting_team::TEAM_ID AS away_team_id,
        home_team::TEAM_ID AS home_team_id,
        CASE day_night WHEN 'N' THEN 'Night' ELSE 'Day' END::TIME_OF_DAY AS time_of_day,
        park_id::PARK_ID AS park_id,
        attendance,
        umpire_h_id AS umpire_home_id,
        umpire_1b_id AS umpire_first_id,
        umpire_2b_id AS umpire_second_id,
        umpire_3b_id AS umpire_third_id,
        visitor_starting_pitcher_id::PLAYER_ID AS away_starting_pitcher_id,
        home_starting_pitcher_id::PLAYER_ID AS home_starting_pitcher_id,
        additional_info,
        CASE WHEN additional_info LIKE '%HTBF%' THEN 'Home' ELSE 'Away' END::SIDE AS bat_first_side,
        -- These two might need to be updated if there's ever another game without acq info
        (EXTRACT(year from date) >= 1973 AND home_team_league = 'AL') AS use_dh,
        'RegularSeason'::GAME_TYPE AS game_type,
        -- Everything below is post-game knowledge
        duration::INT2 AS duration_minutes,
        -- TODO: Fix spelling in original
        vistor_line_score AS away_line_score,
        home_line_score,
        -- TODO: Fix spelling in original
        visitor_runs_scored::UTINYINT AS away_runs_scored,
        home_runs_score::UTINYINT AS home_runs_scored,
        forfeit_info,
        'GameLog' AS source_type
    FROM source
)

SELECT * FROM renamed