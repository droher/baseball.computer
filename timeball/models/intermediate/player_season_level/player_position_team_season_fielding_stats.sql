{{
  config(
    materialized = 'table',
    )
}}
WITH databank AS (
    SELECT
        field.season,
        field.team_id,
        people.retrosheet_player_id AS player_id,
        COALESCE(field.fielding_position, 0) AS fielding_position,
        ANY_VALUE('RegularSeason') AS game_type,
        ANY_VALUE(field.fielding_position_category) AS fielding_position_category,
        SUM(field.games)::USMALLINT AS games,
        SUM(field.games_started)::USMALLINT AS games_started,
        CASE WHEN ANY_VALUE(field.fielding_position_category) = 'OF'
                THEN COALESCE(SUM(of_games.games_left_field), 0)
            ELSE 0
        END::USMALLINT AS games_left_field,
        CASE WHEN ANY_VALUE(field.fielding_position_category) = 'OF'
                THEN COALESCE(SUM(of_games.games_center_field), 0)
            ELSE 0
        END::USMALLINT AS games_center_field,
        CASE WHEN ANY_VALUE(field.fielding_position_category) = 'OF'
                THEN COALESCE(SUM(of_games.games_right_field), 0)
            ELSE 0
        END::USMALLINT AS games_right_field,
        SUM(field.outs_played)::USMALLINT AS outs_played,
        SUM(field.putouts)::USMALLINT AS putouts,
        SUM(field.assists)::USMALLINT AS assists,
        SUM(field.errors)::USMALLINT AS errors,
        SUM(field.double_plays)::USMALLINT AS double_plays,
        SUM(field.passed_balls)::USMALLINT AS passed_balls,
        SUM(field.stolen_bases)::USMALLINT AS stolen_bases,
        SUM(field.caught_stealing)::USMALLINT AS caught_stealing,
    FROM {{ ref('stg_databank_fielding') }} AS field
    LEFT JOIN {{ ref('stg_databank_fielding_of') }} AS of_games USING (databank_player_id, season, stint)
    INNER JOIN {{ ref('stg_people') }} AS people USING (databank_player_id)
    -- We'd need to do something different for partial coverage seasons but
    -- currently box scores are all or nothing for a given year
    WHERE field.season NOT IN (SELECT DISTINCT season FROM {{ ref('stg_games') }})
    GROUP BY 1, 2, 3, 4
),

game_agg AS (
    SELECT
        games.season,
        stats.team_id,
        stats.player_id,
        stats.fielding_position,
        games.game_type,
        ANY_VALUE(CASE
            WHEN stats.fielding_position = 1 THEN 'P'
            WHEN stats.fielding_position = 2 THEN 'C'
            WHEN stats.fielding_position BETWEEN 3 AND 6 THEN 'IF'
            WHEN stats.fielding_position BETWEEN 7 AND 9 THEN 'OF'
            WHEN stats.fielding_position = 10 THEN 'DH'
        END) AS fielding_position_category,
        COUNT(*)::USMALLINT AS games,
        SUM(stats.games_started)::USMALLINT AS games_started,
        SUM(stats.outs_played)::USMALLINT AS outs_played,
        SUM(stats.plate_appearances_in_field)::USMALLINT AS plate_appearances_in_field,
        SUM(stats.plate_appearances_in_field_with_ball_in_play)::USMALLINT AS plate_appearances_in_field_with_ball_in_play,
        SUM(stats.putouts)::USMALLINT AS putouts,
        SUM(stats.assists)::USMALLINT AS assists,
        SUM(stats.errors)::USMALLINT AS errors,
        SUM(stats.fielders_choices)::USMALLINT AS fielders_choices,
        SUM(stats.reaching_errors)::USMALLINT AS reaching_errors,
        SUM(stats.double_plays)::USMALLINT AS double_plays,
        SUM(stats.triple_plays)::USMALLINT AS triple_plays,
        SUM(stats.ground_ball_double_plays)::USMALLINT AS ground_ball_double_plays,
        SUM(stats.passed_balls)::USMALLINT AS passed_balls,
        SUM(stats.balls_hit_to)::USMALLINT AS balls_hit_to,
        SUM(stats.stolen_bases)::USMALLINT AS stolen_bases,
        SUM(stats.caught_stealing)::USMALLINT AS caught_stealing,
        COUNT_IF(stats.fielding_position = 7)::USMALLINT AS games_left_field,
        COUNT_IF(stats.fielding_position = 8)::USMALLINT AS games_center_field,
        COUNT_IF(stats.fielding_position = 9)::USMALLINT AS games_right_field,
    FROM {{ ref('stg_games') }} AS games
    INNER JOIN {{ ref('player_position_game_fielding_stats') }} AS stats USING (game_id)
    GROUP BY 1, 2, 3, 4, 5
),

final AS (
    SELECT * FROM game_agg
    UNION ALL BY NAME
    SELECT * FROM databank
)

SELECT * FROM final
