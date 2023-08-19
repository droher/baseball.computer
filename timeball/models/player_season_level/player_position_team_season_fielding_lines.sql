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
        field.fielding_position,
        ANY_VALUE(field.fielding_position_category) AS fielding_position_category,
        SUM(field.games) AS games,
        SUM(field.games_started) AS games_started,
        COALESCE(SUM(of_games.games_left_field), 0) AS games_left_field,
        COALESCE(SUM(of_games.games_center_field), 0) AS games_center_field,
        COALESCE(SUM(of_games.games_right_field), 0) AS games_right_field,
        SUM(field.outs_played) AS outs_played,
        SUM(field.putouts) AS putouts,
        SUM(field.assists) AS assists,
        SUM(field.errors) AS errors,
        SUM(field.double_plays) AS double_plays,
        SUM(field.passed_balls) AS passed_balls,
        SUM(field.stolen_bases) AS stolen_bases,
        SUM(field.caught_stealing) AS caught_stealing,
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
        ANY_VALUE(CASE
            WHEN fielding_position = 1 THEN 'P'
            WHEN fielding_position = 2 THEN 'C'
            WHEN fielding_position BETWEEN 3 AND 6 THEN 'IF'
            WHEN fielding_position BETWEEN 7 AND 9 THEN 'OF'
        END) AS fielding_position_category,
        COUNT(*) AS games,
        SUM(stats.games_started) AS games_started,
        SUM(stats.outs_played) AS outs_played,
        SUM(stats.plate_appearances_in_field) AS plate_appearances_in_field,
        SUM(stats.plate_appearances_in_field_with_ball_in_play) AS plate_appearances_in_field_with_ball_in_play,
        SUM(stats.putouts) AS putouts,
        SUM(stats.assists) AS assists,
        SUM(stats.errors) AS errors,
        SUM(stats.fielders_choices) AS fielders_choices,
        SUM(stats.double_plays) AS double_plays,
        SUM(stats.triple_plays) AS triple_plays,
        SUM(stats.ground_ball_double_plays) AS ground_ball_double_plays,
        SUM(stats.passed_balls) AS passed_balls,
        SUM(stats.balls_hit_to) AS balls_hit_to,
        COUNT_IF(fielding_position = 7) AS games_left_field,
        COUNT_IF(fielding_position = 8) AS games_center_field,
        COUNT_IF(fielding_position = 9) AS games_right_field,
    FROM {{ ref('stg_games') }} AS games
    INNER JOIN {{ ref('player_position_game_fielding_lines') }} AS stats USING (game_id)
    GROUP BY 1, 2, 3, 4
),

final AS (
    SELECT * FROM game_agg
    UNION ALL BY NAME
    SELECT * FROM databank
)

SELECT * FROM final
