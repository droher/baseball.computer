{{
  config(
    materialized = 'table',
    )
}}
WITH box_agg AS (
    SELECT
        game_id,
        stats.fielder_id AS player_id,
        stats.fielding_position,
        MIN(CASE WHEN stats.side = 'Home' THEN games.home_team_id ELSE games.away_team_id END) AS team_id,
        SUM(stats.outs_played) AS outs_played,
        SUM(stats.putouts) AS putouts,
        SUM(stats.assists) AS assists,
        SUM(stats.errors) AS errors,
        SUM(stats.double_plays) AS double_plays,
        SUM(stats.triple_plays) AS triple_plays,
        SUM(stats.passed_balls) AS passed_balls
    FROM {{ ref('stg_box_score_fielding_lines') }} AS stats
    INNER JOIN {{ ref('stg_games') }} AS games USING (game_id)
    GROUP BY 1, 2, 3
),

event_agg AS (
    SELECT
        game_id,
        player_id,
        fielding_position,
        MIN(team_id) AS team_id,
        SUM(outs_played)::UTINYINT AS outs_played,
        SUM(plate_appearances_in_field)::UTINYINT AS plate_appearances_in_field,
        SUM(plate_appearances_in_field_with_ball_in_play)::UTINYINT AS plate_appearances_in_field_with_ball_in_play,
        SUM(putouts)::UTINYINT AS putouts,
        SUM(assists)::UTINYINT AS assists,
        SUM(errors)::UTINYINT AS errors,
        SUM(fielders_choices)::UTINYINT AS fielders_choices,
        SUM(double_plays)::UTINYINT AS double_plays,
        SUM(triple_plays)::UTINYINT AS triple_plays,
        SUM(ground_ball_double_plays)::UTINYINT AS ground_ball_double_plays,
        SUM(stolen_bases)::UTINYINT AS stolen_bases,
        SUM(caught_stealing)::UTINYINT AS caught_stealing,
        SUM(passed_balls)::UTINYINT AS passed_balls,
        SUM(balls_hit_to)::UTINYINT AS balls_hit_to,
        SUM(reaching_errors)::UTINYINT AS reaching_errors
    FROM {{ ref('event_player_fielding_stats') }}
    GROUP BY 1, 2, 3
),

-- Unlike batting/fielding, we join the data instead of unioning
-- because box scores are more reliable for fielding plays
final AS (
    SELECT
        game_id,
        player_id,
        fielding_position,
        COALESCE(event_agg.team_id, box_agg.team_id) AS team_id,
        CASE
            WHEN appearances.first_fielding_position = fielding_position
                OR appearances.games_ohtani_rule = 1 AND fielding_position IN (1, 10)
                THEN appearances.games_started
            ELSE 0
        END::UTINYINT AS games_started,
        COALESCE(event_agg.outs_played, box_agg.outs_played)::UTINYINT AS outs_played,
        event_agg.plate_appearances_in_field,
        event_agg.plate_appearances_in_field_with_ball_in_play,
        COALESCE(box_agg.putouts, event_agg.putouts)::UTINYINT AS putouts,
        COALESCE(box_agg.assists, event_agg.assists)::UTINYINT AS assists,
        COALESCE(box_agg.errors, event_agg.errors)::UTINYINT AS errors,
        event_agg.reaching_errors,
        event_agg.fielders_choices,
        event_agg.balls_hit_to,
        COALESCE(box_agg.double_plays, event_agg.double_plays)::UTINYINT AS double_plays,
        COALESCE(box_agg.triple_plays, event_agg.triple_plays)::UTINYINT AS triple_plays,
        event_agg.ground_ball_double_plays,
        COALESCE(box_agg.passed_balls, event_agg.passed_balls)::UTINYINT AS passed_balls,
        event_agg.stolen_bases,
        event_agg.caught_stealing,

    FROM box_agg
    FULL OUTER JOIN event_agg USING (game_id, player_id, fielding_position)
    LEFT JOIN {{ ref('player_game_appearances') }} AS appearances USING (game_id, player_id)
)

SELECT * FROM final
