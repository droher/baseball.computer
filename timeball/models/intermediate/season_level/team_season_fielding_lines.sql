{{
  config(
    materialized = 'table',
    )
}}
WITH game_agg AS (
    SELECT
        season,
        team_id,
        game_type,
        COUNT(*) AS games
    FROM {{ ref('team_game_start_info') }}
    GROUP BY 1, 2, 3
),

-- Need player-level data for seasons without box scores
databank AS (
    SELECT
        season,
        team_id,
        ANY_VALUE('RegularSeason') AS game_type,
        ANY_VALUE(game_agg.games) AS games,
        SUM(f.putouts) AS outs_played,
        SUM(f.putouts) AS putouts,
        SUM(f.assists) AS assists,
        SUM(f.errors) AS errors,
        SUM(f.passed_balls) AS passed_balls,
        -- These stats aren't duplicated because they only
        -- appear for catchers, not pitchers
        SUM(f.stolen_bases) AS stolen_bases,
        SUM(f.caught_stealing) AS caught_stealing,
    FROM {{ ref('player_position_team_season_fielding_lines') }} AS f
    INNER JOIN game_agg USING (season, team_id)
    WHERE f.season NOT IN (SELECT DISTINCT season FROM {{ ref('game_start_info') }})
    GROUP BY 1, 2
),

retrosheet AS (
    SELECT
        games.season,
        stats.team_id,
        games.game_type,
        COUNT(*) AS games,
        SUM(stats.outs_played) AS outs_played,
        SUM(stats.plate_appearances_in_field) AS plate_appearances_in_field,
        SUM(stats.plate_appearances_in_field_with_ball_in_play) AS plate_appearances_in_field_with_ball_in_play,
        SUM(stats.putouts) AS putouts,
        SUM(stats.assists) AS assists,
        SUM(stats.errors) AS errors,
        SUM(stats.fielders_choices) AS fielders_choices,
        SUM(stats.reaching_errors) AS reaching_errors,
        SUM(stats.stolen_bases) AS stolen_bases,
        SUM(stats.caught_stealing) AS caught_stealing,
        SUM(stats.pickoffs) AS pickoffs,
        SUM(stats.passed_balls) AS passed_balls,
        SUM(stats.double_plays) AS double_plays,
        SUM(stats.triple_plays) AS triple_plays,
        SUM(stats.ground_ball_double_plays) AS ground_ball_double_plays
    FROM {{ ref('game_start_info') }} AS games
    INNER JOIN {{ ref('team_game_fielding_stats') }} AS stats USING (game_id)
    GROUP BY 1, 2, 3
),

final AS (
    SELECT * FROM databank
    UNION ALL BY NAME
    SELECT * FROM retrosheet
)

SELECT * FROM final
