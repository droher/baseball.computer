
WITH game_agg AS (
    SELECT
        season,
        team_id,
        game_type,
        COUNT(*) AS games
    FROM "timeball"."main_models"."team_game_start_info"
    GROUP BY 1, 2, 3
),

-- Need player-level data for seasons without box scores
databank AS (
    SELECT
        season,
        team_id,
        ANY_VALUE('RegularSeason') AS game_type,
        ANY_VALUE(game_agg.games)::USMALLINT AS games,
        SUM(f.putouts)::USMALLINT AS outs_played,
        SUM(f.putouts)::USMALLINT AS putouts,
        SUM(f.assists)::USMALLINT AS assists,
        SUM(f.errors)::USMALLINT AS errors,
        SUM(f.passed_balls)::USMALLINT AS passed_balls,
        -- These stats aren't duplicated because they only
        -- appear for catchers, not pitchers
        SUM(f.stolen_bases)::USMALLINT AS stolen_bases,
        SUM(f.caught_stealing)::USMALLINT AS caught_stealing,
    FROM "timeball"."main_models"."player_position_team_season_fielding_stats" AS f
    INNER JOIN game_agg USING (season, team_id)
    WHERE f.season NOT IN (SELECT DISTINCT season FROM "timeball"."main_models"."game_start_info")
    GROUP BY 1, 2
),

retrosheet AS (
    SELECT
        games.season,
        stats.team_id,
        games.game_type,
        COUNT(*)::USMALLINT AS games,
        SUM(stats.outs_played)::USMALLINT AS outs_played,
        SUM(stats.plate_appearances_in_field)::USMALLINT AS plate_appearances_in_field,
        SUM(stats.plate_appearances_in_field_with_ball_in_play)::USMALLINT AS plate_appearances_in_field_with_ball_in_play,
        SUM(stats.putouts)::USMALLINT AS putouts,
        SUM(stats.assists)::USMALLINT AS assists,
        SUM(stats.errors)::USMALLINT AS errors,
        SUM(stats.fielders_choices)::USMALLINT AS fielders_choices,
        SUM(stats.reaching_errors)::USMALLINT AS reaching_errors,
        SUM(stats.stolen_bases)::USMALLINT AS stolen_bases,
        SUM(stats.caught_stealing)::USMALLINT AS caught_stealing,
        SUM(stats.pickoffs)::USMALLINT AS pickoffs,
        SUM(stats.passed_balls)::USMALLINT AS passed_balls,
        SUM(stats.double_plays)::USMALLINT AS double_plays,
        SUM(stats.triple_plays)::USMALLINT AS triple_plays,
        SUM(stats.ground_ball_double_plays)::USMALLINT AS ground_ball_double_plays
    FROM "timeball"."main_models"."game_start_info" AS games
    INNER JOIN "timeball"."main_models"."team_game_fielding_stats" AS stats USING (game_id)
    GROUP BY 1, 2, 3
),

final AS (
    SELECT * FROM databank
    UNION ALL BY NAME
    SELECT * FROM retrosheet
)

SELECT * FROM final