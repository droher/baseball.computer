
-- We don't need to merge in box score data for this like we do with
-- player-level data, because the event-level data will contain all
-- info even if it isn't credited to a specific player.
WITH game_event_agg AS (
    SELECT
        team_id,
        game_id,
        SUM(outs_played)::UTINYINT AS outs_played,
        SUM(plate_appearances_in_field)::UTINYINT AS plate_appearances_in_field,
        SUM(plate_appearances_in_field_with_ball_in_play)::UTINYINT AS plate_appearances_in_field_with_ball_in_play,
        SUM(putouts)::UTINYINT AS putouts,
        SUM(assists)::UTINYINT AS assists,
        SUM(errors)::UTINYINT AS errors,
        SUM(stolen_bases)::UTINYINT AS stolen_bases,
        SUM(caught_stealing)::UTINYINT AS caught_stealing,
        SUM(pickoffs)::UTINYINT AS pickoffs,
        SUM(double_plays)::UTINYINT AS double_plays,
        SUM(triple_plays)::UTINYINT AS triple_plays,
        SUM(ground_ball_double_plays)::UTINYINT AS ground_ball_double_plays,
        SUM(reaching_errors)::UTINYINT AS reaching_errors,
        SUM(unknown_putouts)::UTINYINT AS unknown_putouts,
        SUM(incomplete_events)::UTINYINT AS incomplete_events,
    FROM "timeball"."main_models"."event_fielding_stats"
    GROUP BY 1, 2
),

game_info AS (
    SELECT
        season,
        game_id,
        team_id,
        team_side AS side
    FROM "timeball"."main_models"."team_game_start_info"
),

box_sb AS (
    SELECT
        game_id,
        CASE WHEN running_side = 'Away' THEN 'Home' ELSE 'Away' END AS side,
        COUNT(*) AS stolen_bases
    FROM "timeball"."main_models"."stg_box_score_stolen_bases"
    GROUP BY 1, 2
),

box_cs AS (
    SELECT
        game_id,
        CASE WHEN running_side = 'Away' THEN 'Home' ELSE 'Away' END AS side,
        COUNT(*) AS caught_stealing
    FROM "timeball"."main_models"."stg_box_score_caught_stealing"
    GROUP BY 1, 2
),

box_dp AS (
    SELECT
        game_id,
        defense_side AS side,
        COUNT(*) AS double_plays
    FROM "timeball"."main_models"."stg_box_score_double_plays"
    GROUP BY 1, 2
),

box_tp AS (
    SELECT
        game_id,
        defense_side AS side,
        COUNT(*) AS triple_plays
    FROM "timeball"."main_models"."stg_box_score_triple_plays"
    GROUP BY 1, 2
),

players AS (
    SELECT
        game_id,
        team_id,
        -- outs_played can't be summed directly,
        -- use putouts to dedupe (usually the same anyway)
        SUM(putouts) AS outs_played,
        SUM(putouts) AS putouts,
        SUM(assists) AS assists,
        SUM(errors) AS errors,
        SUM(fielders_choices)::UTINYINT AS fielders_choices,
        SUM(assisted_putouts)::UTINYINT AS assisted_putouts,
        SUM(in_play_putouts)::UTINYINT AS in_play_putouts,
        SUM(in_play_assists)::UTINYINT AS in_play_assists,
        SUM(passed_balls)::UTINYINT AS passed_balls,
    FROM "timeball"."main_models"."player_position_game_fielding_stats"
    GROUP BY 1, 2
),

final AS (
    SELECT
        game_info.season,
        game_id,
        team_id,
        COALESCE(t.outs_played, g.outs_played, p.outs_played)::UTINYINT AS outs_played,
        -- At the moment, there are some box score accounts that are less reliable
        -- than their PBP counterparts for fielding. The largest of the options here
        -- is the most likely to be consistent and correct.
        -- TODO: Revisit after NLB data quality improvements
        GREATEST(t.putouts, p.putouts, g.putouts)::UTINYINT AS putouts,
        GREATEST(t.assists, p.assists, g.assists)::UTINYINT AS assists,
        GREATEST(t.errors, p.errors, g.errors)::UTINYINT AS errors,
        p.fielders_choices,
        p.assisted_putouts,
        p.in_play_putouts,
        p.in_play_assists,
        COALESCE(t.passed_balls, p.passed_balls)::UTINYINT AS passed_balls,
        -- We trust PBP events over box events
        COALESCE(g.stolen_bases, box_sb.stolen_bases, 0)::UTINYINT AS stolen_bases,
        COALESCE(g.caught_stealing, box_cs.caught_stealing, 0)::UTINYINT AS caught_stealing,
        COALESCE(g.double_plays, box_dp.double_plays, 0)::UTINYINT AS double_plays,
        COALESCE(g.triple_plays, box_tp.triple_plays, 0)::UTINYINT AS triple_plays,
        g.pickoffs,
        g.plate_appearances_in_field,
        g.plate_appearances_in_field_with_ball_in_play,
        g.ground_ball_double_plays,
        g.reaching_errors,
        g.unknown_putouts,
        g.incomplete_events
    FROM players AS p
    INNER JOIN game_info USING (game_id, team_id)
    LEFT JOIN game_event_agg AS g USING (game_id, team_id)
    LEFT JOIN "timeball"."main_models"."stg_box_score_team_fielding_lines" AS t USING (game_id, side)
    LEFT JOIN box_sb USING (game_id, side)
    LEFT JOIN box_cs USING (game_id, side)
    LEFT JOIN box_dp USING (game_id, side)
    LEFT JOIN box_tp USING (game_id, side)
)

SELECT * FROM final