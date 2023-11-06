{{
  config(
    materialized = 'table',
    )
}}
-- We don't need to merge in box score data for this like we do with
-- player-level data, because the event-level data will contain all
-- info even if it isn't credited to a specific player.
WITH game_event_agg AS (
    SELECT
        team_id,
        game_id,
        ANY_VALUE(season) AS season,
        SUM(outs_played)::UTINYINT AS outs_played,
        SUM(plate_appearances_in_field)::UTINYINT AS plate_appearances_in_field,
        SUM(plate_appearances_in_field_with_ball_in_play)::UTINYINT AS plate_appearances_in_field_with_ball_in_play,
        SUM(fielding_plays)::UTINYINT AS fielding_plays,
        SUM(putouts)::UTINYINT AS putouts,
        SUM(assists)::UTINYINT AS assists,
        SUM(errors)::UTINYINT AS errors,
        SUM(fielders_choices)::UTINYINT AS fielders_choices,
        SUM(stolen_bases)::UTINYINT AS stolen_bases,
        SUM(caught_stealing)::UTINYINT AS caught_stealing,
        SUM(pickoffs)::UTINYINT AS pickoffs,
        SUM(passed_balls)::UTINYINT AS passed_balls,
        SUM(double_plays)::UTINYINT AS double_plays,
        SUM(triple_plays)::UTINYINT AS triple_plays,
        SUM(ground_ball_double_plays)::UTINYINT AS ground_ball_double_plays,
        SUM(reaching_errors)::UTINYINT AS reaching_errors,
    FROM {{ ref('event_fielding_stats') }}
    GROUP BY 1, 2
),

box_games AS (
    SELECT
        season,
        game_id,
        team_id,
        team_side AS side
    FROM {{ ref('team_game_start_info') }}
    WHERE source_type = 'BoxScore'
),

box_sb AS (
    SELECT
        game_id,
        CASE WHEN running_side = 'Away' THEN 'Home' ELSE 'Away' END AS side,
        COUNT(*) AS stolen_bases
    FROM {{ ref('stg_box_score_stolen_bases') }}
    GROUP BY 1, 2
),

box_cs AS (
    SELECT
        game_id,
        CASE WHEN running_side = 'Away' THEN 'Home' ELSE 'Away' END AS side,
        COUNT(*) AS caught_stealing
    FROM {{ ref('stg_box_score_caught_stealing') }}
    GROUP BY 1, 2
),

box_dp AS (
    SELECT
        game_id,
        defense_side AS side,
        COUNT(*) AS double_plays
    FROM {{ ref('stg_box_score_double_plays') }}
    GROUP BY 1, 2
),

box_tp AS (
    SELECT
        game_id,
        defense_side AS side,
        COUNT(*) AS triple_plays
    FROM {{ ref('stg_box_score_triple_plays') }}
    GROUP BY 1, 2
),

-- Only a small fraction of box scores have team fielding totals, so we need to
-- aggregate the player-level data (which is more likely to be missing data) as a backup
box_player AS (
    SELECT
        season,
        game_id,
        team_id,
        -- Use putouts to dedupe (generally the same anyway)
        SUM(putouts) AS outs_played,
        SUM(putouts) AS putouts,
        SUM(assists) AS assists,
        SUM(errors) AS errors,
        SUM(passed_balls) AS passed_balls
    FROM {{ ref('player_position_game_fielding_lines') }}
    -- Redundant with later joins, but shrinks the agg
    WHERE game_id IN (SELECT game_id FROM box_games)
    GROUP BY 1, 2, 3
),

box_final AS (
    SELECT
        box_games.season,
        game_id,
        team_id,
        COALESCE(t.outs_played, p.outs_played)::UTINYINT AS outs_played,
        COALESCE(t.putouts, p.putouts)::UTINYINT AS putouts,
        COALESCE(t.assists, p.assists)::UTINYINT AS assists,
        COALESCE(t.errors, p.errors)::UTINYINT AS errors,
        COALESCE(t.passed_balls, p.passed_balls)::UTINYINT AS passed_balls,
        COALESCE(box_sb.stolen_bases, 0)::UTINYINT AS stolen_bases,
        COALESCE(box_cs.caught_stealing, 0)::UTINYINT AS caught_stealing,
        COALESCE(box_dp.double_plays, 0)::UTINYINT AS double_plays,
        COALESCE(box_tp.triple_plays, 0)::UTINYINT AS triple_plays
    FROM box_games
    LEFT JOIN box_player AS p USING (game_id, team_id)
    LEFT JOIN {{ ref('stg_box_score_team_fielding_lines') }} AS t USING (game_id, side)
    LEFT JOIN box_sb USING (game_id, side)
    LEFT JOIN box_cs USING (game_id, side)
    LEFT JOIN box_dp USING (game_id, side)
    LEFT JOIN box_tp USING (game_id, side)
),

final AS (
    SELECT * FROM game_event_agg
    UNION ALL BY NAME
    SELECT * FROM box_final
    WHERE game_id NOT IN (SELECT game_id FROM game_event_agg)
)

SELECT * FROM final
