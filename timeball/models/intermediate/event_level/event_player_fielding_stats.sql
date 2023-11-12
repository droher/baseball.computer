-- TODO: Unclear whether this should exist as such.
-- Per-event-fielding stats for positions not involved in the play
-- are only there for innings/PA played, which can be tabulated
-- in more efficient ways.
{{
  config(
    materialized = 'table',
    )
}}
WITH final AS (
    SELECT
        e.event_key,
        p.fielding_position,
        p.game_id,
        p.player_id,
        p.fielding_team_id AS team_id,
        -- DHs are in this table, which makes the nomenclature for the 3 cols below
        -- a little ambigious, but keeping for now because it's useful to keep track of
        -- for them.
        e.outs_played,
        e.plate_appearances_in_field,
        e.plate_appearances_in_field_with_ball_in_play,
        CASE WHEN e.batted_to_fielder = p.fielding_position THEN 1 ELSE 0 END::UTINYINT AS balls_hit_to,
        COALESCE(fp.fielding_plays, 0)::UTINYINT AS fielding_plays,
        COALESCE(fp.putouts, 0)::UTINYINT AS putouts,
        COALESCE(fp.assists, 0)::UTINYINT AS assists,
        COALESCE(fp.errors, 0)::UTINYINT AS errors,
        COALESCE(fp.fielders_choices, 0)::UTINYINT AS fielders_choices,
        COALESCE(fp.assisted_putouts)::UTINYINT AS assisted_putouts,
        CASE WHEN e.plate_appearances_in_field_with_ball_in_play > 0
                THEN COALESCE(fp.putouts, 0)
            ELSE 0
        END::UTINYINT AS in_play_putouts,
        CASE WHEN e.plate_appearances_in_field_with_ball_in_play > 0
                THEN COALESCE(fp.assists, 0)
            ELSE 0
        END::UTINYINT AS in_play_assists,
        CASE WHEN fp.first_errors = 1 THEN e.reaching_errors ELSE 0 END::UTINYINT AS reaching_errors,
        CASE WHEN p.fielding_position IN (1, 2) THEN e.stolen_bases ELSE 0 END::UTINYINT AS stolen_bases,
        CASE WHEN p.fielding_position IN (1, 2) THEN e.caught_stealing ELSE 0 END::UTINYINT AS caught_stealing,
        CASE WHEN p.fielding_position IN (1, 2) AND fp.assists > 0 THEN e.pickoffs ELSE 0 END::UTINYINT AS pickoffs,
        CASE WHEN p.fielding_position = 2 THEN e.passed_balls ELSE 0 END AS passed_balls,
        -- Only count double plays for the fielder who made a putout
        -- or assist on the play
        CASE WHEN fp.putouts + fp.assists > 0
                THEN e.double_plays
            ELSE 0
        END::UTINYINT AS double_plays,
        CASE WHEN fp.putouts + fp.assists > 0
                THEN e.triple_plays
            ELSE 0
        END::UTINYINT AS triple_plays,
        CASE WHEN fp.putouts + fp.assists > 0
                THEN e.ground_ball_double_plays
            ELSE 0
        END::UTINYINT AS ground_ball_double_plays,
        CASE WHEN fp.plays_started > 0
                THEN e.double_plays
            ELSE 0
        END::UTINYINT AS double_plays_started,
        CASE WHEN fp.plays_started > 0
                THEN e.ground_ball_double_plays
            ELSE 0
        END::UTINYINT AS ground_ball_double_plays_started,
    FROM {{ ref('event_fielding_stats') }} AS e
    INNER JOIN {{ ref('personnel_fielding_states') }} AS p USING (personnel_fielding_key)
    LEFT JOIN {{ ref('calc_fielding_play_agg') }} AS fp USING (event_key, fielding_position)
)

SELECT * FROM final
