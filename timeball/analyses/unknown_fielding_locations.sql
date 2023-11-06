WITH e AS (
    SELECT
        game_id,
        event_key
    FROM {{ ref('event_fielding_stats') }}
    WHERE unknown_events = 1
),

g AS (
    SELECT DISTINCT ON (game_id, team_id, fielding_position)
        game_id,
        team_id,
        fielding_position,
        SUM(putouts) OVER fielder AS putouts,
        GREATEST(SUM(event_unaccounted_putouts) OVER fielder, 0) AS unaccounted_putouts,
        GREATEST(SUM(event_unaccounted_assists) OVER fielder, 0) AS unaccounted_assists,
        
        SUM(event_unaccounted_putouts) OVER game AS team_unaccounted_putouts,
        SUM(event_unaccounted_assists) OVER game AS team_unaccounted_assists,
        SUM(putouts) OVER game AS team_putouts,
        SUM(assists) OVER game AS approx_team_assisted_putouts,
        SUM(CASE WHEN fielding_position > 6 THEN putouts END) OVER game AS team_outfield_putouts,
        SUM(CASE WHEN fielding_position = 2 THEN putouts END) OVER game AS team_catcher_putouts,
        team_putouts - approx_team_assisted_putouts AS approx_team_unassisted_putouts,
        approx_team_unassisted_putouts - team_outfield_putouts - team_catcher_putouts AS approx_team_infield_unassisted_putouts,
        GREATEST(approx_team_infield_unassisted_putouts / team_putouts, 0) AS infield_putout_multiplier,
        GREATEST(approx_team_assisted_putouts / team_putouts, 0) AS assist_multiplier,

        CASE WHEN fielding_position > 6
                THEN unaccounted_putouts + unaccounted_assists * assist_multiplier
            ELSE
                unaccounted_putouts * infield_putout_multiplier + unaccounted_assists * assist_multiplier
        END AS shares
    FROM {{ ref('player_position_game_fielding_lines') }}
    WINDOW fielder AS (PARTITION BY game_id, team_id, fielding_position),
        game AS (PARTITION BY game_id, team_id)
    --QUALIFY unaccounted_putouts > 0 OR unaccounted_assists > 0
)

SELECT game_id, team_id, putouts - approx_team_assisted_putouts, putouts, team_unaccounted_putouts FROM g
WHERE game_id NOT IN (SELECT game_id FROM {{ ref('stg_box_score_team_fielding_lines') }})
AND fielding_position = 3
AND game_id IN (SELECT game_id FROM {{ ref('game_start_info') }} WHERE home_league in ('AL', 'NL'))
AND approx_team_infield_unassisted_putouts > 0
ORDER BY 3 DESC