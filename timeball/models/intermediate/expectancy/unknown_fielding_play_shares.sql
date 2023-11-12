{{
  config(
    materialized = 'table',
    )
}}
WITH incomplete_games AS (
    SELECT
        game_id,
        team_id
    FROM {{ ref('team_game_fielding_stats') }}
    WHERE unknown_putouts > 0
),

unassisted_putout_rates AS (
    SELECT
        f.fielding_position,
        SUM(f.putouts) AS total,
        SUM(f.putouts - f.assisted_putouts) AS unassisted,
        unassisted / total AS unassisted_putout_rate
    FROM {{ ref('calc_fielding_play_agg') }} f
    INNER JOIN {{ ref('stg_events') }} AS e USING (event_key)
    WHERE e.plate_appearance_result = 'InPlayOut'
        AND e.batted_to_fielder > 0
        AND e.outs_on_play = 1
    GROUP BY 1
),

team_totals AS (
    SELECT
        game_id,
        team_id,
        GREATEST(SUM(surplus_box_putouts), 0) AS total_surplus_putouts,
        GREATEST(SUM(CASE WHEN fielding_position > 6 THEN surplus_box_putouts END), 0) AS surplus_of_putouts,
        GREATEST(SUM(CASE WHEN fielding_position < 6 THEN surplus_box_assists END), 0) AS surplus_if_assists,
        GREATEST(total_surplus_putouts - surplus_of_putouts - surplus_if_assists, 0) AS surplus_if_unassisted_putouts
    FROM {{ ref('player_position_game_fielding_lines') }}
    INNER JOIN incomplete_games USING (game_id, team_id)
    GROUP BY 1, 2
),

-- Outfielder shares: surplus putouts
-- Infielder shares: surplus assists + surplus unassisted putouts
-- Need to estimate the total number of surplus unassisted infield putouts
-- method: total unknown putouts - OF surplus putouts - IF surplus assists
-- Then we need to estimate the share that went to each infielder
-- First adjust each number based historical rate of unassisted putouts
-- on in-play outs at each position
-- Then split based on the normalized total
calc_shares AS (
    SELECT
        p.*,
        
        CASE WHEN fielding_position <= 6 AND t.surplus_if_unassisted_putouts > 0
                THEN p.surplus_box_putouts / t.surplus_if_unassisted_putouts
            ELSE 0
        END AS unadjusted_if_putout_rate,
        GREATEST(unadjusted_if_putout_rate * r.unassisted_putout_rate, 0) AS adjusted_if_putout_rate,
        COALESCE(adjusted_if_putout_rate / SUM(adjusted_if_putout_rate) OVER w, 0) AS if_putout_share,

        GREATEST(
            CASE WHEN fielding_position > 6
                    THEN p.surplus_box_putouts
                ELSE (if_putout_share * t.surplus_if_unassisted_putouts)
            END, 0
        ) AS estimated_unknown_plays_putouts,
        GREATEST(
            CASE WHEN fielding_position > 6
                    THEN 0
                ELSE p.surplus_box_assists
            END, 0
        ) AS estimated_unknown_plays_assists,
        estimated_unknown_plays_putouts + estimated_unknown_plays_assists AS estimated_unknown_plays
    FROM {{ ref('player_position_game_fielding_lines') }} AS p
    INNER JOIN team_totals AS t USING (game_id, team_id)
    INNER JOIN unassisted_putout_rates AS r USING (fielding_position)
    WINDOW w AS (PARTITION BY game_id, team_id)
),

final AS (
    SELECT
        game_id,
        team_id,
        fielding_position,
        player_id,
        estimated_unknown_plays,
        SUM(estimated_unknown_plays) OVER w AS estimated_unknown_plays_team,
        estimated_unknown_plays / estimated_unknown_plays_team AS play_share,
        estimated_unknown_plays_assists / estimated_unknown_plays_team AS play_share_subset_assists,
        estimated_unknown_plays_putouts / estimated_unknown_plays_team AS play_share_subset_putouts,
    FROM calc_shares
    WHERE estimated_unknown_plays > 0
    WINDOW w AS (PARTITION BY game_id, team_id)
)

SELECT * FROM final
