{{
  config(
    materialized = 'table',
    )
}}
WITH event_based AS (
    SELECT game_id
    FROM {{ ref('stg_games') }}
    WHERE source_type = 'PlayByPlay'
),

box_offense AS (
    SELECT
        bat.game_id,
        bat.batter_id AS player_id,
        bat.side,
        bat.lineup_position,
        CASE
            WHEN bat.nth_player_at_position = 1 THEN 'Starter'
            WHEN pinch_hit.pinch_hitter_id IS NOT NULL THEN 'PinchHitter'
            WHEN pinch_run.pinch_runner_id IS NOT NULL THEN 'PinchRunner'
            ELSE 'DefensiveSubstitution'
        END AS entered_game_as,
        nth_player_at_position AS position_order
    FROM {{ ref('stg_box_score_batting_lines') }} AS bat
    LEFT JOIN {{ ref('stg_box_score_pinch_hitting_lines') }} AS pinch_hit
        ON pinch_hit.game_id = bat.game_id
            AND pinch_hit.pinch_hitter_id = bat.batter_id
            AND pinch_hit.side = bat.side
    LEFT JOIN {{ ref('stg_box_score_pinch_running_lines') }} AS pinch_run
        ON pinch_run.game_id = bat.game_id
            AND pinch_run.pinch_runner_id = bat.batter_id
            AND pinch_run.side = bat.side
),

offense_union AS (
    SELECT
        game_id,
        player_id,
        side,
        lineup_position,
        entered_game_as,
        -- We're just using this to order so gaps don't matter
        start_event_id AS position_order
    FROM {{ ref('stg_game_lineup_appearances') }}
    UNION ALL
    SELECT
        game_id,
        player_id,
        side,
        lineup_position,
        entered_game_as,
        position_order
    FROM box_offense
    WHERE game_id NOT IN (SELECT game_id FROM event_based)
),

fielding_union AS (
    SELECT
        game_id,
        player_id,
        side,
        fielding_position,
        start_event_id AS position_order
    FROM {{ ref('stg_game_fielding_appearances') }}
    UNION ALL
    SELECT
        game_id,
        fielder_id AS player_id,
        side,
        fielding_position,
        nth_position_played_by_player AS position_order
    FROM {{ ref('stg_box_score_fielding_lines') }}
    WHERE game_id NOT IN (SELECT game_id FROM event_based)
),

offense_agg AS (
    SELECT
        game_id,
        player_id,
        ANY_VALUE(side) AS side,
        BOOL_OR(entered_game_as = 'Starter')::INT1 AS games_started,
        BOOL_OR(entered_game_as = 'PinchHitter')::INT1 AS games_pinch_hit,
        BOOL_OR(entered_game_as = 'PinchRunner')::INT1 AS games_pinch_run,
        BOOL_OR(entered_game_as = 'DefensiveSubstitution')::INT1 AS games_defensive_sub,
        -- Just choose first sub location - We can track courtesy runner situations somewhere else
        LIST(lineup_position ORDER BY position_order)[1] AS lineup_position
    -- Ignore pitcher in DH lineups
    FROM offense_union
    WHERE lineup_position > 0
    GROUP BY 1, 2
),

fielding_agg AS (
    SELECT
        game_id,
        player_id,
        ANY_VALUE(side) AS side,
        -- Sort by fielding position to choose pitcher first in event of Ohtani rule
        LIST(fielding_position ORDER BY position_order, fielding_position) AS fielding_positions,
        (BOOL_OR(fielding_position = 1 AND position_order = 1) AND BOOL_OR(fielding_position = 10 AND position_order = 1) 
        )::INT1 AS games_ohtani_rule
    FROM fielding_union
    -- Keep DH, but ignore PH/PR
    WHERE fielding_position BETWEEN 1 AND 10
    GROUP BY 1, 2 
),

final AS (
    SELECT
        game_id,
        player_id,
        offense_agg.side,
        offense_agg.games_started,
        offense_agg.games_pinch_hit,
        offense_agg.games_pinch_run,
        offense_agg.games_defensive_sub,
        COALESCE(fielding_agg.games_ohtani_rule, 0) AS games_ohtani_rule,
        offense_agg.lineup_position,
        fielding_agg.fielding_positions[1] AS first_fielding_position,
        fielding_agg.fielding_positions,
    FROM offense_agg
    LEFT JOIN fielding_agg USING (game_id, player_id)
)

SELECT * FROM final
