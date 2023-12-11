{{ config(
    materialized = 'table',
    )
}}
WITH joined AS (
    SELECT
        events.game_id,
        events.event_key,
        events.batting_side,
        CASE WHEN events.batting_side = 'Home' THEN 'Away' ELSE 'Home' END::SIDE AS fielding_side,
        events.batting_team_id,
        events.fielding_team_id,
        events.batter_id,
        events.batter_lineup_position,
        batter_field.fielding_position AS batter_fielding_position,
        events.pitcher_id,
        CASE
            WHEN events.specified_batter_hand IS NOT NULL THEN events.specified_batter_hand
            WHEN batters.bats = 'B' AND pitchers.throws = 'L' THEN 'R'
            WHEN batters.bats = 'B' AND pitchers.throws = 'R' THEN 'L'
            ELSE NULLIF(batters.bats, 'B')
        END::HAND AS batter_hand,
        CASE
            WHEN events.specified_pitcher_hand IS NOT NULL THEN events.specified_pitcher_hand
            WHEN pitchers.throws = 'B' AND batter_hand = 'L' THEN 'R'
            WHEN pitchers.throws = 'B' AND batter_hand = 'R' THEN 'L'
            ELSE NULLIF(pitchers.throws, 'B')
        END::HAND AS pitcher_hand,
        events.strikeout_responsible_batter_id,
        events.walk_responsible_pitcher_id,
        {# fielders.catcher_id,
        fielders.first_base_id,
        fielders.second_base_id,
        fielders.third_base_id,
        fielders.shortstop_id,
        fielders.left_field_id,
        fielders.center_field_id,
        fielders.right_field_id #}
    FROM {{ ref('stg_events') }} AS events
    {# INNER JOIN {{ ref('event_fielders_flat') }} AS fielders USING (event_key) #}
    LEFT JOIN {{ ref('people') }} AS batters
        ON events.batter_id = batters.player_id
    LEFT JOIN {{ ref('people') }} AS pitchers
        ON events.pitcher_id = pitchers.player_id
    LEFT JOIN {{ ref('stg_game_fielding_appearances') }} AS batter_field
        ON events.game_id = batter_field.game_id
            AND events.batter_id = batter_field.player_id
            AND events.event_id BETWEEN batter_field.start_event_id AND batter_field.end_event_id
),

the_singular_exception_of_shohhei_ohtani AS (
    SELECT *
    FROM joined
    -- Choose DH when he's both, just because
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY batter_fielding_position DESC) = 1
)


SELECT * FROM the_singular_exception_of_shohhei_ohtani
