{{
  config(
    materialized = 'table',
    )
}}
WITH event_offense AS (
    SELECT
        game_id,
        player_id,
        side,
        lineup_position
    FROM {{ ref('stg_game_lineup_appearances') }}
    WHERE start_event_id = 1
        -- Full outer join will cover pitchers
        AND lineup_position > 0
),

event_fielding AS (
    SELECT
        game_id,
        player_id,
        side,
        -- This is to choose pitcher over DH
        -- when Ohtani is doing both.
        MIN(fielding_position) AS fielding_position
    FROM {{ ref('stg_game_fielding_appearances') }}
    WHERE start_event_id = 1
    GROUP BY 1, 2, 3
),

box_offense AS (
    SELECT
        game_id,
        batter_id,
        side,
        lineup_position
    FROM {{ ref('stg_box_score_batting_lines') }}
    WHERE nth_player_at_position = 1
),

box_fielding AS (
    SELECT
        game_id,
        fielder_id,
        side,
        fielding_position
    FROM {{ ref('stg_box_score_fielding_lines') }}
    -- This doesn't fully filter down to starters,
    -- just the first position played by any player,
    -- starter or sub.
    WHERE nth_position_played_by_player = 1
    -- TODO: Remove deduper after resolution of PH5194105241
    QUALIFY COUNT(*) OVER (PARTITION BY game_id, side, fielding_position) = 1
),

event_joined AS (
    SELECT
        game_id,
        player_id,
        side,
        COALESCE(event_offense.lineup_position, 0) AS lineup_position,
        event_fielding.fielding_position
    -- TODO: Figure out Ohtani filter
    FROM event_offense
    FULL OUTER JOIN event_fielding
        USING (game_id, player_id, side)
),

box_joined AS (
    SELECT
        box_offense.game_id,
        box_offense.batter_id AS player_id,
        box_offense.side,
        box_offense.lineup_position,
        box_fielding.fielding_position
    FROM box_offense
    INNER JOIN box_fielding
        ON box_offense.game_id = box_fielding.game_id
            AND box_offense.batter_id = box_fielding.fielder_id
            AND box_offense.side = box_fielding.side
    WHERE box_offense.game_id NOT IN (
            SELECT DISTINCT game_id
            FROM event_joined
        )
),

unioned AS (
    SELECT *
    FROM event_joined
    UNION ALL
    SELECT *
    FROM box_joined
),

agged AS (
    SELECT
        game_id,
        MAP(
            LIST(lineup_position ORDER BY lineup_position) FILTER (WHERE side = 'Away'),
            LIST(player_id ORDER BY lineup_position) FILTER (WHERE side = 'Away')
        ) AS lineup_map_away,
        MAP(
            LIST(fielding_position ORDER BY fielding_position) FILTER (WHERE side = 'Away'),
            LIST(player_id ORDER BY fielding_position) FILTER (WHERE side = 'Away')
        ) AS fielding_map_away,
        MAP(
            LIST(lineup_position ORDER BY lineup_position) FILTER (WHERE side = 'Home'),
            LIST(player_id ORDER BY lineup_position) FILTER (WHERE side = 'Home')
        ) AS lineup_map_home,
        MAP(
            LIST(fielding_position ORDER BY fielding_position) FILTER (WHERE side = 'Home'),
            LIST(player_id ORDER BY fielding_position) FILTER (WHERE side = 'Home')
        ) AS fielding_map_home
    FROM unioned
    GROUP BY 1
)

SELECT * FROM agged
