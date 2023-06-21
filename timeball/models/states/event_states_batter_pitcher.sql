{{ config(
    materialized = 'table',
    )
}}
WITH hand_overrides AS (
    -- This subquery isn't semantically necessary,
    -- but the WHERE clause makes the join much faster
    SELECT
        event_key,
        specified_batter_hand AS bats,
        specified_pitcher_hand AS throws
    FROM {{ ref('stg_events') }}
    WHERE specified_batter_hand IS NOT NULL
        OR specified_pitcher_hand IS NOT NULL
)
SELECT
    lineup.game_id,
    event_key,
    lineup.team_id AS batting_team_id,
    fielding.team_id AS fielding_team_id,
    lineup.batting_side,
    fielding.fielding_side,
    lineup.player_id AS batter_id,
    lineup.lineup_position AS batter_lineup_position,
    fielding.player_id AS pitcher_id,
    -- TODO: Update "weird state" section to get overrides,
    -- and get handedness from chadwick register as fallback
    CASE
        WHEN hand_overrides.bats IS NOT NULL THEN hand_overrides.bats
        WHEN batters.bats = 'B' AND pitchers.throws = 'L' THEN 'R'
        WHEN batters.bats = 'B' AND pitchers.throws = 'R' THEN 'L'
        ELSE NULLIF(batters.bats, 'B')
    END AS batter_hand,
    CASE
        WHEN hand_overrides.throws IS NOT NULL THEN hand_overrides.throws
        WHEN pitchers.throws = 'B' AND batter_hand = 'L' THEN 'R'
        WHEN pitchers.throws = 'B' AND batter_hand = 'R' THEN 'L'
        ELSE NULLIF(pitchers.throws, 'B')
    END AS pitcher_hand,
FROM {{ ref('stg_games') }} AS games
INNER JOIN {{ ref('event_lineup_states') }} AS lineup USING (game_id)
INNER JOIN {{ ref('event_fielding_states') }} AS fielding USING (event_key)
LEFT JOIN hand_overrides USING (event_key)
LEFT JOIN {{ ref('stg_rosters') }} AS batters
    ON lineup.player_id = batters.player_id
        AND games.season = batters.year
        AND lineup.team_id = batters.team_id
LEFT JOIN {{ ref('stg_rosters') }} AS pitchers
    ON fielding.player_id = pitchers.player_id
        AND games.season = pitchers.year
        AND fielding.team_id = pitchers.team_id
WHERE lineup.is_at_bat
    AND fielding.fielding_position = 1
