{{ config(
    materialized = 'table',
    )
}}
WITH overrides AS (
    -- This subquery isn't semantically necessary,
    -- but the WHERE clause makes the join much faster
    SELECT
        event_key,
        specified_batter_hand AS bats,
        specified_pitcher_hand AS throws,
        strikeout_responsible_batter_id,
        walk_responsible_pitcher_id
    FROM {{ ref('stg_events') }}
    WHERE specified_batter_hand IS NOT NULL
        OR specified_pitcher_hand IS NOT NULL
        OR strikeout_responsible_batter_id IS NOT NULL
        OR walk_responsible_pitcher_id IS NOT NULL
)
SELECT
    events.game_id,
    event_key,
    events.batting_side,
    CASE WHEN events.batting_side = 'Home' THEN 'Away' ELSE 'Home' END AS fielding_side,
    CASE WHEN events.batting_side = 'Home' THEN games.home_team_id ELSE games.away_team_id END AS batting_team_id,
    CASE WHEN events.batting_side = 'Home' THEN games.away_team_id ELSE games.home_team_id END AS fielding_team_id,
    events.batter_id,
    events.batter_lineup_position,
    events.pitcher_id,
    -- TODO: Update "weird state" section to get overrides,
    -- and get handedness from chadwick register as fallback
    CASE
        WHEN overrides.bats IS NOT NULL THEN overrides.bats
        WHEN batters.bats = 'B' AND pitchers.throws = 'L' THEN 'R'
        WHEN batters.bats = 'B' AND pitchers.throws = 'R' THEN 'L'
        ELSE NULLIF(batters.bats, 'B')
    END AS batter_hand,
    CASE
        WHEN overrides.throws IS NOT NULL THEN overrides.throws
        WHEN pitchers.throws = 'B' AND batter_hand = 'L' THEN 'R'
        WHEN pitchers.throws = 'B' AND batter_hand = 'R' THEN 'L'
        ELSE NULLIF(pitchers.throws, 'B')
    END AS pitcher_hand,
    overrides.strikeout_responsible_batter_id,
    overrides.walk_responsible_pitcher_id,
FROM {{ ref('stg_events') }} AS events
INNER JOIN {{ ref('stg_games') }} AS games USING (game_id)
LEFT JOIN overrides USING (event_key)
LEFT JOIN {{ ref('stg_rosters') }} AS batters
    ON events.batter_id = batters.player_id
        AND games.season = batters.year
        AND CASE WHEN events.batting_side = 'Home' THEN games.home_team_id ELSE games.away_team_id END = batters.team_id
LEFT JOIN {{ ref('stg_rosters') }} AS pitchers
    ON events.pitcher_id = pitchers.player_id
        AND games.season = pitchers.year
        AND CASE WHEN events.batting_side = 'Home' THEN games.away_team_id ELSE games.home_team_id END = pitchers.team_id
