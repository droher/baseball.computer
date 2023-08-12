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
    lineup.game_id,
    event_key,
    lineup.batting_team_id,
    fielding.fielding_team_id,
    lineup.batting_side,
    fielding.fielding_side,
    lineup.player_id AS batter_id,
    lineup.lineup_position AS batter_lineup_position,
    fielding.player_id AS pitcher_id,
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
INNER JOIN {{ ref('event_personnel_lookup') }} AS lookup USING (event_key)
INNER JOIN {{ ref('personnel_fielding_states') }} AS fielding
    ON lookup.personnel_fielding_key = fielding.personnel_fielding_key
        AND fielding.fielding_position = 1
INNER JOIN {{ ref('personnel_lineup_states') }} AS lineup
    ON lookup.personnel_lineup_key = lineup.personnel_lineup_key
        AND lineup.lineup_position = events.at_bat
LEFT JOIN overrides USING (event_key)
LEFT JOIN {{ ref('stg_rosters') }} AS batters
    ON lineup.player_id = batters.player_id
        AND games.season = batters.year
        AND lineup.batting_team_id = batters.team_id
LEFT JOIN {{ ref('stg_rosters') }} AS pitchers
    ON fielding.player_id = pitchers.player_id
        AND games.season = pitchers.year
        AND fielding.fielding_team_id = pitchers.team_id
