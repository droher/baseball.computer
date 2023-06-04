{{
  config(
    materialized = 'table',
    )
}}
WITH add_bio AS (
    SELECT
        states.event_key,
        states.batting_team_id,
        states.fielding_team_id,
        states.batter_id,
        states.batter_lineup_position,
        states.defense_1_id AS pitcher_id,
        -- TODO: Update "weird state" section to get overrides,
        -- and get handedness from chadwick register as fallback
        CASE
            WHEN batters.bats = 'B' AND pitchers.throws = 'L' THEN 'R'
            WHEN batters.bats = 'B' AND pitchers.throws = 'R' THEN 'L'
            ELSE batters.bats
        END AS batter_hand,
        CASE
            WHEN pitchers.throws = 'B' AND batters.bats = 'L' THEN 'R'
            WHEN pitchers.throws = 'B' AND batters.bats = 'R' THEN 'L'
            ELSE pitchers.throws
        END AS pitcher_hand,
    FROM {{ ref('event_states_wide') }} AS states
    INNER JOIN {{ ref('stg_games') }} AS games USING (game_id)
    LEFT JOIN {{ ref('stg_rosters') }} AS batters
        ON states.batter_id = batters.player_id
            AND games.season = batters.year
            AND states.batting_team_id = batters.team_id
    LEFT JOIN {{ ref('stg_rosters') }} AS pitchers
        ON states.defense_1_id = pitchers.player_id
            AND games.season = pitchers.year
            AND states.fielding_team_id = pitchers.team_id
),

game_full AS (
    SELECT
        games.*,
        franchises.league,
        away.team_id AS away_team_id,
        home.team_id AS home_team_id,
    FROM {{ ref('stg_games') }} AS games
    INNER JOIN {{ ref('stg_game_teams') }} AS away
        ON games.game_id = away.game_id
            AND away.side = 'Away'
    INNER JOIN {{ ref('stg_game_teams') }} AS home
        ON games.game_id = home.game_id
            AND home.side = 'Home'
    INNER JOIN {{ ref('stg_franchises') }} AS franchises
        ON home.team_id = franchises.team_id
            AND games.season BETWEEN franchises.season_start AND franchises.season_end
),

final AS (
    SELECT
        game_id,
        e.event_id,
        e.event_key,
        g.season,
        g.league,
        g.game_type,
        g.date,
        g.park_id,
        e.frame,
        e.inning,
        e.outs,
        e.count_balls,
        e.count_strikes,
        e.batting_side,
        CASE WHEN e.batting_side = 'Home' THEN 'Away' ELSE 'Home' END AS fielding_side,
        COALESCE(b.base_state, 0) AS base_state,
        runs.score_home_start AS score_home,
        runs.score_away_start AS score_away,
        add_bio.batter_hand,
        add_bio.pitcher_hand,
        add_bio.batter_lineup_position,
        g.away_team_id,
        g.home_team_id,
        add_bio.batting_team_id,
        add_bio.fielding_team_id,
        add_bio.batter_id,
        add_bio.pitcher_id,
        b.first_base_runner_id AS runner_on_first_id,
        b.second_base_runner_id AS runner_on_second_id,
        b.third_base_runner_id AS runner_on_third_id,
        -- TODO: Future state ok to include here?
        runs.runs_on_play,
    FROM {{ ref('stg_events') }} AS e
    INNER JOIN game_full AS g USING (game_id)
    LEFT JOIN {{ ref('event_base_states') }} AS b USING (event_key)
    LEFT JOIN {{ ref('event_score_states') }} AS runs USING (event_key)
    LEFT JOIN add_bio USING (event_key)
)

SELECT * FROM final
