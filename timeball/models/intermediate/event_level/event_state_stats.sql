{{
  config(
    materialized = 'table',
    )
}}
WITH games AS (
    SELECT * FROM {{ ref('stg_games') }}
),

franchises AS (
    SELECT * FROM {{ ref('stg_franchises') }}
),

teams AS (
    SELECT * FROM {{ ref('stg_game_teams') }}
),

events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

rosters AS (
    SELECT * FROM {{ ref('stg_rosters') }}
),

base_states AS (
    SELECT * FROM {{ ref('event_base_states') }}
),

runs AS (
    SELECT * FROM {{ ref('event_score_states') }}
),

personnel AS (
    SELECT * FROM {{ ref('event_states_wide') }}
),

add_bio AS (
    SELECT
        p.*,
        r_b.bats,
        r_p.throws
    FROM personnel AS p
    INNER JOIN games AS g USING (game_id)
    LEFT JOIN rosters AS r_b
        ON p.batter_id = r_b.player_id
            AND g.season = r_b.year
            AND p.batting_team_id = r_b.team_id
    LEFT JOIN rosters AS r_p
        ON p.defense_1_id = r_p.player_id
            AND g.season = r_p.year
            AND p.fielding_team_id = r_p.team_id
),

game_full AS (
    SELECT
        games.*,
        franchises.league,
        away.team_id AS away_team_id,
        home.team_id AS home_team_id,
    FROM games
    INNER JOIN teams AS away
        ON games.game_id = away.game_id
            AND away.side = 'Away'
    INNER JOIN teams AS home
        ON games.game_id = home.game_id
            AND home.side = 'Home'
    INNER JOIN franchises
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
        -- TODO: Update "weird state" section to get overrides,
        -- and get handedness from chadwick register as fallback
        CASE
            WHEN add_bio.bats = 'B' AND add_bio.throws = 'L' THEN 'R'
            WHEN add_bio.bats = 'B' AND add_bio.throws = 'R' THEN 'L'
            ELSE add_bio.bats
        END AS batter_hand,
        CASE
            WHEN add_bio.throws = 'B' AND add_bio.bats = 'L' THEN 'R'
            WHEN add_bio.throws = 'B' AND add_bio.bats = 'R' THEN 'L'
            ELSE add_bio.throws
        END AS pitcher_hand,
        add_bio.batter_lineup_position,
        g.away_team_id,
        g.home_team_id,
        CASE
            WHEN e.frame = 'Top' AND g.bat_first_side = 'Away' THEN g.away_team_id
            WHEN e.frame = 'Bottom' AND g.bat_first_side = 'Away' THEN g.home_team_id
            WHEN e.frame = 'Top' AND g.bat_first_side = 'Home' THEN g.home_team_id
            WHEN e.frame = 'Bottom' AND g.bat_first_side = 'Home' THEN g.away_team_id
        END AS batting_team_id,
        CASE
            WHEN e.frame = 'Top' AND g.bat_first_side = 'Away' THEN g.home_team_id
            WHEN e.frame = 'Bottom' AND g.bat_first_side = 'Away' THEN g.away_team_id
            WHEN e.frame = 'Top' AND g.bat_first_side = 'Home' THEN g.away_team_id
            WHEN e.frame = 'Bottom' AND g.bat_first_side = 'Home' THEN g.home_team_id
        END AS pitching_team_id,
        add_bio.batter_id,
        add_bio.defense_1_id AS pitcher_id,
        b.first_base_runner_id AS runner_on_first_id,
        b.second_base_runner_id AS runner_on_second_id,
        b.third_base_runner_id AS runner_on_third_id,
    FROM events AS e
    INNER JOIN game_full AS g USING (game_id)
    LEFT JOIN base_states AS b USING (event_key)
    LEFT JOIN runs USING (event_key)
    LEFT JOIN add_bio USING (event_key)
)

SELECT * FROM final
