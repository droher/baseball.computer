WITH base AS (
    SELECT
        game_id,
        home_team_id AS team_id,
        away_team_id AS opponent_id,
        'Home' AS team_side,
        * EXCLUDE (game_id, home_team_id, away_team_id) -- noqa,
    FROM {{ ref('game_start_info') }}
    UNION ALL
    SELECT
        game_id,
        away_team_id AS team_id,
        home_team_id AS opponent_id,
        'Away' AS team_side,
        * EXCLUDE (game_id, home_team_id, away_team_id) -- noqa
    FROM {{ ref('game_start_info') }}
),

add_series_start_flag AS (
    SELECT
        *,
        CASE
            WHEN LAG(opponent_id, 1, 'N/A') OVER season_series != opponent_id
                THEN game_id
        END AS series_id
    FROM base
    WINDOW season_series AS (
        PARTITION BY season, team_id, game_type, opponent_id
        ORDER BY date, game_id
    )
),

assign_series_id AS (
    SELECT -- noqa: AM04
        * REPLACE (
            -- The closest non-null value to the current row (inclusive) is the proper series_id.
            COALESCE(LAG(series_id IGNORE NULLS) OVER season_series, series_id) AS series_id
        )
    FROM add_series_start_flag
    WINDOW season_series AS (
        PARTITION BY season, team_id, game_type, opponent_id
        ORDER BY date, game_id
    )
)

SELECT
    *,
    COUNT(*) OVER season AS season_game_number,
    COUNT(*) OVER series AS series_game_number,
    DATEDIFF('day', LAG(date) OVER season, date) AS days_since_last_game,
FROM assign_series_id
WINDOW
    season AS (
        PARTITION BY season, team_id, game_type
        ORDER BY date, game_id
    ),
    series AS (
        PARTITION BY team_id, series_id
        ORDER BY date, game_id
    )
