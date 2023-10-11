WITH date_spine AS (
    SELECT UNNEST(GENERATE_SERIES(
        (SELECT MIN(date) FROM {{ ref('game_start_info') }}),
        (SELECT MAX(date) FROM {{ ref('game_start_info') }}) + 1,
        INTERVAL '1 day'
    ))::DATE AS date
),

cum_wins AS (
    SELECT
        tsi.season,
        tsi.date,
        tsi.league,
        tsi.division,
        tsi.team_name,
        COUNT(CASE WHEN team_id = winning_team_id THEN 1 END) OVER team AS wins,
        COUNT(CASE WHEN team_id = losing_team_id THEN 1 END) OVER team AS losses,
    FROM {{ ref('game_results') }} AS r
    LEFT JOIN  {{ ref('team_game_start_info') }} AS tsi USING (game_id)
    WINDOW team AS (
        PARTITION BY tsi.team_id, tsi.season, tsi.league
        ORDER BY r.game_finish_date
        RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
)

SELECT * FROM cum_wins
WHERE season = 1871
AND team_name = 'New York Mutuals'
ORDER BY 1, 2, 3, 5 DESC
