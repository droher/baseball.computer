WITH source AS (
    SELECT * FROM {{ source('misc', 'gamelog') }}
),

renamed AS (
    SELECT
        extract(year from date) AS season,
        date::DATE AS date, -- noqa: RF04
        CASE double_header
            WHEN 0 THEN 'SingleGame'
            WHEN 1 THEN 'DoubleHeaderGame1'
            WHEN 2 THEN 'DoubleHeaderGame2'
            WHEN 3 THEN 'DoubleHeaderGame3'
        END AS doubleheader_status,
        home_team || strftime(date, '%Y%m%d') || double_header AS game_id,
        visiting_team,
        home_team,
        -- TODO: Fix spelling in original
        visitor_runs_scored AS visitor_runs_score,
        home_runs_score,
        forfeit_info,
        park_id,
        attendance,
        duration,
        -- TODO: Fix spelling in original
        vistor_line_score AS visitor_line_score,
        home_line_score,
        umpire_h_id,
        umpire_1b_id,
        umpire_2b_id,
        umpire_3b_id,
        visitor_starting_pitcher_id,
        home_starting_pitcher_id,
        additional_info,
    FROM source
)

SELECT date, visiting_team, home_team, visitor_manager_id, home_manager_id
FROM source
WHERE visiting_team = 'FW1' OR home_team = 'FW1'
AND EXTRACT(year FROM date) = 1871
ORDER BY date