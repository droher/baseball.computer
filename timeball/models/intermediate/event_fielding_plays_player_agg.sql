{{
  config(
    materialized = 'table',
    )
}}
WITH fielding_plays AS (
    SELECT *
    FROM {{ ref('stg_event_fielding_plays') }}
),

final AS (
    SELECT
        event_key,
        fielding_position,
        COUNT(*) AS fielding_plays,
        COUNT(CASE WHEN fielding_play = 'Putout' THEN 1 END) AS putouts,
        COUNT(CASE WHEN fielding_play = 'Assist' THEN 1 END) AS assists,
        COUNT(CASE WHEN fielding_play = 'Error' THEN 1 END) AS errors,
        COUNT(CASE WHEN fielding_play = 'FieldersChoice' THEN 1 END) AS fielders_choices,
    FROM fielding_plays
    GROUP BY 1, 2
)

SELECT * FROM final
