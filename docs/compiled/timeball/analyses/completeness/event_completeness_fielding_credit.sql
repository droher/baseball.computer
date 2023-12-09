WITH fielding_play_agg AS (
    SELECT
        event_key,
        NOT BOOL_OR(
            fielding_position = 0 AND fielding_play = 'Putout'
        ) AS has_fielder_putouts,
        NOT BOOL_OR(
            -- Some assists are explicitly recorded as Unknown, but if the putout is unknown
            -- then assists are usually missing entirely
            fielding_position = 0 AND fielding_play IN ('Putout', 'Assist')
        ) AS has_fielder_assists,
        -- As of now we always have the fielder for an error, but just in case
        NOT BOOL_OR(
            fielding_position = 0 AND fielding_play = 'Error'
        ) AS has_fielder_errors
    FROM "timeball"."main_models"."stg_event_fielding_plays"
    GROUP BY 1
),

final AS (
    SELECT
        event_key,
        -- Fielding data may only be present for some plate appearances
        -- and its absence doesn't indicate missing data
        COALESCE(fpa.has_fielder_putouts, TRUE) AS has_fielder_putouts,
        COALESCE(fpa.has_fielder_assists, TRUE) AS has_fielder_assists,
        COALESCE(fpa.has_fielder_errors, TRUE) AS has_fielder_errors
    FROM fielding_play_agg AS fpa
    FULL OUTER JOIN "timeball"."main_models"."stg_events" USING (event_key)
    WHERE plate_appearance_result IS NOT NULL
)

SELECT * FROM final