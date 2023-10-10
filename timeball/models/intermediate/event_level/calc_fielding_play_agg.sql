WITH final AS (
    SELECT
        event_key,
        fielding_position,
        COUNT(*)::UTINYINT AS fielding_plays,
        COUNT(*) FILTER (WHERE fielding_play = 'Putout')::UTINYINT AS putouts,
        COUNT(*) FILTER (WHERE fielding_play = 'Assist')::UTINYINT AS assists,
        COUNT(*) FILTER (WHERE fielding_play = 'Error')::UTINYINT AS errors,
        COUNT(*) FILTER (WHERE fielding_play = 'FieldersChoice')::UTINYINT AS fielders_choices,
        COUNT(*) FILTER (WHERE sequence_id = 1 AND fielding_play != 'Error')::UTINYINT AS plays_started,
    FROM {{ ref('stg_event_fielding_plays') }}
    GROUP BY 1, 2
)

SELECT * FROM final
