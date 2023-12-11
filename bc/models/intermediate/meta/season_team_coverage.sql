WITH final AS (
    SELECT
        season,
        team_id,
        ANY_VALUE(league) AS league,
        CASE WHEN BOOL_AND(source_type = 'PlayByPlay')
                THEN 'PlayByPlay'
            WHEN BOOL_AND(source_type = 'PlayByPlay' OR source_type = 'BoxScore')
                THEN 'BoxScore'
            ELSE 'GameLog'
        END AS least_granular_source_type
    FROM {{ ref('team_game_start_info') }}
    WHERE game_id NOT IN (SELECT game_id FROM {{ ref('game_forfeits') }})
        AND (game_type != 'Exhibition' OR league IS NULL)
    GROUP BY 1, 2
)

SELECT * FROM final
