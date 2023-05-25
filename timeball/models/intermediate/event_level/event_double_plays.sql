WITH flags AS (
    SELECT *
    FROM {{ ref('stg_event_flags') }}
),

dp_flag_types AS (
    SELECT *
    FROM {{ ref('seed_double_play_flag_types') }}
),

final AS (
    SELECT
        flags.event_key,
        BOOL_OR(dp_flag_types.is_double_play) AS is_double_play,
        BOOL_OR(dp_flag_types.is_triple_play) AS is_triple_play,
        BOOL_OR(
            dp_flag_types.is_ground_ball_double_play
        ) AS is_ground_ball_double_play
    FROM flags
    INNER JOIN dp_flag_types USING (flag)
    GROUP BY 1
)

SELECT * FROM final
