WITH t AS (
    SELECT
        e.batter_id,
        c.contact,
        e.batted_to_fielder,
        COUNT(*)
    FROM {{ ref('stg_events') }} e
    INNER JOIN {{ ref('calc_batted_ball_type') }} AS c USING (event_key)
    WHERE e.plate_appearance_result = 'InPlayOut'
    GROUP BY 1, 2, 3

)

SELECT * FROM t
