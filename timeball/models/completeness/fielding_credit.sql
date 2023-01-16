WITH hit_locations AS (
    SELECT * FROM {{ ref('event_hit_locations') }}
),

plate_appearances AS (
    SELECT * FROM {{ ref('event_plate_appearances') }}
    WHERE
        plate_appearance_result IN (
            SELECT name FROM {{ ref('plate_appearance_result_types') }} WHERE is_fielded
        )
),

fielding_plays AS (
    SELECT * FROM {{ ref('event_fielding_plays') }}
)

SELECT
    fielding_plays.*,
    event_key,
    pa.plate_appearance_result,
    pa.contact,
    pa.hit_to_fielder,
    hl.general_location,
    hl.depth,
    hl.angle,
    hl.strength
FROM plate_appearances AS pa
LEFT JOIN fielding_plays USING (event_key)
LEFT JOIN hit_locations AS hl USING (event_key)
WHERE pa.hit_to_fielder IS NULL
    AND fielding_plays.event_key IS NULL
    AND hit_locations.event_key IS NOT NULL
ORDER BY event_key
