WITH overall_rates AS (
    SELECT
        substring(game_id, 4, 4)::int AS season,
        team_id,
        COUNT(*) AS total_singles,
        COUNT_IF(batted_location_known)/total_singles AS total_singles_location_rate,
        COUNT_IF(contact_broad_type_known)/total_singles AS total_singles_contact_rate,
        COUNT_IF(contact_type_ground_ball)/COUNT_IF(contact_broad_type_known) AS total_ground_ball_rate,
        COUNT_IF(contact_type_line_drive)/COUNT_IF(contact_broad_type_air_ball) AS total_line_drive_rate,
    FROM {{ ref('event_offense_stats') }} e
    WHERE singles = 1
        AND bunts = 0
    AND season < 2003
    GROUP BY 1, 2
),

final AS (
    SELECT
        season,
        player_id,
        o.team_id,
        COUNT(*) AS hits_in_play,
        COUNT_IF(batted_location_known)/hits_in_play AS hits_location_rate,
        COUNT_IF(contact_broad_type_known)/hits_in_play AS hits_contact_rate,
        COUNT_IF(contact_type_ground_ball)/COUNT_IF(contact_broad_type_known) AS ground_ball_rate,
        COUNT_IF(contact_type_line_drive)/COUNT_IF(contact_broad_type_air_ball) AS line_drive_rate,
        hits_location_rate - ANY_VALUE(total_singles_location_rate) AS location_rate_diff,
        hits_contact_rate - ANY_VALUE(total_singles_contact_rate) AS contact_rate_diff,
        ground_ball_rate - ANY_VALUE(total_ground_ball_rate) AS ground_ball_rate_diff,
        line_drive_rate - ANY_VALUE(total_line_drive_rate) AS line_drive_rate_diff
    FROM {{ ref('event_offense_stats') }} e
    INNER JOIN overall_rates o
        ON o.season = substring(e.game_id, 4, 4)::int
        AND o.team_id = e.team_id
    WHERE balls_in_play = 1
        AND singles = 1
        AND bunts = 0
    GROUP BY 1, 2, 3
)

SELECT * FROM final
WHERE player_id LIKE 'youne%'