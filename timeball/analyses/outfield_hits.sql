
WITH t AS (
    SELECT bats,
    substring(eos.game_id, 4, 4)::int as year,
    contact
FROM {{ ref('event_offense_stats') }} eos
JOIN {{ ref('calc_batted_ball_type') }} USING (event_key)
JOIN {{ ref('stg_people') }} p ON player_id = retrosheet_player_id
WHERE balls_in_play = 1
)
PIVOT t
ON contact
USING COUNT(*)
GROUP BY bats, year
ORDER BY year desc, bats