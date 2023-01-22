{{
  config(
    materialized = 'table',
    )
}}
WITH states AS (
    SELECT *
    FROM {{ ref('event_fielding_states') }}
),

fielding_plays AS (
    SELECT *
    FROM {{ ref('event_fielding_plays') }}
),

double_plays AS (
    SELECT *
    FROM {{ ref('event_double_plays') }}
),

outs AS (
    SELECT *
    FROM {{ ref('event_outs') }}
),

plate_appearances AS (
    SELECT *
    FROM {{ ref('event_plate_appearances') }}
),

pa_result_types AS (
    SELECT *
    FROM {{ ref('plate_appearance_result_types') }}
),

outs_agg AS (
    SELECT
        event_key,
        ROUND(COUNT(*) / 3::NUMERIC, 2) AS innings,
    FROM outs
    GROUP BY 1
),

fielding_plays_agg AS (
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
),

final AS (
    SELECT
        event_key,
        fielding_position,
        s.player_id AS fielder_id,
        s.team_id AS fielding_team_id,
        CASE WHEN fielding_position != 'Unknown' THEN oa.innings ELSE 0 END AS innings_played,
        CASE WHEN fielding_position = 'Unknown' THEN oa.innings ELSE 0 END AS unknown_innings_played,
        COALESCE((pa.event_key IS NOT NULL), 0) AS plate_appearances_in_field,
        CASE WHEN prt.is_in_play THEN 1 ELSE 0 END AS plate_appearances_in_field_with_ball_in_play,
        CASE WHEN pa.hit_to_fielder = fielding_position THEN 1 ELSE 0 END AS balls_fielded,
        COALESCE(fp.fielding_plays, 0) AS fielding_plays,
        COALESCE(fp.putouts, 0) AS putouts,
        COALESCE(fp.assists, 0) AS assists,
        COALESCE(fp.errors, 0) AS errors,
        COALESCE(fp.fielders_choices, 0) AS fielders_choices,
        COALESCE((dp.is_double_play)::INT, 0) AS double_plays,
        COALESCE((dp.is_triple_play)::INT, 0) AS triple_plays,
        COALESCE((dp.is_ground_ball_double_play)::INT, 0) AS ground_ball_double_plays,
    FROM states AS s
    LEFT JOIN outs_agg AS oa USING (event_key)
    LEFT JOIN plate_appearances AS pa USING (event_key)
    LEFT JOIN pa_result_types AS prt ON pa.plate_appearance_result = prt.name
    LEFT JOIN double_plays AS dp USING (event_key)
    FULL OUTER JOIN fielding_plays_agg AS fp USING (event_key, fielding_position)
)

SELECT * FROM final
