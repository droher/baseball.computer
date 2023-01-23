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
    FROM {{ ref('stg_event_fielding_plays') }}
),

double_plays AS (
    SELECT *
    FROM {{ ref('event_double_plays') }}
),

outs AS (
    SELECT *
    FROM {{ ref('stg_event_outs') }}
),

plate_appearances AS (
    SELECT *
    FROM {{ ref('stg_event_plate_appearances') }}
),

pa_result_types AS (
    SELECT *
    FROM {{ ref('plate_appearance_result_types') }}
),

fielding_plays_agg AS (
    SELECT *
    FROM {{ ref('event_fielding_plays_player_agg') }}
),

outs_agg AS (
    SELECT
        event_key,
        ROUND(COUNT(*) / 3::NUMERIC, 2) AS innings_played,
    FROM outs
    GROUP BY 1
),

-- Join these together before the end to avoid an
-- unnecessary fanout
event_level_agg AS (
    SELECT
        event_key,
        oa.innings_played,
        pa.hit_to_fielder,
        (pa.event_key IS NOT NULL)::INT AS plate_appearances_in_field,
        CASE WHEN prt.is_in_play THEN 1 ELSE 0 END AS plate_appearances_in_field_with_ball_in_play,
        dp.is_double_play,
        dp.is_triple_play,
        dp.is_ground_ball_double_play
    FROM outs_agg AS oa
    FULL OUTER JOIN plate_appearances AS pa USING (event_key)
    LEFT JOIN pa_result_types AS prt ON pa.plate_appearance_result = prt.name
    LEFT JOIN double_plays AS dp USING (event_key)
),

final AS (
    SELECT
        event_key,
        s.fielding_position,
        s.player_id AS fielder_id,
        s.team_id AS fielding_team_id,
        e.innings_played,
        e.plate_appearances_in_field,
        e.plate_appearances_in_field_with_ball_in_play,
        CASE WHEN e.hit_to_fielder = s.fielding_position THEN 1 ELSE 0 END AS balls_fielded,
        COALESCE(fp.fielding_plays, 0) AS fielding_plays,
        COALESCE(fp.putouts, 0) AS putouts,
        COALESCE(fp.assists, 0) AS assists,
        COALESCE(fp.errors, 0) AS errors,
        COALESCE(fp.fielders_choices, 0) AS fielders_choices,
        -- Only count double plays for the fielder who made a putout
        -- or assist on the play
        CASE WHEN e.is_double_play AND fp.putouts + fp.assists > 0
            THEN 1
            ELSE 0
        END AS double_plays,
        CASE WHEN e.is_triple_play AND fp.putouts + fp.assists > 0
            THEN 1
            ELSE 0
        END AS triple_plays,
        CASE WHEN e.is_ground_ball_double_play AND fp.putouts + fp.assists > 0
            THEN 1
            ELSE 0
        END AS ground_ball_double_plays
    FROM states AS s
    LEFT JOIN event_level_agg AS e USING (event_key)
    LEFT JOIN fielding_plays_agg AS fp USING (event_key, fielding_position)
)

SELECT * FROM final
