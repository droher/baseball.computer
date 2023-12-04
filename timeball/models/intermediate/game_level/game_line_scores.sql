WITH event_lines AS (
    SELECT
        game_id,
        batting_side,
        inning_start AS inning,
        SUM(runs_on_play) AS runs,
        SUM(outs_on_play) AS outs,
    FROM {{ ref('event_states_full') }}
    GROUP BY 1, 2, 3
),

unioned AS (
    SELECT *
    FROM event_lines
    UNION ALL BY NAME
    
    SELECT *
    FROM {{ ref('stg_box_score_line_scores') }}
    WHERE game_id NOT IN (SELECT DISTINCT game_id FROM event_lines)
),

game_agg AS (
    SELECT
        game_id,
        batting_side AS side,
        SUM(runs) AS total_runs,
        STRING_AGG(CASE
            WHEN runs >= 10
                THEN CONCAT('(', runs, ')')
            ELSE runs::STRING
        END, '') AS line_score,
        SUM(outs) AS duration_outs,
        LIST(runs::UTINYINT ORDER BY inning) AS line_score_list
    FROM unioned
    GROUP BY 1, 2
),

box_outs AS (
    SELECT
        game_id,
        side,
        SUM(outs_recorded) AS duration_outs
    FROM {{ ref('stg_box_score_pitching_lines') }}
    GROUP BY 1, 2
),

side_agg AS (
    SELECT
        game_id,
        FIRST(g.total_runs) FILTER (WHERE side = 'Home') AS home_runs_scored,
        FIRST(g.total_runs) FILTER (WHERE side = 'Away') AS away_runs_scored,
        FIRST(g.line_score) FILTER (WHERE side = 'Home') AS home_line_score,
        FIRST(g.line_score) FILTER (WHERE side = 'Away') AS away_line_score,
        FIRST(g.line_score_list) FILTER (WHERE side = 'Home') AS home_line_score_list,
        FIRST(g.line_score_list) FILTER (WHERE side = 'Away') AS away_line_score_list,
        SUM(COALESCE(g.duration_outs, box_outs.duration_outs))::UTINYINT AS duration_outs,
    FROM game_agg AS g
    LEFT JOIN box_outs USING (game_id, side)
    GROUP BY 1
),

final AS (
    SELECT * REPLACE (
        CASE WHEN LENGTH(home_line_score) > LENGTH(away_line_score)
            THEN away_line_score || 'x'
            ELSE away_line_score
        END AS away_line_score,
        CASE WHEN LENGTH(away_line_score) > LENGTH(home_line_score)
            THEN home_line_score || 'x'
            ELSE home_line_score
        END AS home_line_score,    
    )
    FROM side_agg
)

SELECT * FROM final
