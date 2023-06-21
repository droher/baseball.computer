WITH inning_agg AS (
    SELECT
        game_id,
        batting_side,
        inning_start,
        SUM(runs_on_play) AS inning_runs,
        CASE
            WHEN inning_runs >= 10
                THEN CONCAT('(', inning_runs, ')')
            ELSE inning_runs::STRING
        END AS inning_runs_string
    FROM {{ ref('event_states_full') }}
    GROUP BY 1, 2, 3
),

game_agg AS (
    SELECT
        game_id,
        batting_side,
        SUM(inning_runs) AS total_runs,
        STRING_AGG(inning_runs_string, '') AS line_score,
        LIST(inning_runs ORDER BY inning_start) AS line_score_list
    FROM inning_agg
    GROUP BY 1, 2
),

side_agg AS (
    SELECT
        game_id,
        FIRST(total_runs) FILTER (WHERE batting_side = 'Home') AS total_runs_home,
        FIRST(total_runs) FILTER (WHERE batting_side = 'Away') AS total_runs_away,
        FIRST(line_score) FILTER (WHERE batting_side = 'Home') AS line_score_home,
        FIRST(line_score) FILTER (WHERE batting_side = 'Away') AS line_score_away,
        FIRST(line_score_list) FILTER (WHERE batting_side = 'Home') AS line_score_list_home,
        FIRST(line_score_list) FILTER (WHERE batting_side = 'Away') AS line_score_list_away
    FROM game_agg
    GROUP BY 1
)

SELECT * REPLACE (
    CASE WHEN LENGTH(line_score_home) > LENGTH(line_score_away)
        THEN line_score_away || 'x'
        ELSE line_score_away
    END AS line_score_away,
    CASE WHEN LENGTH(line_score_away) > LENGTH(line_score_home)
        THEN line_score_home || 'x'
        ELSE line_score_home
    END AS line_score_home,
)
FROM side_agg
