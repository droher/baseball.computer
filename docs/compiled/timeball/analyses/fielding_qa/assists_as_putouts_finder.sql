WITH game_info AS (
    SELECT *
    FROM "timeball"."main_models"."game_start_info"
),

basic_groundouts AS (
    SELECT
        event_key
    FROM "timeball"."main_models"."calc_fielding_play_agg"
    GROUP BY 1
    HAVING COUNT_IF(fielding_position = 3 and assisted_putouts = 1) = 1
        AND SUM(assists) = 1
        AND COUNT(*) = 2
),

e AS (
    SELECT
        e.game_id,
        s.team_id,
        COUNT_IF(
            e.batted_to_fielder BETWEEN 1 AND 6
            AND e.outs_on_play = 1
            AND e.batted_trajectory = 'Unknown'
            AND f.event_key IS NULL
            AND s.putouts - s.assisted_putouts = 1
            AND e.base_state % 2 = 0
        ) AS no_force_unmarked_popups,
        COUNT_IF(basic_groundouts.event_key IS NOT NULL) AS infield_conventional_groundouts,
        GREATEST(2 * no_force_unmarked_popups - infield_conventional_groundouts, no_force_unmarked_popups) AS rating
    FROM "timeball"."main_models"."stg_events" e
    INNER JOIN "timeball"."main_models"."event_fielding_stats" s USING (event_key)
    LEFT JOIN "timeball"."main_models"."stg_event_flags" f USING (event_key)
    LEFT JOIN basic_groundouts USING (event_key)
    WHERE COALESCE(f.flag, 'Foul') = 'Foul'
    GROUP BY 1, 2
)

SELECT
    e.*,
    filename,
    line_number,
    scorer,
    park_id,
    scoring_method,
    inputter,
    translator,
    date_inputted,
FROM e
INNER JOIN "timeball"."main_models"."stg_games" USING (game_id)
INNER JOIN "timeball"."main_models"."stg_event_audit" a USING (game_id)
WHERE a.event_id = 1
    AND no_force_unmarked_popups > 1
ORDER BY rating DESC, game_id