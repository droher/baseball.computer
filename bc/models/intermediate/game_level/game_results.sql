{{
  config(
    materialized = 'table',
    )
}}
WITH event_and_box_results AS (
    SELECT
        game_id,
        games.date,
        games.duration_minutes,
        games.winning_pitcher_id,
        games.losing_pitcher_id,
        games.save_pitcher_id,
        games.game_winning_rbi_player_id,
        line_scores.home_runs_scored,
        line_scores.away_runs_scored,
        line_scores.home_line_score,
        line_scores.away_line_score,
        line_scores.duration_outs,
    FROM {{ ref('stg_games') }} AS games
    LEFT JOIN {{ ref('game_line_scores') }} AS line_scores USING (game_id)

),

gamelog_results AS (
    SELECT
        game_id,
        date,
        duration_minutes,
        home_runs_scored,
        away_runs_scored,
        away_line_score,
        home_line_score,
    FROM {{ ref('stg_gamelog') }}
    WHERE game_id NOT IN (SELECT game_id FROM event_and_box_results)
),

unioned AS (
    SELECT *
    FROM gamelog_results
    UNION ALL BY NAME
    SELECT *
    FROM event_and_box_results
),

final AS (
    SELECT
        game_id,
        start_info.season,
        start_info.game_type,
        COALESCE(suspensions.date_resumed, unioned.date) AS game_finish_date,
        start_info.home_team_id,
        start_info.away_team_id,
        CASE
            WHEN forfeits.winning_side = 'Home'
                THEN start_info.home_team_id
            WHEN forfeits.winning_side = 'Away'
                THEN start_info.away_team_id
            WHEN unioned.home_runs_scored > unioned.away_runs_scored
                THEN start_info.home_team_id
            WHEN unioned.home_runs_scored < unioned.away_runs_scored
                THEN start_info.away_team_id
        END AS winning_team_id,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN start_info.away_team_id
            WHEN winning_team_id = start_info.away_team_id
                THEN start_info.home_team_id
        END AS losing_team_id,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN unioned.home_runs_scored
            WHEN winning_team_id = start_info.away_team_id
                THEN unioned.away_runs_scored
        END::UTINYINT AS winning_team_score,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN unioned.away_runs_scored
            WHEN winning_team_id = start_info.away_team_id
                THEN unioned.home_runs_scored
        END::UTINYINT AS losing_team_score,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN 'Home'
            WHEN winning_team_id = start_info.away_team_id
                THEN 'Away'
        END::SIDE AS winning_side,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN 'Away'
            WHEN winning_team_id = start_info.away_team_id
                THEN 'Home'
        END::SIDE AS losing_side,
        forfeits.game_id IS NOT NULL AS forfeit_flag,
        suspensions.game_id IS NOT NULL AS suspension_flag,
        winning_team_id IS NULL AS tie_flag,
        unioned.winning_pitcher_id,
        unioned.losing_pitcher_id,
        unioned.save_pitcher_id,
        unioned.game_winning_rbi_player_id,
        unioned.home_runs_scored::UTINYINT AS home_runs_scored,
        unioned.away_runs_scored::UTINYINT AS away_runs_scored,
        unioned.away_line_score,
        unioned.home_line_score,
        unioned.duration_minutes::USMALLINT AS duration_minutes,
        unioned.duration_outs,
        -- We'll assume for now that games without recorded outs are 9 innings by default
        COALESCE(unioned.duration_outs BETWEEN 51 AND 54, TRUE) AS is_nine_inning_game,
        COALESCE(unioned.duration_outs > 54, FALSE) AS is_extra_inning_game,
        COALESCE(unioned.duration_outs < 51, FALSE) AS is_shortened_game,
    FROM unioned
    INNER JOIN {{ ref('game_start_info') }} AS start_info USING (game_id)
    LEFT JOIN {{ ref('game_suspensions') }} AS suspensions USING (game_id)
    LEFT JOIN {{ ref('game_forfeits') }} AS forfeits USING (game_id)
)

SELECT * FROM final
