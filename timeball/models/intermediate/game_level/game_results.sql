WITH gamelog_results AS (
    SELECT
        game_id,
        date,
        duration_minutes,
        home_runs_scored,
        away_runs_scored,
        away_line_score,
        home_line_score,
    FROM {{ ref('stg_gamelog') }}
),

event_and_box_results AS (
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

unioned AS (
    SELECT *
    FROM gamelog_results
    UNION ALL BY NAME
    SELECT *
    FROM event_and_box_results
),

joined AS (
    SELECT
        game_id,
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
        END AS winning_team_score,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN unioned.away_runs_scored
            WHEN winning_team_id = start_info.away_team_id
                THEN unioned.home_runs_scored
        END AS losing_team_score,
        CASE
            WHEN winning_team_id = start_info.home_team_id
                THEN 'Home'
            WHEN winning_team_id = start_info.away_team_id
                THEN 'Away'
        END AS winning_side,
        forfeits.game_id IS NOT NULL AS forfeit_flag,
        suspensions.game_id IS NOT NULL AS suspension_flag,
        winning_team_id IS NULL AS tie_flag,
        unioned.winning_pitcher_id,
        unioned.losing_pitcher_id,
        unioned.save_pitcher_id,
        unioned.game_winning_rbi_player_id,
        unioned.home_runs_scored,
        unioned.away_runs_scored,
        unioned.away_line_score,
        unioned.home_line_score,
        unioned.duration_minutes,
        unioned.duration_outs
    FROM unioned
    INNER JOIN {{ ref('game_start_info') }} AS start_info USING (game_id)
    LEFT JOIN {{ ref('game_suspensions') }} AS suspensions USING (game_id)
    LEFT JOIN {{ ref('game_forfeits') }} AS forfeits USING (game_id)

),

final AS (
    SELECT
        game_id,
        game_finish_date,
        winning_team_id,
        losing_team_id,
        home_runs_scored,
        away_runs_scored,
        winning_team_score,
        losing_team_score,
        winning_side,
        forfeit_flag,
        suspension_flag,
        tie_flag,
        winning_pitcher_id,
        losing_pitcher_id,
        save_pitcher_id,
        game_winning_rbi_player_id,
        away_line_score,
        home_line_score,
        duration_outs,
        duration_minutes
    FROM joined
)

SELECT * FROM final
