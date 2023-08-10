-- SELECT game_id,
--     pitcher_id,
--     team_id,
--     opponent_id,
--     wins,
--     losses,
--     --games,
--     games_started,
--     games_finished,
--     complete_games,
--     shutouts,
--     saves,
--     innings_pitched,
--     --hits,
--     runs,
--     earned_runs,
--     --home_runs,
--     --walks,
--     --intentional_walks,
--     --strikeouts,
--     --hit_by_pitch,
--     balks,
--     wild_pitches,
--     batters_faced,
--     wins_in_games_started,
--     losses_in_games_started,
--     team_wins_in_games_started,
--     team_losses_in_games_started,
--     no_decisions,
--     quality_starts,
--     cheap_wins,
--     tough_losses,
--     losses_in_save_situations,
--     game_score,
--     bequeathed_runners,
--     bequeathed_runners_scored,
--     days_rest,
--     run_support,

--     games_relieved,
--     wins_in_games_relieved,
--     losses_in_games_relieved,
--     save_opportunities,
--     blown_saves,
--     save_situations,
--     holds,
--     inherited_runners,
--     inherited_runners_scored
WITH win_loss_save AS (
    SELECT game_id,
        winning_pitcher_id,
        losing_pitcher_id,
        save_pitcher_id
    FROM {{ ref('stg_games') }}
),

earned_runs AS (
    SELECT game_id,
        player_id,
        earned_runs
    FROM {{ ref('stg_game_earned_runs') }}
),

SELECT game_id,
    pitcher_id,
    CASE WHEN pitcher_id = winning_pitcher_id THEN 1 ELSE 0 END AS wins,
    CASE WHEN pitcher_id = losing_pitcher_id THEN 1 ELSE 0 END AS losses,
    CASE WHEN pitcher_id = save_pitcher_id THEN 1 ELSE 0 END AS saves,
    CASE
        WHEN COUNT(*) OVER team_game = 1
            THEN 1
        ELSE 0
    END AS complete_games,
    -- It's possible to record a shutout without a complete game
    -- if no other pitchers record outs (see Ernie Shore)
    CASE
        WHEN SUM(runs) OVER team_game = 0 
            AND SUM(outs) OVER team_game = outs
            THEN 1
        ELSE 0
    END AS shutouts,
    CASE WHEN complete_games = 1 AND hits = 0 AND outs >= 27 THEN 1 ELSE 0 END AS no_hitters,
    CASE WHEN complete_games = 1 AND baserunners = 0 AND outs >= 27 THEN 1 ELSE 0 END AS perfect_games,
WINDOW
    team_game AS (PARTITION BY team_id, game_id),
