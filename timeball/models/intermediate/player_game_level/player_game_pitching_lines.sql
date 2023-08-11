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
WITH event_agg AS (
    SELECT
        game_id,
        player_id,
        ANY_VALUE(team_id) AS team_id,
        {% for stat in event_level_pitching_stats() -%}
            SUM({{ stat }}) AS {{ stat }},
        {% endfor %}
    FROM {{ ref('event_pitching_stats') }}
    GROUP BY 1, 2
),

flag_agg AS (
    SELECT
        game_id,
        pitcher_id AS player_id,
        -- Some of these are SUM/COUNT because a pitcher could record separate appearances during the game
        -- so, theoretically, a pitcher could blow multiple saves in the same game
        BOOL_OR(starting_pitcher_flag)::INT AS games_started,
        SUM(inherited_runners) AS inherited_runners,
        -- TODO: A bequeathed runner appears to be defined as the number of runners left on base
        -- when a pitcher leaves the game, regardless of whether those runners were inherited
        -- from a previous pitcher. This causes a double-counting issue, which we'll have to
        -- address either by applying bequeathed runner scoring to multiple pitchers
        -- or a bequeathal to a single pitcher.
        SUM(bequeathed_runners) AS bequeathed_runners,
        BOOL_OR(new_relief_pitcher_flag)::INT AS games_relieved,
        BOOL_OR(pitcher_finish_flag)::INT AS games_finished,
        COUNT_IF(save_situation_start_flag) AS save_situations_entered,
        COUNT_IF(hold_flag) AS holds,
        COUNT_IF(blown_save_flag) AS blown_saves,
        -- This could differ from save info in the game-level table if e.g.
        -- the scorekeeper decided to award a win by judgement
        BOOL_OR(save_flag)::INT AS saves_by_rule,
    FROM {{ ref('event_pitching_flags') }}
    GROUP BY 1, 2
),

joined AS (
    SELECT
        game_id,
        player_id,
        event_agg.team_id,
        ROUND(event_agg.outs_recorded / 3, 4) AS innings_pitched,
        CASE WHEN player_id = games.winning_pitcher_id THEN 1 ELSE 0 END AS wins,
        CASE WHEN player_id = games.losing_pitcher_id THEN 1 ELSE 0 END AS losses,
        CASE WHEN player_id = games.save_pitcher_id THEN 1 ELSE 0 END AS saves,
        earned_runs.earned_runs,
        event_agg.* EXCLUDE (game_id, player_id, team_id),
        flag_agg.* EXCLUDE (game_id, player_id),
        saves + flag_agg.blown_saves AS save_opportunities,
    FROM event_agg
    LEFT JOIN flag_agg USING (game_id, player_id)
    LEFT JOIN {{ ref('stg_games') }} AS games USING (game_id)
    LEFT JOIN {{ ref('stg_game_earned_runs') }} AS earned_runs USING (game_id, player_id)
),

add_special_calcs AS (
    SELECT
        *,
        CASE WHEN COUNT(*) OVER team_game = 1
                THEN 1
            ELSE 0
        END AS complete_games,
        -- It's possible to record a shutout without a complete game
        -- if no other pitchers record outs (see Ernie Shore)
        CASE WHEN SUM(runs) OVER team_game = 0 
                AND SUM(outs_recorded) OVER team_game = outs_recorded
                THEN 1
            ELSE 0
        END AS shutouts,
        CASE WHEN games_started = 1
                THEN 50
                    + outs_recorded
                    + GREATEST(outs_recorded // 3 - 4, 0)
                    + strikeouts
                    - 2 * hits
                    - 4 * earned_runs
                    - 2 * (runs - earned_runs)
                    - walks
        END AS game_score,
        CASE WHEN games_started = 1
                THEN 40
                    + 2 * outs_recorded
                    + strikeouts
                    - 2 * hits
                    - 3 * runs
                    - 2 * walks
                    - 6 * home_runs
        END AS game_score_tango,
        CASE WHEN games_started = 1 AND outs_recorded >= 18 AND earned_runs <= 3 THEN 1 ELSE 0 END AS quality_starts,
        CASE WHEN games_started = 1 AND quality_starts = 0 AND wins = 1 THEN 1 ELSE 0 END AS cheap_wins,
        CASE WHEN quality_starts = 1 AND losses = 1 THEN 1 ELSE 0 END AS tough_losses,
        CASE WHEN games_started = 1 AND wins + losses = 0 THEN 1 ELSE 0 END AS no_decisions,
        CASE WHEN complete_games = 1 AND hits = 0 AND outs_recorded >= 27 THEN 1 ELSE 0 END AS no_hitters,
        CASE WHEN no_hitters = 1 AND times_reached_base = 0 THEN 1 ELSE 0 END AS perfect_games,
    FROM joined
    WINDOW team_game AS (PARTITION BY team_id, game_id)
)

SELECT * FROM add_special_calcs
