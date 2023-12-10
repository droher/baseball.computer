{{
  config(
    materialized = 'table',
    )
}}
WITH event_agg AS (
    SELECT
        game_id,
        player_id,
        MIN(team_id) AS team_id,
        {% for stat in event_level_pitching_stats() -%}
            {% if stat.count("bases_advanced") > 0 %}
                {% set dtype = "INT2" %}
            {% elif stat.startswith("pitches") %}
                {% set dtype = "USMALLINT" %}
            {% else %}
                {% set dtype = "UTINYINT" %}
            {% endif %}
            SUM({{ stat }})::{{ dtype }} AS {{ stat }},
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
        BOOL_OR(starting_pitcher_flag)::UTINYINT AS games_started,
        SUM(inherited_runners)::UTINYINT AS inherited_runners,
        -- TODO: A bequeathed runner appears to be defined as the number of runners left on base
        -- when a pitcher leaves the game, regardless of whether those runners were inherited
        -- from a previous pitcher. This causes a double-counting issue, which we'll have to
        -- address either by applying bequeathed runner scoring to multiple pitchers
        -- or a bequeathal to a single pitcher.
        SUM(bequeathed_runners)::UTINYINT AS bequeathed_runners,
        BOOL_OR(new_relief_pitcher_flag)::UTINYINT AS games_relieved,
        BOOL_OR(pitcher_finish_flag)::UTINYINT AS games_finished,
        COUNT_IF(save_situation_start_flag)::UTINYINT AS save_situations_entered,
        COUNT_IF(hold_flag)::UTINYINT AS holds,
        COUNT_IF(blown_save_flag)::UTINYINT AS blown_saves,
        -- This could differ from save info in the game-level table if e.g.
        -- the scorekeeper decided to award a win by judgement
        BOOL_OR(save_flag)::UTINYINT AS saves_by_rule,
    FROM {{ ref('event_pitching_flags') }}
    GROUP BY 1, 2  
),

events_with_flags AS (
    SELECT
        event_agg.*,
        flag_agg.* EXCLUDE (game_id, player_id),
    FROM event_agg
    LEFT JOIN flag_agg USING (game_id, player_id)
),

box_agg AS (
    SELECT
        game_id,
        stats.pitcher_id AS player_id,
        ANY_VALUE(CASE WHEN stats.side = 'Home' THEN games.home_team_id ELSE games.away_team_id END) AS team_id,
        SUM(stats.outs_recorded)::UTINYINT AS outs_recorded,
        SUM(stats.batters_faced)::UTINYINT AS batters_faced,
        SUM(stats.hits)::UTINYINT AS hits,
        SUM(stats.doubles)::UTINYINT AS doubles,
        SUM(stats.triples)::UTINYINT AS triples,
        SUM(stats.home_runs)::UTINYINT AS home_runs,
        SUM(stats.runs)::UTINYINT AS runs,
        SUM(stats.earned_runs)::UTINYINT AS earned_runs,
        SUM(stats.walks)::UTINYINT AS walks,
        SUM(stats.intentional_walks)::UTINYINT AS intentional_walks,
        SUM(stats.strikeouts)::UTINYINT AS strikeouts,
        SUM(stats.hit_by_pitches)::UTINYINT AS hit_by_pitches,
        SUM(stats.wild_pitches)::UTINYINT AS wild_pitches,
        SUM(stats.balks)::UTINYINT AS balks,
        SUM(stats.sacrifice_hits)::UTINYINT AS sacrifice_hits,
        SUM(stats.sacrifice_flies)::UTINYINT AS sacrifice_flies,
        SUM(stats.singles)::UTINYINT AS singles,
        SUM(stats.total_bases)::UTINYINT AS total_bases,
        SUM(stats.on_base_opportunities)::UTINYINT AS on_base_opportunities,
        SUM(stats.on_base_successes)::UTINYINT AS on_base_successes,
        SUM(stats.games_started)::UTINYINT AS games_started,
        SUM(stats.games_relieved)::UTINYINT AS games_relieved,
        SUM(stats.games_finished)::UTINYINT AS games_finished,
    FROM {{ ref('stg_box_score_pitching_lines') }} AS stats
    -- This join ensures that we only get the box score lines for games that
    -- do not have an event file.
    INNER JOIN {{ ref('stg_games') }} AS games USING (game_id)
    WHERE games.source_type = 'BoxScore'
    GROUP BY 1, 2
),

unioned AS (
    SELECT * FROM events_with_flags
    UNION ALL BY NAME
    SELECT * FROM box_agg
),

with_game_info AS (
    SELECT
        game_id,
        player_id,
        unioned.team_id,
        ROUND(unioned.outs_recorded / 3, 4)::DECIMAL(6, 4) AS innings_pitched,
        CASE WHEN player_id = games.winning_pitcher_id THEN 1 ELSE 0 END::UTINYINT AS wins,
        CASE WHEN player_id = games.losing_pitcher_id THEN 1 ELSE 0 END::UTINYINT AS losses,
        CASE WHEN player_id = games.save_pitcher_id THEN 1 ELSE 0 END::UTINYINT AS saves,
        -- Box score will have ER directly, but event data will need the join
        COALESCE(earned_runs.earned_runs, unioned.earned_runs)::UTINYINT AS earned_runs,
        unioned.* EXCLUDE (game_id, player_id, team_id, earned_runs),
        (saves + unioned.blown_saves)::UTINYINT AS save_opportunities,
    FROM unioned
    LEFT JOIN {{ ref('stg_games') }} AS games USING (game_id)
    LEFT JOIN {{ ref('stg_game_earned_runs') }} AS earned_runs USING (game_id, player_id)
),

final AS (
    SELECT
        *,
        CASE WHEN COUNT(*) OVER team_game = 1
                THEN 1
            ELSE 0
        END::UTINYINT AS complete_games,
        -- It's possible to record a shutout without a complete game
        -- if no other pitchers record outs (see Ernie Shore)
        CASE WHEN SUM(runs) OVER team_game = 0
                AND SUM(outs_recorded) OVER team_game = outs_recorded
                THEN 1
            ELSE 0
        END::UTINYINT AS shutouts,
        CASE WHEN games_started = 1 AND outs_recorded >= 18 AND earned_runs <= 3 THEN 1 ELSE 0 END::UTINYINT AS quality_starts,
        CASE WHEN games_started = 1 AND quality_starts = 0 AND wins = 1 THEN 1 ELSE 0 END::UTINYINT AS cheap_wins,
        CASE WHEN quality_starts = 1 AND losses = 1 THEN 1 ELSE 0 END::UTINYINT AS tough_losses,
        CASE WHEN games_started = 1 AND wins + losses = 0 THEN 1 ELSE 0 END::UTINYINT AS no_decisions,
        CASE WHEN complete_games = 1 AND hits = 0 AND outs_recorded >= 27 THEN 1 ELSE 0 END::UTINYINT AS no_hitters,
        -- Easy to calculate perfect games for games with event files, but box scores don't have ROEs.
        -- The logic here would be broken if a batter reached on an error and then was out on the bases,
        -- but no such event happened in prior to the event data era (maybe ever?)
        (
        CASE WHEN no_hitters = 1 AND (times_reached_base = 0
                OR (outs_recorded >= batters_faced AND COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) = 0) 
                ) THEN 1 
            ELSE 0 END)::UTINYINT AS perfect_games, 
    FROM with_game_info
    WINDOW team_game AS (PARTITION BY team_id, game_id)
)

SELECT * FROM final
