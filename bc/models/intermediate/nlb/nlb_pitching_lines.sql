MODEL (
  name main_models.nlb_pitching_lines,
  kind FULL,
  description 'Negro Leagues pitching lines for games recorded only in Retrosheet''s negroleagues.zip (game_start_info.source_type = ''NegroLeaguesStats''), one row per pitcher and game in the shape of stg_box_score_pitching_lines. Starts and finishes come from Retrosheet''s p_gs and p_gf when any pitcher on the team has one in that game (a blank then counts as 0), and otherwise from the pitching order. A stat Retrosheet gives only as a bound is NULL here; the bounds are in nlb_player_game_stat_ranges.',
  grain (game_id, pitcher_id, team_id),
  columns (
    game_id VARCHAR,
    pitcher_id VARCHAR,
    team_id TEAM_ID,
    side SIDE,
    nth_pitcher UTINYINT,
    outs_recorded UTINYINT,
    no_out_batters UTINYINT,
    batters_faced UTINYINT,
    hits UTINYINT,
    doubles UTINYINT,
    triples UTINYINT,
    home_runs UTINYINT,
    runs UTINYINT,
    earned_runs UTINYINT,
    walks UTINYINT,
    intentional_walks UTINYINT,
    strikeouts UTINYINT,
    hit_by_pitches UTINYINT,
    wild_pitches UTINYINT,
    balks UTINYINT,
    sacrifice_hits UTINYINT,
    sacrifice_flies UTINYINT,
    singles UTINYINT,
    total_bases UTINYINT,
    on_base_opportunities UTINYINT,
    on_base_successes UTINYINT,
    games_started UTINYINT,
    games_relieved UTINYINT,
    games_finished UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    pitcher_id = @doc('pitcher_id'),
    team_id = @doc('team_id'),
    side = @doc('side'),
    nth_pitcher = 'Nth player to pitch for this team in this game, when Retrosheet records the order.',
    outs_recorded = @doc('outs_recorded'),
    games_started = @doc('games_started')
  ),
  audits (
    not_null(columns := (game_id, pitcher_id, team_id, side)),
    unique_grain(columns := (game_id, pitcher_id, team_id))
  ),
);

@DEF(stats, [outs_recorded, no_out_batters, batters_faced, hits, doubles, triples, home_runs, runs, earned_runs, walks, intentional_walks, strikeouts, hit_by_pitches, wild_pitches, balks, sacrifice_hits, sacrifice_flies]);

WITH nlb_games AS (
    SELECT game_id, home_team_id, away_team_id
    FROM main_models.game_start_info
    WHERE source_type = 'NegroLeaguesStats'
),

renamed AS (
    SELECT
        pit.game_id,
        pit.player_id AS pitcher_id,
        CASE WHEN pit.is_home THEN games.home_team_id ELSE games.away_team_id END AS team_id,
        CASE WHEN pit.is_home THEN 'Home' ELSE 'Away' END AS side,
        pit.sequence,
        pit.p_gs,
        pit.p_gf,
        pit.p_ipouts AS outs_recorded,
        pit.p_noout AS no_out_batters,
        pit.p_bfp AS batters_faced,
        pit.p_h AS hits,
        pit.p_d AS doubles,
        pit.p_t AS triples,
        pit.p_hr AS home_runs,
        pit.p_r AS runs,
        pit.p_er AS earned_runs,
        pit.p_w AS walks,
        pit.p_iw AS intentional_walks,
        pit.p_k AS strikeouts,
        pit.p_hbp AS hit_by_pitches,
        pit.p_wp AS wild_pitches,
        pit.p_bk AS balks,
        pit.p_sh AS sacrifice_hits,
        pit.p_sf AS sacrifice_flies
    FROM main_models.stg_nlb_pitching AS pit
    INNER JOIN nlb_games AS games USING (game_id)
),

merged AS (
    SELECT
        game_id,
        pitcher_id,
        team_id,
        side,
        MIN(sequence) AS nth_pitcher,
        MAX(p_gs) AS p_gs,
        MAX(p_gf) AS p_gf,
        @EACH(@stats, s -> CASE WHEN COUNT(s) = COUNT(*) THEN SUM(s) END AS s)
    FROM renamed
    GROUP BY game_id, pitcher_id, team_id, side
),

final AS (
    SELECT
        * EXCLUDE (p_gs, p_gf),
        CASE WHEN hits::INT - (home_runs + triples + doubles) < 0 THEN NULL
            ELSE hits - (home_runs + triples + doubles)
        END::UTINYINT AS singles,
        (singles + doubles * 2 + triples * 3 + home_runs * 4)::UTINYINT AS total_bases,
        (batters_faced - COALESCE(sacrifice_hits, 0))::UTINYINT AS on_base_opportunities,
        (hits + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0))::UTINYINT AS on_base_successes,
        CASE
            WHEN MAX(p_gs) OVER (PARTITION BY game_id, side) IS NOT NULL THEN COALESCE(p_gs, 0)
            ELSE (nth_pitcher = 1)::INT
        END::UTINYINT AS games_started,
        (1 - games_started)::UTINYINT AS games_relieved,
        CASE
            WHEN MAX(p_gf) OVER (PARTITION BY game_id, side) IS NOT NULL THEN COALESCE(p_gf, 0)
            ELSE (nth_pitcher = MAX(nth_pitcher) OVER (PARTITION BY game_id, side))::INT
        END::UTINYINT AS games_finished
    FROM merged
)

SELECT * FROM final
