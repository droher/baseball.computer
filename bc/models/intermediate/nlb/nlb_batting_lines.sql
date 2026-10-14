MODEL (
  name main_models.nlb_batting_lines,
  kind FULL,
  description 'Negro Leagues batting lines for games recorded only in Retrosheet''s negroleagues.zip (game_start_info.source_type = ''NegroLeaguesStats''), one row per player and game in the shape of stg_box_score_batting_lines. A player''s split lines in one game are merged. A stat Retrosheet gives only as a bound is NULL here; the bounds are in nlb_player_game_stat_ranges.',
  grain (game_id, batter_id, team_id),
  columns (
    game_id VARCHAR,
    batter_id VARCHAR,
    team_id TEAM_ID,
    side SIDE,
    lineup_position UTINYINT,
    nth_player_at_position UTINYINT,
    is_pinch_hitter BOOLEAN,
    is_pinch_runner BOOLEAN,
    at_bats UTINYINT,
    runs UTINYINT,
    hits UTINYINT,
    doubles UTINYINT,
    triples UTINYINT,
    home_runs UTINYINT,
    runs_batted_in UTINYINT,
    sacrifice_hits UTINYINT,
    sacrifice_flies UTINYINT,
    hit_by_pitches UTINYINT,
    walks UTINYINT,
    intentional_walks UTINYINT,
    strikeouts UTINYINT,
    stolen_bases UTINYINT,
    caught_stealing UTINYINT,
    grounded_into_double_plays UTINYINT,
    reached_on_interferences UTINYINT,
    reached_on_errors UTINYINT,
    singles UTINYINT,
    total_bases UTINYINT,
    plate_appearances UTINYINT,
    on_base_opportunities UTINYINT,
    on_base_successes UTINYINT,
    batting_outs UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    batter_id = @doc('batter_id'),
    team_id = @doc('team_id'),
    side = @doc('side'),
    lineup_position = @doc('lineup_position'),
    nth_player_at_position = 'The nth player to occupy this spot in the order during the game',
    is_pinch_hitter = 'Retrosheet marks the player as a pinch hitter in this game.',
    is_pinch_runner = 'Retrosheet marks the player as a pinch runner in this game.'
  ),
  audits (
    not_null(columns := (game_id, batter_id, team_id, side)),
    unique_grain(columns := (game_id, batter_id, team_id))
  ),
);

@DEF(stats, [at_bats, runs, hits, doubles, triples, home_runs, runs_batted_in, sacrifice_hits, sacrifice_flies, hit_by_pitches, walks, intentional_walks, strikeouts, stolen_bases, caught_stealing, grounded_into_double_plays, reached_on_interferences, reached_on_errors]);

WITH nlb_games AS (
    SELECT game_id, home_team_id, away_team_id
    FROM main_models.game_start_info
    WHERE source_type = 'NegroLeaguesStats'
),

renamed AS (
    SELECT
        bat.game_id,
        bat.player_id AS batter_id,
        CASE WHEN bat.is_home THEN games.home_team_id ELSE games.away_team_id END AS team_id,
        CASE WHEN bat.is_home THEN 'Home' ELSE 'Away' END AS side,
        bat.lineup_position,
        bat.sequence,
        bat.is_pinch_hitter,
        bat.is_pinch_runner,
        bat.b_ab AS at_bats,
        bat.b_r AS runs,
        bat.b_h AS hits,
        bat.b_d AS doubles,
        bat.b_t AS triples,
        bat.b_hr AS home_runs,
        bat.b_rbi AS runs_batted_in,
        bat.b_sh AS sacrifice_hits,
        bat.b_sf AS sacrifice_flies,
        bat.b_hbp AS hit_by_pitches,
        bat.b_w AS walks,
        bat.b_iw AS intentional_walks,
        bat.b_k AS strikeouts,
        bat.b_sb AS stolen_bases,
        bat.b_cs AS caught_stealing,
        bat.b_gdp AS grounded_into_double_plays,
        bat.b_xi AS reached_on_interferences,
        bat.b_roe AS reached_on_errors
    FROM main_models.stg_nlb_batting AS bat
    INNER JOIN nlb_games AS games USING (game_id)
),

merged AS (
    SELECT
        game_id,
        batter_id,
        team_id,
        side,
        MIN(lineup_position) AS lineup_position,
        MIN(sequence) AS nth_player_at_position,
        BOOL_OR(is_pinch_hitter) AS is_pinch_hitter,
        BOOL_OR(is_pinch_runner) AS is_pinch_runner,
        @EACH(@stats, s -> CASE WHEN COUNT(s) = COUNT(*) THEN SUM(s) END AS s)
    FROM renamed
    GROUP BY game_id, batter_id, team_id, side
),

final AS (
    SELECT
        *,
        CASE
            WHEN hits IS NULL OR doubles IS NULL OR triples IS NULL OR home_runs IS NULL THEN NULL
            WHEN hits < doubles + triples + home_runs THEN NULL
            ELSE hits - home_runs - triples - doubles
        END::UTINYINT AS singles,
        (singles::BIGINT + doubles::BIGINT * 2 + triples::BIGINT * 3 + home_runs::BIGINT * 4)::UTINYINT AS total_bases,
        (at_bats + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) + COALESCE(sacrifice_flies, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(reached_on_interferences, 0))::UTINYINT AS plate_appearances,
        (at_bats + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) + COALESCE(sacrifice_flies, 0))::UTINYINT AS on_base_opportunities,
        (hits + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0))::UTINYINT AS on_base_successes,
        CASE WHEN hits > at_bats THEN NULL ELSE at_bats - hits + COALESCE(grounded_into_double_plays, 0) END::UTINYINT AS batting_outs
    FROM merged
)

SELECT * FROM final
