MODEL (
  name main_models.stg_ngl_batting,
  kind FULL,
  description 'Negro Leagues batting lines (batting.csv), one row per player line, from Retrosheet''s negroleagues.zip. Where Retrosheet does not know a stat exactly, its value is NULL and the _lower or _upper column holds the bound Retrosheet gives; a known value is its own lower and upper bound, and a NULL bound is unbounded. Stat names are Retrosheet''s.',
  grain (game_id, player_id, team_id, lineup_position, sequence),
  audits (
    not_null(columns := (game_id, player_id)),
    unique_combination_of_columns(columns := (game_id, player_id, team_id, lineup_position, sequence))
  ),
);

@DEF(attributes, [date, game_number, park_id, is_home, opponent_team_id, game_type, is_win, is_loss, is_tie, box_score_source, play_by_play_source, is_designated_hitter, is_pinch_hitter, is_pinch_runner]);
@DEF(stats, [b_pa, b_ab, b_r, b_h, b_d, b_t, b_hr, b_rbi, b_sh, b_sf, b_hbp, b_w, b_iw, b_k, b_sb, b_cs, b_gdp, b_xi, b_roe]);

WITH source AS (
    SELECT
        gid AS game_id,
        id AS player_id,
        team AS team_id,
        b_lp AS lineup_position,
        b_seq AS sequence,
        STRPTIME(date::VARCHAR, '%Y%m%d')::DATE AS date,
        number AS game_number,
        site AS park_id,
        vishome = 'h' AS is_home,
        opp AS opponent_team_id,
        gametype AS game_type,
        win::BOOLEAN AS is_win,
        loss::BOOLEAN AS is_loss,
        tie::BOOLEAN AS is_tie,
        box AS box_score_source,
        pbp AS play_by_play_source,
        dh::BOOLEAN AS is_designated_hitter,
        ph::BOOLEAN AS is_pinch_hitter,
        pr::BOOLEAN AS is_pinch_runner,
        stattype,
        TRY_CAST(b_pa AS INTEGER) AS b_pa,
        TRY_CAST(b_ab AS INTEGER) AS b_ab,
        TRY_CAST(b_r AS INTEGER) AS b_r,
        TRY_CAST(b_h AS INTEGER) AS b_h,
        TRY_CAST(b_d AS INTEGER) AS b_d,
        TRY_CAST(b_t AS INTEGER) AS b_t,
        TRY_CAST(b_hr AS INTEGER) AS b_hr,
        TRY_CAST(b_rbi AS INTEGER) AS b_rbi,
        TRY_CAST(b_sh AS INTEGER) AS b_sh,
        TRY_CAST(b_sf AS INTEGER) AS b_sf,
        TRY_CAST(b_hbp AS INTEGER) AS b_hbp,
        TRY_CAST(b_w AS INTEGER) AS b_w,
        TRY_CAST(b_iw AS INTEGER) AS b_iw,
        TRY_CAST(b_k AS INTEGER) AS b_k,
        TRY_CAST(b_sb AS INTEGER) AS b_sb,
        TRY_CAST(b_cs AS INTEGER) AS b_cs,
        TRY_CAST(b_gdp AS INTEGER) AS b_gdp,
        TRY_CAST(b_xi AS INTEGER) AS b_xi,
        TRY_CAST(b_roe AS INTEGER) AS b_roe
    FROM ngl_stats.batting
)

SELECT
    game_id,
    player_id,
    team_id,
    lineup_position,
    sequence,
    @EACH(@attributes, a -> COALESCE(MAX(a) FILTER (WHERE stattype = 'value'), MAX(a)) AS a),
    @EACH(@stats, s -> MAX(s) FILTER (WHERE stattype = 'value') AS s),
    @EACH(@stats, s -> COALESCE(MAX(s) FILTER (WHERE stattype = 'lower'), MAX(s) FILTER (WHERE stattype = 'value')) AS @{s}_lower),
    @EACH(@stats, s -> COALESCE(MAX(s) FILTER (WHERE stattype = 'upper'), MAX(s) FILTER (WHERE stattype = 'value')) AS @{s}_upper)
FROM source
GROUP BY game_id, player_id, team_id, lineup_position, sequence
