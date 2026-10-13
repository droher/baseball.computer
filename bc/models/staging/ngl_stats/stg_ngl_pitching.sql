MODEL (
  name main_models.stg_ngl_pitching,
  kind FULL,
  description 'Negro Leagues pitching lines (pitching.csv), one row per pitcher line, from Retrosheet''s negroleagues.zip. Where Retrosheet does not know a stat exactly, its value is NULL and the _lower or _upper column holds the bound Retrosheet gives; a known value is its own lower and upper bound, and a NULL bound is unbounded. Stat names are Retrosheet''s.',
  grain (game_id, player_id, team_id, sequence),
  audits (
    not_null(columns := (game_id, player_id)),
    unique_combination_of_columns(columns := (game_id, player_id, team_id, sequence))
  ),
);

@DEF(attributes, [date, game_number, park_id, is_home, opponent_team_id, game_type, is_win, is_loss, is_tie, box_score_source, play_by_play_source, is_winning_pitcher, is_losing_pitcher, is_save]);
@DEF(stats, [p_ipouts, p_noout, p_bfp, p_h, p_d, p_t, p_hr, p_r, p_er, p_w, p_iw, p_k, p_hbp, p_wp, p_bk, p_sh, p_sf, p_sb, p_cs, p_pb, p_gs, p_gf, p_cg]);

WITH source AS (
    SELECT
        gid AS game_id,
        id AS player_id,
        team AS team_id,
        p_seq AS sequence,
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
        wp::BOOLEAN AS is_winning_pitcher,
        lp::BOOLEAN AS is_losing_pitcher,
        save::BOOLEAN AS is_save,
        stattype,
        TRY_CAST(p_ipouts AS INTEGER) AS p_ipouts,
        TRY_CAST(p_noout AS INTEGER) AS p_noout,
        TRY_CAST(p_bfp AS INTEGER) AS p_bfp,
        TRY_CAST(p_h AS INTEGER) AS p_h,
        TRY_CAST(p_d AS INTEGER) AS p_d,
        TRY_CAST(p_t AS INTEGER) AS p_t,
        TRY_CAST(p_hr AS INTEGER) AS p_hr,
        TRY_CAST(p_r AS INTEGER) AS p_r,
        TRY_CAST(p_er AS INTEGER) AS p_er,
        TRY_CAST(p_w AS INTEGER) AS p_w,
        TRY_CAST(p_iw AS INTEGER) AS p_iw,
        TRY_CAST(p_k AS INTEGER) AS p_k,
        TRY_CAST(p_hbp AS INTEGER) AS p_hbp,
        TRY_CAST(p_wp AS INTEGER) AS p_wp,
        TRY_CAST(p_bk AS INTEGER) AS p_bk,
        TRY_CAST(p_sh AS INTEGER) AS p_sh,
        TRY_CAST(p_sf AS INTEGER) AS p_sf,
        TRY_CAST(p_sb AS INTEGER) AS p_sb,
        TRY_CAST(p_cs AS INTEGER) AS p_cs,
        TRY_CAST(p_pb AS INTEGER) AS p_pb,
        TRY_CAST(p_gs AS INTEGER) AS p_gs,
        TRY_CAST(p_gf AS INTEGER) AS p_gf,
        TRY_CAST(p_cg AS INTEGER) AS p_cg
    FROM ngl_stats.pitching
)

SELECT
    game_id,
    player_id,
    team_id,
    sequence,
    @EACH(@attributes, a -> COALESCE(MAX(a) FILTER (WHERE stattype = 'value'), MAX(a)) AS a),
    @EACH(@stats, s -> MAX(s) FILTER (WHERE stattype = 'value') AS s),
    @EACH(@stats, s -> COALESCE(MAX(s) FILTER (WHERE stattype = 'lower'), MAX(s) FILTER (WHERE stattype = 'value')) AS @{s}_lower),
    @EACH(@stats, s -> COALESCE(MAX(s) FILTER (WHERE stattype = 'upper'), MAX(s) FILTER (WHERE stattype = 'value')) AS @{s}_upper)
FROM source
GROUP BY game_id, player_id, team_id, sequence
