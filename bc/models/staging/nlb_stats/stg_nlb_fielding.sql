MODEL (
  name main_models.stg_nlb_fielding,
  kind FULL,
  description 'Negro Leagues fielding lines (fielding.csv), one row per player, position, and sequence, from Retrosheet''s negroleagues.zip. Where Retrosheet does not know a stat exactly, its value is NULL and the _lower or _upper column holds the bound Retrosheet gives; a known value is its own lower and upper bound, and a NULL bound is unbounded. Stat names are Retrosheet''s.',
  grain (game_id, player_id, team_id, sequence, position),
  audits (
    not_null(columns := (game_id, player_id)),
    unique_combination_of_columns(columns := (game_id, player_id, team_id, sequence, position))
  ),
);

@DEF(attributes, [date, game_number, park_id, is_home, opponent_team_id, game_type, is_win, is_loss, is_tie, box_score_source, play_by_play_source]);
@DEF(stats, [d_ifouts, d_po, d_a, d_e, d_dp, d_tp, d_pb, d_wp, d_sb, d_cs, d_gs]);

WITH source AS (
    SELECT
        gid AS game_id,
        id AS player_id,
        team AS team_id,
        d_seq AS sequence,
        d_pos AS position,
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
        stattype,
        TRY_CAST(d_ifouts AS INTEGER) AS d_ifouts,
        TRY_CAST(d_po AS INTEGER) AS d_po,
        TRY_CAST(d_a AS INTEGER) AS d_a,
        TRY_CAST(d_e AS INTEGER) AS d_e,
        TRY_CAST(d_dp AS INTEGER) AS d_dp,
        TRY_CAST(d_tp AS INTEGER) AS d_tp,
        TRY_CAST(d_pb AS INTEGER) AS d_pb,
        TRY_CAST(d_wp AS INTEGER) AS d_wp,
        TRY_CAST(d_sb AS INTEGER) AS d_sb,
        TRY_CAST(d_cs AS INTEGER) AS d_cs,
        TRY_CAST(d_gs AS INTEGER) AS d_gs
    FROM nlb_stats.fielding
)

SELECT
    game_id,
    player_id,
    team_id,
    sequence,
    position,
    @EACH(@attributes, a -> COALESCE(MAX(a) FILTER (WHERE stattype = 'value'), MAX(a)) AS a),
    @EACH(@stats, s -> MAX(s) FILTER (WHERE stattype = 'value') AS s),
    @EACH(@stats, s -> COALESCE(MAX(s) FILTER (WHERE stattype = 'lower'), MAX(s) FILTER (WHERE stattype = 'value')) AS @{s}_lower),
    @EACH(@stats, s -> COALESCE(MAX(s) FILTER (WHERE stattype = 'upper'), MAX(s) FILTER (WHERE stattype = 'value')) AS @{s}_upper)
FROM source
GROUP BY game_id, player_id, team_id, sequence, position
