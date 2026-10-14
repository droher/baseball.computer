MODEL (
  name main_models.nlb_fielding_lines,
  kind FULL,
  description 'Negro Leagues fielding lines for games recorded only in Retrosheet''s negroleagues.zip (game_start_info.source_type = ''NegroLeaguesStats''), one row per player, position, and game in the shape of stg_box_score_fielding_lines. Lines with no known position are left out. A stat Retrosheet gives only as a bound is NULL here; the bounds are in nlb_player_game_stat_ranges.',
  grain (game_id, fielder_id, team_id, fielding_position),
  columns (
    game_id VARCHAR,
    fielder_id VARCHAR,
    team_id TEAM_ID,
    side SIDE,
    fielding_position UTINYINT,
    nth_position_played_by_player UTINYINT,
    games_started UTINYINT,
    outs_played UTINYINT,
    putouts UTINYINT,
    assists UTINYINT,
    errors UTINYINT,
    double_plays UTINYINT,
    triple_plays UTINYINT,
    passed_balls UTINYINT,
    stolen_bases UTINYINT,
    caught_stealing UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    team_id = @doc('team_id'),
    side = @doc('side'),
    fielding_position = @doc('fielding_position'),
    nth_position_played_by_player = 'Retrosheet''s order of this position among the player''s positions in the game, when recorded.',
    games_started = @doc('games_started'),
    outs_played = @doc('outs_played'),
    stolen_bases = @doc('stolen_bases'),
    caught_stealing = @doc('caught_stealing')
  ),
  audits (
    not_null(columns := (game_id, fielder_id, team_id, side, fielding_position)),
    unique_grain(columns := (game_id, fielder_id, team_id, fielding_position))
  ),
);

@DEF(stats, [games_started, outs_played, putouts, assists, errors, double_plays, triple_plays, passed_balls, stolen_bases, caught_stealing]);

WITH nlb_games AS (
    SELECT game_id, home_team_id, away_team_id
    FROM main_models.game_start_info
    WHERE source_type = 'NegroLeaguesStats'
),

renamed AS (
    SELECT
        fld.game_id,
        fld.player_id AS fielder_id,
        CASE WHEN fld.is_home THEN games.home_team_id ELSE games.away_team_id END AS team_id,
        CASE WHEN fld.is_home THEN 'Home' ELSE 'Away' END AS side,
        TRY_CAST(fld.position AS UTINYINT) AS fielding_position,
        fld.sequence,
        fld.d_gs AS games_started,
        fld.d_ifouts AS outs_played,
        fld.d_po AS putouts,
        fld.d_a AS assists,
        fld.d_e AS errors,
        fld.d_dp AS double_plays,
        fld.d_tp AS triple_plays,
        fld.d_pb AS passed_balls,
        fld.d_sb AS stolen_bases,
        fld.d_cs AS caught_stealing
    FROM main_models.stg_nlb_fielding AS fld
    INNER JOIN nlb_games AS games USING (game_id)
    WHERE TRY_CAST(fld.position AS UTINYINT) BETWEEN 1 AND 9
),

final AS (
    SELECT
        game_id,
        fielder_id,
        team_id,
        side,
        fielding_position,
        MIN(sequence) AS nth_position_played_by_player,
        @EACH(@stats, s -> CASE WHEN COUNT(s) = COUNT(*) THEN SUM(s) END AS s)
    FROM renamed
    GROUP BY game_id, fielder_id, team_id, side, fielding_position
)

SELECT * FROM final
