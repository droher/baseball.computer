MODEL (
  name main_models.nlb_team_lines,
  kind FULL,
  description 'Retrosheet''s Negro Leagues team lines for games recorded only in negroleagues.zip (game_start_info.source_type = ''NegroLeaguesStats''), one row per team and game, with the warehouse team id and side. The team-game models use these totals for those games instead of summing player lines, since many player stats in them are unknown while the team totals are known. Stat names are Retrosheet''s; a stat Retrosheet gives only as a bound is NULL.',
  grain (game_id, team_id),
  audits (
    not_null(columns := (game_id, team_id, side)),
    unique_grain(columns := (game_id, team_id))
  ),
);

WITH nlb_games AS (
    SELECT game_id, home_team_id, away_team_id
    FROM main_models.game_start_info
    WHERE source_type = 'NegroLeaguesStats'
),

final AS (
    SELECT
        teams.game_id,
        CASE WHEN teams.is_home THEN games.home_team_id ELSE games.away_team_id END AS team_id,
        CASE WHEN teams.is_home THEN 'Home' ELSE 'Away' END AS side,
        teams.lob,
        teams.b_pa,
        teams.b_ab,
        teams.b_r,
        teams.b_h,
        teams.b_d,
        teams.b_t,
        teams.b_hr,
        teams.b_rbi,
        teams.b_sh,
        teams.b_sf,
        teams.b_hbp,
        teams.b_w,
        teams.b_iw,
        teams.b_k,
        teams.b_sb,
        teams.b_cs,
        teams.b_gdp,
        teams.b_xi,
        teams.b_roe,
        teams.p_ipouts,
        teams.p_bfp,
        teams.p_h,
        teams.p_d,
        teams.p_t,
        teams.p_hr,
        teams.p_r,
        teams.p_er,
        teams.p_w,
        teams.p_iw,
        teams.p_k,
        teams.p_hbp,
        teams.p_wp,
        teams.p_bk,
        teams.p_sh,
        teams.p_sf,
        teams.d_po,
        teams.d_a,
        teams.d_e,
        teams.d_dp,
        teams.d_tp,
        teams.d_pb,
        teams.d_sb,
        teams.d_cs
    FROM main_models.stg_nlb_teams AS teams
    INNER JOIN nlb_games AS games USING (game_id)
)

SELECT * FROM final
