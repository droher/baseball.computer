MODEL (
  name main_models.pbp_imputed_games,
  kind VIEW,
  grain (game_id),
  audits (estimated_contract_complete, min_row_count(threshold := 1), unique_grain(columns := (game_id))),
  description 'Imputed game context for existing PBP games. Source bookkeeping and recorded officials remain verbatim; candidate identities are in pbp_imputed_officials. Empty until imputation artifacts are selected.'
);

SELECT
    g.* EXCLUDE (start_time, time_of_day, sky, field_condition, precipitation,
        wind_direction, temperature_fahrenheit, attendance, wind_speed_mph),
    c.* EXCLUDE (game_id, season),
    r.* EXCLUDE (game_id, season, game_type, home_team_id, away_team_id, duration_minutes)
FROM main_models.game_start_info g
JOIN main_models.pbp_imputed_game_context c USING (game_id)
LEFT JOIN main_models.game_results r USING (game_id)
WHERE g.source_type = 'PlayByPlay'
