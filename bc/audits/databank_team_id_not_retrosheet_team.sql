AUDIT (
  name databank_team_id_not_retrosheet_team
);

SELECT season, databank_team_id, team_id
FROM @this_model AS src
WHERE is_databank_team_id
  AND EXISTS (
    SELECT 1
    FROM main_models.stg_nlb_players AS players
    WHERE players.season = src.season
      AND players.team_id = src.team_id
  )
