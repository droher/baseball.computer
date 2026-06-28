AUDIT (
  name unknown_putouts_null_iff_no_fielding_row
);

SELECT event_key, fielding_team_id, unknown_putouts, fielding_evidence_status
FROM @this_model
WHERE (unknown_putouts IS NULL) != (fielding_evidence_status = 'no_fielding_row')
