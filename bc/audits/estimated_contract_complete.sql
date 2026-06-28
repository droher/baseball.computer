AUDIT (
  name estimated_contract_complete
);

SELECT *
FROM @this_model
WHERE artifact_id IS NULL
  OR model_name IS NULL
  OR model_version IS NULL
  OR source_snapshot_id IS NULL
  OR method IS NULL
  OR observed_status IS NULL
  OR confidence_status IS NULL
  OR weak_identification_flag IS NULL
