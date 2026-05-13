AUDIT (
  name confirmed_issue_not_allowed
);

SELECT data_error_key
FROM @this_model
WHERE data_error_class = 'confirmed_issue'
  AND training_action = 'allow'
