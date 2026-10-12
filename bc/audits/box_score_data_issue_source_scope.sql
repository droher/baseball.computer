AUDIT (
  name box_score_data_issue_source_scope
);

SELECT src.*
FROM @this_model AS src
LEFT JOIN main_models.game_start_info AS info USING (game_id)
WHERE LIST_CONTAINS(@issue_types, src.issue_type)
  AND (@is_less_complete_source(info) IS TRUE) = @excused_population
