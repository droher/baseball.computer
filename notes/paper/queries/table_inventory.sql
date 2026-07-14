SELECT 'scorer_observation_propensities' AS table_name, COUNT(*) AS n_rows, COUNT(DISTINCT artifact_id) AS n_artifacts,
       STRING_AGG(DISTINCT model_version, ', ') AS model_versions, STRING_AGG(DISTINCT confidence_status, ', ') AS confidence_statuses
FROM main_models.scorer_observation_propensities
UNION ALL
SELECT 'imputed_batted_ball_geometry', COUNT(*), COUNT(DISTINCT artifact_id),
       STRING_AGG(DISTINCT model_version, ', '), STRING_AGG(DISTINCT confidence_status, ', ')
FROM main_models.imputed_batted_ball_geometry
UNION ALL
SELECT 'imputed_ball_handler_probabilities', COUNT(*), COUNT(DISTINCT artifact_id),
       STRING_AGG(DISTINCT model_version, ', '), STRING_AGG(DISTINCT confidence_status, ', ')
FROM main_models.imputed_ball_handler_probabilities
UNION ALL
SELECT 'imputed_fielding_credit', COUNT(*), COUNT(DISTINCT artifact_id),
       STRING_AGG(DISTINCT model_version, ', '), STRING_AGG(DISTINCT confidence_status, ', ')
FROM main_models.imputed_fielding_credit
UNION ALL
SELECT 'imputed_advancement_probabilities', COUNT(*), COUNT(DISTINCT artifact_id),
       STRING_AGG(DISTINCT model_version, ', '), STRING_AGG(DISTINCT confidence_status, ', ')
FROM main_models.imputed_advancement_probabilities
UNION ALL
SELECT 'pitch_count_coverage', COUNT(*), COUNT(DISTINCT artifact_id),
       STRING_AGG(DISTINCT model_version, ', '), STRING_AGG(DISTINCT confidence_status, ', ')
FROM main_models.pitch_count_coverage
UNION ALL
SELECT 'run_expectancy_summary', COUNT(*), COUNT(DISTINCT artifact_id),
       STRING_AGG(DISTINCT model_version, ', '), STRING_AGG(DISTINCT confidence_status, ', ')
FROM main_models.run_expectancy_summary
UNION ALL
SELECT 'state_transition_summary', COUNT(*), COUNT(DISTINCT artifact_id),
       STRING_AGG(DISTINCT model_version, ', '), STRING_AGG(DISTINCT confidence_status, ', ')
FROM main_models.state_transition_summary
UNION ALL
SELECT 'park_factor_summary', COUNT(*), COUNT(DISTINCT artifact_id),
       STRING_AGG(DISTINCT model_version, ', '), STRING_AGG(DISTINCT confidence_status, ', ')
FROM main_models.park_factor_summary
UNION ALL
SELECT 'pitch_summary_distribution', COUNT(*), COUNT(DISTINCT artifact_id),
       STRING_AGG(DISTINCT model_version, ', '), STRING_AGG(DISTINCT confidence_status, ', ')
FROM main_models.pitch_summary_distribution
UNION ALL
SELECT 'assist_count_distribution', COUNT(*), COUNT(DISTINCT artifact_id),
       STRING_AGG(DISTINCT model_version, ', '), STRING_AGG(DISTINCT confidence_status, ', ')
FROM main_models.assist_count_distribution
UNION ALL
SELECT 'linear_weights_estimated', COUNT(*), COUNT(DISTINCT artifact_id),
       STRING_AGG(DISTINCT model_version, ', '), STRING_AGG(DISTINCT confidence_status, ', ')
FROM main_models.linear_weights_estimated
ORDER BY table_name;
