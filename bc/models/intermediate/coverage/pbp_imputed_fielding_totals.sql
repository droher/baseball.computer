MODEL (
  name main_models.pbp_imputed_fielding_totals,
  kind FULL,
  grain (game_id, player_id, completed_fielding_position, credit_type),
  audits (estimated_contract_complete, min_row_count(threshold := 1), unique_grain(columns := (game_id, player_id, completed_fielding_position, credit_type))),
  description 'Fielding totals from recorded and imputed play credits, by game, player, position, and credit type. A nullable player identifies an explicit unresolved personnel slot.'
);

WITH plays AS (
    SELECT
        completed.game_id,
        completed.player_id,
        completed.completed_fielding_position,
        completed.credit_type,
        completed.fielding_play,
        completed.completion_status,
        completed.constraint_disposition,
        completed.artifact_id,
        completed.source_snapshot_id,
        completed.confidence_status,
        completed.weak_identification_flag
    FROM main_models.pbp_imputed_fielding_plays AS completed
),
rolled AS (
    SELECT
        game_id,
        player_id,
        completed_fielding_position,
        credit_type,
        COUNT(*)::BIGINT AS play_credits,
        COUNT(*) FILTER (WHERE fielding_play = 'Putout')::BIGINT AS putouts,
        COUNT(*) FILTER (WHERE fielding_play = 'Assist')::BIGINT AS assists,
        COUNT(*) FILTER (WHERE fielding_play = 'Error')::BIGINT AS errors,
        COUNT(*) FILTER (WHERE fielding_play = 'FieldersChoice')::BIGINT AS fielders_choices,
        COUNT(*) FILTER (WHERE fielding_play = 'Putout')::BIGINT AS outs_recorded,
        COUNT(*) FILTER (WHERE completion_status = 'observed')::BIGINT AS observed_play_credits,
        COUNT(*) FILTER (WHERE completion_status <> 'observed')::BIGINT AS estimated_play_credits,
        COUNT(*) FILTER (WHERE constraint_disposition = 'source_preserved_without_rewrite')::BIGINT
            AS source_preserved_play_credits,
        COUNT(*) FILTER (WHERE constraint_disposition <> 'source_preserved_without_rewrite')::BIGINT
            AS constrained_or_unresolved_play_credits,
        MIN(artifact_id) AS artifact_id,
        MIN(source_snapshot_id) AS source_snapshot_id,
        MIN(confidence_status) AS confidence_status,
        BOOL_OR(weak_identification_flag) AS weak_identification_flag
    FROM plays
    GROUP BY ALL
)
SELECT
    rolled.*,
    'pbp_imputed_fielding_totals' AS model_name,
    '1' AS model_version,
    'completed_raw_fielding_play_rollup' AS method,
    CASE WHEN estimated_play_credits = 0 THEN 'observed' ELSE 'mixed' END AS observed_status
FROM rolled
