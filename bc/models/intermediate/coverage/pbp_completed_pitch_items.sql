MODEL (
  name main_models.pbp_completed_pitch_items,
  kind FULL,
  grain (event_key, sequence_index),
  audits (estimated_contract_complete, min_row_count(threshold := 1), unique_grain(columns := (event_key, sequence_index))),
  description 'Normalized completed pitch items. Source items and flags are retained when their sequence index resolves; reconstructed items state their prior or structural-zero provenance.'
);

WITH completed_items AS (
    SELECT
        completed.event_key,
        completed.game_id,
        completed.event_id,
        completed.season,
        completed.appearance_start_event_id,
        event.batter_id,
        event.pitcher_id,
        completed.source_resolution_status,
        completed.raw_pitch_sequence,
        completed.source_pitch_sequence,
        completed.completed_pitch_sequence,
        item.ordinality::BIGINT AS item_ordinality,
        item.completed_sequence_item::VARCHAR AS completed_sequence_item,
        completed.pitch_sequence_method,
        completed.pitch_token_completion_method,
        completed.constraint_status,
        completed.constraint_disposition,
        completed.artifact_id,
        completed.source_snapshot_id,
        completed.model_version AS source_model_version,
        completed.confidence_status AS source_confidence_status,
        completed.weak_identification_flag AS source_weak_identification_flag
    FROM main_models.pbp_completed_pitches AS completed
    INNER JOIN main_models.stg_events AS event USING (event_key)
    CROSS JOIN UNNEST(STRING_SPLIT(completed.completed_pitch_sequence, '|'))
        WITH ORDINALITY AS item(completed_sequence_item, ordinality)
    WHERE completed.completed_pitch_sequence <> ''
),
source_items AS (
    SELECT
        event_key,
        sequence_id::UTINYINT AS source_sequence_index,
        ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY sequence_id) AS item_ordinality,
        sequence_item::VARCHAR AS source_sequence_item,
        runners_going_flag,
        blocked_by_catcher_flag,
        catcher_pickoff_attempt_at_base
    FROM main_models.stg_event_pitch_sequences
)
SELECT
    completed.* EXCLUDE (
        item_ordinality,
        artifact_id,
        source_snapshot_id,
        source_model_version,
        source_confidence_status,
        source_weak_identification_flag
    ),
    COALESCE(source.source_sequence_index, completed.item_ordinality - 1)::UTINYINT
        AS sequence_index,
    source.source_sequence_index,
    source.source_sequence_item,
    source.runners_going_flag AS source_runners_going_flag,
    source.blocked_by_catcher_flag AS source_blocked_by_catcher_flag,
    source.catcher_pickoff_attempt_at_base AS source_catcher_pickoff_attempt_at_base,
    CASE WHEN source.source_sequence_item IS NOT NULL
        THEN COALESCE(source.runners_going_flag, FALSE) ELSE FALSE END
        AS completed_runners_going_flag,
    CASE WHEN source.source_sequence_item IS NOT NULL
        THEN COALESCE(source.blocked_by_catcher_flag, FALSE) ELSE FALSE END
        AS completed_blocked_by_catcher_flag,
    source.catcher_pickoff_attempt_at_base AS completed_catcher_pickoff_attempt_at_base,
    CASE WHEN source.source_sequence_item IS NOT NULL THEN 'source_flag_preserved'
        ELSE 'reconstructed_no_action_zero' END AS runners_going_completion_method,
    CASE WHEN source.source_sequence_item IS NOT NULL THEN 'source_flag_preserved'
        ELSE 'reconstructed_no_action_zero' END AS blocked_by_catcher_completion_method,
    CASE WHEN source.source_sequence_item IS NOT NULL THEN 'source_action_preserved'
        ELSE 'reconstructed_no_action_null' END AS catcher_pickoff_completion_method,
    CASE
        WHEN completed.source_resolution_status = 'Resolved'
          AND source.source_sequence_item IS NOT NULL THEN 'matched_source_sequence_ordinality'
        WHEN completed.source_resolution_status = 'Resolved' THEN 'source_index_absent'
        WHEN completed.pitch_sequence_method = 'structural_automatic_intentional_walk'
          THEN 'structural_zero_has_no_item'
        ELSE 'reconstructed_prior_or_count_boundary'
    END AS source_item_evidence_status,
    CASE
        WHEN completed.source_resolution_status = 'Resolved'
          AND source.source_sequence_item = completed.completed_sequence_item
            THEN 'observed_source_item'
        WHEN completed.source_resolution_status = 'Resolved'
          AND source.source_sequence_item IS NOT NULL THEN 'declared_token_fallback'
        WHEN completed.pitch_sequence_method = 'structural_automatic_intentional_walk'
          THEN 'structural_zero'
        ELSE 'reconstructed_prior'
    END AS item_method,
    completed.artifact_id,
    'pbp_completed_pitch_items' AS model_name,
    '1' AS model_version,
    completed.source_snapshot_id,
    'normalized_source_preserving_pitch_items' AS method,
    CASE WHEN completed.source_resolution_status = 'Resolved'
          AND source.source_sequence_item = completed.completed_sequence_item
        THEN 'observed' ELSE 'estimated' END AS observed_status,
    completed.source_confidence_status AS confidence_status,
    completed.source_weak_identification_flag AS weak_identification_flag
FROM completed_items AS completed
LEFT JOIN source_items AS source USING (event_key, item_ordinality)
