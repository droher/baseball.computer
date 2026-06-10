MODEL (
  name main_models.official_credit_authority,
  kind FULL,
  description 'Per (game_id, team_id, player_id, fielding_position, credit_type) decision ledger resolving which source owns the official fielding credit at the player-position-game grain. Drives directly off official_aggregate_availability with one output row per oaa row (credit_type = oaa.stat_name). authority_source bundles the box-score aggregate status, event-side evidence presence, and data_error_risk signal into one of six classes (event, box, event_box_reconciled, estimated_with_aggregate_constraint, estimated_no_aggregate_constraint, withheld). authority_reason names the rule that triggered. can_publish_official is TRUE only when source authority is official at the target grain (event / box / event_box_reconciled). can_publish_estimated is TRUE for everything except withheld (estimated namespaces can publish estimated counters even without a box constraint). estimated_no_aggregate_constraint reserved in accepted_values but unreachable in v1 (oaa missing + event_value IS NULL rows do occur in fielding scope — a NULL-valued box row with no event evidence — but they classify as withheld/no_evidence; no-box-unknown detection waits on a follow-up that joins fielding_credit_gaps gap_class up to the team-game).',
  grain (game_id, team_id, player_id, fielding_position, credit_type),
  columns (
    game_id VARCHAR,
    team_id TEAM_ID,
    player_id VARCHAR,
    fielding_position UTINYINT,
    credit_type VARCHAR,
    authority_source VARCHAR,
    authority_reason VARCHAR,
    can_publish_official BOOLEAN,
    can_publish_estimated BOOLEAN
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    team_id = @doc('team_id'),
    player_id = @doc('player_id'),
    fielding_position = @doc('fielding_position'),
    credit_type = 'Fielding credit dimension. Mirrors official_aggregate_availability.stat_name in v1: putouts, assists, errors, double_plays.',
    authority_source = 'Which source owns the official credit at this grain: event (PBP-only, no box), box (box-only, no event evidence), event_box_reconciled (box and event agree), estimated_with_aggregate_constraint (box vs event disagree but a clean aggregate exists to constrain allocation), estimated_no_aggregate_constraint (no box, event has unknown-fielder credit; reserved, not emitted in v1), withheld (data error excluded the row or the residual is structurally invalid).',
    authority_reason = 'Short rule name for the authority decision. clean_match, box_only, event_only_no_box, contradicted_residual, contradicted_with_risk_signal, negative_residual_invalid, source_error_excluded, present_issue_flagged, no_evidence, no_box_unknown (reserved).',
    can_publish_official = 'TRUE iff authority_source IN (event, box, event_box_reconciled) — the row carries an official-grade counter.',
    can_publish_estimated = 'TRUE iff authority_source != withheld — the row can feed an estimated namespace counter (official-grade rows trivially can; estimated_with/no_aggregate_constraint rows belong only to estimated namespaces).'
  ),
  audits (
    not_null(columns := (
      game_id, team_id, fielding_position, credit_type,
      authority_source, authority_reason, can_publish_official, can_publish_estimated
    )),
    unique_grain(columns := (game_id, team_id, player_id, fielding_position, credit_type)),
    accepted_values(column := credit_type, is_in := (
      'putouts', 'assists', 'errors', 'double_plays'
    )),
    accepted_values(column := authority_source, is_in := (
      'event', 'box', 'event_box_reconciled',
      'estimated_with_aggregate_constraint', 'estimated_no_aggregate_constraint',
      'withheld'
    )),
    accepted_values(column := authority_reason, is_in := (
      'clean_match', 'box_only', 'event_only_no_box',
      'contradicted_residual', 'contradicted_with_risk_signal',
      'negative_residual_invalid', 'source_error_excluded',
      'present_issue_flagged', 'no_evidence', 'no_box_unknown'
    )),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  )
);

WITH classified AS (
    SELECT
        game_id,
        team_id,
        player_id,
        fielding_position,
        stat_name AS credit_type,
        CASE
            WHEN data_error_risk = 'exclude' THEN 'withheld'
            WHEN aggregate_status = 'negative_residual' THEN 'withheld'
            WHEN aggregate_status = 'contradicted' THEN 'estimated_with_aggregate_constraint'
            WHEN aggregate_status = 'present_clean' AND event_value IS NULL THEN 'box'
            WHEN aggregate_status = 'present_clean' THEN 'event_box_reconciled'
            WHEN aggregate_status = 'missing' AND event_value IS NOT NULL THEN 'event'
            WHEN aggregate_status = 'missing' THEN 'withheld'
            WHEN aggregate_status = 'present_issue_flagged' THEN 'estimated_with_aggregate_constraint'
            ELSE 'withheld'
        END AS authority_source,
        CASE
            WHEN data_error_risk = 'exclude' THEN 'source_error_excluded'
            WHEN aggregate_status = 'negative_residual' THEN 'negative_residual_invalid'
            WHEN aggregate_status = 'contradicted' AND data_error_risk IN ('downweight', 'diagnostic_only', 'constraint_only') THEN 'contradicted_with_risk_signal'
            WHEN aggregate_status = 'contradicted' THEN 'contradicted_residual'
            WHEN aggregate_status = 'present_clean' AND event_value IS NULL THEN 'box_only'
            WHEN aggregate_status = 'present_clean' THEN 'clean_match'
            WHEN aggregate_status = 'missing' AND event_value IS NOT NULL THEN 'event_only_no_box'
            WHEN aggregate_status = 'missing' THEN 'no_evidence'
            WHEN aggregate_status = 'present_issue_flagged' THEN 'present_issue_flagged'
            ELSE 'no_evidence'
        END AS authority_reason
    FROM main_models.official_aggregate_availability
)

SELECT
    game_id,
    team_id,
    player_id,
    fielding_position,
    credit_type,
    authority_source,
    authority_reason,
    authority_source IN ('event', 'box', 'event_box_reconciled') AS can_publish_official,
    authority_source != 'withheld' AS can_publish_estimated
FROM classified
