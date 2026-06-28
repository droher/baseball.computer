MODEL (
  name main_models.fielding_credit_gaps,
  kind FULL,
  description 'Per (event_key, fielding_team_id) ledger classifying fielding-credit trustworthiness. fielding_evidence_status sorts events into complete_with_zero_unknowns / complete_with_known_unknowns / no_fielding_row / incomplete_event_flag (the last is unreachable in v1 — event_states_full does not surface a structural-incompleteness flag yet, so the driver inlines FALSE; enum value reserved for an enrichment PR). gap_class refines further into eight buckets driving Phase-2 fielding-credit allocation. fielding_row_present keys on calc_fielding_play_agg only — pre-implementation EDA showed 5.32M events have event_player_fielding_stats rows but no calc_fielding_play_agg rows (walks/HBP/K with fielders on the field but no fielding play), and doc-01 intends those to register as no_fielding_row. Aggregate residuals come from official_aggregate_availability summed per (game_id, team_id) across all (player, position) at stat_name in (putouts, assists, errors). personnel_hard_mask_available COALESCEs to FALSE so no_play events (which lack personnel_state_reliability rows because PSR filters WHERE NOT no_play_flag) are correctly ineligible for allocation.',
  grain (event_key, fielding_team_id),
  columns (
    event_key UINTEGER,
    fielding_team_id TEAM_ID,
    fielding_evidence_status VARCHAR,
    gap_class VARCHAR,
    unknown_putouts DOUBLE,
    known_putouts DOUBLE,
    known_assists DOUBLE,
    known_errors DOUBLE,
    has_clean_aggregate_total BOOLEAN,
    aggregate_residual_putouts DOUBLE,
    aggregate_residual_assists DOUBLE,
    aggregate_residual_errors DOUBLE,
    personnel_hard_mask_available BOOLEAN,
    eligible_for_allocation BOOLEAN
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    fielding_team_id = @doc('team_id'),
    fielding_evidence_status = 'Top-level fielding-credit trust bucket: complete_with_zero_unknowns, complete_with_known_unknowns, no_fielding_row, incomplete_event_flag. The last is unreachable in v1.',
    gap_class = 'Refined gap taxonomy: complete, unknown_putout, unknown_assist_risk, box_residual_positive, box_residual_negative, no_box_unknown, data_error_flagged, not_applicable. unknown_assist_risk and data_error_flagged unreachable in v1.',
    unknown_putouts = 'SUM(unknown_putouts) over calc_fielding_play_agg per event. NULL iff no fielding row exists; 0 iff fielding row exists with zero unknowns; >0 iff at least one putout was credited to fielding_position=0.',
    known_putouts = 'SUM(putouts) per (event, team) over event_player_fielding_stats. NULL when no player-fielding-stat row exists for the team on the event.',
    known_assists = 'SUM(assists) per (event, team) over event_player_fielding_stats. NULL when no player-fielding-stat row exists for the team on the event.',
    known_errors = 'SUM(errors) per (event, team) over event_player_fielding_stats. NULL when no player-fielding-stat row exists for the team on the event.',
    has_clean_aggregate_total = 'BOOL_OR(aggregate_status = present_clean) per (game_id, team_id) over official_aggregate_availability. TRUE if any (player, position, stat) row in the team-game has a clean aggregate; FALSE otherwise.',
    aggregate_residual_putouts = 'SUM of residual_value for stat_name=putouts across the team-game. COALESCED to 0 when the team-game has no aggregate row.',
    aggregate_residual_assists = 'SUM of residual_value for stat_name=assists across the team-game. COALESCED to 0 when the team-game has no aggregate row.',
    aggregate_residual_errors = 'SUM of residual_value for stat_name=errors across the team-game. COALESCED to 0 when the team-game has no aggregate row.',
    personnel_hard_mask_available = 'BOOL_AND(hard_zero_allowed) per event over personnel_state_reliability. COALESCED to FALSE for no_play_flag events (PSR filters WHERE NOT no_play_flag upstream).',
    eligible_for_allocation = 'TRUE iff gap_class IN (unknown_putout, no_box_unknown, box_residual_positive) AND personnel_hard_mask_available. Marks rows the Phase-2 fielding-credit allocation models should target.'
  ),
  audits (
    not_null(columns := (
      event_key, fielding_team_id, fielding_evidence_status, gap_class,
      has_clean_aggregate_total, eligible_for_allocation
    )),
    unique_grain(columns := (event_key, fielding_team_id)),
    accepted_values(column := fielding_evidence_status, is_in := (
      'complete_with_zero_unknowns', 'complete_with_known_unknowns',
      'no_fielding_row', 'incomplete_event_flag'
    )),
    accepted_values(column := gap_class, is_in := (
      'complete', 'unknown_putout', 'unknown_assist_risk',
      'box_residual_positive', 'box_residual_negative',
      'no_box_unknown', 'data_error_flagged', 'not_applicable'
    )),
    bounded_range(column := unknown_putouts, min_v := 0, max_v := 999),
    relationships(
      column := event_key,
      to_model := main_models.event_states_full,
      to_column := event_key
    ),
    unknown_putouts_null_iff_no_fielding_row(),
    eligible_for_allocation_requires_hard_mask()
  )
);

WITH events AS (
    SELECT
        event_key,
        season,
        game_id,
        fielding_team_id,
        FALSE AS is_incomplete_event
    FROM main_models.event_states_full
    WHERE season BETWEEN @start_season AND @end_season
),

fielding_agg AS (
    SELECT
        event_key,
        SUM(unknown_putouts)::DOUBLE AS unknown_putouts,
        TRUE AS fielding_row_present
    FROM main_models.calc_fielding_play_agg
    GROUP BY event_key
),

player_stat_agg AS (
    SELECT
        event_key,
        team_id,
        SUM(putouts)::DOUBLE AS known_putouts,
        SUM(assists)::DOUBLE AS known_assists,
        SUM(errors)::DOUBLE AS known_errors
    FROM main_models.event_player_fielding_stats
    GROUP BY event_key, team_id
),

aggregate_totals AS (
    SELECT
        game_id,
        team_id,
        SUM(CASE WHEN stat_name = 'putouts' THEN residual_value ELSE 0 END)::DOUBLE AS aggregate_residual_putouts,
        SUM(CASE WHEN stat_name = 'assists' THEN residual_value ELSE 0 END)::DOUBLE AS aggregate_residual_assists,
        SUM(CASE WHEN stat_name = 'errors' THEN residual_value ELSE 0 END)::DOUBLE AS aggregate_residual_errors,
        BOOL_OR(aggregate_status = 'present_clean') AS has_clean_aggregate_total
    FROM main_models.official_aggregate_availability
    GROUP BY game_id, team_id
),

personnel AS (
    SELECT
        event_key,
        BOOL_AND(hard_zero_allowed) AS personnel_hard_mask_available
    FROM main_models.personnel_state_reliability
    GROUP BY event_key
),

joined AS (
    SELECT
        ev.event_key,
        ev.fielding_team_id,
        ev.is_incomplete_event,
        COALESCE(fa.fielding_row_present, FALSE) AS fielding_row_present,
        fa.unknown_putouts,
        ps.known_putouts,
        ps.known_assists,
        ps.known_errors,
        COALESCE(at.has_clean_aggregate_total, FALSE) AS has_clean_aggregate_total,
        COALESCE(at.aggregate_residual_putouts, 0)::DOUBLE AS aggregate_residual_putouts,
        COALESCE(at.aggregate_residual_assists, 0)::DOUBLE AS aggregate_residual_assists,
        COALESCE(at.aggregate_residual_errors, 0)::DOUBLE AS aggregate_residual_errors,
        COALESCE(p.personnel_hard_mask_available, FALSE) AS personnel_hard_mask_available
    FROM events AS ev
    LEFT JOIN fielding_agg AS fa USING (event_key)
    LEFT JOIN player_stat_agg AS ps
        ON ev.event_key = ps.event_key
        AND ev.fielding_team_id = ps.team_id
    LEFT JOIN aggregate_totals AS at
        ON ev.game_id = at.game_id
        AND ev.fielding_team_id = at.team_id
    LEFT JOIN personnel AS p USING (event_key)
),

classified AS (
    SELECT
        event_key,
        fielding_team_id,
        CASE
            WHEN is_incomplete_event THEN 'incomplete_event_flag'
            WHEN NOT fielding_row_present THEN 'no_fielding_row'
            WHEN COALESCE(unknown_putouts, 0) > 0 THEN 'complete_with_known_unknowns'
            ELSE 'complete_with_zero_unknowns'
        END AS fielding_evidence_status,
        CASE
            WHEN is_incomplete_event THEN 'data_error_flagged'
            WHEN NOT fielding_row_present THEN 'not_applicable'
            WHEN COALESCE(unknown_putouts, 0) > 0 AND has_clean_aggregate_total THEN 'unknown_putout'
            WHEN COALESCE(unknown_putouts, 0) > 0 THEN 'no_box_unknown'
            WHEN aggregate_residual_putouts > 0 THEN 'box_residual_positive'
            WHEN aggregate_residual_putouts < 0 THEN 'box_residual_negative'
            ELSE 'complete'
        END AS gap_class,
        unknown_putouts,
        known_putouts,
        known_assists,
        known_errors,
        has_clean_aggregate_total,
        aggregate_residual_putouts,
        aggregate_residual_assists,
        aggregate_residual_errors,
        personnel_hard_mask_available
    FROM joined
)

SELECT
    event_key,
    fielding_team_id,
    fielding_evidence_status,
    gap_class,
    unknown_putouts,
    known_putouts,
    known_assists,
    known_errors,
    has_clean_aggregate_total,
    aggregate_residual_putouts,
    aggregate_residual_assists,
    aggregate_residual_errors,
    personnel_hard_mask_available,
    (gap_class IN ('unknown_putout', 'no_box_unknown', 'box_residual_positive')
     AND personnel_hard_mask_available) AS eligible_for_allocation
FROM classified
