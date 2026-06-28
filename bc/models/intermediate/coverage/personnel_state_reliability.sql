MODEL (
  name main_models.personnel_state_reliability,
  kind FULL,
  description 'Per (event_key, fielding_side, fielding_position, player_id) reliability ledger for fielding personnel assignments. v1 emits direct_event (a personnel_fielding_state row covers the event/side/position) and missing (no personnel_fielding_state row for the slot). lineup_derived, box_derived, synthetic, duplicate_position, and ambiguous_substitution stay in accepted_values for future enrichment but are unreachable under current upstreams: event_personnel_lookup grain (event_key) and personnel_fielding_states QUALIFY-dedupe collapse multi-row cases upstream. reliability_class maps direct_event -> direct, missing -> inferred. hard_zero_allowed = TRUE only for reliability_class IN (direct, derived) (mirrors seed_reliability_class.is_hard_mask_eligible); in v1 only direct_event rows are hard-mask eligible. Consumed by fielding-credit-allocation modeling datasets and by official_credit_authority.',
  grain (event_key, fielding_side, fielding_position, player_id),
  columns (
    season SMALLINT,
    game_id VARCHAR,
    event_key UINTEGER,
    fielding_side SIDE,
    fielding_team_id TEAM_ID,
    fielding_position UTINYINT,
    player_id VARCHAR,
    eligibility_status VARCHAR,
    reliability_class VARCHAR,
    hard_zero_allowed BOOLEAN,
    personnel_confidence VARCHAR,
    issue_reason VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_key = @doc('event_key'),
    fielding_side = @doc('side'),
    fielding_team_id = @doc('team_id'),
    fielding_position = @doc('fielding_position'),
    player_id = @doc('player_id'),
    season = @doc('season'),
    eligibility_status = 'How the (event, side, position, player) assignment was established. v1 reachable values: direct_event (covered by a personnel_fielding_state row), missing (no personnel_fielding_state row for this slot). Reserved for follow-up: lineup_derived, box_derived, synthetic, duplicate_position, ambiguous_substitution.',
    reliability_class = 'FK to seed_reliability_class. v1 reachable values: direct (from direct_event) and inferred (from missing). Hard-mask eligibility is gated by hard_zero_allowed, which mirrors seed_reliability_class.is_hard_mask_eligible.',
    hard_zero_allowed = 'TRUE when a fielding allocation model may assign zero probability to players outside this state. v1: TRUE only when eligibility_status = direct_event.',
    personnel_confidence = 'Qualitative confidence token. v1 values: high (direct_event), low (missing). Promoted to a seed FK if/when a follow-up adds derived/box/synthetic classes.',
    issue_reason = 'When the slot is not direct_event, a short reason token. v1 values: no_personnel_assignment (for missing). NULL for direct_event.'
  ),
  audits (
    not_null(columns := (season, game_id, event_key, fielding_side, fielding_position, eligibility_status, reliability_class, hard_zero_allowed)),
    unique_grain(columns := (event_key, fielding_side, fielding_position, player_id)),
    accepted_values(column := eligibility_status, is_in := ('direct_event', 'lineup_derived', 'box_derived', 'synthetic', 'missing', 'duplicate_position', 'ambiguous_substitution')),
    accepted_values(column := reliability_class, is_in := ('direct', 'derived', 'inferred', 'synthetic', 'ambiguous')),
    relationships(column := reliability_class, to_model := main_seeds.seed_reliability_class, to_column := reliability_class),
    at_most_one_hard_mask_per_position()
  )
);

WITH events_filtered AS (
    SELECT
        event_key,
        game_id,
        event_id,
        season,
        fielding_team_id,
        CASE WHEN batting_side = 'Home' THEN 'Away'::SIDE ELSE 'Home'::SIDE END AS fielding_side
    FROM main_models.stg_events
    WHERE NOT no_play_flag
),

events_with_key AS (
    SELECT
        e.event_key,
        e.game_id,
        e.event_id,
        e.season,
        e.fielding_team_id,
        e.fielding_side,
        epl.personnel_fielding_key
    FROM events_filtered AS e
    LEFT JOIN main_models.event_personnel_lookup AS epl USING (game_id, event_id, event_key)
),

fielding_slots AS (
    SELECT
        ewk.event_key,
        ewk.game_id,
        ewk.season,
        ewk.fielding_side,
        ewk.fielding_team_id,
        pfs.fielding_position,
        pfs.player_id
    FROM events_with_key AS ewk
    INNER JOIN main_models.personnel_fielding_states AS pfs
        ON pfs.personnel_fielding_key = ewk.personnel_fielding_key
),

expected_slots AS (
    SELECT
        ewk.event_key,
        ewk.game_id,
        ewk.season,
        ewk.fielding_side,
        ewk.fielding_team_id,
        p.fielding_position
    FROM events_with_key AS ewk
    CROSS JOIN (VALUES
        (CAST(1 AS UTINYINT)),
        (CAST(2 AS UTINYINT)),
        (CAST(3 AS UTINYINT)),
        (CAST(4 AS UTINYINT)),
        (CAST(5 AS UTINYINT)),
        (CAST(6 AS UTINYINT)),
        (CAST(7 AS UTINYINT)),
        (CAST(8 AS UTINYINT)),
        (CAST(9 AS UTINYINT))
    ) AS p(fielding_position)
),

merged AS (
    SELECT
        es.event_key,
        es.game_id,
        es.season,
        es.fielding_side,
        es.fielding_team_id,
        es.fielding_position,
        fs.player_id
    FROM expected_slots AS es
    LEFT JOIN fielding_slots AS fs
        USING (event_key, game_id, season, fielding_side, fielding_team_id, fielding_position)
),

classified AS (
    SELECT
        m.event_key,
        m.game_id,
        m.season,
        m.fielding_side,
        m.fielding_team_id,
        m.fielding_position,
        m.player_id,
        CASE
            WHEN m.player_id IS NULL THEN 'missing'
            ELSE 'direct_event'
        END AS eligibility_status
    FROM merged AS m
)

SELECT
    season,
    game_id,
    event_key,
    fielding_side,
    fielding_team_id,
    fielding_position,
    player_id,
    eligibility_status,
    CASE eligibility_status
        WHEN 'direct_event' THEN 'direct'
        WHEN 'missing' THEN 'inferred'
    END AS reliability_class,
    eligibility_status = 'direct_event' AS hard_zero_allowed,
    CASE eligibility_status
        WHEN 'direct_event' THEN 'high'
        WHEN 'missing' THEN 'low'
    END AS personnel_confidence,
    CASE eligibility_status
        WHEN 'missing' THEN 'no_personnel_assignment'
    END AS issue_reason
FROM classified
