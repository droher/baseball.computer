MODEL (
  name main_models.official_aggregate_availability,
  kind FULL,
  description 'Per (game_id, team_id, player_id, fielding_position, stat_name) decision ledger of whether an official box-score aggregate total is available, agrees with event-derived evidence, or is flagged by a data-error signal. Fielding stats only in this PR: putouts, assists, errors, double_plays. Box totals come from stg_box_score_fielding_lines (team_id derived via side + stg_games). Event totals come from event_player_fielding_stats. Personnel presence (a player took the field with zero plays) surfaces event_value = 0 vs event_value IS NULL when no event source exists. aggregate_status is one of: present_clean, present_issue_flagged, missing, not_applicable, negative_residual, contradicted. missing means box_value IS NULL — whether no box-score row exists for the player-position-game or a box row exists with a NULL value for this stat. data_error_risk is the worst training_action from source_data_error_risk_ledger joined on (game_id, team_id, player_id, stat_name) with NULL-tolerant fan-out for team-grain and game-grain risk rows. Consumed by official_credit_authority and every fielding-credit-allocation modeling dataset.',
  grain (game_id, team_id, player_id, fielding_position, stat_name),
  columns (
    game_id VARCHAR,
    team_id TEAM_ID,
    player_id VARCHAR,
    fielding_position UTINYINT,
    stat_name VARCHAR,
    aggregate_grain VARCHAR,
    aggregate_status VARCHAR,
    aggregate_value DOUBLE,
    event_value DOUBLE,
    residual_value DOUBLE,
    authority_rank UTINYINT,
    data_error_risk VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    team_id = @doc('team_id'),
    player_id = @doc('player_id'),
    fielding_position = @doc('fielding_position'),
    stat_name = 'Aggregate stat key. Fielding scope (this PR): putouts, assists, errors, double_plays. Batting / pitching / line_score stat_names will be added in follow-up PRs.',
    aggregate_grain = 'Target grain for the aggregate value. player_position_game for fielding stats. Reserved for team_game, player_game etc. in future stat additions.',
    aggregate_status = 'How the box-score aggregate compares to event evidence: present_clean (box present, agrees with event or no event evidence), present_issue_flagged (box present and agrees but a confirmed/audit-exception risk signal flags it), missing (box_value IS NULL — no box row exists, or a box row exists with a NULL value for this stat), not_applicable (stat does not apply to this position), negative_residual (box value < event value), contradicted (box value > event value and event evidence is present).',
    aggregate_value = 'Box-score aggregate value. NULL exactly when aggregate_status = missing (no box row, or a NULL-valued box row).',
    event_value = 'Event-derived comparison value: SUM of event_player_fielding_stats when event source present, 0 when only personnel presence is present (player took the field with zero plays), NULL when no event evidence exists.',
    residual_value = 'aggregate_value - event_value when both are non-null.',
    authority_rank = 'Rank within target grain/stat for resolving the official aggregate. Hardcoded to 1 in this PR (box is the sole source of player-position-game fielding totals).',
    data_error_risk = 'Worst training_action collapsed across all source_data_error_risk_ledger rows matching this (game_id, team_id, player_id, stat_name), with NULL team_id / player_id in the risk ledger fanning out to all players in the team / game. one of: none, allow, diagnostic_only, downweight, constraint_only, exclude.'
  ),
  audits (
    not_null(columns := (game_id, team_id, fielding_position, stat_name, aggregate_grain, aggregate_status, authority_rank, data_error_risk)),
    unique_grain(columns := (game_id, team_id, player_id, fielding_position, stat_name)),
    accepted_values(column := stat_name, is_in := ('putouts', 'assists', 'errors', 'double_plays')),
    accepted_values(column := aggregate_grain, is_in := ('player_position_game')),
    accepted_values(column := aggregate_status, is_in := ('present_clean', 'present_issue_flagged', 'missing', 'not_applicable', 'negative_residual', 'contradicted')),
    accepted_values(column := data_error_risk, is_in := ('none', 'allow', 'diagnostic_only', 'downweight', 'constraint_only', 'exclude')),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id),
    residual_value_matches_status()
  )
);

WITH games AS (
    SELECT
        game_id,
        home_team_id,
        away_team_id
    FROM main_models.stg_games
),

box_agg AS (
    SELECT
        b.game_id,
        CASE WHEN b.side = 'Home' THEN g.home_team_id ELSE g.away_team_id END AS team_id,
        b.fielder_id AS player_id,
        b.fielding_position,
        SUM(b.putouts)::DOUBLE AS putouts,
        SUM(b.assists)::DOUBLE AS assists,
        SUM(b.errors)::DOUBLE AS errors,
        SUM(b.double_plays)::DOUBLE AS double_plays
    FROM main_models.stg_box_score_fielding_lines AS b
    INNER JOIN games AS g USING (game_id)
    WHERE b.fielder_id IS NOT NULL
        AND b.fielding_position IS NOT NULL
        AND b.side IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

event_agg AS (
    SELECT
        game_id,
        team_id,
        player_id,
        fielding_position,
        SUM(putouts)::DOUBLE AS putouts,
        SUM(assists)::DOUBLE AS assists,
        SUM(errors)::DOUBLE AS errors,
        SUM(double_plays)::DOUBLE AS double_plays
    FROM main_models.event_player_fielding_stats
    WHERE player_id IS NOT NULL
        AND fielding_position IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

personnel_presence AS (
    SELECT DISTINCT
        game_id,
        fielding_team_id AS team_id,
        player_id,
        fielding_position
    FROM main_models.personnel_fielding_states
    WHERE player_id IS NOT NULL
        AND fielding_position IS NOT NULL
),

wide AS (
    SELECT
        COALESCE(b.game_id, e.game_id, p.game_id) AS game_id,
        COALESCE(b.team_id, e.team_id, p.team_id) AS team_id,
        COALESCE(b.player_id, e.player_id, p.player_id) AS player_id,
        COALESCE(b.fielding_position, e.fielding_position, p.fielding_position) AS fielding_position,
        b.putouts AS box_putouts,
        b.assists AS box_assists,
        b.errors AS box_errors,
        b.double_plays AS box_double_plays,
        e.putouts AS event_putouts,
        e.assists AS event_assists,
        e.errors AS event_errors,
        e.double_plays AS event_double_plays,
        e.game_id IS NOT NULL AS event_present,
        p.game_id IS NOT NULL AS personnel_present
    FROM box_agg AS b
    FULL OUTER JOIN event_agg AS e USING (game_id, team_id, player_id, fielding_position)
    FULL OUTER JOIN personnel_presence AS p USING (game_id, team_id, player_id, fielding_position)
),

stat_long AS (
    SELECT
        game_id, team_id, player_id, fielding_position,
        'putouts' AS stat_name,
        box_putouts AS box_value,
        CASE
            WHEN event_present THEN COALESCE(event_putouts, 0)
            WHEN personnel_present THEN 0
            ELSE NULL
        END AS event_value,
        event_present OR personnel_present AS event_evidence_present
    FROM wide
    UNION ALL BY NAME
    SELECT
        game_id, team_id, player_id, fielding_position,
        'assists' AS stat_name,
        box_assists AS box_value,
        CASE
            WHEN event_present THEN COALESCE(event_assists, 0)
            WHEN personnel_present THEN 0
            ELSE NULL
        END AS event_value,
        event_present OR personnel_present AS event_evidence_present
    FROM wide
    UNION ALL BY NAME
    SELECT
        game_id, team_id, player_id, fielding_position,
        'errors' AS stat_name,
        box_errors AS box_value,
        CASE
            WHEN event_present THEN COALESCE(event_errors, 0)
            WHEN personnel_present THEN 0
            ELSE NULL
        END AS event_value,
        event_present OR personnel_present AS event_evidence_present
    FROM wide
    UNION ALL BY NAME
    SELECT
        game_id, team_id, player_id, fielding_position,
        'double_plays' AS stat_name,
        box_double_plays AS box_value,
        CASE
            WHEN event_present THEN COALESCE(event_double_plays, 0)
            WHEN personnel_present THEN 0
            ELSE NULL
        END AS event_value,
        event_present OR personnel_present AS event_evidence_present
    FROM wide
),

risk_expanded AS (
    SELECT
        r.game_id,
        r.team_id,
        r.player_id,
        'putouts' AS stat_name,
        r.training_action
    FROM main_models.source_data_error_risk_ledger AS r
    WHERE r.field_name = 'fielding_putouts'
    UNION ALL BY NAME
    SELECT
        r.game_id,
        r.team_id,
        r.player_id,
        s.stat_name,
        r.training_action
    FROM main_models.source_data_error_risk_ledger AS r
    CROSS JOIN (VALUES ('putouts'), ('assists')) AS s(stat_name)
    WHERE r.field_name = 'fielding_putouts_assists'
    UNION ALL BY NAME
    SELECT
        r.game_id,
        r.team_id,
        r.player_id,
        s.stat_name,
        r.training_action
    FROM main_models.source_data_error_risk_ledger AS r
    CROSS JOIN (VALUES ('putouts'), ('assists'), ('errors')) AS s(stat_name)
    WHERE r.field_name = 'fielding_putouts_assists_errors'
),

action_severity AS (
    SELECT * FROM (VALUES
        ('exclude',         5),
        ('constraint_only', 4),
        ('downweight',      3),
        ('diagnostic_only', 2),
        ('allow',           1)
    ) AS t(training_action, severity_rank)
),

risk_with_severity AS (
    SELECT
        r.game_id,
        r.team_id,
        r.player_id,
        r.stat_name,
        s.severity_rank
    FROM risk_expanded AS r
    INNER JOIN action_severity AS s USING (training_action)
),

stat_long_with_risk AS (
    SELECT
        sl.game_id,
        sl.team_id,
        sl.player_id,
        sl.fielding_position,
        sl.stat_name,
        sl.box_value,
        sl.event_value,
        sl.event_evidence_present,
        MAX(rs.severity_rank) AS severity_rank
    FROM stat_long AS sl
    LEFT JOIN risk_with_severity AS rs
        ON rs.game_id = sl.game_id
        AND (rs.team_id IS NULL OR rs.team_id = sl.team_id)
        AND (rs.player_id IS NULL OR rs.player_id = sl.player_id)
        AND rs.stat_name = sl.stat_name
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

scored AS (
    SELECT
        game_id,
        team_id,
        player_id,
        fielding_position,
        stat_name,
        box_value,
        event_value,
        event_evidence_present,
        CASE severity_rank
            WHEN 5 THEN 'exclude'
            WHEN 4 THEN 'constraint_only'
            WHEN 3 THEN 'downweight'
            WHEN 2 THEN 'diagnostic_only'
            WHEN 1 THEN 'allow'
            ELSE 'none'
        END AS data_error_risk,
        severity_rank
    FROM stat_long_with_risk
)

SELECT
    game_id,
    team_id,
    player_id,
    fielding_position,
    stat_name,
    'player_position_game' AS aggregate_grain,
    CASE
        WHEN box_value IS NULL THEN 'missing'
        WHEN NOT event_evidence_present THEN
            CASE WHEN severity_rank >= 4 THEN 'present_issue_flagged' ELSE 'present_clean' END
        WHEN box_value < event_value THEN 'negative_residual'
        WHEN box_value = event_value THEN
            CASE WHEN severity_rank >= 4 THEN 'present_issue_flagged' ELSE 'present_clean' END
        ELSE 'contradicted'
    END AS aggregate_status,
    box_value::DOUBLE AS aggregate_value,
    event_value::DOUBLE AS event_value,
    CASE
        WHEN box_value IS NOT NULL AND event_value IS NOT NULL THEN (box_value - event_value)::DOUBLE
    END AS residual_value,
    CAST(1 AS UTINYINT) AS authority_rank,
    data_error_risk
FROM scored
