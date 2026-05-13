MODEL (
  name main_models.source_data_error_risk_ledger,
  kind FULL,
  description 'Per (source_table, game, team, player, field, issue_source) ledger of known data-error risk on source rows. Confirmed arithmetic violations (box_score_data_issues), audit-exempt artifacts (team_game_data_issues), box-vs-event contradictions (box_event_fielding_discrepancies), suspected source/parser issues for missing box rows (unknown_play_no_box), and suspected assists-miscoded-as-putouts scorer/parser pattern (assists_as_putouts_finder). Sibling to source_acquisition_ledger: that ledger says whether a source exists, this one says whether — when it exists — it is trustworthy. Every downstream Phase 1 event-observation ledger joins to both ledgers to compute model_input_eligible. data_error_key is a stable MD5 hex digest over the natural-key columns and is the unique grain.',
  grain (data_error_key),
  columns (
    data_error_key VARCHAR,
    source_table VARCHAR,
    game_id VARCHAR,
    team_id TEAM_ID,
    player_id VARCHAR,
    field_name VARCHAR,
    data_error_class VARCHAR,
    training_action VARCHAR,
    training_weight DOUBLE,
    issue_source VARCHAR
  ),
  column_descriptions (
    data_error_key = 'MD5 hex digest over (source_table, game_id, team_id, player_id, field_name, issue_source). Stable, fixed-width 32-char key; the unique grain. Downstream ledgers join on this column.',
    source_table = 'Upstream table or view where the data-error signal originates.',
    game_id = @doc('game_id'),
    team_id = @doc('team_id'),
    player_id = @doc('player_id'),
    field_name = 'Affected field or composite stat. For multi-stat rollups (e.g. fielding putouts/assists/errors) a composite name is used so downstream sums by (game, player, field_name) do not double-count weight.',
    data_error_class = 'Risk class: confirmed_issue, suspected_source_issue, suspected_parser_issue, contradiction, audit_exception.',
    training_action = 'How training pipelines should treat the row: allow, downweight, exclude, constraint_only, diagnostic_only. confirmed_issue rows can never use allow (enforced by custom audit).',
    training_weight = 'Multiplicative weight in [0, 1] applied to the affected row/field during model fitting.',
    issue_source = 'Originating audit, issue table, or discrepancy view; includes the specific issue_type when the upstream enumerates one.'
  ),
  audits (
    not_null(columns := (data_error_key, source_table, game_id, field_name, data_error_class, training_action, training_weight, issue_source)),
    unique_grain(columns := (data_error_key)),
    accepted_values(column := data_error_class, is_in := ('confirmed_issue', 'suspected_source_issue', 'suspected_parser_issue', 'contradiction', 'audit_exception')),
    accepted_values(column := training_action, is_in := ('allow', 'downweight', 'exclude', 'constraint_only', 'diagnostic_only')),
    bounded_range(column := training_weight, min_v := 0.0, max_v := 1.0),
    confirmed_issue_not_allowed(),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  )
);

WITH box_score_issues AS (
    SELECT
        bsi.source_table,
        bsi.game_id,
        CAST(NULL AS TEAM_ID) AS team_id,
        bsi.player_id,
        CASE bsi.issue_type
            WHEN 'hits_gt_at_bats' THEN 'hits'
            WHEN 'strikeouts_gt_plate_appearances' THEN 'strikeouts'
            WHEN 'extra_base_hits_gt_hits' THEN 'extra_base_hits'
            WHEN 'hits_gt_batters_faced' THEN 'hits'
            WHEN 'home_runs_gt_hits' THEN 'home_runs'
            WHEN 'strikeouts_gt_batters_faced' THEN 'strikeouts'
            WHEN 'earned_runs_gt_runs' THEN 'earned_runs'
        END AS field_name,
        'confirmed_issue' AS data_error_class,
        'downweight' AS training_action,
        0.25 AS training_weight,
        'box_score_data_issues:' || bsi.issue_type AS issue_source
    FROM main_models.box_score_data_issues AS bsi
),

team_game_issues AS (
    SELECT
        'main_models.team_game_data_issues' AS source_table,
        tgi.game_id,
        tgi.team_id,
        CAST(NULL AS VARCHAR) AS player_id,
        'games_started' AS field_name,
        'audit_exception' AS data_error_class,
        'constraint_only' AS training_action,
        1.0 AS training_weight,
        'team_game_data_issues:' || tgi.issue_type AS issue_source
    FROM main_models.team_game_data_issues AS tgi
    WHERE tgi.issue_type = 'starting_pitcher_no_appearance'
),

fielding_discrepancies AS (
    SELECT
        'main_models.box_event_fielding_discrepancies' AS source_table,
        bfd.game_id,
        bfd.team_id,
        CAST(NULL AS VARCHAR) AS player_id,
        'fielding_putouts_assists_errors' AS field_name,
        'contradiction' AS data_error_class,
        'downweight' AS training_action,
        0.5 AS training_weight,
        'box_event_fielding_discrepancies' AS issue_source
    FROM main_models.box_event_fielding_discrepancies AS bfd
),

unknown_play AS (
    SELECT
        'main_models.unknown_play_no_box' AS source_table,
        upnb.game_id,
        CAST(NULL AS TEAM_ID) AS team_id,
        CAST(NULL AS VARCHAR) AS player_id,
        'fielding_putouts' AS field_name,
        'suspected_source_issue' AS data_error_class,
        'downweight' AS training_action,
        0.25 AS training_weight,
        'unknown_play_no_box' AS issue_source
    FROM main_models.unknown_play_no_box AS upnb
),

assists_as_putouts AS (
    SELECT
        'main_models.assists_as_putouts_finder' AS source_table,
        aap.game_id,
        aap.team_id,
        CAST(NULL AS VARCHAR) AS player_id,
        'fielding_putouts_assists' AS field_name,
        'suspected_source_issue' AS data_error_class,
        'downweight' AS training_action,
        0.25 AS training_weight,
        'assists_as_putouts_finder' AS issue_source
    FROM main_models.assists_as_putouts_finder AS aap
),

unioned AS (
    SELECT * FROM box_score_issues
    UNION ALL BY NAME
    SELECT * FROM team_game_issues
    UNION ALL BY NAME
    SELECT * FROM fielding_discrepancies
    UNION ALL BY NAME
    SELECT * FROM unknown_play
    UNION ALL BY NAME
    SELECT * FROM assists_as_putouts
)

SELECT
    MD5(CONCAT_WS('|',
        source_table,
        game_id,
        COALESCE(team_id::VARCHAR, ''),
        COALESCE(player_id, ''),
        field_name,
        issue_source
    )) AS data_error_key,
    source_table,
    game_id,
    team_id,
    player_id,
    field_name,
    data_error_class,
    training_action,
    training_weight,
    issue_source
FROM unioned
