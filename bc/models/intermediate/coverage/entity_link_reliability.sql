MODEL (
  name main_models.entity_link_reliability,
  kind FULL,
  description 'Per (entity_type, source_system, source_id) provenance ledger for entity identifiers: players, teams, parks, umpires, leagues, scorers, inputters, translators. v1 emits direct rows from master sources (stg_bio, stg_teams_master, stg_parks, stg_umpires), crosswalk rows from the baseballdatabank ↔ retrosheet people map (stg_people), and unresolved rows for game-grain person strings without a master record (stg_games scorer/inputter/translator). link_status drives reliability_class per the doc-01 mapping: direct→direct; crosswalk/alias→derived; inferred→inferred; conflict/unresolved→ambiguous. Park aka annotations and missing-from-master baseballdatabank entries surface as conflict_reason rather than separate link statuses. Reserved for follow-up: stg_rosters/stg_box/stg_events traffic that references player_ids absent from stg_bio (would emit unresolved player rows); park renovation episodes; per-game scorer→bio name resolution; ledger-specific entity types beyond v1 list. Consumers (publication-tier Bayes random effects, park-factor models) filter on reliability_class IN (direct, derived) and source-system-specific allow-lists.',
  grain (entity_type, source_system, source_id),
  columns (
    entity_type VARCHAR,
    source_system VARCHAR,
    source_id VARCHAR,
    canonical_id VARCHAR,
    valid_from DATE,
    valid_to DATE,
    reliability_class VARCHAR,
    link_status VARCHAR,
    link_confidence VARCHAR,
    conflict_reason VARCHAR
  ),
  column_descriptions (
    entity_type = 'Entity family: player, team, park, umpire, league, scorer, inputter, translator.',
    source_system = 'Registry that owns this row''s source_id (e.g., retrosheet_bio, baseballdatabank_people, retrosheet_parks, retrosheet_games_scorer).',
    source_id = 'Raw identifier as stored in source_system. VARCHAR; UDT-typed ids (TEAM_ID, PARK_ID) are cast to text.',
    canonical_id = 'Project canonical identifier (typically the Retrosheet id). NULL when the row is unresolved against the canonical registry.',
    valid_from = 'Earliest date this identity is known active (NULL = unknown / open lower bound).',
    valid_to = 'Latest date this identity is known active (NULL = unknown / open upper bound).',
    reliability_class = 'FK to seed_reliability_class. Derived deterministically from link_status: direct→direct; crosswalk/alias→derived; inferred→inferred; conflict/unresolved→ambiguous.',
    link_status = 'Ledger-specific extension: direct (master record), crosswalk (cross-registry mapping with canonical resolution), alias (known alternate label tied to a canonical id), inferred (probabilistic match), conflict (ambiguous mapping), unresolved (no canonical resolution available).',
    link_confidence = 'Qualitative confidence: high (direct master / clean crosswalk), medium (crosswalk with missing temporal bounds), low (unresolved / free-text-only identity).',
    conflict_reason = 'Short token explaining a weak/aliased/unresolved link (e.g., has_aka_alias, no_master_record, missing_retrosheet_mapping). NULL when link_status = direct and no aka annotation.'
  ),
  audits (
    not_null(columns := (entity_type, source_system, source_id, reliability_class, link_status, link_confidence)),
    unique_grain(columns := (entity_type, source_system, source_id)),
    accepted_values(column := entity_type, is_in := (
      'player', 'team', 'park', 'umpire', 'league', 'scorer', 'inputter', 'translator'
    )),
    accepted_values(column := link_status, is_in := (
      'direct', 'crosswalk', 'alias', 'inferred', 'conflict', 'unresolved'
    )),
    accepted_values(column := reliability_class, is_in := (
      'direct', 'derived', 'inferred', 'synthetic', 'ambiguous'
    )),
    accepted_values(column := link_confidence, is_in := ('high', 'medium', 'low')),
    relationships(
      column := reliability_class,
      to_model := main_seeds.seed_reliability_class,
      to_column := reliability_class
    )
  )
);

WITH players_master AS (
    SELECT
        'player' AS entity_type,
        'retrosheet_bio' AS source_system,
        player_id AS source_id,
        player_id AS canonical_id,
        CAST(TRY_STRPTIME(player_debut_date, '%m/%d/%Y') AS DATE) AS valid_from,
        CAST(TRY_STRPTIME(player_last_game_date, '%m/%d/%Y') AS DATE) AS valid_to,
        'direct' AS link_status,
        'high' AS link_confidence,
        CAST(NULL AS VARCHAR) AS conflict_reason
    FROM main_models.stg_bio
    WHERE player_id IS NOT NULL
),

players_databank AS (
    SELECT
        'player' AS entity_type,
        'baseballdatabank_people' AS source_system,
        databank_player_id AS source_id,
        retrosheet_player_id AS canonical_id,
        CAST(debut AS DATE) AS valid_from,
        CAST(final_game AS DATE) AS valid_to,
        CASE
            WHEN retrosheet_player_id IS NULL THEN 'unresolved'
            ELSE 'crosswalk'
        END AS link_status,
        CASE
            WHEN retrosheet_player_id IS NULL THEN 'low'
            WHEN debut IS NULL OR final_game IS NULL THEN 'medium'
            ELSE 'medium'
        END AS link_confidence,
        CASE
            WHEN retrosheet_player_id IS NULL THEN 'missing_retrosheet_mapping'
        END AS conflict_reason
    FROM main_models.stg_people
    WHERE databank_player_id IS NOT NULL
),

players_bbref AS (
    SELECT
        'player' AS entity_type,
        'baseball_reference_people' AS source_system,
        baseball_reference_player_id AS source_id,
        retrosheet_player_id AS canonical_id,
        CAST(debut AS DATE) AS valid_from,
        CAST(final_game AS DATE) AS valid_to,
        CASE
            WHEN retrosheet_player_id IS NULL THEN 'unresolved'
            ELSE 'crosswalk'
        END AS link_status,
        CASE
            WHEN retrosheet_player_id IS NULL THEN 'low'
            ELSE 'medium'
        END AS link_confidence,
        CASE
            WHEN retrosheet_player_id IS NULL THEN 'missing_retrosheet_mapping'
        END AS conflict_reason
    FROM main_models.stg_people
    WHERE baseball_reference_player_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY baseball_reference_player_id
        ORDER BY retrosheet_player_id NULLS LAST, databank_player_id
    ) = 1
),

teams_master AS (
    SELECT
        'team' AS entity_type,
        'retrosheet_teams_master' AS source_system,
        CAST(team_id AS VARCHAR) AS source_id,
        CAST(team_id AS VARCHAR) AS canonical_id,
        MAKE_DATE(first_year, 1, 1) AS valid_from,
        MAKE_DATE(last_year, 12, 31) AS valid_to,
        'direct' AS link_status,
        'high' AS link_confidence,
        CAST(NULL AS VARCHAR) AS conflict_reason
    FROM main_models.stg_teams_master
    WHERE team_id IS NOT NULL
),

parks_master AS (
    SELECT
        'park' AS entity_type,
        'retrosheet_parks' AS source_system,
        CAST(park_id AS VARCHAR) AS source_id,
        CAST(park_id AS VARCHAR) AS canonical_id,
        CAST(TRY_STRPTIME(start_date, '%m/%d/%Y') AS DATE) AS valid_from,
        CAST(TRY_STRPTIME(end_date, '%m/%d/%Y') AS DATE) AS valid_to,
        'direct' AS link_status,
        'high' AS link_confidence,
        CASE WHEN aka IS NOT NULL THEN 'has_aka_alias' END AS conflict_reason
    FROM main_models.stg_parks
    WHERE park_id IS NOT NULL
),

umpires_master AS (
    SELECT
        'umpire' AS entity_type,
        'retrosheet_umpires' AS source_system,
        person_id AS source_id,
        person_id AS canonical_id,
        TRY_CAST(first_game_date AS DATE) AS valid_from,
        TRY_CAST(last_game_date AS DATE) AS valid_to,
        'direct' AS link_status,
        'high' AS link_confidence,
        CAST(NULL AS VARCHAR) AS conflict_reason
    FROM main_models.stg_umpires
    WHERE person_id IS NOT NULL
),

leagues_raw AS (
    SELECT league FROM main_models.stg_teams_master WHERE league IS NOT NULL
    UNION
    SELECT league FROM main_models.stg_parks WHERE league IS NOT NULL
),

leagues_master AS (
    SELECT
        'league' AS entity_type,
        'retrosheet_master_codes' AS source_system,
        league AS source_id,
        league AS canonical_id,
        CAST(NULL AS DATE) AS valid_from,
        CAST(NULL AS DATE) AS valid_to,
        'direct' AS link_status,
        'high' AS link_confidence,
        CAST(NULL AS VARCHAR) AS conflict_reason
    FROM leagues_raw
),

games_in_scope AS (
    SELECT
        scorer,
        inputter,
        translator,
        date
    FROM main_models.stg_games
    WHERE season BETWEEN @start_season AND @end_season
),

scorers_distinct AS (
    SELECT
        'scorer' AS entity_type,
        'retrosheet_games_scorer' AS source_system,
        scorer AS source_id,
        CAST(NULL AS VARCHAR) AS canonical_id,
        MIN(date) AS valid_from,
        MAX(date) AS valid_to,
        'unresolved' AS link_status,
        'low' AS link_confidence,
        'no_master_record' AS conflict_reason
    FROM games_in_scope
    WHERE scorer IS NOT NULL
    GROUP BY scorer
),

inputters_distinct AS (
    SELECT
        'inputter' AS entity_type,
        'retrosheet_games_inputter' AS source_system,
        inputter AS source_id,
        CAST(NULL AS VARCHAR) AS canonical_id,
        MIN(date) AS valid_from,
        MAX(date) AS valid_to,
        'unresolved' AS link_status,
        'low' AS link_confidence,
        'no_master_record' AS conflict_reason
    FROM games_in_scope
    WHERE inputter IS NOT NULL
    GROUP BY inputter
),

translators_distinct AS (
    SELECT
        'translator' AS entity_type,
        'retrosheet_games_translator' AS source_system,
        translator AS source_id,
        CAST(NULL AS VARCHAR) AS canonical_id,
        MIN(date) AS valid_from,
        MAX(date) AS valid_to,
        'unresolved' AS link_status,
        'low' AS link_confidence,
        'no_master_record' AS conflict_reason
    FROM games_in_scope
    WHERE translator IS NOT NULL
    GROUP BY translator
),

unioned AS (
    SELECT * FROM players_master
    UNION ALL BY NAME
    SELECT * FROM players_databank
    UNION ALL BY NAME
    SELECT * FROM players_bbref
    UNION ALL BY NAME
    SELECT * FROM teams_master
    UNION ALL BY NAME
    SELECT * FROM parks_master
    UNION ALL BY NAME
    SELECT * FROM umpires_master
    UNION ALL BY NAME
    SELECT * FROM leagues_master
    UNION ALL BY NAME
    SELECT * FROM scorers_distinct
    UNION ALL BY NAME
    SELECT * FROM inputters_distinct
    UNION ALL BY NAME
    SELECT * FROM translators_distinct
)

SELECT
    entity_type,
    source_system,
    source_id,
    canonical_id,
    valid_from,
    valid_to,
    CASE link_status
        WHEN 'direct' THEN 'direct'
        WHEN 'crosswalk' THEN 'derived'
        WHEN 'alias' THEN 'derived'
        WHEN 'inferred' THEN 'inferred'
        WHEN 'conflict' THEN 'ambiguous'
        WHEN 'unresolved' THEN 'ambiguous'
    END AS reliability_class,
    link_status,
    link_confidence,
    conflict_reason
FROM unioned
