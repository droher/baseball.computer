MODEL (
  name main_models.source_acquisition_ledger,
  kind FULL,
  description 'Per (game_id, team_id, dimension) provenance ledger classifying which source family covers each modeling dimension and what each row can be used for. Side-dependent dimensions (box_batting, box_pitching, box_fielding, line_score) carry a team_id; game-wide dimensions (event, pitch_sequence, batted_ball, gamelog) have team_id IS NULL. Joined by every downstream Phase 1 ledger and every modeling dataset for target_population_status, source_block_status, usable_for_event_imputation, usable_as_aggregate_constraint, and authority_rank. A play-by-play game containing any Unresolved pitch appearance (game_data_completeness.has_unresolved_pitch_appearance) has its pitch_sequence row classified as contradicted rather than ordinary sparse coverage, and neither usable flag is set for it.',
  grain (game_id, team_id, dimension),
  columns (
    game_id VARCHAR,
    team_id TEAM_ID,
    dimension VARCHAR,
    source_family VARCHAR,
    source_type VARCHAR,
    target_population_status VARCHAR,
    source_block_status VARCHAR,
    source_availability_status VARCHAR,
    usable_for_event_imputation BOOLEAN,
    usable_as_aggregate_constraint BOOLEAN,
    authority_rank UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    team_id = 'Team key for side-dependent dimensions; NULL for game-wide dimensions.',
    dimension = 'Modeling dimension covered by this row: box_batting, box_pitching, box_fielding, line_score, event, pitch_sequence, batted_ball, or gamelog.',
    source_family = 'Coarsest source family observed at this game/dimension: play_by_play, box_score, gamelog, derived, absent.',
    source_type = 'Raw game_start_info.source_type when known: PlayByPlay, BoxScore, GameLog. NULL when no source covers the game.',
    target_population_status = 'Whether and how this dimension can populate target rows: event_level, aggregate_only, gamelog_only, structural_absence, coverage_within_source_sparse, out_of_scope.',
    source_block_status = 'Whether the source block backing this dimension exists and is fully populated: present_fully_populated, present_partial_coverage, coverage_within_source_sparse, block_missing, not_applicable. present_partial_coverage on pitch_sequence marks a game whose quarantined Unresolved appearance removed part of the pitch block.',
    source_availability_status = 'Whether the source was acquired and is usable: observed, not_acquired, not_applicable, contradicted, data_error_prone. contradicted on pitch_sequence when the game has any Unresolved pitch appearance.',
    usable_for_event_imputation = 'TRUE only for event-level dimensions with non-sparse, non-missing, non-contradicted source coverage.',
    usable_as_aggregate_constraint = 'TRUE when the source can constrain an aggregate target (event_level or aggregate_only) and is not contradicted.',
    authority_rank = 'Lower rank wins when multiple source families can populate the same target. play_by_play=1, box_score=2, gamelog=3, otherwise 9.'
  ),
  audits (
    not_null(columns := (game_id, dimension, source_family, source_block_status, source_availability_status)),
    unique_grain(columns := (game_id, team_id, dimension)),
    accepted_values(column := target_population_status, is_in := ('event_level', 'aggregate_only', 'gamelog_only', 'structural_absence', 'out_of_scope', 'coverage_within_source_sparse')),
    accepted_values(column := source_block_status, is_in := ('present_fully_populated', 'present_partial_coverage', 'coverage_within_source_sparse', 'block_missing', 'not_applicable')),
    accepted_values(column := source_availability_status, is_in := ('observed', 'not_acquired', 'not_applicable', 'contradicted', 'data_error_prone')),
    accepted_values(column := source_family, is_in := ('play_by_play', 'box_score', 'gamelog', 'derived', 'absent')),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  )
);

WITH dims_side_dependent AS (
    SELECT *
    FROM (VALUES
        ('box_batting'),
        ('box_pitching'),
        ('box_fielding'),
        ('line_score')
    ) AS t(dimension)
),

dims_game_wide AS (
    SELECT *
    FROM (VALUES
        ('event'),
        ('pitch_sequence'),
        ('batted_ball'),
        ('gamelog')
    ) AS t(dimension)
),

team_games AS (
    SELECT
        game_id,
        season,
        team_id,
        source_type
    FROM main_models.team_game_start_info
    WHERE season BETWEEN @start_season AND @end_season
),

game_source AS (
    SELECT
        game_id,
        season,
        MIN(source_type) AS source_type
    FROM team_games
    GROUP BY 1, 2
),

coverage AS (
    SELECT
        game_id,
        has_pitches,
        has_count,
        has_unresolved_pitch_appearance,
        (has_trajectory OR has_location OR has_batted_to_fielder) AS has_batted_ball
    FROM main_models.game_data_completeness
),

side_dependent_dims AS (
    SELECT
        tg.game_id,
        tg.team_id,
        d.dimension,
        CASE
            WHEN tg.source_type = 'PlayByPlay' THEN 'play_by_play'
            WHEN tg.source_type = 'BoxScore' THEN 'box_score'
            WHEN tg.source_type = 'GameLog' THEN 'gamelog'
            ELSE 'absent'
        END AS source_family,
        tg.source_type,
        CASE
            WHEN tg.source_type = 'PlayByPlay' AND d.dimension IN ('box_batting', 'box_pitching', 'box_fielding') THEN 'aggregate_only'
            WHEN tg.source_type = 'PlayByPlay' AND d.dimension = 'line_score' THEN 'aggregate_only'
            WHEN tg.source_type = 'BoxScore' THEN 'aggregate_only'
            WHEN tg.source_type = 'GameLog' AND d.dimension = 'line_score' THEN 'aggregate_only'
            WHEN tg.source_type = 'GameLog' THEN 'structural_absence'
            ELSE 'structural_absence'
        END AS target_population_status,
        CASE
            WHEN tg.source_type IN ('PlayByPlay', 'BoxScore') THEN 'present_fully_populated'
            WHEN tg.source_type = 'GameLog' AND d.dimension = 'line_score' THEN 'present_fully_populated'
            WHEN tg.source_type IS NULL THEN 'block_missing'
            ELSE 'not_applicable'
        END AS source_block_status,
        CASE
            WHEN tg.source_type IS NULL THEN 'not_acquired'
            WHEN tg.source_type IN ('PlayByPlay', 'BoxScore', 'GameLog') THEN 'observed'
            ELSE 'contradicted'
        END AS source_availability_status
    FROM team_games AS tg
    CROSS JOIN dims_side_dependent AS d
),

game_wide_dims AS (
    SELECT
        gs.game_id,
        CAST(NULL AS TEAM_ID) AS team_id,
        d.dimension,
        CASE
            WHEN gs.source_type = 'PlayByPlay' THEN 'play_by_play'
            WHEN gs.source_type = 'BoxScore' THEN 'box_score'
            WHEN gs.source_type = 'GameLog' THEN 'gamelog'
            ELSE 'absent'
        END AS source_family,
        gs.source_type,
        CASE
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'event' THEN 'event_level'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'pitch_sequence' AND COALESCE(c.has_unresolved_pitch_appearance, FALSE) THEN 'event_level'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'pitch_sequence' AND COALESCE(c.has_pitches, FALSE) THEN 'event_level'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'pitch_sequence' THEN 'coverage_within_source_sparse'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'batted_ball' AND COALESCE(c.has_batted_ball, FALSE) THEN 'event_level'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'batted_ball' THEN 'coverage_within_source_sparse'
            WHEN gs.source_type IN ('PlayByPlay', 'BoxScore', 'GameLog') AND d.dimension = 'gamelog' THEN 'gamelog_only'
            ELSE 'structural_absence'
        END AS target_population_status,
        CASE
            WHEN gs.source_type IS NULL THEN 'block_missing'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'event' THEN 'present_fully_populated'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'pitch_sequence' AND COALESCE(c.has_unresolved_pitch_appearance, FALSE) THEN 'present_partial_coverage'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'pitch_sequence' AND COALESCE(c.has_pitches, FALSE) THEN 'present_fully_populated'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'pitch_sequence' THEN 'coverage_within_source_sparse'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'batted_ball' AND COALESCE(c.has_batted_ball, FALSE) THEN 'present_partial_coverage'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'batted_ball' THEN 'coverage_within_source_sparse'
            WHEN gs.source_type IN ('PlayByPlay', 'BoxScore', 'GameLog') AND d.dimension = 'gamelog' THEN 'present_fully_populated'
            ELSE 'not_applicable'
        END AS source_block_status,
        CASE
            WHEN gs.source_type IS NULL THEN 'not_acquired'
            WHEN gs.source_type = 'PlayByPlay' AND d.dimension = 'pitch_sequence' AND COALESCE(c.has_unresolved_pitch_appearance, FALSE) THEN 'contradicted'
            WHEN gs.source_type IN ('PlayByPlay', 'BoxScore', 'GameLog') THEN 'observed'
            ELSE 'contradicted'
        END AS source_availability_status
    FROM game_source AS gs
    CROSS JOIN dims_game_wide AS d
    LEFT JOIN coverage AS c USING (game_id)
),

classified AS (
    SELECT * FROM side_dependent_dims
    UNION ALL BY NAME
    SELECT * FROM game_wide_dims
)

SELECT
    *,
    target_population_status = 'event_level'
        AND source_block_status NOT IN ('coverage_within_source_sparse', 'block_missing')
        AND source_availability_status != 'contradicted' AS usable_for_event_imputation,
    target_population_status IN ('event_level', 'aggregate_only')
        AND source_availability_status != 'contradicted' AS usable_as_aggregate_constraint,
    CAST(CASE source_family
        WHEN 'play_by_play' THEN 1
        WHEN 'box_score' THEN 2
        WHEN 'gamelog' THEN 3
        ELSE 9
    END AS UTINYINT) AS authority_rank
FROM classified
