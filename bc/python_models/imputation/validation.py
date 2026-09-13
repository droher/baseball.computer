from __future__ import annotations

import logging

import duckdb

from python_models.imputation.artifacts import ComponentArtifact, sql_literal
from python_models.imputation.context import CONTEXT_FIELDS, ContextCompletionConfig

_log = logging.getLogger(__name__)


def validate_component(
    connection: duckdb.DuckDBPyConnection,
    artifact: ComponentArtifact,
    config: ContextCompletionConfig,
) -> dict[str, int]:
    if artifact.name in {
        "event_values",
        "park_factors",
        "run_expectancy",
        "state_transitions",
        "linear_weights",
    }:
        from python_models.imputation.values_validation import validate_values_component

        return validate_values_component(connection, artifact, config)
    parquet = f"read_parquet({sql_literal(artifact.data.path)})"
    joins = {
        "context": ("game_id",),
        "officials": ("game_id", "role"),
        "geometry": ("event_key",),
        "pitches": ("event_key",),
        "runners": ("event_key", "baserunner"),
        "fielding": ("event_key", "sequence_id"),
    }
    grain = joins[artifact.name]
    game_filter = f"g.season BETWEEN {config.start_season} AND {config.end_season} AND g.source_type = 'PlayByPlay'"
    if config.sample_games is not None:
        game_filter += f" AND g.game_id IN (SELECT DISTINCT game_id FROM {parquet})"
    sources = {
        "context": f"SELECT g.game_id FROM main_models.game_start_info g WHERE {game_filter}",
        "officials": f"SELECT g.game_id, role FROM main_models.game_start_info g, unnest(['official_scorer','umpire_home_id','umpire_first_id','umpire_second_id','umpire_third_id','umpire_left_id','umpire_right_id']) AS roles(role) WHERE {game_filter}",
        "geometry": f"SELECT e.event_key FROM main_models.stg_events e JOIN main_models.game_start_info g USING(game_id) WHERE {game_filter} AND (e.batted_trajectory IS NOT NULL OR e.batted_to_fielder IS NOT NULL)",
        "pitches": f"SELECT e.event_key FROM main_models.stg_events e JOIN main_models.game_start_info g USING(game_id) WHERE {game_filter}",
        "runners": f"SELECT e.event_key, e.baserunner::VARCHAR AS baserunner FROM main_models.stg_event_baserunners e JOIN main_models.game_start_info g USING(game_id) WHERE {game_filter}",
        "fielding": f"SELECT e.event_key,e.sequence_id FROM main_models.stg_event_fielding_plays e JOIN main_models.game_start_info g USING(game_id) WHERE {game_filter}",
    }
    grain_sql = ", ".join(grain)
    source = sources[artifact.name]
    checks = {
        "missing_source_keys": f"SELECT count(*) FROM (({source}) EXCEPT (SELECT {grain_sql} FROM {parquet}))",
        "unexpected_keys": f"SELECT count(*) FROM ((SELECT {grain_sql} FROM {parquet}) EXCEPT ({source}))",
        "duplicate_keys": f"SELECT coalesce(sum(n-1),0)::BIGINT FROM (SELECT count(*) n FROM {parquet} GROUP BY {grain_sql} HAVING count(*) > 1)",
    }
    if artifact.name == "context":
        missing = " OR ".join(
            f"{field} IS NULL"
            + (f" OR {field} = 'Unknown'" if kind == "VARCHAR" else "")
            for field, kind in CONTEXT_FIELDS.items()
        )
        changed = " OR ".join(
            f"({field}_method = 'observed' AND {field} IS DISTINCT FROM {field}_raw)"
            for field in CONTEXT_FIELDS
        )
        checks.update(
            missing_values=f"SELECT count(*) FROM {parquet} WHERE {missing}",
            changed_recorded_values=f"SELECT count(*) FROM {parquet} WHERE {changed}",
        )
    elif artifact.name == "geometry":
        checks.update(
            missing_values=f"SELECT count(*) FROM {parquet} WHERE trajectory IS NULL OR general_location IS NULL OR handler_position IS NULL OR contact_strength IS NULL OR location_depth_modifier IS NULL OR location_angle IS NULL OR location_side IS NULL OR location_depth IS NULL OR location_edge IS NULL",
            invalid_probabilities=f"SELECT count(*) FROM {parquet} WHERE (trajectory_status='estimated' AND trajectory_distribution_probabilities IS NULL) OR (general_location_status='estimated' AND location_distribution_probabilities IS NULL) OR abs(list_sum(trajectory_distribution_probabilities)-1)>1e-8 OR abs(list_sum(location_distribution_probabilities)-1)>1e-8",
            invalid_standardized_trajectory=f"SELECT count(*) FROM {parquet} WHERE NOT is_bunt AND (standardized_trajectory IS NULL OR p_standardized_ground_ball IS NULL OR p_standardized_fly IS NULL OR p_standardized_line_drive IS NULL OR p_standardized_pop_up IS NULL OR NOT isfinite(p_standardized_ground_ball+p_standardized_fly+p_standardized_line_drive+p_standardized_pop_up) OR least(p_standardized_ground_ball,p_standardized_fly,p_standardized_line_drive,p_standardized_pop_up)<0 OR abs(p_standardized_ground_ball+p_standardized_fly+p_standardized_line_drive+p_standardized_pop_up-1)>1e-8 OR CASE standardized_trajectory WHEN 'GroundBall' THEN p_standardized_ground_ball WHEN 'Fly' THEN p_standardized_fly WHEN 'LineDrive' THEN p_standardized_line_drive WHEN 'PopUp' THEN p_standardized_pop_up ELSE 0 END <= 0)",
            changed_recorded_values=f"SELECT count(*) FROM {parquet} WHERE (trajectory_status='observed' AND trajectory IS DISTINCT FROM raw_trajectory) OR (general_location_status='observed' AND general_location IS DISTINCT FROM raw_general_location) OR (handler_position_status='observed' AND handler_position IS DISTINCT FROM raw_handler_position)",
        )
    elif artifact.name == "runners":
        checks.update(
            missing_values=f"SELECT count(*) FROM {parquet} WHERE completed_destination IS NULL OR completed_charged_pitcher_id IS NULL",
            invalid_probabilities=f"SELECT count(*) FROM {parquet} WHERE abs(p_first+p_second+p_third+p_home+p_out-1)>1e-8",
            changed_recorded_values=f"SELECT count(*) FROM {parquet} WHERE (raw_base_end IS NOT NULL AND completed_base_end IS DISTINCT FROM raw_base_end) OR (raw_explicit_charged_pitcher_id IS NOT NULL AND completed_charged_pitcher_id IS DISTINCT FROM raw_explicit_charged_pitcher_id)",
        )
    elif artifact.name == "fielding":
        checks.update(
            missing_values=f"SELECT count(*) FROM {parquet} WHERE completed_fielding_position IS NULL OR completed_fielding_position NOT BETWEEN 1 AND 9",
            invalid_probabilities=f"SELECT count(*) FROM {parquet} WHERE abs(list_sum(candidate_probabilities)-1)>1e-8",
            changed_recorded_values=f"SELECT count(*) FROM {parquet} WHERE raw_fielding_position BETWEEN 1 AND 9 AND completed_fielding_position IS DISTINCT FROM raw_fielding_position",
        )
    elif artifact.name == "officials":
        checks.update(
            missing_disposition=f"SELECT count(*) FROM {parquet} WHERE recorded_identity IS NULL AND unresolved_slot IS NULL",
            invalid_probabilities=f"SELECT count(*) FROM {parquet} WHERE len(candidate_probabilities)>0 AND abs(list_sum(candidate_probabilities)-1)>1e-8",
        )
    elif artifact.name == "pitches":
        checks.update(
            missing_values=f"SELECT count(*) FROM {parquet} WHERE completed_pitch_sequence IS NULL OR completed_pitches IS NULL",
            invalid_count_totals=f"SELECT count(*) FROM {parquet} WHERE source_resolution_status <> 'Resolved' AND completed_pitches <> completed_balls + completed_strikes + completed_unknown_pitches",
        )
    results: dict[str, int] = {}
    for name, query in checks.items():
        row = connection.execute(query).fetchone()
        if row is None:
            raise ValueError(f"Validation returned no result: {artifact.name}.{name}")
        results[name] = int(row[0])
    _log.info("Validated %s: %s", artifact.name, results)
    failures = {name: count for name, count in results.items() if count}
    if failures:
        raise ValueError(
            f"Completion validation failed for {artifact.name}: {failures}"
        )
    return results
