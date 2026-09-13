from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Final, cast

import duckdb

from python_models.imputation.artifacts import (
    ComponentArtifact,
    FileIdentity,
    file_identity,
    verify_component,
)
from python_models.imputation.completion_registry import (
    CompletionRegistryPayload,
    SourcePopulation,
    completion_targets,
    query_source_population,
    validate_completion_targets,
)
from python_models.imputation.context import CONTEXT_FIELDS
from python_models.imputation.coverage import CoverageReport
from python_models.imputation.coverage_breakdown import CoverageBreakdownReport
from python_models.imputation.pitch_validation import PitchValidationReport
from python_models.imputation.pitches import OUTPUT_SCHEMA as PITCH_OUTPUT_SCHEMA

_IDENTIFIER: Final = re.compile(r"^[a-z][a-z0-9_]*$")
_COMPONENTS: Final = (
    "context",
    "officials",
    "geometry",
    "pitches",
    "runners",
    "fielding",
    "event_values",
    "park_factors",
    "run_expectancy",
    "state_transitions",
    "linear_weights",
)
_CONSUMER_TABLES: Final = (
    "pbp_imputed_event_values",
    "pbp_imputed_events",
    "pbp_imputed_fielding_plays",
    "pbp_imputed_fielding_totals",
    "pbp_imputed_game_context",
    "pbp_imputed_games",
    "pbp_imputed_geometry",
    "pbp_imputed_linear_weights",
    "pbp_imputed_officials",
    "pbp_imputed_park_factors",
    "pbp_imputed_pitch_items",
    "pbp_imputed_pitch_totals",
    "pbp_imputed_pitches",
    "pbp_imputed_run_expectancy",
    "pbp_imputed_runners",
    "pbp_imputed_state_transitions",
)
_PITCH_COUNTERS: Final = tuple(
    field.removeprefix("completed_")
    for field in PITCH_OUTPUT_SCHEMA
    if field.startswith("completed_")
    and field
    not in {
        "completed_count_balls",
        "completed_count_strikes",
        "completed_pitch_sequence",
    }
)


def _relation(schema: str, table: str) -> str:
    if not _IDENTIFIER.fullmatch(schema) or not _IDENTIFIER.fullmatch(table):
        raise ValueError("consumer schema and relation names must be identifiers")
    return f'"{schema}"."{table}"'


def _consumer_relation(relation: str, schema: str) -> tuple[str, str]:
    source_schema, table = relation.split(".", 1)
    return (schema if table in _CONSUMER_TABLES else source_schema, table)


def _scalar(connection: duckdb.DuckDBPyConnection, query: str) -> int:
    row = connection.execute(query).fetchone()
    if row is None or not isinstance(row[0], int):
        raise ValueError("validation query did not return an integer")
    return row[0]


def _check(checks: dict[str, object], name: str, action: object) -> bool:
    try:
        value = action() if callable(action) else action
    except (OSError, ValueError, duckdb.Error, json.JSONDecodeError) as error:
        checks[name] = {"passed": False, "error": str(error)}
        return False
    passed = value if isinstance(value, bool) else True
    checks[name] = {"passed": passed, "value": value}
    return passed


def _zero_mapping(value: object) -> bool:
    values = cast(Mapping[str, object], value) if isinstance(value, Mapping) else None
    return values is not None and all(
        isinstance(item, int) and not isinstance(item, bool) and item == 0
        for item in values.values()
    )


def _load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return cast(dict[str, object], payload)


def _artifact_checks(root: Path, manifest: Mapping[str, object]) -> dict[str, object]:
    artifacts_payload = manifest.get("artifacts")
    validation_payload = manifest.get("validation")
    if not isinstance(artifacts_payload, Mapping) or not isinstance(
        validation_payload, Mapping
    ):
        raise ValueError("manifest must include artifacts and validation")
    artifacts = cast(Mapping[str, object], artifacts_payload)
    validation = cast(Mapping[str, object], validation_payload)
    if not set(_COMPONENTS).issubset(artifacts):
        raise ValueError("manifest must contain the eleven release components")
    identities: dict[str, object] = {}
    for component in _COMPONENTS:
        artifact = ComponentArtifact.model_validate(artifacts[component])
        if artifact.name != component:
            raise ValueError(f"artifact name mismatch for {component}")
        for identity in (artifact.data, artifact.query, *artifact.dependencies):
            if not Path(identity.path).resolve().is_relative_to(root.resolve()):
                raise ValueError(f"artifact identity escapes root for {component}")
        verify_component(artifact)
        component_validation: object = validation.get(component)
        component_checks = (
            cast(Mapping[str, object], component_validation)
            if isinstance(component_validation, Mapping)
            else None
        )
        required_checks = {"missing_source_keys", "unexpected_keys", "duplicate_keys"}
        if (
            component_checks is None
            or not _zero_mapping(component_checks)
            or not required_checks.issubset(component_checks)
        ):
            raise ValueError(
                f"component validation is missing or nonzero for {component}"
            )
        identities[component] = artifact.data.model_dump()
    coverage = FileIdentity.model_validate(artifacts.get("coverage"))
    if not Path(coverage.path).resolve().is_relative_to(root.resolve()):
        raise ValueError("coverage identity escapes root")
    if file_identity(Path(coverage.path)) != coverage:
        raise ValueError("coverage artifact content changed")
    identities["coverage"] = coverage.model_dump()
    return identities


def _full_scope(
    connection: duckdb.DuckDBPyConnection, manifest: Mapping[str, object]
) -> bool:
    population = query_source_population(connection)
    claimed = SourcePopulation.model_validate(manifest.get("source_population"))
    start_season = manifest.get("start_season")
    end_season = manifest.get("end_season")
    if not isinstance(start_season, int) or not isinstance(end_season, int):
        return False
    return (
        manifest.get("status") == "components_complete"
        and manifest.get("scope") == "existing_pbp_games_only"
        and manifest.get("sample_games") is None
        and start_season <= population.min_season
        and end_season >= population.max_season
        and claimed == population
    )


def _completion_registry_check(
    root: Path, manifest: Mapping[str, object], source: FileIdentity
) -> bool:
    identity = FileIdentity.model_validate(manifest.get("completion_registry"))
    path = Path(identity.path)
    if (
        not path.resolve().is_relative_to(root.resolve())
        or file_identity(path) != identity
    ):
        return False
    registry = CompletionRegistryPayload.model_validate_json(
        path.read_text(encoding="utf-8")
    )
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, Mapping):
        return False
    coverage = FileIdentity.model_validate(
        cast(Mapping[str, object], artifacts).get("coverage")
    )
    return (
        registry.source_snapshot_id == source.sha256
        and registry.coverage_snapshot_id == coverage.sha256
    )


def _columns_by_relation(
    connection: duckdb.DuckDBPyConnection, schema: str, relations: set[str]
) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for relation in relations:
        target_schema, table = _consumer_relation(relation, schema)
        rows = connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            """,
            [target_schema, table],
        ).fetchall()
        result[relation] = {str(row[0]) for row in rows}
    return result


def _consumer_schema_counts(
    connection: duckdb.DuckDBPyConnection, schema: str
) -> dict[str, int]:
    return {
        table: _scalar(
            connection,
            "SELECT count(*) FROM information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'",
        )
        for table in _CONSUMER_TABLES
    }


def _population_checks(
    connection: duckdb.DuckDBPyConnection, schema: str
) -> dict[str, int]:
    source_games = _relation("main_models", "game_start_info")
    source_events = _relation("main_models", "stg_events")
    games = _relation(schema, "pbp_imputed_games")
    events = _relation(schema, "pbp_imputed_events")
    values = _relation(schema, "pbp_imputed_event_values")
    pbp_games = f"SELECT game_id FROM {source_games} WHERE source_type = 'PlayByPlay'"
    pbp_events = f"SELECT event_key FROM {source_events} WHERE game_id IN ({pbp_games})"
    return {
        "games_count_difference": _scalar(
            connection,
            f"SELECT abs(({_scalar(connection, f'SELECT count(*) FROM ({pbp_games})')}) - (SELECT count(*) FROM {games}))",
        ),
        "events_count_difference": _scalar(
            connection,
            f"SELECT abs(({_scalar(connection, f'SELECT count(*) FROM ({pbp_events})')}) - (SELECT count(*) FROM {events}))",
        ),
        "event_values_count_difference": _scalar(
            connection,
            f"SELECT abs(({_scalar(connection, f'SELECT count(*) FROM ({pbp_events})')}) - (SELECT count(*) FROM {values}))",
        ),
        "duplicate_completed_games": _scalar(
            connection, f"SELECT count(*) - count(DISTINCT game_id) FROM {games}"
        ),
        "duplicate_completed_events": _scalar(
            connection, f"SELECT count(*) - count(DISTINCT event_key) FROM {events}"
        ),
        "duplicate_completed_event_values": _scalar(
            connection, f"SELECT count(*) - count(DISTINCT event_key) FROM {values}"
        ),
        "missing_completed_games": _scalar(
            connection,
            f"SELECT count(*) FROM (({pbp_games}) EXCEPT (SELECT game_id FROM {games}))",
        ),
        "missing_completed_events": _scalar(
            connection,
            f"SELECT count(*) FROM (({pbp_events}) EXCEPT (SELECT event_key FROM {events}))",
        ),
        "missing_completed_event_value_keys": _scalar(
            connection,
            f"SELECT count(*) FROM (({pbp_events}) EXCEPT (SELECT event_key FROM {values}))",
        ),
        "non_pbp_completed_games": _scalar(
            connection,
            f"SELECT count(*) FROM ((SELECT game_id FROM {games}) EXCEPT ({pbp_games}))",
        ),
        "non_pbp_completed_events": _scalar(
            connection,
            f"SELECT count(*) FROM ((SELECT event_key FROM {events}) EXCEPT ({pbp_events}))",
        ),
        "missing_completed_game_context": _scalar(
            connection,
            f"SELECT count(*) FROM {games} WHERE "
            + " OR ".join(f'"{field}" IS NULL' for field in CONTEXT_FIELDS),
        ),
        "missing_completed_event_counts": _scalar(
            connection,
            f"SELECT count(*) FROM {events} WHERE completed_count_balls IS NULL OR completed_count_strikes IS NULL",
        ),
        "missing_completed_event_values": _scalar(
            connection,
            f"SELECT count(*) FROM {values} WHERE expected_runs_change IS NULL OR expected_batting_win_change IS NULL",
        ),
    }


def _reconciliation_checks(
    connection: duckdb.DuckDBPyConnection, schema: str
) -> dict[str, object]:
    pitches = _relation(schema, "pbp_imputed_pitches")
    totals = _relation(schema, "pbp_imputed_pitch_totals")
    items = _relation(schema, "pbp_imputed_pitch_items")
    fielding = _relation(schema, "pbp_imputed_fielding_plays")
    fielding_totals = _relation(schema, "pbp_imputed_fielding_totals")
    counters = {
        counter: _scalar(
            connection,
            f"SELECT abs((SELECT coalesce(sum(completed_{counter}), 0) FROM {pitches}) - (SELECT coalesce(sum(completed_{counter}), 0) FROM {totals}))",
        )
        for counter in _PITCH_COUNTERS
    }
    rollups = {
        counter: _scalar(
            connection,
            f"SELECT count(*) FROM {totals} WHERE observed_{counter} + estimated_{counter} IS DISTINCT FROM completed_{counter}",
        )
        for counter in _PITCH_COUNTERS
    }
    return {
        "pitch_counter_differences": counters,
        "observed_estimated_rollup_mismatches": rollups,
        "normalized_pitch_item_difference": _scalar(
            connection,
            f"SELECT abs((SELECT count(*) FROM {items}) - (SELECT coalesce(sum(len(string_split(completed_pitch_sequence, '|'))), 0) FROM {pitches} WHERE completed_pitch_sequence <> ''))",
        ),
        "duplicate_completed_pitch_item_keys": _scalar(
            connection,
            f"SELECT coalesce(sum(n - 1), 0)::BIGINT FROM (SELECT count(*) AS n FROM {items} GROUP BY event_key, sequence_index HAVING count(*) > 1)",
        ),
        "pitch_appearance_partition_mismatches": _scalar(
            connection,
            f"SELECT count(*) FROM {totals} WHERE pitching_appearances IS DISTINCT FROM completed_plate_appearances + interrupted_appearances",
        ),
        "fielding_total_play_credit_difference": _scalar(
            connection,
            f"SELECT abs((SELECT count(*) FROM {fielding}) - (SELECT coalesce(sum(play_credits), 0) FROM {fielding_totals}))",
        ),
        "fielding_allocation_delta_nonzero": _scalar(
            connection,
            f"SELECT count(*) FROM {fielding} WHERE constraint_disposition = 'aggregate_capacity_assignment_satisfied' AND aggregate_constraint_delta IS DISTINCT FROM 0",
        ),
        "pitch_conflicts": _scalar(
            connection,
            f"SELECT count(*) FROM {pitches} WHERE constraint_disposition LIKE '%conflict%' OR constraint_status LIKE '%conflict%'",
        ),
        "fielding_conflicts": _scalar(
            connection,
            f"SELECT count(*) FROM {fielding} WHERE constraint_disposition LIKE '%incompatible%' OR constraint_disposition LIKE '%conflict%'",
        ),
        "runner_conflicts": _scalar(
            connection,
            f"SELECT count(*) FROM {_relation(schema, 'pbp_imputed_runners')} WHERE constraint_disposition LIKE '%conflict%'",
        ),
    }


def _materialized_identity_checks(
    connection: duckdb.DuckDBPyConnection,
    schema: str,
    manifest: Mapping[str, object],
    source: FileIdentity,
) -> dict[str, int]:
    artifacts = cast(Mapping[str, object], manifest["artifacts"])

    def artifact_id(component: str) -> str:
        return ComponentArtifact.model_validate(artifacts[component]).data.sha256

    checks: dict[str, int] = {}
    direct = {
        "context": "pbp_imputed_game_context",
        "officials": "pbp_imputed_officials",
        "geometry": "pbp_imputed_geometry",
        "pitches": "pbp_imputed_pitches",
        "runners": "pbp_imputed_runners",
        "fielding": "pbp_imputed_fielding_plays",
        "event_values": "pbp_imputed_event_values",
        "park_factors": "pbp_imputed_park_factors",
        "run_expectancy": "pbp_imputed_run_expectancy",
        "state_transitions": "pbp_imputed_state_transitions",
        "linear_weights": "pbp_imputed_linear_weights",
    }
    inherited = {
        "pbp_imputed_games": "context",
        "pbp_imputed_events": "pitches",
        "pbp_imputed_pitch_items": "pitches",
        "pbp_imputed_pitch_totals": "pitches",
        "pbp_imputed_fielding_totals": "fielding",
    }
    for component, table in direct.items():
        relation = _relation(schema, table)
        checks[table] = _scalar(
            connection,
            f"SELECT count(*) FROM {relation} WHERE artifact_id IS DISTINCT FROM '{artifact_id(component)}' OR source_snapshot_id IS DISTINCT FROM '{source.sha256}'",
        )
        expected_rows = ComponentArtifact.model_validate(artifacts[component]).rows
        checks[f"{table}_row_count"] = _scalar(
            connection,
            f"SELECT abs(count(*) - {expected_rows}) FROM {relation}",
        )
    for table, component in inherited.items():
        relation = _relation(schema, table)
        checks[table] = _scalar(
            connection,
            f"SELECT count(*) FROM {relation} WHERE artifact_id IS DISTINCT FROM '{artifact_id(component)}' OR source_snapshot_id IS DISTINCT FROM '{source.sha256}'",
        )
    events = _relation(schema, "pbp_imputed_events")
    checks["pbp_imputed_events_geometry"] = _scalar(
        connection,
        f"SELECT count(*) FROM {events} WHERE (raw_batted_trajectory IS NOT NULL OR raw_batted_to_fielder IS NOT NULL) AND geometry_artifact_id IS DISTINCT FROM '{artifact_id('geometry')}'",
    )
    checks["pbp_imputed_events_values"] = _scalar(
        connection,
        f"SELECT count(*) FROM {events} WHERE value_artifact_id IS DISTINCT FROM '{artifact_id('event_values')}'",
    )
    return checks


def validate_candidate(
    connection: duckdb.DuckDBPyConnection,
    root: Path,
    consumer_schema: str,
    pitch_report_path: Path,
    grouped_report_path: Path,
) -> dict[str, object]:
    if not _IDENTIFIER.fullmatch(consumer_schema):
        raise ValueError("consumer_schema must be an identifier")
    checks: dict[str, object] = {}
    manifest = _load_json(root / "manifest.json")
    source = FileIdentity.model_validate(manifest.get("source_database"))
    checks["source_identity"] = source.model_dump()
    ready = _check(
        checks,
        "full_scope",
        lambda: _full_scope(connection, manifest),
    )
    ready = (
        _check(checks, "artifact_integrity", lambda: _artifact_checks(root, manifest))
        and ready
    )
    ready = (
        _check(
            checks,
            "completion_registry",
            lambda: _completion_registry_check(root, manifest, source),
        )
        and ready
    )
    pitch_report = PitchValidationReport.model_validate_json(
        pitch_report_path.read_text(encoding="utf-8")
    )
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise ValueError("manifest must include artifacts")
    pitch_artifact = ComponentArtifact.model_validate(
        cast(Mapping[str, object], artifacts)["pitches"]
    )
    ready = (
        _check(
            checks,
            "pitch_validation",
            lambda: pitch_report.passed
            and pitch_report.input_sha256 == pitch_artifact.data.sha256
            and all(
                counter.unflagged_violations == 0
                and counter.explicit_conflict_violations == 0
                for counter in pitch_report.counters
            ),
        )
        and ready
    )
    grouped_payload = _load_json(grouped_report_path)
    grouped = CoverageBreakdownReport.model_validate(grouped_payload)
    grouped_manifest = _load_json(grouped_report_path.parent / "manifest.json")
    ready = (
        _check(
            checks,
            "grouped_coverage",
            lambda: grouped.sample_game_limit is None
            and grouped.full_population_match is True
            and grouped_manifest.get("status") == "complete"
            and grouped_manifest.get("database_sha256") == source.sha256
            and grouped_manifest.get("report_sha256")
            == file_identity(grouped_report_path).sha256,
        )
        and ready
    )
    coverage = CoverageReport.model_validate_json(
        (root / "coverage.json").read_text(encoding="utf-8")
    )
    targets = completion_targets(coverage)
    relations = {target.completed_field.rsplit(".", 1)[0] for target in targets}
    consumer_schema_counts: dict[str, int] = {}
    ready = (
        _check(
            checks,
            "consumer_schemas",
            lambda: consumer_schema_counts.update(
                _consumer_schema_counts(connection, consumer_schema)
            )
            or consumer_schema_counts,
        )
        and ready
    )
    if consumer_schema_counts and any(
        count == 0 for count in consumer_schema_counts.values()
    ):
        ready = False
    materialized: dict[str, int] = {}
    ready = (
        _check(
            checks,
            "materialized_artifact_identities",
            lambda: materialized.update(
                _materialized_identity_checks(
                    connection, consumer_schema, manifest, source
                )
            )
            or materialized,
        )
        and ready
    )
    if materialized and any(count != 0 for count in materialized.values()):
        ready = False
    ready = (
        _check(
            checks,
            "completion_targets",
            lambda: validate_completion_targets(
                targets, _columns_by_relation(connection, consumer_schema, relations)
            ),
        )
        and ready
    )
    population: dict[str, int] = {}
    ready = (
        _check(
            checks,
            "population",
            lambda: population.update(_population_checks(connection, consumer_schema))
            or population,
        )
        and ready
    )
    if population and any(value != 0 for value in population.values()):
        ready = False
    reconciliation: dict[str, object] = {}
    ready = (
        _check(
            checks,
            "reconciliation",
            lambda: reconciliation.update(
                _reconciliation_checks(connection, consumer_schema)
            )
            or reconciliation,
        )
        and ready
    )
    for key, value in reconciliation.items():
        if key.endswith("conflicts"):
            continue
        if isinstance(value, Mapping) and any(
            item != 0 for item in cast(Mapping[str, object], value).values()
        ):
            ready = False
        elif isinstance(value, int) and value != 0:
            ready = False
    return {
        "candidate_ready": ready,
        "source_identity": source.model_dump(),
        "artifact_root": str(root.resolve()),
        "consumer_schema": consumer_schema,
        "checks": checks,
    }
