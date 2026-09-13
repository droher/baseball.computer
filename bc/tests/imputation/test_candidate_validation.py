from __future__ import annotations

import json
from pathlib import Path

import duckdb

from python_models.imputation.artifacts import file_identity
from python_models.imputation.candidate_validation import validate_candidate
from python_models.imputation.coverage import CoverageReport
from python_models.imputation.coverage_breakdown import CoverageBreakdownReport
from python_models.imputation.completion_registry import (
    CompletionRegistryPayload,
    CompletionTarget,
)
from python_models.imputation.pitch_validation import (
    CounterValidation,
    PitchValidationReport,
    TOKEN_COUNTERS,
)


def test_candidate_validator_reports_schema_failure_without_claiming_ready(
    tmp_path: Path,
) -> None:
    root = tmp_path / "candidate"
    root.mkdir()
    source = root / "source.db"
    source.touch()
    coverage_path = root / "coverage.json"
    coverage_path.write_text(
        CoverageReport(
            season_start=1903,
            season_end=1903,
            sample_game_limit=None,
            sampled_games=1,
            fields=(),
            excluded_fields=(),
        ).model_dump_json(),
        encoding="utf-8",
    )
    artifacts: dict[str, object] = {}
    validation: dict[str, object] = {}
    for component in (
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
    ):
        data = root / f"{component}.parquet"
        query = root / f"{component}.sql"
        data.write_bytes(b"data")
        query.write_text("select 1", encoding="utf-8")
        artifacts[component] = {
            "name": component,
            "rows": 1,
            "data": file_identity(data).model_dump(),
            "query": file_identity(query).model_dump(),
            "columns": {},
        }
        validation[component] = {
            "missing_source_keys": 0,
            "unexpected_keys": 0,
            "duplicate_keys": 0,
        }
    artifacts["coverage"] = file_identity(coverage_path).model_dump()
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "source_database": file_identity(source).model_dump(),
                "scope": "existing_pbp_games_only",
                "sample_games": None,
                "status": "components_complete",
                "artifacts": artifacts,
                "validation": validation,
            }
        )
    )
    pitch_report = tmp_path / "pitch.json"
    pitch_payload = PitchValidationReport(
        input_path="pitches.parquet",
        input_bytes=4,
        input_sha256=artifacts["pitches"]["data"]["sha256"],
        seed_path="seed.csv",
        seed_sha256="0" * 64,
        rows=1,
        games=1,
        appearances=1,
        season_start=1903,
        season_end=1903,
        counters=tuple(
            CounterValidation(
                counter=name,
                checked_rows=1,
                unflagged_violations=0,
                explicit_conflict_violations=0,
            )
            for name in TOKEN_COUNTERS
        ),
        unflagged_appearance_violations={},
        explicit_conflict_appearance_violations={},
        constraint_statuses={},
        unclassified_constraint_statuses={},
        unknown_tokens={},
        examples=(),
        passed=True,
    ).model_dump_json()
    pitch_report.write_text(pitch_payload, encoding="utf-8")
    grouped_root = tmp_path / "grouped"
    grouped_root.mkdir()
    grouped = grouped_root / "coverage_breakdown.json"
    grouped.write_text(
        CoverageBreakdownReport(
            season_start=1903,
            season_end=1903,
            sample_game_limit=None,
            sampled_games=1,
            sampled_events=1,
            league_definition="test",
            source_relation_queries=1,
            fields=(),
            reconciliation=(),
            full_population_match=True,
        ).model_dump_json()
    )
    (grouped_root / "manifest.json").write_text(
        json.dumps(
            {
                "database_sha256": file_identity(source).sha256,
                "report_sha256": file_identity(grouped).sha256,
                "status": "complete",
            }
        )
    )
    connection = duckdb.connect(":memory:")
    report = validate_candidate(
        connection,
        root,
        "main_models__pbp_completion_verification",
        pitch_report,
        grouped,
    )
    assert report["candidate_ready"] is False
    checks = report["checks"]
    assert checks["artifact_integrity"]["passed"] is True
    assert checks["population"]["passed"] is False


def test_candidate_validator_accepts_a_complete_small_fixture(tmp_path: Path) -> None:
    root = tmp_path / "candidate"
    root.mkdir()
    source = root / "source.db"
    source.touch()
    coverage_path = root / "coverage.json"
    coverage_path.write_text(
        CoverageReport(
            season_start=1903,
            season_end=1903,
            sample_game_limit=None,
            sampled_games=1,
            fields=(),
            excluded_fields=(),
        ).model_dump_json(),
        encoding="utf-8",
    )
    artifacts: dict[str, object] = {
        "coverage": file_identity(coverage_path).model_dump()
    }
    validation: dict[str, object] = {}
    for component in (
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
    ):
        data = root / f"{component}.parquet"
        query = root / f"{component}.sql"
        data.write_bytes(b"data")
        query.write_text("select 1", encoding="utf-8")
        artifacts[component] = {
            "name": component,
            "rows": 1,
            "data": file_identity(data).model_dump(),
            "query": file_identity(query).model_dump(),
            "columns": {},
        }
        validation[component] = {
            "missing_source_keys": 0,
            "unexpected_keys": 0,
            "duplicate_keys": 0,
        }
    registry_path = root / "completion_registry.json"
    registry_path.write_text(
        CompletionRegistryPayload(
            source_snapshot_id=file_identity(source).sha256,
            coverage_snapshot_id=file_identity(coverage_path).sha256,
            targets=(
                CompletionTarget(
                    source_field="main_models.stg_events.event_key",
                    completed_field="main_models.pbp_imputed_events.event_key",
                    disposition="source_complete_on_applicable_rows",
                    source_missing_or_unspecified=0,
                    source_missing_blocks=0,
                ),
            ),
        ).model_dump_json()
    )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "source_database": file_identity(source).model_dump(),
                "source_population": {
                    "games": 1,
                    "min_season": 1903,
                    "max_season": 1903,
                },
                "start_season": 1903,
                "end_season": 1903,
                "completion_registry": file_identity(registry_path).model_dump(),
                "scope": "existing_pbp_games_only",
                "sample_games": None,
                "status": "components_complete",
                "artifacts": artifacts,
                "validation": validation,
            }
        )
    )
    pitch = tmp_path / "pitch.json"
    PitchValidationReport(
        input_path="pitches.parquet",
        input_bytes=4,
        input_sha256=artifacts["pitches"]["data"]["sha256"],
        seed_path="seed.csv",
        seed_sha256="0" * 64,
        rows=1,
        games=1,
        appearances=1,
        season_start=1903,
        season_end=1903,
        counters=tuple(
            CounterValidation(
                counter=name,
                checked_rows=1,
                unflagged_violations=0,
                explicit_conflict_violations=0,
            )
            for name in TOKEN_COUNTERS
        ),
        unflagged_appearance_violations={},
        explicit_conflict_appearance_violations={},
        constraint_statuses={},
        unclassified_constraint_statuses={},
        unknown_tokens={},
        examples=(),
        passed=True,
    ).model_dump_json()
    pitch.write_text(
        PitchValidationReport(
            input_path="pitches.parquet",
            input_bytes=4,
            input_sha256=artifacts["pitches"]["data"]["sha256"],
            seed_path="seed.csv",
            seed_sha256="0" * 64,
            rows=1,
            games=1,
            appearances=1,
            season_start=1903,
            season_end=1903,
            counters=tuple(
                CounterValidation(
                    counter=name,
                    checked_rows=1,
                    unflagged_violations=0,
                    explicit_conflict_violations=0,
                )
                for name in TOKEN_COUNTERS
            ),
            unflagged_appearance_violations={},
            explicit_conflict_appearance_violations={},
            constraint_statuses={},
            unclassified_constraint_statuses={},
            unknown_tokens={},
            examples=(),
            passed=True,
        ).model_dump_json()
    )
    grouped_root = tmp_path / "grouped"
    grouped_root.mkdir()
    grouped = grouped_root / "coverage_breakdown.json"
    grouped.write_text(
        CoverageBreakdownReport(
            season_start=1903,
            season_end=1903,
            sample_game_limit=None,
            sampled_games=1,
            sampled_events=1,
            league_definition="test",
            source_relation_queries=1,
            fields=(),
            reconciliation=(),
            full_population_match=True,
        ).model_dump_json()
    )
    (grouped_root / "manifest.json").write_text(
        json.dumps(
            {
                "database_sha256": file_identity(source).sha256,
                "report_sha256": file_identity(grouped).sha256,
                "status": "complete",
            }
        )
    )
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE SCHEMA main_models")
    connection.execute("CREATE SCHEMA main_models__pbp_completion_verification")
    connection.execute(
        "CREATE TABLE main_models.game_start_info (game_id VARCHAR, source_type VARCHAR, season INTEGER)"
    )
    connection.execute(
        "INSERT INTO main_models.game_start_info VALUES ('g', 'PlayByPlay', 1903)"
    )
    connection.execute(
        "CREATE TABLE main_models.stg_events (event_key BIGINT, game_id VARCHAR)"
    )
    connection.execute("INSERT INTO main_models.stg_events VALUES (1, 'g')")
    schema = "main_models__pbp_completion_verification"
    source_id = file_identity(source).sha256
    component_ids = {
        name: artifacts[name]["data"]["sha256"]
        for name in (
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
    }
    simple_tables = (
        "pbp_imputed_geometry",
        "pbp_imputed_officials",
        "pbp_imputed_game_context",
        "pbp_imputed_park_factors",
        "pbp_imputed_run_expectancy",
        "pbp_imputed_state_transitions",
        "pbp_imputed_linear_weights",
    )
    for table in simple_tables:
        connection.execute(
            f"CREATE TABLE {schema}.{table} (id BIGINT, artifact_id VARCHAR, source_snapshot_id VARCHAR)"
        )
    simple_table_components = {
        "pbp_imputed_geometry": "geometry",
        "pbp_imputed_officials": "officials",
        "pbp_imputed_game_context": "context",
        "pbp_imputed_park_factors": "park_factors",
        "pbp_imputed_run_expectancy": "run_expectancy",
        "pbp_imputed_state_transitions": "state_transitions",
        "pbp_imputed_linear_weights": "linear_weights",
    }
    for table, component in simple_table_components.items():
        connection.execute(
            f"INSERT INTO {schema}.{table} VALUES (1, '{component_ids[component]}', '{source_id}')"
        )
    context_columns = ", ".join(
        f"{name} VARCHAR"
        for name in (
            "game_id",
            "time_of_day",
            "sky",
            "field_condition",
            "precipitation",
            "wind_direction",
            "temperature_fahrenheit",
            "attendance",
            "wind_speed_mph",
            "start_time",
            "duration_minutes",
        )
    )
    connection.execute(
        f"CREATE TABLE {schema}.pbp_imputed_games ({context_columns}, artifact_id VARCHAR, source_snapshot_id VARCHAR)"
    )
    connection.execute(
        f"INSERT INTO {schema}.pbp_imputed_games VALUES ('g', 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x', '{component_ids['context']}', '{source_id}')"
    )
    connection.execute(
        f"CREATE TABLE {schema}.pbp_imputed_events (event_key BIGINT, completed_count_balls BIGINT, completed_count_strikes BIGINT, artifact_id VARCHAR, source_snapshot_id VARCHAR, raw_batted_trajectory VARCHAR, raw_batted_to_fielder BIGINT, geometry_artifact_id VARCHAR, value_artifact_id VARCHAR)"
    )
    connection.execute(
        f"INSERT INTO {schema}.pbp_imputed_events VALUES (1, 0, 0, '{component_ids['pitches']}', '{source_id}', NULL, NULL, '{component_ids['geometry']}', '{component_ids['event_values']}')"
    )
    connection.execute(
        f"CREATE TABLE {schema}.pbp_imputed_event_values (event_key BIGINT, expected_runs_change DOUBLE, expected_batting_win_change DOUBLE, artifact_id VARCHAR, source_snapshot_id VARCHAR)"
    )
    connection.execute(
        f"INSERT INTO {schema}.pbp_imputed_event_values VALUES (1, 0, 0, '{component_ids['event_values']}', '{source_id}')"
    )
    counters = tuple(
        name.removeprefix("completed_")
        for name in (
            "completed_pitches",
            "completed_swings",
            "completed_swings_with_contact",
            "completed_strikes",
            "completed_strikes_called",
            "completed_strikes_swinging",
            "completed_strikes_foul",
            "completed_strikes_foul_tip",
            "completed_strikes_in_play",
            "completed_strikes_unknown",
            "completed_balls",
            "completed_balls_called",
            "completed_balls_intentional",
            "completed_balls_automatic",
            "completed_unknown_pitches",
            "completed_pitchouts",
            "completed_pitcher_pickoff_attempts",
            "completed_catcher_pickoff_attempts",
            "completed_pitches_blocked_by_catcher",
            "completed_pitches_with_runners_going",
            "completed_passed_balls",
            "completed_wild_pitches",
            "completed_balks",
        )
    )
    pitch_columns = ", ".join(
        [
            "completed_pitch_sequence VARCHAR",
            "constraint_disposition VARCHAR",
            "constraint_status VARCHAR",
            "artifact_id VARCHAR",
            "source_snapshot_id VARCHAR",
            *(f"completed_{name} BIGINT" for name in counters),
        ]
    )
    total_columns = ", ".join(
        [
            "artifact_id VARCHAR",
            "source_snapshot_id VARCHAR",
            "pitching_appearances BIGINT",
            "completed_plate_appearances BIGINT",
            "interrupted_appearances BIGINT",
            *(f"completed_{name} BIGINT" for name in counters),
            *(f"observed_{name} BIGINT" for name in counters),
            *(f"estimated_{name} BIGINT" for name in counters),
        ]
    )
    connection.execute(f"CREATE TABLE {schema}.pbp_imputed_pitches ({pitch_columns})")
    connection.execute(
        f"INSERT INTO {schema}.pbp_imputed_pitches VALUES ({', '.join(["''", "''", "''", f"'{component_ids['pitches']}'", f"'{source_id}'", *('0' for _ in counters)])})"
    )
    connection.execute(
        f"CREATE TABLE {schema}.pbp_imputed_pitch_totals ({total_columns})"
    )
    connection.execute(
        f"INSERT INTO {schema}.pbp_imputed_pitch_totals VALUES ('{component_ids['pitches']}', '{source_id}', 1, 1, 0, {', '.join('0' for _ in range(len(counters) * 3))})"
    )
    connection.execute(
        f"CREATE TABLE {schema}.pbp_imputed_pitch_items (event_key BIGINT, sequence_index BIGINT, artifact_id VARCHAR, source_snapshot_id VARCHAR)"
    )
    connection.execute(
        f"CREATE TABLE {schema}.pbp_imputed_fielding_plays (constraint_disposition VARCHAR, aggregate_constraint_delta DOUBLE, artifact_id VARCHAR, source_snapshot_id VARCHAR)"
    )
    connection.execute(
        f"INSERT INTO {schema}.pbp_imputed_fielding_plays VALUES ('aggregate_capacity_assignment_satisfied', 0, '{component_ids['fielding']}', '{source_id}')"
    )
    connection.execute(
        f"CREATE TABLE {schema}.pbp_imputed_fielding_totals (play_credits BIGINT, artifact_id VARCHAR, source_snapshot_id VARCHAR)"
    )
    connection.execute(
        f"INSERT INTO {schema}.pbp_imputed_fielding_totals VALUES (1, '{component_ids['fielding']}', '{source_id}')"
    )
    connection.execute(
        f"CREATE TABLE {schema}.pbp_imputed_runners (constraint_disposition VARCHAR, artifact_id VARCHAR, source_snapshot_id VARCHAR)"
    )
    connection.execute(
        f"INSERT INTO {schema}.pbp_imputed_runners VALUES ('satisfied', '{component_ids['runners']}', '{source_id}')"
    )
    valid_report = validate_candidate(connection, root, schema, pitch, grouped)
    assert valid_report["candidate_ready"] is True
    connection.execute(f"DELETE FROM {schema}.pbp_imputed_runners")
    truncated_runners = validate_candidate(connection, root, schema, pitch, grouped)
    assert truncated_runners["candidate_ready"] is False
    assert (
        truncated_runners["checks"]["materialized_artifact_identities"]["value"][
            "pbp_imputed_runners_row_count"
        ]
        == 1
    )
    connection.execute(
        f"INSERT INTO {schema}.pbp_imputed_runners VALUES ('satisfied', '{component_ids['runners']}', '{source_id}')"
    )
    manifest = json.loads((root / "manifest.json").read_text())
    manifest["status"] = "failed"
    (root / "manifest.json").write_text(json.dumps(manifest))
    assert (
        validate_candidate(connection, root, schema, pitch, grouped)["checks"][
            "full_scope"
        ]["passed"]
        is False
    )
    manifest["status"] = "components_complete"
    (root / "manifest.json").write_text(json.dumps(manifest))
    pitch_payload = json.loads(pitch.read_text())
    pitch_payload["input_sha256"] = "f" * 64
    pitch.write_text(json.dumps(pitch_payload))
    assert (
        validate_candidate(connection, root, schema, pitch, grouped)["checks"][
            "pitch_validation"
        ]["passed"]
        is False
    )
    pitch_payload["input_sha256"] = artifacts["pitches"]["data"]["sha256"]
    pitch.write_text(json.dumps(pitch_payload))
    (grouped_root / "manifest.json").write_text(
        json.dumps(
            {
                "database_sha256": "f" * 64,
                "report_sha256": file_identity(grouped).sha256,
                "status": "complete",
            }
        )
    )
    assert (
        validate_candidate(connection, root, schema, pitch, grouped)["checks"][
            "grouped_coverage"
        ]["passed"]
        is False
    )
    (grouped_root / "manifest.json").write_text(
        json.dumps(
            {
                "database_sha256": source_id,
                "report_sha256": file_identity(grouped).sha256,
                "status": "complete",
            }
        )
    )
    connection.execute(
        f"UPDATE {schema}.pbp_imputed_pitch_totals SET estimated_pitches = NULL"
    )
    null_rollup = validate_candidate(connection, root, schema, pitch, grouped)
    assert null_rollup["candidate_ready"] is False
    assert (
        null_rollup["checks"]["reconciliation"]["value"][
            "observed_estimated_rollup_mismatches"
        ]["pitches"]
        == 1
    )
    connection.execute(
        f"UPDATE {schema}.pbp_imputed_pitch_totals SET estimated_pitches = 0"
    )
    connection.execute(
        f"UPDATE {schema}.pbp_imputed_pitch_totals SET interrupted_appearances = 1"
    )
    invalid_appearances = validate_candidate(connection, root, schema, pitch, grouped)
    assert invalid_appearances["candidate_ready"] is False
    assert (
        invalid_appearances["checks"]["reconciliation"]["value"][
            "pitch_appearance_partition_mismatches"
        ]
        == 1
    )
    connection.execute(
        f"UPDATE {schema}.pbp_imputed_pitch_totals SET interrupted_appearances = 0"
    )
    connection.execute(
        f"INSERT INTO {schema}.pbp_imputed_pitch_items VALUES (1, 0, '{component_ids['pitches']}', '{source_id}'), (1, 0, '{component_ids['pitches']}', '{source_id}')"
    )
    duplicate_items = validate_candidate(connection, root, schema, pitch, grouped)
    assert duplicate_items["candidate_ready"] is False
    assert (
        duplicate_items["checks"]["reconciliation"]["value"][
            "duplicate_completed_pitch_item_keys"
        ]
        == 1
    )
    connection.execute(
        f"UPDATE {schema}.pbp_imputed_events SET artifact_id = 'stale'"
    )
    stale = validate_candidate(connection, root, schema, pitch, grouped)
    assert stale["candidate_ready"] is False
    assert (
        stale["checks"]["materialized_artifact_identities"]["value"][
            "pbp_imputed_events"
        ]
        == 1
    )
