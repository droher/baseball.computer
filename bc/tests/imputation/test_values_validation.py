from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from python_models.imputation.artifacts import ComponentArtifact, export_component
from python_models.imputation.context import ContextCompletionConfig
from python_models.imputation.validation import validate_component
from python_models.imputation.values import (
    EVENT_VALUES_OUTPUT_SCHEMA,
    OUTPUT_SCHEMAS,
    Config,
    build_values_component_sql,
)
from python_models.imputation.values_validation import validate_values_component
from tests.imputation.test_values import _connection


def _export(
    connection: duckdb.DuckDBPyConnection,
    root: Path,
    component: str,
    config: Config,
    query: str | None = None,
) -> ComponentArtifact:
    return export_component(
        connection,
        root / component,
        name=component,
        query=query or build_values_component_sql(component, config),
        expected_columns=OUTPUT_SCHEMAS[component],
    )


def test_all_values_components_validate_independent_source_grains(
    tmp_path: Path,
) -> None:
    with _connection() as connection:
        config = ContextCompletionConfig()
        for component in OUTPUT_SCHEMAS:
            artifact = _export(connection, tmp_path, component, Config())
            result = validate_component(connection, artifact, config)
            assert result["missing_source_keys"] == 0
            assert result["unexpected_keys"] == 0
            assert result["duplicate_keys"] == 0
            assert result["missing_values"] == 0
            assert result["invalid_ranges"] == 0
            assert result["invalid_probability_normalization"] == 0
            assert result["changed_recorded_values"] == 0


def test_sampled_expected_scope_reproduces_builder_game_selection(
    tmp_path: Path,
) -> None:
    with _connection() as connection:
        build_config = Config(sample_games=1)
        artifact = _export(connection, tmp_path, "event_values", build_config)
        result = validate_values_component(
            connection,
            artifact,
            ContextCompletionConfig(sample_games=1),
        )

    assert artifact.rows == 1
    assert result["missing_source_keys"] == 0
    assert result["unexpected_keys"] == 0


def test_missing_source_key_is_detected_without_using_artifact_scope(
    tmp_path: Path,
) -> None:
    with _connection() as connection:
        base_query = build_values_component_sql("event_values", Config())
        artifact = _export(
            connection,
            tmp_path,
            "event_values",
            Config(),
            f"SELECT * FROM ({base_query}) WHERE event_key <> 1",
        )
        with pytest.raises(ValueError, match="missing_source_keys.*1"):
            validate_values_component(connection, artifact, ContextCompletionConfig())


def test_duplicate_and_changed_raw_value_are_rejected(tmp_path: Path) -> None:
    with _connection() as connection:
        base_query = build_values_component_sql("event_values", Config())
        duplicate_query = (
            f"WITH completed AS ({base_query}) SELECT * FROM completed "
            "UNION ALL SELECT * FROM completed WHERE event_key = 1"
        )
        duplicate = _export(
            connection,
            tmp_path,
            "event_values",
            Config(),
            duplicate_query,
        )
        with pytest.raises(ValueError, match="duplicate_keys.*1"):
            validate_values_component(connection, duplicate, ContextCompletionConfig())

        changed_query = f"""
        SELECT * REPLACE (
            CASE WHEN expected_runs_change_raw IS NOT NULL THEN 999.0
                ELSE expected_runs_change END AS expected_runs_change
        )
        FROM ({base_query})
        """
        changed = export_component(
            connection,
            tmp_path / "changed",
            name="event_values",
            query=changed_query,
            expected_columns=EVENT_VALUES_OUTPUT_SCHEMA,
        )
        with pytest.raises(ValueError, match="changed_recorded_values.*1"):
            validate_values_component(connection, changed, ContextCompletionConfig())
