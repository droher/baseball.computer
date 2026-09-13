from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from python_models.imputation.artifacts import (
    export_component,
    file_identity,
    verify_component,
)


def test_export_is_read_only_content_bound_and_not_overwritten(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            "CREATE TABLE source_data AS SELECT 1 AS event_key, 'observed' AS status"
        )
    before = file_identity(database)
    with duckdb.connect(str(database), read_only=True) as connection:
        artifact = export_component(
            connection,
            tmp_path / "outputs",
            name="example",
            query="SELECT * FROM source_data",
            expected_columns={"event_key": "INTEGER", "status": "VARCHAR"},
        )
        verify_component(artifact)
        with pytest.raises(FileExistsError):
            export_component(
                connection,
                tmp_path / "outputs",
                name="example",
                query="SELECT * FROM source_data",
                expected_columns={"event_key": "INTEGER", "status": "VARCHAR"},
            )
    assert artifact.rows == 1
    assert file_identity(database) == before
    Path(artifact.query.path).write_text("SELECT 2")
    with pytest.raises(ValueError, match="changed"):
        verify_component(artifact)


def test_schema_error_is_not_published(tmp_path: Path) -> None:
    with duckdb.connect() as connection:
        with pytest.raises(ValueError, match="Schema mismatch"):
            export_component(
                connection,
                tmp_path,
                name="bad",
                query="SELECT 1 AS x",
                expected_columns={"y": "INTEGER"},
            )
    assert not (tmp_path / "bad.json").exists()
    assert not (tmp_path / "bad.parquet").exists()


def test_safe_path_and_invalid_component_name(tmp_path: Path) -> None:
    with duckdb.connect() as connection:
        artifact = export_component(
            connection,
            tmp_path / "quote'path",
            name="valid",
            query="SELECT 1 AS x",
            expected_columns={"x": "INTEGER"},
        )
        assert artifact.rows == 1
        with pytest.raises(ValueError, match="Invalid component"):
            export_component(
                connection,
                tmp_path,
                name="../bad",
                query="SELECT 1",
                expected_columns={},
            )


def test_schema_type_error_is_rejected_before_export(tmp_path: Path) -> None:
    with duckdb.connect() as connection:
        with pytest.raises(ValueError, match="Schema type mismatch"):
            export_component(
                connection,
                tmp_path,
                name="wrong_type",
                query="SELECT 1 AS value",
                expected_columns={"value": "DOUBLE"},
            )
    assert not (tmp_path / "wrong_type.parquet").exists()
