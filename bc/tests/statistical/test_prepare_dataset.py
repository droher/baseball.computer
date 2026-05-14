"""End-to-end exercise of ``prepare_dataset`` against in-memory DuckDB."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from python_models.statistical.dataset_registry import DatasetSpec
from python_models.statistical.datasets import prepare_dataset
from python_models.statistical.manifests import query_hash
from python_models.statistical.schemas import DatasetMetadata


@pytest.fixture
def con() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    connection = duckdb.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


def _seed_view(
    con: duckdb.DuckDBPyConnection,
    *,
    schema: str = "main_models",
    snapshot: str = "dev",
) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(
        f"""
        CREATE OR REPLACE VIEW {schema}.model_input_test AS
        SELECT *
        FROM (VALUES
            (1::UINTEGER, 'trajectory', 'observed', 1.0::DOUBLE, 'TRAIN',     'AL', 'play_by_play', '{snapshot}'),
            (2::UINTEGER, 'trajectory', 'unknown',  0.0::DOUBLE, 'TRAIN',     'NL', 'play_by_play', '{snapshot}'),
            (3::UINTEGER, 'location',   'observed', 1.0::DOUBLE, 'VALIDATE',  'AL', 'box_score',    '{snapshot}'),
            (4::UINTEGER, 'location',   'observed', 1.0::DOUBLE, 'TEST',      'NL', 'box_score',    '{snapshot}'),
            (5::UINTEGER, 'trajectory', 'observed', 1.0::DOUBLE, 'TRAIN',     'AL', NULL,           '{snapshot}')
        )
        AS t(event_key, dimension, observed_status, training_weight, primary_fold, league, source_family, source_snapshot_id)
        """
    )


def _spec() -> DatasetSpec:
    return DatasetSpec(
        name="model_input_test",
        sqlmesh_table="model_input_test",
        dataset_version="0.0.1",
        grain=("event_key", "dimension"),
        categorical_columns=(
            "dimension",
            "observed_status",
            "primary_fold",
            "league",
            "source_family",
        ),
    )


def test_prepare_dataset_writes_parquet_and_metadata(
    tmp_path: Path, con: duckdb.DuckDBPyConnection
) -> None:
    _seed_view(con)
    manifest = prepare_dataset(
        _spec(),
        artifact_id="aid-1",
        con=con,
        ledger_schema="main_models",
        output_root=tmp_path,
        artifact_versions={"duckdb": "test"},
    )
    artifact_dir = tmp_path / "model_input_test" / "aid-1"
    parquet_path = artifact_dir / "dataset.parquet"
    metadata_path = artifact_dir / "dataset_metadata.json"
    manifest_path = artifact_dir / "manifest.json"

    assert parquet_path.exists()
    assert metadata_path.exists()
    assert manifest_path.exists()

    table = pq.read_table(parquet_path)
    assert table.num_rows == 5
    assert set(table.column_names) >= {"event_key", "dimension", "training_weight"}

    metadata = DatasetMetadata.model_validate_json(
        metadata_path.read_text(encoding="utf-8")
    )
    assert metadata.row_count == 5
    assert metadata.observed_truth_count == 4
    assert metadata.target_population_count == 5
    assert metadata.source_snapshot_id == "dev"
    assert metadata.dataset_name == "model_input_test"
    assert metadata.split_policy == "game_hash_70_15_15"
    assert metadata.query_hash == query_hash(
        "SELECT * FROM main_models.model_input_test"
    )

    assert metadata.category_maps["dimension"] == {"location": 0, "trajectory": 1}
    assert metadata.category_maps["observed_status"] == {"observed": 0, "unknown": 1}
    assert metadata.category_maps["primary_fold"] == {
        "TEST": 0,
        "TRAIN": 1,
        "VALIDATE": 2,
    }
    assert "play_by_play" in metadata.category_maps["source_family"]
    assert None not in metadata.category_maps["source_family"]

    assert manifest.artifact_id == "aid-1"
    assert manifest.kind == "dataset"
    assert manifest.query_hash == metadata.query_hash
    assert manifest.source_snapshot_id == "dev"
    assert manifest.metadata["row_count"] == 5
    assert manifest.metadata["observed_truth_count"] == 4
    assert manifest.metadata["ledger_schema"] == "main_models"


def test_prepare_dataset_idempotent_rerun(
    tmp_path: Path, con: duckdb.DuckDBPyConnection
) -> None:
    _seed_view(con)
    first = prepare_dataset(
        _spec(),
        artifact_id="aid-2",
        con=con,
        ledger_schema="main_models",
        output_root=tmp_path,
        artifact_versions={"duckdb": "test"},
    )
    parquet_path = tmp_path / "model_input_test" / "aid-2" / "dataset.parquet"
    parquet_mtime = parquet_path.stat().st_mtime_ns

    second = prepare_dataset(
        _spec(),
        artifact_id="aid-2",
        con=con,
        ledger_schema="main_models",
        output_root=tmp_path,
        artifact_versions={"duckdb": "test"},
    )

    assert first.artifact_id == second.artifact_id
    assert first.query_hash == second.query_hash
    assert parquet_path.stat().st_mtime_ns == parquet_mtime


def test_prepare_dataset_rejects_snapshot_id_drift(
    tmp_path: Path, con: duckdb.DuckDBPyConnection
) -> None:
    _seed_view(con, snapshot="snap-A")
    _ = prepare_dataset(
        _spec(),
        artifact_id="aid-3",
        con=con,
        ledger_schema="main_models",
        output_root=tmp_path,
        artifact_versions={"duckdb": "test"},
    )
    _seed_view(con, snapshot="snap-B")
    with pytest.raises(ValueError, match="source_snapshot_id"):
        _ = prepare_dataset(
            _spec(),
            artifact_id="aid-3",
            con=con,
            ledger_schema="main_models",
            output_root=tmp_path,
            artifact_versions={"duckdb": "test"},
        )


def test_prepare_dataset_rejects_unknown_categorical(
    tmp_path: Path, con: duckdb.DuckDBPyConnection
) -> None:
    _seed_view(con)
    bad_spec = _spec().model_copy(update={"categorical_columns": ("does_not_exist",)})
    with pytest.raises(ValueError, match="declared categorical columns"):
        _ = prepare_dataset(
            bad_spec,
            artifact_id="aid-bad",
            con=con,
            ledger_schema="main_models",
            output_root=tmp_path,
            artifact_versions={"duckdb": "test"},
        )


def test_prepare_dataset_fails_on_multiple_snapshot_ids(
    tmp_path: Path, con: duckdb.DuckDBPyConnection
) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS main_models")
    con.execute(
        """
        CREATE OR REPLACE VIEW main_models.model_input_test AS
        SELECT *
        FROM (VALUES
            (1::UINTEGER, 'a', 'observed', 1.0::DOUBLE, 'TRAIN', 'AL', 'pbp', 'snap-A'),
            (2::UINTEGER, 'a', 'observed', 1.0::DOUBLE, 'TRAIN', 'AL', 'pbp', 'snap-B')
        )
        AS t(event_key, dimension, observed_status, training_weight, primary_fold, league, source_family, source_snapshot_id)
        """
    )
    with pytest.raises(ValueError, match="distinct source_snapshot_id"):
        _ = prepare_dataset(
            _spec(),
            artifact_id="aid-multi",
            con=con,
            ledger_schema="main_models",
            output_root=tmp_path,
            artifact_versions={"duckdb": "test"},
        )


def test_prepare_dataset_writes_manifest_json_payload(
    tmp_path: Path, con: duckdb.DuckDBPyConnection
) -> None:
    _seed_view(con)
    manifest = prepare_dataset(
        _spec(),
        artifact_id="aid-payload",
        con=con,
        ledger_schema="main_models",
        output_root=tmp_path,
        artifact_versions={"duckdb": "test"},
    )
    raw = json.loads(
        (tmp_path / "model_input_test" / "aid-payload" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert raw["artifact_id"] == "aid-payload"
    assert raw["kind"] == "dataset"
    assert raw["query_hash"] == manifest.query_hash
    assert raw["output_paths"]["dataset"].endswith("dataset.parquet")
