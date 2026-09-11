"""End-to-end exercise of ``prepare_dataset`` against in-memory DuckDB."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from python_models.statistical.dataset_registry import DatasetSpec, get_spec
from python_models.statistical.datasets import _canonical_arrow_type, prepare_dataset
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
    weight_type: str = "DOUBLE",
    list_inner_type: str = "DOUBLE",
    extra_column: bool = False,
) -> None:
    extra_select = ", 42::INTEGER AS extra_col" if extra_column else ""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(
        f"""
        CREATE OR REPLACE VIEW {schema}.model_input_test AS
        SELECT *{extra_select}
        FROM (VALUES
            (1::UINTEGER, 'trajectory', 'observed', 1.0::{weight_type}, [0.1, 0.9]::{list_inner_type}[], {{'is_holdout': TRUE}},  'TRAIN',     'AL', 'play_by_play', '{snapshot}'),
            (2::UINTEGER, 'trajectory', 'unknown',  0.0::{weight_type}, [0.5, 0.5]::{list_inner_type}[], {{'is_holdout': FALSE}}, 'TRAIN',     'NL', 'play_by_play', '{snapshot}'),
            (3::UINTEGER, 'location',   'observed', 1.0::{weight_type}, [0.2, 0.8]::{list_inner_type}[], {{'is_holdout': FALSE}}, 'VALIDATE',  'AL', 'box_score',    '{snapshot}'),
            (4::UINTEGER, 'location',   'observed', 1.0::{weight_type}, [0.7, 0.3]::{list_inner_type}[], {{'is_holdout': TRUE}},  'TEST',      'NL', 'box_score',    '{snapshot}'),
            (5::UINTEGER, 'trajectory', 'observed', 1.0::{weight_type}, [0.4, 0.6]::{list_inner_type}[], {{'is_holdout': FALSE}}, 'TRAIN',     'AL', NULL,           '{snapshot}')
        )
        AS t(event_key, dimension, observed_status, training_weight, dl_p_class, holdout_flags, primary_fold, league, source_family, source_snapshot_id)
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
    assert set(table.column_names) >= {
        "event_key",
        "dimension",
        "training_weight",
        "dl_p_class",
        "holdout_flags",
    }
    assert pa.types.is_list(table.schema.field("dl_p_class").type)
    assert pa.types.is_struct(table.schema.field("holdout_flags").type)

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
    assert metadata.schema_hash is not None
    assert metadata.content_hash is not None
    assert metadata.transformation_hash is not None
    assert metadata.dependency_hash is not None
    assert metadata.eligible_count == 4

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
    assert manifest.schema_hash == metadata.schema_hash
    assert manifest.content_hash == metadata.content_hash
    assert manifest.transformation_hash == metadata.transformation_hash
    assert manifest.dependency_hash == metadata.dependency_hash
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


@pytest.mark.parametrize("snapshot", ["dev", "named-snapshot"])
def test_prepare_dataset_rerun_rejects_same_schema_content_drift(
    tmp_path: Path, con: duckdb.DuckDBPyConnection, snapshot: str
) -> None:
    con.execute("CREATE SCHEMA main_models")
    con.execute(
        """
        CREATE TABLE main_models.source_test AS
        SELECT * FROM (VALUES
            (1, 'trajectory', 'observed', 1.0, 'TRAIN', 'AL', 'pbp', 'dev')
        ) AS t(event_key, dimension, observed_status, training_weight, primary_fold, league, source_family, source_snapshot_id)
        """
    )
    con.execute(
        "CREATE VIEW main_models.model_input_test AS "
        "SELECT * FROM main_models.source_test"
    )
    con.execute("UPDATE main_models.source_test SET source_snapshot_id = ?", [snapshot])
    _ = prepare_dataset(
        _spec(),
        artifact_id="aid-content-drift",
        con=con,
        output_root=tmp_path,
        artifact_versions={"duckdb": "test"},
    )
    parquet_path = (
        tmp_path / "model_input_test" / "aid-content-drift" / "dataset.parquet"
    )
    frozen_payload = pq.read_table(parquet_path).column("event_key").to_pylist()
    con.execute("UPDATE main_models.source_test SET event_key = 99")

    with pytest.raises(ValueError, match="current dataset content differs"):
        _ = prepare_dataset(
            _spec(),
            artifact_id="aid-content-drift",
            con=con,
            output_root=tmp_path,
            artifact_versions={"duckdb": "test"},
        )

    assert pq.read_table(parquet_path).column("event_key").to_pylist() == frozen_payload


def test_prepare_dataset_rejects_legacy_metadata_without_rewriting(
    tmp_path: Path, con: duckdb.DuckDBPyConnection
) -> None:
    _seed_view(con)
    _ = prepare_dataset(
        _spec(),
        artifact_id="aid-legacy",
        con=con,
        output_root=tmp_path,
        artifact_versions={"duckdb": "test"},
    )
    artifact_dir = tmp_path / "model_input_test" / "aid-legacy"
    metadata_path = artifact_dir / "dataset_metadata.json"
    parquet_path = artifact_dir / "dataset.parquet"
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    for key in (
        "schema_hash",
        "content_hash",
        "transformation_hash",
        "dependency_hash",
    ):
        payload.pop(key)
    metadata_path.write_text(json.dumps(payload), encoding="utf-8")
    parquet_mtime = parquet_path.stat().st_mtime_ns

    with pytest.raises(ValueError, match="legacy metadata"):
        _ = prepare_dataset(
            _spec(),
            artifact_id="aid-legacy",
            con=con,
            output_root=tmp_path,
            artifact_versions={"duckdb": "test"},
        )

    assert parquet_path.stat().st_mtime_ns == parquet_mtime


def test_geometry_spec_separates_observed_eligible_derived_and_inference_counts(
    tmp_path: Path, con: duckdb.DuckDBPyConnection
) -> None:
    con.execute("CREATE SCHEMA main_models")
    con.execute(
        """
        CREATE VIEW main_models.model_input_geometry AS
        SELECT * FROM (VALUES
            (1, TRUE,  'observed',     TRUE,  1.0, 'dev'),
            (2, FALSE, 'derived',      TRUE,  1.0, 'dev'),
            (3, FALSE, 'missing',      TRUE,  1.0, 'dev'),
            (4, FALSE, 'unknown_code', FALSE, 1.0, 'dev'),
            (5, FALSE, 'missing',      TRUE,  0.0, 'dev')
        ) AS t(event_key, is_observed_class, observed_status, model_input_eligible, training_weight, source_snapshot_id)
        """
    )
    registered = get_spec("model_input_geometry")
    spec = registered.model_copy(
        update={
            "categorical_columns": (),
            "grain": ("event_key",),
        }
    )
    _ = prepare_dataset(
        spec,
        artifact_id="aid-geometry-counts",
        con=con,
        output_root=tmp_path,
        artifact_versions={"duckdb": "test"},
    )
    metadata = DatasetMetadata.model_validate_json(
        (
            tmp_path
            / "model_input_geometry"
            / "aid-geometry-counts"
            / "dataset_metadata.json"
        ).read_text(encoding="utf-8")
    )

    assert metadata.observed_truth_count == 1
    assert metadata.eligible_count == 3
    assert metadata.derived_truth_count == 1
    assert metadata.inference_count == 2


def test_prepare_dataset_rerun_rejects_added_column(
    tmp_path: Path, con: duckdb.DuckDBPyConnection
) -> None:
    _seed_view(con)
    _ = prepare_dataset(
        _spec(),
        artifact_id="aid-schema-add",
        con=con,
        ledger_schema="main_models",
        output_root=tmp_path,
        artifact_versions={"duckdb": "test"},
    )
    _seed_view(con, extra_column=True)
    with pytest.raises(
        ValueError, match="view definition changed under artifact_id='aid-schema-add'"
    ):
        _ = prepare_dataset(
            _spec(),
            artifact_id="aid-schema-add",
            con=con,
            ledger_schema="main_models",
            output_root=tmp_path,
            artifact_versions={"duckdb": "test"},
        )


def test_prepare_dataset_rerun_rejects_retyped_column(
    tmp_path: Path, con: duckdb.DuckDBPyConnection
) -> None:
    _seed_view(con)
    _ = prepare_dataset(
        _spec(),
        artifact_id="aid-schema-retype",
        con=con,
        ledger_schema="main_models",
        output_root=tmp_path,
        artifact_versions={"duckdb": "test"},
    )
    _seed_view(con, weight_type="REAL")
    with pytest.raises(
        ValueError,
        match="view definition changed under artifact_id='aid-schema-retype'",
    ):
        _ = prepare_dataset(
            _spec(),
            artifact_id="aid-schema-retype",
            con=con,
            ledger_schema="main_models",
            output_root=tmp_path,
            artifact_versions={"duckdb": "test"},
        )


def test_prepare_dataset_rerun_rejects_retyped_list_child(
    tmp_path: Path, con: duckdb.DuckDBPyConnection
) -> None:
    _seed_view(con)
    _ = prepare_dataset(
        _spec(),
        artifact_id="aid-schema-list-retype",
        con=con,
        ledger_schema="main_models",
        output_root=tmp_path,
        artifact_versions={"duckdb": "test"},
    )
    _seed_view(con, list_inner_type="VARCHAR")
    with pytest.raises(
        ValueError,
        match="view definition changed under artifact_id='aid-schema-list-retype'",
    ):
        _ = prepare_dataset(
            _spec(),
            artifact_id="aid-schema-list-retype",
            con=con,
            ledger_schema="main_models",
            output_root=tmp_path,
            artifact_versions={"duckdb": "test"},
        )


def test_canonical_arrow_type_ignores_nested_field_names() -> None:
    duck_style = pa.list_(pa.field("l", pa.float64()))
    parquet_style = pa.list_(pa.field("element", pa.float64()))
    assert _canonical_arrow_type(duck_style) == _canonical_arrow_type(parquet_style)

    nested_duck = pa.struct([pa.field("flags", pa.list_(pa.field("l", pa.bool_())))])
    nested_parquet = pa.struct(
        [pa.field("flags", pa.list_(pa.field("element", pa.bool_())))]
    )
    assert _canonical_arrow_type(nested_duck) == _canonical_arrow_type(nested_parquet)

    large_variant = pa.large_list(pa.field("element", pa.large_string()))
    small_variant = pa.list_(pa.field("l", pa.string()))
    assert _canonical_arrow_type(large_variant) == _canonical_arrow_type(small_variant)


def test_canonical_arrow_type_preserves_child_type_differences() -> None:
    doubles = pa.list_(pa.field("l", pa.float64()))
    strings = pa.list_(pa.field("element", pa.string()))
    assert _canonical_arrow_type(doubles) != _canonical_arrow_type(strings)

    struct_a = pa.struct([pa.field("x", pa.float64())])
    struct_b = pa.struct([pa.field("x", pa.string())])
    assert _canonical_arrow_type(struct_a) != _canonical_arrow_type(struct_b)


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
