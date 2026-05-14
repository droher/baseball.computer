"""Dataset export, schema validation, category-map creation, query hashing."""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Iterable
from pathlib import Path

import duckdb
import polars as pl
import pyarrow.parquet as pq

from python_models.statistical.config import DATASETS_ROOT
from python_models.statistical.dataset_registry import DatasetSpec
from python_models.statistical.manifests import (
    package_versions as snapshot_package_versions,
    query_hash,
    read_manifest,
    utc_now,
    write_manifest,
)
from python_models.statistical.outputs import write_parquet_atomic
from python_models.statistical.schemas import (
    ArtifactManifest,
    DatasetColumn,
    DatasetMetadata,
)

_log = logging.getLogger(__name__)


def build_category_map(values: Iterable[str | None]) -> dict[str, int]:
    """Map sorted distinct non-null tokens to dense integer codes.

    Codes start at 0 and are stable across runs given the same set of
    inputs. Callers exercise an explicit unseen-category policy when
    encoding new rows; this builder never silently invents codes.
    """
    distinct = sorted({v for v in values if v is not None})
    return {token: idx for idx, token in enumerate(distinct)}


def encode_with_map(
    values: Iterable[str | None],
    mapping: dict[str, int],
    *,
    unseen_policy: str = "error",
) -> list[int | None]:
    """Encode tokens against a frozen category map.

    ``unseen_policy``:

    - ``error`` — raise on any token absent from ``mapping``.
    - ``null`` — emit ``None`` for unseen tokens (callers must filter).
    - ``add`` — extend ``mapping`` in place with new codes.
    """
    out: list[int | None] = []
    for v in values:
        if v is None:
            out.append(None)
            continue
        if v in mapping:
            out.append(mapping[v])
            continue
        match unseen_policy:
            case "error":
                raise ValueError(f"unseen category {v!r} under policy=error")
            case "null":
                out.append(None)
            case "add":
                code = len(mapping)
                mapping[v] = code
                out.append(code)
            case other:
                raise ValueError(f"unknown unseen_policy {other!r}")
    return out


def export_dataset(
    df: pl.DataFrame,
    metadata: DatasetMetadata,
    *,
    dataset_path: Path,
) -> None:
    """Write the Parquet + metadata JSON atomically alongside each other."""
    write_parquet_atomic(df, dataset_path)
    metadata_path = dataset_path.with_name("dataset_metadata.json")
    metadata_path.write_text(metadata.model_dump_json(indent=2), encoding="utf-8")


def hash_dataset_query(query_text: str) -> str:
    return query_hash(query_text)


def validate_schema(
    df: pl.DataFrame,
    expected: tuple[DatasetColumn, ...],
) -> None:
    """Verify required columns exist and dtypes match expected schema."""
    actual = {name: str(dt) for name, dt in zip(df.columns, df.dtypes)}
    missing = [c.name for c in expected if c.name not in actual]
    if missing:
        raise ValueError(f"dataset missing required columns: {missing}")
    mismatches = [
        (c.name, c.dtype, actual[c.name]) for c in expected if actual[c.name] != c.dtype
    ]
    if mismatches:
        raise ValueError(f"dataset dtype mismatches: {mismatches}")


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _copy_to_parquet_atomic(
    con: duckdb.DuckDBPyConnection,
    *,
    select_sql: str,
    target: Path,
    compression: str = "ZSTD",
) -> None:
    """Run DuckDB ``COPY ... TO`` into a temp file and rename to ``target``.

    DuckDB writes the temp file directly so the dataset never has to
    fit in Python memory.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.stem}.", suffix=".parquet"
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        con.execute(
            f"COPY ({select_sql}) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION {compression})"
        )
        os.replace(tmp_path, target)
    except BaseException:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def _resolve_source_snapshot_id(con: duckdb.DuckDBPyConnection, view_sql: str) -> str:
    rows = con.execute(
        f"SELECT DISTINCT source_snapshot_id FROM ({view_sql}) AS d"
    ).fetchall()
    if len(rows) != 1:
        raise ValueError(
            f"dataset emits {len(rows)} distinct source_snapshot_id values; "
            "exporter requires exactly one."
        )
    value = rows[0][0]
    if value is None:
        raise ValueError("dataset emits NULL source_snapshot_id; cannot pin snapshot.")
    return str(value)


def _build_category_maps_via_sql(
    con: duckdb.DuckDBPyConnection,
    *,
    view_sql: str,
    columns: tuple[str, ...],
) -> dict[str, dict[str, int]]:
    maps: dict[str, dict[str, int]] = {}
    for column in columns:
        quoted = _quote_ident(column)
        rows = con.execute(
            f"""
            SELECT DISTINCT {quoted}::VARCHAR AS value
            FROM ({view_sql}) AS d
            WHERE {quoted} IS NOT NULL
            ORDER BY 1
            """
        ).fetchall()
        maps[column] = {value: idx for idx, (value,) in enumerate(rows)}
    return maps


def _columns_from_parquet(parquet_path: Path) -> tuple[DatasetColumn, ...]:
    schema = pq.read_schema(parquet_path)
    return tuple(
        DatasetColumn(name=field.name, dtype=str(field.type), role="payload")
        for field in schema
    )


def _resolve_artifact_root(
    spec: DatasetSpec, *, artifact_id: str, output_root: Path | None
) -> Path:
    root = output_root if output_root is not None else DATASETS_ROOT
    return root / spec.name / artifact_id


def _verify_rerun(
    *,
    existing_metadata_path: Path,
    new_query_hash: str,
    new_source_snapshot_id: str,
) -> DatasetMetadata:
    existing = DatasetMetadata.model_validate_json(
        existing_metadata_path.read_text(encoding="utf-8")
    )
    if existing.query_hash != new_query_hash:
        raise ValueError(
            "rerun of existing dataset artifact_id with a different query_hash "
            f"is forbidden ({existing.query_hash!r} on disk vs {new_query_hash!r} requested)."
        )
    if existing.source_snapshot_id != new_source_snapshot_id:
        raise ValueError(
            "rerun of existing dataset artifact_id with a different "
            f"source_snapshot_id is forbidden ({existing.source_snapshot_id!r} "
            f"on disk vs {new_source_snapshot_id!r} requested)."
        )
    return existing


def prepare_dataset(
    spec: DatasetSpec,
    *,
    artifact_id: str,
    con: duckdb.DuckDBPyConnection,
    ledger_schema: str = "main_models",
    output_root: Path | None = None,
    source_snapshot_id_override: str | None = None,
    artifact_versions: dict[str, str] | None = None,
) -> ArtifactManifest:
    """Snapshot a ``main_models.model_input_*`` view to versioned Parquet.

    Idempotent on ``artifact_id``: rerunning with the same ID succeeds
    only when the on-disk metadata matches the current query hash and
    source snapshot. A mismatch fails loudly instead of overwriting.
    """
    table = f"{ledger_schema}.{spec.sqlmesh_table}"
    select_sql = f"SELECT * FROM {table}"
    qh = query_hash(select_sql)
    artifact_root = _resolve_artifact_root(
        spec, artifact_id=artifact_id, output_root=output_root
    )
    dataset_path = artifact_root / "dataset.parquet"
    metadata_path = artifact_root / "dataset_metadata.json"
    manifest_path = artifact_root / "manifest.json"

    _log.info(
        "prepare_dataset start name=%s artifact_id=%s ledger_schema=%s",
        spec.name,
        artifact_id,
        ledger_schema,
    )

    source_snapshot_id = (
        source_snapshot_id_override
        if source_snapshot_id_override is not None
        else _resolve_source_snapshot_id(con, select_sql)
    )

    if metadata_path.exists():
        existing = _verify_rerun(
            existing_metadata_path=metadata_path,
            new_query_hash=qh,
            new_source_snapshot_id=source_snapshot_id,
        )
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"dataset metadata present but manifest missing at {manifest_path}; "
                "delete the artifact directory to rebuild."
            )
        _log.info(
            "prepare_dataset verified existing artifact_id=%s row_count=%d",
            artifact_id,
            existing.row_count,
        )
        return read_manifest(manifest_path)

    _copy_to_parquet_atomic(con, select_sql=select_sql, target=dataset_path)

    parquet_columns = _columns_from_parquet(dataset_path)
    declared_columns = {c.name for c in parquet_columns}

    missing_categoricals = [
        c for c in spec.categorical_columns if c not in declared_columns
    ]
    if missing_categoricals:
        raise ValueError(
            f"dataset {spec.name!r} declared categorical columns not present in "
            f"parquet schema: {missing_categoricals}"
        )

    category_maps = _build_category_maps_via_sql(
        con, view_sql=select_sql, columns=spec.categorical_columns
    )

    counts = con.execute(
        f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT_IF({spec.observed_truth_predicate}) AS observed_truth_count
        FROM ({select_sql}) AS d
        """
    ).fetchone()
    if counts is None:
        raise RuntimeError("count query returned no rows; expected exactly one.")
    row_count = int(counts[0])
    observed_truth_count = int(counts[1])

    metadata = DatasetMetadata(
        dataset_name=spec.name,
        dataset_version=spec.dataset_version,
        source_snapshot_id=source_snapshot_id,
        query_hash=qh,
        row_count=row_count,
        target_population_count=row_count,
        observed_truth_count=observed_truth_count,
        parquet_path=dataset_path,
        columns=parquet_columns,
        split_policy="game_hash_70_15_15",
        category_maps=category_maps,
    )
    _write_metadata_atomic(metadata, metadata_path)

    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="dataset",
        name=spec.name,
        version=spec.dataset_version,
        created_at=utc_now(),
        source_snapshot_id=source_snapshot_id,
        query_hash=qh,
        output_paths={"dataset": dataset_path, "metadata": metadata_path},
        package_versions=artifact_versions
        if artifact_versions is not None
        else snapshot_package_versions(),
        metadata={
            "row_count": row_count,
            "observed_truth_count": observed_truth_count,
            "ledger_schema": ledger_schema,
        },
    )
    write_manifest(manifest, manifest_path)

    _log.info(
        "prepare_dataset done name=%s artifact_id=%s row_count=%d observed_truth_count=%d",
        spec.name,
        artifact_id,
        row_count,
        observed_truth_count,
    )
    return manifest


def _write_metadata_atomic(metadata: DatasetMetadata, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=".dataset_metadata.", suffix=".json"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(metadata.model_dump_json(indent=2))
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise
