"""Dataset export, schema validation, category-map creation, query hashing."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path

import polars as pl

from python_models.statistical.manifests import query_hash
from python_models.statistical.outputs import write_parquet_atomic
from python_models.statistical.schemas import DatasetColumn, DatasetMetadata

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
        (c.name, c.dtype, actual[c.name])
        for c in expected
        if actual[c.name] != c.dtype
    ]
    if mismatches:
        raise ValueError(f"dataset dtype mismatches: {mismatches}")
