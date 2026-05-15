"""Atomic writes for deep-artifact directory layout.

A deep artifact directory looks like::

    artifacts/statistical/deep/<target_name>/<artifact_id>/
        manifest.json
        exports/
            probabilities.parquet
            class_labels.json
            calibration_report.parquet   (PR3+)
        model/                            (Keras checkpoint, PR3+)

Every file is written via a same-directory tempfile + ``os.replace`` so
partial writes never surface to readers.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Literal

import polars as pl

from python_models.statistical.config import DEEP_ROOT

ParquetCompression = Literal["zstd", "snappy", "gzip", "brotli", "lz4", "uncompressed"]

_log = logging.getLogger(__name__)


def deep_artifact_dir(target_name: str, artifact_id: str, *, root: Path = DEEP_ROOT) -> Path:
    return root / target_name / artifact_id


def deep_exports_dir(target_name: str, artifact_id: str, *, root: Path = DEEP_ROOT) -> Path:
    return deep_artifact_dir(target_name, artifact_id, root=root) / "exports"


def deep_model_dir(target_name: str, artifact_id: str, *, root: Path = DEEP_ROOT) -> Path:
    return deep_artifact_dir(target_name, artifact_id, root=root) / "model"


def _atomic_write_bytes(target: Path, payload: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "wb") as fh:
            _ = fh.write(payload)
        os.replace(tmp_name, target)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def write_class_labels_json(
    target_name: str,
    artifact_id: str,
    *,
    class_labels: tuple[str, ...] | dict[str, tuple[str, ...]],
    root: Path = DEEP_ROOT,
) -> Path:
    """Persist the ordered class labels for one (or many per-dim) heads.

    For single-head specs pass a flat tuple; for multi-dimensional
    artifacts (e.g. composite Geometry) pass a ``{dimension: labels}``
    dict. The ingestion ``@model`` reads this back to map LIST index ->
    label.
    """
    path = deep_exports_dir(target_name, artifact_id, root=root) / "class_labels.json"
    if isinstance(class_labels, tuple):
        payload = json.dumps({"labels": list(class_labels)}, indent=2)
    else:
        payload = json.dumps(
            {k: list(v) for k, v in class_labels.items()}, indent=2, sort_keys=True
        )
    _atomic_write_bytes(path, payload.encode("utf-8"))
    return path


def write_probabilities_parquet(
    target_name: str,
    artifact_id: str,
    *,
    df: pl.DataFrame,
    compression: ParquetCompression = "zstd",
    root: Path = DEEP_ROOT,
) -> Path:
    path = deep_exports_dir(target_name, artifact_id, root=root) / "probabilities.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=".probabilities.", suffix=".parquet"
    )
    try:
        os.close(fd)
        df.write_parquet(tmp_name, compression=compression)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise
    return path
