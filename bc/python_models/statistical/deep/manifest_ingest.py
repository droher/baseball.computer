"""Helpers backing the ``dl_*_proposal_manifest`` SQLMesh ``@model`` files.

The Python ``@model`` decorators expect a function body that yields
DataFrames. Pulling that body into a regular library function lets us
test it in isolation (importing the ``@model`` file directly invokes the
decorator, which requires a SQLMesh project context to be loaded).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

import polars as pl

from python_models.statistical.deep.registry import (
    SiblingManifestName,
    all_target_names,
    get_target,
    sibling_manifest_for,
)
from python_models.statistical.manifests import (
    find_published_manifest,
    read_manifest,
)
from python_models.statistical.schemas import PublishedPointer

_log = logging.getLogger(__name__)

PROPOSAL_MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "dimension": pl.Utf8(),
    "dl_artifact_id": pl.Utf8(),
    "dl_p_class": pl.List(pl.Float64()),
}


def iterate_published_target_frames(
    sibling_manifest_name: SiblingManifestName,
) -> Iterator[pl.DataFrame]:
    """Yield one frame per published target whose sibling matches.

    Each yielded frame has ``event_key`` cast to ``UInt32``, a
    ``dimension`` column equal to the target's ``proposal_dimension``,
    and the ``dl_artifact_id`` stamped from the artifact manifest. Other
    columns from ``probabilities.parquet`` pass through unchanged.
    """
    for name in all_target_names():
        if sibling_manifest_for(name) != sibling_manifest_name:
            continue
        spec = get_target(name)
        pointer_path = find_published_manifest(spec.published_manifest_name())
        if pointer_path is None:
            _log.info(
                "manifest_ingest: no pointer for %s; skipping",
                spec.published_manifest_name(),
            )
            continue
        pointer = PublishedPointer.model_validate_json(
            pointer_path.read_text(encoding="utf-8")
        )
        manifest = read_manifest(pointer.manifest_path)
        probabilities_path = manifest.output_paths["probabilities"]
        df = pl.read_parquet(str(probabilities_path))
        if df.height == 0:
            _log.info(
                "manifest_ingest: probabilities empty for %s",
                spec.published_manifest_name(),
            )
            continue
        df = df.with_columns(
            pl.col("event_key").cast(pl.UInt32),
            pl.lit(spec.proposal_dimension, dtype=pl.Utf8).alias("dimension"),
            pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("dl_artifact_id"),
        )
        _log.info(
            "manifest_ingest: %d rows from %s (dimension=%s)",
            df.height,
            spec.published_manifest_name(),
            spec.proposal_dimension,
        )
        yield df


def empty_proposal_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=PROPOSAL_MANIFEST_SCHEMA)


def aggregate_proposal_manifest_frames(
    sibling_manifest_name: SiblingManifestName,
) -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one (possibly empty) frame.

    ``@model`` callers want a non-empty iterator so SQLMesh can persist the
    typed schema even when no targets have published yet.
    """
    emitted = False
    for frame in iterate_published_target_frames(sibling_manifest_name):
        emitted = True
        yield frame.select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("dimension"),
                pl.col("dl_artifact_id"),
                pl.col("dl_p_class"),
            ]
        )
    if not emitted:
        _log.info(
            "manifest_ingest: no targets published yet for %s; yielding empty frame",
            sibling_manifest_name,
        )
        yield empty_proposal_frame()


def read_published_probabilities(parquet_path: Path) -> pl.DataFrame:
    return pl.read_parquet(str(parquet_path))
