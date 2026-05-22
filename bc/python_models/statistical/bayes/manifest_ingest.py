"""Helpers backing the ``scorer_observation_propensities`` SQLMesh ``@model``.

Iterates every registered Bayes observation ``BayesTargetSpec``, resolves
the published pointer via ``find_published_manifest(spec.published_manifest_name())``,
reads the winner artifact's ``exports/event_propensity.parquet``, and
yields one polars frame per dimension stamped with ``bayes_artifact_id``.
Always yields at least one (possibly empty) typed frame so the @model
schema persists when no targets have been published yet.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

import polars as pl

from python_models.statistical.bayes.registry import all_target_names, get_target
from python_models.statistical.manifests import (
    find_published_manifest,
    read_manifest,
)
from python_models.statistical.schemas import PublishedPointer

_log = logging.getLogger(__name__)


PROPENSITY_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "dimension": pl.Utf8(),
    "p_observed_mean": pl.Float64(),
    "bayes_artifact_id": pl.Utf8(),
}


def empty_propensity_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=PROPENSITY_SCHEMA)


def iterate_published_observation_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes observation target."""
    from python_models.statistical.bayes import targets as _targets  # noqa: F401  # pyright: ignore[reportUnusedImport]

    for name in all_target_names():
        spec = get_target(name)
        pointer_path = find_published_manifest(spec.published_manifest_name())
        if pointer_path is None:
            _log.info(
                "bayes.manifest_ingest: no pointer for %s; skipping",
                spec.published_manifest_name(),
            )
            continue
        pointer = PublishedPointer.model_validate_json(
            pointer_path.read_text(encoding="utf-8")
        )
        manifest = read_manifest(pointer.manifest_path)
        event_propensity_path = (
            pointer.manifest_path.parent / "exports" / "event_propensity.parquet"
        )
        if not event_propensity_path.exists():
            _log.warning(
                "bayes.manifest_ingest: missing event_propensity.parquet for %s at %s",
                spec.published_manifest_name(),
                event_propensity_path,
            )
            continue
        df = pl.read_parquet(str(event_propensity_path))
        if df.height == 0:
            _log.info(
                "bayes.manifest_ingest: event_propensity empty for %s",
                spec.published_manifest_name(),
            )
            continue
        df = df.with_columns(
            pl.col("event_key").cast(pl.UInt32),
            pl.col("dimension").cast(pl.Utf8),
            pl.col("p_observed_mean").cast(pl.Float64),
            pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("bayes_artifact_id"),
        )
        _log.info(
            "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
            df.height,
            spec.published_manifest_name(),
            spec.dimension,
        )
        yield df


def aggregate_observation_propensity_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    emitted = False
    for frame in iterate_published_observation_frames():
        emitted = True
        yield frame.select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("dimension").cast(pl.Utf8),
                pl.col("p_observed_mean").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        _log.info(
            "bayes.manifest_ingest: no observation targets published; yielding empty frame"
        )
        yield empty_propensity_frame()
