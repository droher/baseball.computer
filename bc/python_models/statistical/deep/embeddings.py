"""Entity-embedding extraction + Parquet export for deep artifacts.

Each high-cardinality categorical column gets a learned ``Embedding``
layer in the Keras trunk; PR6 extracts those weight matrices, pairs
them with their vocabulary, and exports a wide ``(entity_type,
entity_id, embedding_value DOUBLE[])`` Parquet under
``exports/embeddings.parquet`` per critique #10C of the Phase-3 plan.

The adversarial source-family probe (``leakage_probes.source_probe_held_out``)
consumes these embeddings; PR2 already implemented the probe primitive.
This module wires the data side: extraction, vocabulary alignment,
parquet write, and the dl_embedding_artifact ingestion helper.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

import numpy as np
import polars as pl
from numpy.typing import NDArray

from python_models.ml.features import FeatureLayout, Vocabulary
from python_models.statistical.deep.artifacts import (
    deep_exports_dir,
)

_log = logging.getLogger(__name__)

EMBEDDING_SCHEMA: dict[str, pl.DataType] = {
    "entity_type": pl.Utf8(),
    "entity_id": pl.Utf8(),
    "embedding_value": pl.List(pl.Float64()),
}


def vocabulary_entity_ids(vocab: Vocabulary) -> list[str]:
    """Index 0 is the OOV slot; indices 1..N map to ``vocab.values[i-1]``.

    The returned list is aligned with the Keras Embedding rows, so
    ``ids[i]`` is the canonical entity_id for embedding row ``i``.
    """
    return ["<oov>", *vocab.values]


def assemble_embeddings_frame(
    *,
    layout: FeatureLayout,
    embedding_matrices: dict[str, NDArray[np.float64]],
    vocabularies: dict[str, Vocabulary],
) -> pl.DataFrame:
    """Stack per-(group-or-column) embeddings into one wide frame.

    ``embedding_matrices`` is keyed by **embedding unit**: group name for
    grouped high-card cols, column name for ungrouped high-card cols.
    ``vocabularies`` is keyed identically. Each unit emits exactly one
    ``(entity_type=<unit>, entity_id, embedding_value)`` block.
    """
    expected_units = set(layout.embedding_unit_names())
    if set(embedding_matrices) != expected_units:
        raise KeyError(
            f"embedding_matrices keys {sorted(embedding_matrices)} != expected units {sorted(expected_units)}"
        )
    if set(vocabularies) != expected_units:
        raise KeyError(
            f"vocabularies keys {sorted(vocabularies)} != expected units {sorted(expected_units)}"
        )

    frames: list[pl.DataFrame] = []
    for unit in layout.embedding_unit_names():
        matrix = embedding_matrices[unit]
        if matrix.ndim != 2:
            raise ValueError(
                f"embedding matrix for {unit!r} must be 2-d; got shape {matrix.shape}"
            )
        entity_ids = vocabulary_entity_ids(vocabularies[unit])
        if matrix.shape[0] != len(entity_ids):
            raise ValueError(
                f"embedding matrix rows for {unit!r}: got {matrix.shape[0]} but vocabulary expects {len(entity_ids)}"
            )
        frames.append(
            pl.DataFrame(
                {
                    "entity_type": pl.Series([unit] * len(entity_ids), dtype=pl.Utf8),
                    "entity_id": pl.Series(entity_ids, dtype=pl.Utf8),
                    "embedding_value": pl.Series(
                        "embedding_value", matrix, dtype=pl.List(pl.Float64)
                    ),
                }
            )
        )
    if not frames:
        return pl.DataFrame(schema=EMBEDDING_SCHEMA)
    return pl.concat(frames, how="vertical_relaxed")


def write_embeddings_parquet(
    target_name: str,
    artifact_id: str,
    *,
    df: pl.DataFrame,
    artifact_root: Path,
) -> Path:
    if df.height > 0:
        if "entity_type" not in df.columns or "entity_id" not in df.columns:
            raise ValueError(
                "embeddings frame must carry entity_type + entity_id columns"
            )
    exports = deep_exports_dir(target_name, artifact_id, root=artifact_root)
    exports.mkdir(parents=True, exist_ok=True)
    path = exports / "embeddings.parquet"
    df.write_parquet(path, compression="zstd")
    return path


def empty_embeddings_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=EMBEDDING_SCHEMA)


def iterate_published_embedding_frames() -> Iterator[pl.DataFrame]:
    """Yield one embeddings frame per published target that exports embeddings.

    Each yielded frame is the ``embeddings.parquet`` contents from a
    target whose manifest's ``output_paths`` includes an ``embeddings``
    entry. The ``dl_embedding_artifact`` SQLMesh ``@model`` consumes
    this stream and persists a single concatenated table.
    """
    from python_models.statistical.deep.registry import (
        all_target_names,
        get_target,
    )
    from python_models.statistical.manifests import (
        find_published_manifest,
    )
    from python_models.statistical.deep.manifest_ingest import (
        read_published_target_manifest,
    )

    for name in all_target_names():
        spec = get_target(name)
        pointer_path = find_published_manifest(spec.published_manifest_name())
        if pointer_path is None:
            continue
        _, manifest = read_published_target_manifest(
            pointer_path, target_name=spec.name
        )
        embeddings_path = manifest.output_paths.get("embeddings")
        if embeddings_path is None:
            continue
        df = pl.read_parquet(str(embeddings_path))
        if df.height == 0:
            continue
        _log.info(
            "iterate_published_embedding_frames: %d rows from %s",
            df.height,
            spec.name,
        )
        yield df.with_columns(
            pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("dl_artifact_id"),
            pl.lit(spec.name, dtype=pl.Utf8).alias("source_target"),
        )


def aggregate_embedding_frames() -> Iterator[pl.DataFrame]:
    emitted = False
    for frame in iterate_published_embedding_frames():
        emitted = True
        yield frame
    if not emitted:
        _log.info(
            "aggregate_embedding_frames: no embeddings published yet; yielding empty frame"
        )
        empty = empty_embeddings_frame().with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("dl_artifact_id"),
            pl.lit(None, dtype=pl.Utf8).alias("source_target"),
        )
        yield empty
