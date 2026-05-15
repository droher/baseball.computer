"""Embedding extraction + export round-trip."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.ml.features import FeatureLayout, Vocabulary
from python_models.statistical.deep.embeddings import (
    EMBEDDING_SCHEMA,
    assemble_embeddings_frame,
    empty_embeddings_frame,
    vocabulary_entity_ids,
    write_embeddings_parquet,
)


def _layout() -> FeatureLayout:
    return FeatureLayout(
        high_card_columns=("batter_id", "park_id"),
        low_card_columns=("league",),
        numeric_columns=("season",),
        grain_column="event_key",
        split_column="primary_fold",
    )


def _vocabs() -> dict[str, Vocabulary]:
    return {
        "batter_id": Vocabulary(column="batter_id", values=("alice", "bob")),
        "park_id": Vocabulary(column="park_id", values=("CHI01",)),
    }


def _matrices() -> dict[str, np.ndarray]:
    return {
        "batter_id": np.array(
            [[0.0, 0.0], [0.1, 0.2], [0.3, 0.4]], dtype=np.float64
        ),
        "park_id": np.array([[0.0, 0.0], [0.5, 0.6]], dtype=np.float64),
    }


def test_vocabulary_entity_ids_prefixes_oov() -> None:
    vocab = Vocabulary(column="x", values=("a", "b"))
    assert vocabulary_entity_ids(vocab) == ["<oov>", "a", "b"]


def test_assemble_embeddings_frame_shape() -> None:
    df = assemble_embeddings_frame(
        layout=_layout(),
        embedding_matrices=_matrices(),
        vocabularies=_vocabs(),
    )
    assert df.height == 5
    assert df.columns == ["entity_type", "entity_id", "embedding_value"]
    assert set(df["entity_type"].unique().to_list()) == {"batter_id", "park_id"}


def test_assemble_embeddings_includes_oov_row_per_column() -> None:
    df = assemble_embeddings_frame(
        layout=_layout(),
        embedding_matrices=_matrices(),
        vocabularies=_vocabs(),
    )
    oov_per_type = (
        df.filter(pl.col("entity_id") == "<oov>")
        .group_by("entity_type")
        .len()
    )
    assert oov_per_type.height == 2


def test_assemble_rejects_mismatched_columns() -> None:
    with pytest.raises(KeyError, match="embedding_matrices keys"):
        _ = assemble_embeddings_frame(
            layout=_layout(),
            embedding_matrices={"batter_id": _matrices()["batter_id"]},
            vocabularies=_vocabs(),
        )


def test_assemble_rejects_wrong_row_count() -> None:
    bad = dict(_matrices())
    bad["batter_id"] = bad["batter_id"][:2]
    with pytest.raises(ValueError, match="rows for 'batter_id'"):
        _ = assemble_embeddings_frame(
            layout=_layout(),
            embedding_matrices=bad,
            vocabularies=_vocabs(),
        )


def test_assemble_rejects_1d_matrix() -> None:
    bad = {**_matrices(), "park_id": np.array([0.5, 0.6])}
    with pytest.raises(ValueError, match="must be 2-d"):
        _ = assemble_embeddings_frame(
            layout=_layout(),
            embedding_matrices=bad,
            vocabularies=_vocabs(),
        )


def test_write_embeddings_parquet_round_trip(tmp_path: Path) -> None:
    df = assemble_embeddings_frame(
        layout=_layout(),
        embedding_matrices=_matrices(),
        vocabularies=_vocabs(),
    )
    path = write_embeddings_parquet(
        "target_x", "aid-1", df=df, artifact_root=tmp_path / "deep"
    )
    assert path.exists()
    read = pl.read_parquet(path)
    assert read.height == 5
    assert set(read.columns) == {"entity_type", "entity_id", "embedding_value"}


def test_empty_embeddings_frame_typed() -> None:
    empty = empty_embeddings_frame()
    assert empty.height == 0
    assert empty.schema == EMBEDDING_SCHEMA
