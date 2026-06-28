"""Shared-Embedding (embedding_groups) plumbing tests."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from python_models.ml.features import FeatureLayout, Vocabulary
from python_models.statistical.deep.embeddings import assemble_embeddings_frame


def _grouped_layout() -> FeatureLayout:
    return FeatureLayout(
        high_card_columns=(
            "batter_id",
            "pitcher_id",
            "runner_on_1b_id",
            "park_id",
        ),
        low_card_columns=("league",),
        numeric_columns=("season",),
        grain_column="event_key",
        split_column="primary_fold",
        embedding_groups=(
            ("player", ("batter_id", "pitcher_id", "runner_on_1b_id")),
        ),
    )


def test_embedding_groups_unit_lookup() -> None:
    layout = _grouped_layout()
    assert layout.embedding_unit_for_column("batter_id") == "player"
    assert layout.embedding_unit_for_column("pitcher_id") == "player"
    assert layout.embedding_unit_for_column("park_id") == "park_id"
    assert layout.group_for_column("park_id") is None
    assert "batter_id" in layout.grouped_columns()
    assert "park_id" not in layout.grouped_columns()
    assert layout.embedding_unit_names() == ("player", "park_id")


def test_embedding_groups_rejects_unknown_col() -> None:
    with pytest.raises(ValueError, match="not in high_card_columns"):
        _ = FeatureLayout(
            high_card_columns=("batter_id",),
            low_card_columns=(),
            numeric_columns=(),
            grain_column="x",
            split_column="y",
            embedding_groups=(("g", ("not_a_col",)),),
        )


def test_embedding_groups_rejects_double_membership() -> None:
    with pytest.raises(ValueError, match="multiple embedding groups"):
        _ = FeatureLayout(
            high_card_columns=("a", "b"),
            low_card_columns=(),
            numeric_columns=(),
            grain_column="x",
            split_column="y",
            embedding_groups=(("g1", ("a", "b")), ("g2", ("b",))),
        )


def test_assemble_uses_group_units() -> None:
    layout = _grouped_layout()
    vocabularies = {
        "player": Vocabulary(column="player", values=("p1", "p2")),
        "park_id": Vocabulary(column="park_id", values=("PARK_A",)),
    }
    matrices = {
        "player": np.array([[0.0, 0.0], [1.0, 2.0], [3.0, 4.0]], dtype=np.float64),
        "park_id": np.array([[0.0, 0.0], [5.0, 6.0]], dtype=np.float64),
    }
    df = assemble_embeddings_frame(
        layout=layout, embedding_matrices=matrices, vocabularies=vocabularies
    )
    assert set(df["entity_type"].unique().to_list()) == {"player", "park_id"}
    player_rows = df.filter(pl.col("entity_type") == "player")
    assert sorted(player_rows["entity_id"].to_list()) == ["<oov>", "p1", "p2"]
