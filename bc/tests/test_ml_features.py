"""Unit tests for the Phase 6 ML feature schema + vocabulary helpers."""

from __future__ import annotations

import polars as pl

from python_models.ml.features import (
    ALL_FEATURE_COLUMNS,
    HIGH_CARD_CATEGORICAL,
    LOW_CARD_CATEGORICAL,
    NUMERIC,
    SAMPLE_WEIGHT_COLUMN,
    SPLIT_COLUMN,
    TARGET_COLUMN,
    Vocabulary,
    build_vocabulary,
    target_class_labels,
)


def test_feature_columns_are_partitioned() -> None:
    """High-card, low-card, and numeric sets must be disjoint and cover ALL_FEATURE_COLUMNS."""
    high = set(HIGH_CARD_CATEGORICAL)
    low = set(LOW_CARD_CATEGORICAL)
    numeric = set(NUMERIC)
    assert high.isdisjoint(low)
    assert high.isdisjoint(numeric)
    assert low.isdisjoint(numeric)
    assert high | low | numeric == set(ALL_FEATURE_COLUMNS)


def test_target_and_weight_columns_distinct() -> None:
    assert TARGET_COLUMN not in ALL_FEATURE_COLUMNS
    assert SAMPLE_WEIGHT_COLUMN not in ALL_FEATURE_COLUMNS
    assert SPLIT_COLUMN not in ALL_FEATURE_COLUMNS


def test_vocabulary_size_includes_oov_slot() -> None:
    vocab = Vocabulary(column="x", values=("a", "b", "c"))
    # 3 known values + 1 OOV slot at index 0
    assert vocab.size == 4


def test_build_vocabulary_skips_nulls_and_sorts() -> None:
    series = pl.Series("x", ["b", None, "a", "c", "a"], dtype=pl.Utf8)
    vocab = build_vocabulary(series, "x")
    assert vocab.values == ("a", "b", "c")


def test_vocabulary_encode_assigns_oov_zero() -> None:
    vocab = Vocabulary(column="x", values=("a", "b", "c"))
    encoded = vocab.encode(pl.Series("x", ["a", "b", "z", None, "c"]))
    # known values map to 1..N preserving vocab order; unknown → 0; null → 0
    assert encoded.to_list() == [1, 2, 0, 0, 3]


def test_vocabulary_encode_roundtrips_full_vocab() -> None:
    vocab = Vocabulary(column="x", values=tuple("abcdefghij"))
    encoded = vocab.encode(pl.Series("x", list("abcdefghij")))
    assert encoded.to_list() == list(range(1, 11))


def test_target_class_labels_sorted_and_unique() -> None:
    series = pl.Series("y", ["B", "A", "B", None, "C"], dtype=pl.Utf8)
    assert target_class_labels(series) == ("A", "B", "C")
