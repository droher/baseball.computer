"""Category map generation + unseen-policy behavior."""

from __future__ import annotations

import pytest

from python_models.statistical.datasets import build_category_map, encode_with_map


def test_build_category_map_dense_codes() -> None:
    mapping = build_category_map(["b", "a", "c", "a", None, "b"])
    assert mapping == {"a": 0, "b": 1, "c": 2}


def test_build_category_map_stable_across_calls() -> None:
    first = build_category_map(["x", "y", "z"])
    second = build_category_map(["z", "y", "x"])
    assert first == second


def test_encode_with_map_round_trip_keeps_nulls() -> None:
    mapping = {"a": 0, "b": 1}
    encoded = encode_with_map(["a", None, "b", "a"], mapping, unseen_policy="error")
    assert encoded == [0, None, 1, 0]


def test_encode_with_map_unseen_error() -> None:
    mapping = {"a": 0}
    with pytest.raises(ValueError):
        _ = encode_with_map(["a", "b"], mapping, unseen_policy="error")


def test_encode_with_map_unseen_null() -> None:
    mapping = {"a": 0}
    encoded = encode_with_map(["a", "b"], mapping, unseen_policy="null")
    assert encoded == [0, None]
    assert "b" not in mapping


def test_encode_with_map_unseen_add_extends_mapping() -> None:
    mapping = {"a": 0}
    encoded = encode_with_map(["a", "b", "c", "b"], mapping, unseen_policy="add")
    assert encoded == [0, 1, 2, 1]
    assert mapping == {"a": 0, "b": 1, "c": 2}
