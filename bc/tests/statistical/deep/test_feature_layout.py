"""Coverage FeatureLayout registry behavior."""

from __future__ import annotations

import importlib

import pytest

from python_models.ml.features import FeatureLayout
from python_models.statistical.deep import feature_layout


@pytest.fixture(autouse=True)
def _reset_registry():
    snapshot = dict(feature_layout._REGISTRY)
    feature_layout._REGISTRY.clear()
    yield
    feature_layout._REGISTRY.clear()
    feature_layout._REGISTRY.update(snapshot)


def _layout() -> FeatureLayout:
    return FeatureLayout(
        high_card_columns=("player_id",),
        low_card_columns=("league",),
        numeric_columns=("season",),
        grain_column="event_key",
        split_column="split",
    )


def test_register_then_lookup_roundtrip() -> None:
    feature_layout.register_coverage_layout("model_input_geometry", _layout())
    looked_up = feature_layout.coverage_layout_for("model_input_geometry")
    assert looked_up.high_card_columns == ("player_id",)
    assert looked_up.grain_column == "event_key"


def test_register_is_idempotent_overwrite() -> None:
    feature_layout.register_coverage_layout("model_input_geometry", _layout())
    alt = FeatureLayout(
        high_card_columns=("alt",),
        low_card_columns=(),
        numeric_columns=(),
        grain_column="event_key",
        split_column="split",
    )
    feature_layout.register_coverage_layout("model_input_geometry", alt)
    assert feature_layout.coverage_layout_for("model_input_geometry").high_card_columns == ("alt",)


def test_unregistered_raises_keyerror() -> None:
    with pytest.raises(KeyError):
        _ = feature_layout.coverage_layout_for("model_input_geometry")


def test_registered_datasets_sorted() -> None:
    feature_layout.register_coverage_layout("b_dataset", _layout())
    feature_layout.register_coverage_layout("a_dataset", _layout())
    assert feature_layout.registered_datasets() == ("a_dataset", "b_dataset")


def test_re_export_is_same_class() -> None:
    """feature_layout.FeatureLayout must be the same class as ml.features.FeatureLayout."""
    mod = importlib.import_module("python_models.statistical.deep.feature_layout")
    assert mod.FeatureLayout is FeatureLayout
