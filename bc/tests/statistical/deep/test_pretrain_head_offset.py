"""Pretrain head offset construction: forward equivalence + back-compat."""

from __future__ import annotations

import numpy as np
import pytest

import python_models.statistical.deep  # noqa: F401  # KERAS_BACKEND=torch


def _np_softmax(x: np.ndarray) -> np.ndarray:
    m = x.max(axis=-1, keepdims=True)
    e = np.exp(x - m)
    return e / e.sum(axis=-1, keepdims=True)

from python_models.ml.features import FeatureLayout
from python_models.ml.model_factory import PretrainHeadBuild, build_pretrain_model


def _toy_layout() -> FeatureLayout:
    return FeatureLayout(
        high_card_columns=(),
        low_card_columns=("flag",),
        numeric_columns=("x",),
        grain_column="event_key",
        split_column="primary_fold",
        embedding_groups=(),
    )


def _toy_inputs(n: int = 8) -> dict[str, np.ndarray]:
    rng = np.random.default_rng(0)
    return {
        "flag": rng.integers(low=0, high=3, size=(n, 1), dtype=np.int64),
        "x": rng.standard_normal(size=(n, 1)).astype(np.float32),
    }


def test_offset_path_equals_softmax_of_sum() -> None:
    import keras

    layout = _toy_layout()
    head = PretrainHeadBuild(name="toy", kind="multiclass", num_classes=4)
    keras.utils.set_random_seed(7)
    offset_input = keras.Input(shape=(4,), dtype="float32", name="offset_toy")
    model = build_pretrain_model(
        layout=layout,
        vocab_sizes={"flag": 3},
        numeric_means={"x": 0.0},
        numeric_variances={"x": 1.0},
        head_builds=(head,),
        offset_inputs={"toy": offset_input},
        steps_per_epoch=1,
        total_epochs=1,
    )

    n = 8
    feat_x = _toy_inputs(n)
    rng = np.random.default_rng(1)
    offset = rng.standard_normal(size=(n, 4)).astype(np.float32)

    full_x = {**feat_x, "offset_toy": offset}
    y_with_offset = model.predict(full_x, verbose="0")["toy"]

    logits_layer = model.get_layer("toy_logits")
    logits_only_model = keras.Model(
        inputs=model.inputs,
        outputs=logits_layer.output,
        name="logits_only",
    )
    zero_offset = np.zeros_like(offset)
    raw_logits = logits_only_model.predict(
        {**feat_x, "offset_toy": zero_offset}, verbose="0"
    )

    expected_np = _np_softmax(np.asarray(raw_logits) + offset)
    np.testing.assert_allclose(y_with_offset, expected_np, atol=1e-5)


def test_back_compat_no_offset_keeps_logits_layer() -> None:
    import keras

    layout = _toy_layout()
    head = PretrainHeadBuild(name="toy", kind="multiclass", num_classes=4)
    keras.utils.set_random_seed(7)
    model = build_pretrain_model(
        layout=layout,
        vocab_sizes={"flag": 3},
        numeric_means={"x": 0.0},
        numeric_variances={"x": 1.0},
        head_builds=(head,),
        steps_per_epoch=1,
        total_epochs=1,
    )

    logits_layer = model.get_layer("toy_logits")
    assert logits_layer is not None

    feat_x = _toy_inputs(8)
    probs = model.predict(feat_x, verbose="0")["toy"]
    assert probs.shape == (8, 4)
    np.testing.assert_allclose(probs.sum(axis=-1), np.ones(8), atol=1e-5)

    logits_only_model = keras.Model(
        inputs=model.inputs, outputs=logits_layer.output, name="lo"
    )
    raw = logits_only_model.predict(feat_x, verbose="0")
    expected = _np_softmax(np.asarray(raw))
    np.testing.assert_allclose(probs, expected, atol=1e-5)


def test_offset_input_has_no_trainable_params() -> None:
    import keras

    layout = _toy_layout()
    head = PretrainHeadBuild(name="toy", kind="multiclass", num_classes=4)
    keras.utils.set_random_seed(7)
    offset_input = keras.Input(shape=(4,), dtype="float32", name="offset_toy")
    model = build_pretrain_model(
        layout=layout,
        vocab_sizes={"flag": 3},
        numeric_means={"x": 0.0},
        numeric_variances={"x": 1.0},
        head_builds=(head,),
        offset_inputs={"toy": offset_input},
        steps_per_epoch=1,
        total_epochs=1,
    )

    sum_layer = model.get_layer("toy_sum")
    assert sum_layer.count_params() == 0

    dense_layer = model.get_layer("toy_logits")
    assert dense_layer.count_params() > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
