"""Is-in-play binary model — thin wrapper around `model_factory`."""

from __future__ import annotations

import python_models.ml  # noqa: F401  # set KERAS_BACKEND before keras import

import keras

from python_models.ml.features import IS_IN_PLAY_BIN
from python_models.ml.model_factory import build_model as _build_model


def build_model(
    *,
    vocab_sizes: dict[str, int],
    numeric_means: dict[str, float],
    numeric_variances: dict[str, float],
) -> keras.Model:
    return _build_model(
        target_spec=IS_IN_PLAY_BIN,
        vocab_sizes=vocab_sizes,
        numeric_means=numeric_means,
        numeric_variances=numeric_variances,
        num_classes=1,
    )
