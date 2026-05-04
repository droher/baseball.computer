"""Plate-appearance-cat model — thin wrapper around `model_factory`.

Kept as its own import path so the existing pin/artifact + tests resolve
cleanly. The actual architecture lives in
`python_models.ml.model_factory`.
"""

from __future__ import annotations

import python_models.ml  # noqa: F401  # set KERAS_BACKEND before keras import

import keras

from python_models.ml.features import PLATE_APPEARANCE_CAT
from python_models.ml.model_factory import build_model as _build_model


def build_model(
    *,
    vocab_sizes: dict[str, int],
    numeric_means: dict[str, float],
    numeric_variances: dict[str, float],
    num_classes: int,
) -> keras.Model:
    return _build_model(
        target_spec=PLATE_APPEARANCE_CAT,
        vocab_sizes=vocab_sizes,
        numeric_means=numeric_means,
        numeric_variances=numeric_variances,
        num_classes=num_classes,
    )
