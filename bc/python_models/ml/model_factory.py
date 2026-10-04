"""Generic Keras model factory for ML targets.

One trunk (per-categorical embeddings, one-hot for low-card, normalized
numerics, two dense ReLU blocks) terminates in a target-specific output
layer chosen from the `TargetSpec.kind`.
"""

from __future__ import annotations

import python_models.ml  # noqa: F401  # set KERAS_BACKEND before keras import

from dataclasses import dataclass
from typing import Any

import keras
from keras import layers

from python_models.ml.features import (
    HIGH_CARD_CATEGORICAL,
    LOW_CARD_CATEGORICAL,
    NUMERIC,
    TargetSpec,
)


def _embedding_dim(vocab_size: int) -> int:
    return min(64, int(round(vocab_size**0.5)) + 1)


@dataclass(frozen=True)
class _Head:
    output: keras.KerasTensor
    loss: keras.losses.Loss
    metrics: tuple[keras.metrics.Metric, ...]


def _make_outputs_layer(
    trunk: keras.KerasTensor, target_spec: TargetSpec, num_classes: int
) -> _Head:
    if target_spec.kind == "multiclass":
        out = layers.Dense(
            num_classes, activation="softmax", name=target_spec.name
        )(trunk)
        return _Head(
            output=out,
            loss=keras.losses.SparseCategoricalCrossentropy(),
            metrics=(keras.metrics.SparseCategoricalAccuracy(),),
        )
    if target_spec.kind == "binary":
        out = layers.Dense(1, activation="sigmoid", name=target_spec.name)(trunk)
        return _Head(
            output=out,
            loss=keras.losses.BinaryCrossentropy(),
            metrics=(keras.metrics.BinaryAccuracy(),),
        )
    if target_spec.kind == "regression":
        out = layers.Dense(1, activation="linear", name=target_spec.name)(trunk)
        return _Head(
            output=out,
            loss=keras.losses.MeanSquaredError(),
            metrics=(
                keras.metrics.MeanSquaredError(name="mse"),
                keras.metrics.MeanAbsoluteError(name="mae"),
            ),
        )
    raise ValueError(f"unsupported target kind {target_spec.kind!r}")


def build_model(
    *,
    target_spec: TargetSpec,
    vocab_sizes: dict[str, int],
    numeric_means: dict[str, float],
    numeric_variances: dict[str, float],
    num_classes: int,
) -> keras.Model:
    # `num_classes` is only consulted for the multiclass head; the
    # binary head always emits a single sigmoid scalar. Callers should
    # still pass `num_classes=1` for binary so the type contract is
    # explicit.
    inputs: dict[str, keras.KerasTensor] = {}
    branches: list[keras.KerasTensor] = []

    for col in HIGH_CARD_CATEGORICAL:
        size = vocab_sizes[col]
        inp = keras.Input(shape=(1,), dtype="int64", name=col)
        emb = layers.Embedding(
            input_dim=size,
            output_dim=_embedding_dim(size),
            name=f"embed_{col}",
        )(inp)
        branches.append(layers.Flatten(name=f"flatten_{col}")(emb))
        inputs[col] = inp

    for col in LOW_CARD_CATEGORICAL:
        size = vocab_sizes[col]
        inp = keras.Input(shape=(1,), dtype="int64", name=col)
        one_hot = layers.CategoryEncoding(
            num_tokens=size,
            output_mode="one_hot",
            name=f"onehot_{col}",
        )(inp)
        branches.append(layers.Flatten(name=f"flatten_{col}")(one_hot))
        inputs[col] = inp

    for col in NUMERIC:
        inp = keras.Input(shape=(1,), dtype="float32", name=col)
        norm = layers.Normalization(
            mean=numeric_means[col],
            variance=numeric_variances[col],
            name=f"norm_{col}",
        )(inp)
        branches.append(norm)
        inputs[col] = inp

    x = layers.Concatenate(name="concat")(branches)
    x = layers.Dense(128, activation="relu", name="dense_1")(x)
    x = layers.Dropout(0.2, name="dropout_1")(x)
    x = layers.Dense(64, activation="relu", name="dense_2")(x)

    head = _make_outputs_layer(x, target_spec, num_classes)

    model = keras.Model(inputs=inputs, outputs=head.output, name=target_spec.name)
    compile_kwargs: dict[str, Any] = {
        "optimizer": keras.optimizers.Adam(learning_rate=1e-3),
        "loss": head.loss,
        "weighted_metrics": list(head.metrics),
    }
    model.compile(**compile_kwargs)
    return model
