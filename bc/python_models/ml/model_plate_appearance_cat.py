"""Multiclass Keras model for `outcome_plate_appearance_cat`.

Architecture:
- Embedding layer per high-cardinality categorical (batter, pitcher,
  park, base runners). Embedding dim derived from vocab size.
- IntegerLookup → one-hot for low-cardinality categoricals.
- Numeric features pass through a stateless Normalization layer
  (mean/variance fit on the training set during the warm-up pass).
- Concatenate all branches, two dense ReLU blocks, softmax head.

Inputs/outputs match the generators in `python_models.ml.training_*`
and `python_models.ml.prediction` so train and inference cannot drift.
"""

from __future__ import annotations

import python_models.ml  # noqa: F401  # set KERAS_BACKEND before keras import

import keras
from keras import layers

from python_models.ml.features import (
    HIGH_CARD_CATEGORICAL,
    LOW_CARD_CATEGORICAL,
    NUMERIC,
)


def _embedding_dim(vocab_size: int) -> int:
    return min(64, int(round(vocab_size**0.5)) + 1)


def build_model(
    *,
    vocab_sizes: dict[str, int],
    numeric_means: dict[str, float],
    numeric_variances: dict[str, float],
    num_classes: int,
) -> keras.Model:
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
    output = layers.Dense(
        num_classes, activation="softmax", name="plate_appearance_cat"
    )(x)

    model = keras.Model(inputs=inputs, outputs=output, name="plate_appearance_cat")
    model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=1e-3),
        loss=keras.losses.SparseCategoricalCrossentropy(),
        weighted_metrics=[keras.metrics.SparseCategoricalAccuracy()],
    )
    return model
