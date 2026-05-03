"""Inference entry point for the plate-appearance-cat model.

Loaded by the SQLMesh `@model` wrapper at
`bc/models/intermediate/machine_learning/predictions_plate_appearance_cat.py`.
Reads the pin JSON, materializes the MLflow model + vocabularies, and
exposes a `Scorer.score(features) -> pl.DataFrame` that runs in O(batch)
memory.
"""

from __future__ import annotations

import python_models.ml  # noqa: F401  # set KERAS_BACKEND before keras import

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import keras
import mlflow
import numpy as np
import polars as pl
from numpy.typing import NDArray

from python_models.ml.features import (
    GRAIN_COLUMN,
    HIGH_CARD_CATEGORICAL,
    LOW_CARD_CATEGORICAL,
    NUMERIC,
    Vocabulary,
)

_log = logging.getLogger(__name__)

PIN_DIR = Path(__file__).resolve().parent / "artifacts"


@dataclass(frozen=True)
class Scorer:
    run_id: str
    model: keras.Model
    vocabularies: dict[str, Vocabulary]
    class_labels: tuple[str, ...]

    def score(self, features: pl.DataFrame) -> pl.DataFrame:
        if features.height == 0:
            return pl.DataFrame(
                schema={
                    GRAIN_COLUMN: pl.UInt32,
                    "predicted_class": pl.Utf8,
                    "predicted_class_proba": pl.Float64,
                    "model_run_id": pl.Utf8,
                }
            )

        inputs: dict[str, NDArray[np.int64] | NDArray[np.float32]] = {}
        for col in (*HIGH_CARD_CATEGORICAL, *LOW_CARD_CATEGORICAL):
            encoded = self.vocabularies[col].encode(features[col]).to_numpy()
            inputs[col] = encoded.astype(np.int64).reshape(-1, 1)
        for col in NUMERIC:
            inputs[col] = (
                features[col]
                .cast(pl.Float32)
                .fill_null(0.0)
                .to_numpy()
                .reshape(-1, 1)
            )

        probs = np.asarray(
            self.model.predict(inputs, batch_size=8192, verbose=0)
        )
        argmax = probs.argmax(axis=1)
        max_proba = probs.max(axis=1)
        labels = np.asarray(self.class_labels, dtype=object)[argmax]

        return pl.DataFrame(
            {
                GRAIN_COLUMN: features[GRAIN_COLUMN],
                "predicted_class": pl.Series(labels, dtype=pl.Utf8),
                "predicted_class_proba": pl.Series(max_proba, dtype=pl.Float64),
                "model_run_id": pl.Series(
                    [self.run_id] * features.height, dtype=pl.Utf8
                ),
            }
        )


def load_scorer(target: str = "plate_appearance_cat") -> Scorer:
    pin_path = PIN_DIR / f"{target}.json"
    payload = json.loads(pin_path.read_text())
    tracking_uri = str(payload["tracking_uri"])
    run_id = str(payload["run_id"])
    class_labels = tuple(str(c) for c in payload["class_labels"])

    mlflow.set_tracking_uri(tracking_uri)
    model = mlflow.keras.load_model(f"runs:/{run_id}/model")

    vocabularies: dict[str, Vocabulary] = {}
    for col, path in payload["vocab_paths"].items():
        df = pl.read_parquet(path)
        values: list[str] = df["value"].cast(pl.Utf8).to_list()
        vocabularies[col] = Vocabulary(column=col, values=tuple(values))

    _log.info("loaded scorer for %s (run %s)", target, run_id)
    return Scorer(
        run_id=run_id,
        model=model,
        vocabularies=vocabularies,
        class_labels=class_labels,
    )
