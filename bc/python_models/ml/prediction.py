"""Inference entry point for Phase 6 ML targets.

Loaded by SQLMesh `@model` wrappers under
`bc/models/intermediate/machine_learning/`. Reads the pin JSON,
materializes the MLflow model + vocabularies, and exposes a
`Scorer.score(features) -> pl.DataFrame` that runs in O(batch) memory.

Schema of the returned frame depends on `Scorer.kind`:
- `multiclass`: `{event_key, predicted_class, predicted_class_proba,
   model_run_id}`. `predicted_class` is the argmax label.
- `binary`: `{event_key, predicted_class_bin, predicted_proba,
   model_run_id}`. `predicted_class_bin` is `1` when the sigmoid prob
  is >= 0.5, else `0`. `predicted_proba` is the raw sigmoid output —
  i.e. the probability of the positive class.
- `regression`: `{event_key, predicted_value, model_run_id}`.
  `predicted_value` is the raw linear output of the regression head.
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
    TargetKind,
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
    kind: TargetKind = "multiclass"

    def _empty_frame(self) -> pl.DataFrame:
        if self.kind == "binary":
            return pl.DataFrame(
                schema={
                    GRAIN_COLUMN: pl.UInt32,
                    "predicted_class_bin": pl.UInt8,
                    "predicted_proba": pl.Float64,
                    "model_run_id": pl.Utf8,
                }
            )
        if self.kind == "regression":
            return pl.DataFrame(
                schema={
                    GRAIN_COLUMN: pl.UInt32,
                    "predicted_value": pl.Float64,
                    "model_run_id": pl.Utf8,
                }
            )
        return pl.DataFrame(
            schema={
                GRAIN_COLUMN: pl.UInt32,
                "predicted_class": pl.Utf8,
                "predicted_class_proba": pl.Float64,
                "model_run_id": pl.Utf8,
            }
        )

    def _encode_features(self, features: pl.DataFrame) -> dict[str, NDArray[np.int64] | NDArray[np.float32]]:
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
        return inputs

    def score(self, features: pl.DataFrame) -> pl.DataFrame:
        if features.height == 0:
            return self._empty_frame()

        inputs = self._encode_features(features)
        raw = np.asarray(self.model.predict(inputs, batch_size=8192, verbose=0))

        if self.kind == "binary":
            proba = raw.reshape(-1).astype(np.float64)
            predicted = (proba >= 0.5).astype(np.uint8)
            return pl.DataFrame(
                {
                    GRAIN_COLUMN: features[GRAIN_COLUMN],
                    "predicted_class_bin": pl.Series(predicted, dtype=pl.UInt8),
                    "predicted_proba": pl.Series(proba, dtype=pl.Float64),
                    "model_run_id": pl.Series(
                        [self.run_id] * features.height, dtype=pl.Utf8
                    ),
                }
            )

        if self.kind == "regression":
            value = raw.reshape(-1).astype(np.float64)
            return pl.DataFrame(
                {
                    GRAIN_COLUMN: features[GRAIN_COLUMN],
                    "predicted_value": pl.Series(value, dtype=pl.Float64),
                    "model_run_id": pl.Series(
                        [self.run_id] * features.height, dtype=pl.Utf8
                    ),
                }
            )

        argmax = raw.argmax(axis=1)
        max_proba = raw.max(axis=1)
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


def load_scorer(target: str) -> Scorer:
    pin_path = PIN_DIR / f"{target}.json"
    payload = json.loads(pin_path.read_text())
    tracking_uri = str(payload["tracking_uri"])
    run_id = str(payload["run_id"])
    class_labels = tuple(str(c) for c in payload["class_labels"])
    kind: TargetKind = payload.get("kind", "multiclass")

    mlflow.set_tracking_uri(tracking_uri)
    model = mlflow.keras.load_model(f"runs:/{run_id}/model")

    vocabularies: dict[str, Vocabulary] = {}
    for col, path in payload["vocab_paths"].items():
        df = pl.read_parquet(path)
        values: list[str] = df["value"].cast(pl.Utf8).to_list()
        vocabularies[col] = Vocabulary(column=col, values=tuple(values))

    _log.info("loaded scorer for %s (run %s, kind %s)", target, run_id, kind)
    return Scorer(
        run_id=run_id,
        model=model,
        vocabularies=vocabularies,
        class_labels=class_labels,
        kind=kind,
    )
