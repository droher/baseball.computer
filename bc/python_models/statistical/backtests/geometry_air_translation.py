from __future__ import annotations

from collections import defaultdict
from typing import Literal, Self, cast

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict, Field, model_validator

AIR_CLASSES = ("Fly", "LineDrive", "PopUp")
TRAJECTORY_CLASSES = ("GroundBall", *AIR_CLASSES)
AirClass = Literal["Fly", "LineDrive", "PopUp"]
Predictor = Literal["marginal", "result", "recorded", "recorded_result"]
FloatArray = npt.NDArray[np.float64]


class AirCountCell(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    recorded_subtype: AirClass
    result_family: str = Field(min_length=1)
    counts: tuple[int, int, int]

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if min(self.counts) < 0 or sum(self.counts) == 0:
            raise ValueError("count cells must be nonnegative and nonempty")
        return self


class AirTranslationFit(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    model_type: Literal["pooled_air_translation_v1"] = "pooled_air_translation_v1"
    target_contract: Literal["trajectory-air-standard-v1"] = (
        "trajectory-air-standard-v1"
    )
    class_labels: tuple[str, ...] = AIR_CLASSES
    prior_strength: float = Field(default=30.0, gt=0, allow_inf_nan=False)
    cells: tuple[AirCountCell, ...]
    training_events: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_fit(self) -> Self:
        keys = [(cell.recorded_subtype, cell.result_family) for cell in self.cells]
        if self.class_labels != AIR_CLASSES:
            raise ValueError("unexpected airborne class order")
        if len(set(keys)) != len(keys) or keys != sorted(keys):
            raise ValueError("count cells must have unique sorted keys")
        if sum(sum(cell.counts) for cell in self.cells) != self.training_events:
            raise ValueError("count cells do not conserve training events")
        return self


def fit_air_translation(
    frame: pl.DataFrame, *, prior_strength: float = 30.0
) -> AirTranslationFit:
    required = (
        "event_key",
        "recorded_air_subtype",
        "result_family",
        "target_class",
        "known_air_evaluation_eligible",
    )
    if not set(required) <= set(frame.columns) or frame.is_empty():
        raise ValueError("nonempty eligible airborne training frame required")
    if any(frame[column].null_count() for column in required):
        raise ValueError("training inputs must not be null")
    if (
        frame["event_key"].n_unique() != frame.height
        or frame["known_air_evaluation_eligible"].dtype != pl.Boolean
        or not frame["known_air_evaluation_eligible"].all()
    ):
        raise ValueError("training rows must be unique and eligible")
    for column in ("recorded_air_subtype", "target_class"):
        if not set(frame[column].to_list()) <= set(AIR_CLASSES):
            raise ValueError(f"invalid airborne class in {column}")
    counts: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0, 0])
    grouped = frame.group_by(
        "recorded_air_subtype", "result_family", "target_class"
    ).len()
    for recorded, result, target, count in grouped.iter_rows():
        counts[(str(recorded), str(result))][AIR_CLASSES.index(str(target))] += int(
            count
        )
    cells = tuple(
        AirCountCell(
            recorded_subtype=cast(AirClass, recorded),
            result_family=result,
            counts=(values[0], values[1], values[2]),
        )
        for (recorded, result), values in sorted(counts.items())
    )
    return AirTranslationFit(
        prior_strength=prior_strength, cells=cells, training_events=frame.height
    )


def predict_air_translation(
    fit: AirTranslationFit,
    frame: pl.DataFrame,
    *,
    predictor: Predictor = "recorded_result",
) -> FloatArray:
    if predictor not in {"marginal", "result", "recorded", "recorded_result"}:
        raise ValueError("unknown predictor")
    if not {"recorded_air_subtype", "result_family"} <= set(frame.columns):
        raise ValueError("prediction inputs are incomplete")
    if frame["result_family"].null_count():
        raise ValueError("result family must use an explicit unknown level")
    if not set(frame["recorded_air_subtype"].to_list()) <= {*AIR_CLASSES, None}:
        raise ValueError("prediction contains an unknown recorded airborne subtype")
    global_counts = np.zeros(3, dtype=np.float64)
    by_recorded: dict[str, FloatArray] = {}
    by_result: dict[str, FloatArray] = {}
    by_pair: dict[tuple[str, str], FloatArray] = {}
    for cell in fit.cells:
        values = np.asarray(cell.counts, dtype=np.float64)
        global_counts += values
        by_pair[(cell.recorded_subtype, cell.result_family)] = values
        by_recorded.setdefault(cell.recorded_subtype, np.zeros(3))[:] += values
        by_result.setdefault(cell.result_family, np.zeros(3))[:] += values
    marginal = (global_counts + 1.0) / (global_counts.sum() + 3.0)

    def pooled(values: FloatArray | None, prior: FloatArray) -> FloatArray:
        if values is None:
            return prior
        return (values + fit.prior_strength * prior) / (
            values.sum() + fit.prior_strength
        )

    result_prob = {key: pooled(values, marginal) for key, values in by_result.items()}
    recorded_prob = {
        key: pooled(values, marginal) for key, values in by_recorded.items()
    }
    pair_prob = {
        key: pooled(values, recorded_prob[key[0]]) for key, values in by_pair.items()
    }
    output = np.empty((frame.height, 3), dtype=np.float64)
    for index, (recorded, result) in enumerate(
        frame.select("recorded_air_subtype", "result_family").iter_rows()
    ):
        result_key = str(result)
        recorded_key = str(recorded) if recorded is not None else None
        if predictor == "marginal":
            probability = marginal
        elif predictor == "result":
            probability = result_prob.get(result_key, marginal)
        elif predictor == "recorded":
            probability = recorded_prob.get(str(recorded_key), marginal)
        elif recorded_key is None:
            probability = result_prob.get(result_key, marginal)
        else:
            probability = pair_prob.get(
                (recorded_key, result_key),
                recorded_prob.get(recorded_key, marginal),
            )
        output[index] = probability
    return output


def predict_preserving_broad_type(
    fit: AirTranslationFit, frame: pl.DataFrame
) -> pl.DataFrame:
    if not {"event_key", "recorded_broad_type"} <= set(frame.columns):
        raise ValueError("local recorded broad type is required")
    if frame["event_key"].null_count() or frame["event_key"].n_unique() != frame.height:
        raise ValueError("prediction events must be non-null and unique")
    broad_values = frame["recorded_broad_type"].to_list()
    if not set(broad_values) <= {"Ground", "Air", None}:
        raise ValueError("invalid recorded broad type")
    airborne = frame.filter(pl.col("recorded_broad_type") == "Air")
    probabilities = iter(
        predict_air_translation(fit, airborne).tolist() if airborne.height else []
    )
    output: list[list[float] | None] = []
    statuses: list[str] = []
    for broad in broad_values:
        if broad == "Ground":
            output.append([1.0, 0.0, 0.0, 0.0])
            statuses.append("ground_preserved")
        elif broad == "Air":
            output.append([0.0, *next(probabilities)])
            statuses.append("air_estimated")
        else:
            output.append(None)
            statuses.append("broad_unknown_unsupported")
    return frame.select("event_key").with_columns(
        pl.Series("probabilities", output, dtype=pl.List(pl.Float64)),
        pl.Series("prediction_status", statuses, dtype=pl.String),
    )
