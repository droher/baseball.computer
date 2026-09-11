from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import hashlib
import json
from typing import Literal, Self, cast

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict, Field, model_validator

from python_models.statistical.backtests.geometry_air_dirichlet import (
    QuadratureConvergenceError,
    posterior_grid,
    posterior_moments,
    sample_posterior,
    validate_quadrature_convergence,
)
from python_models.statistical.backtests.geometry_air_translation import (
    AIR_CLASSES,
    Predictor,
)


FloatArray = npt.NDArray[np.float64]
PredictionStatus = Literal[
    "ground_preserved",
    "posterior_cell",
    "prior_only_cell",
    "broad_unknown_unsupported",
    "regime_unsupported",
    "fine_label_missing_unsupported",
    "fine_label_unknown_unsupported",
    "result_family_missing_unsupported",
]
REGIMES = ("early", "late")
SEASON_REGIME = {2015: "early", 2019: "early", 2023: "late", 2025: "late"}
ARMS: tuple[Predictor, ...] = (
    "marginal",
    "result",
    "recorded",
    "recorded_result",
)
ORDER_PAIRS = ((64, 128), (128, 256), (256, 512))


class MomentConvergence(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    lower_order: int = Field(ge=2)
    higher_order: int = Field(ge=2)
    tolerance: float = Field(gt=0, allow_inf_nan=False)
    global_mean_error: float = Field(ge=0, allow_inf_nan=False)
    global_second_error: float = Field(ge=0, allow_inf_nan=False)
    regime_mean_error: float = Field(ge=0, allow_inf_nan=False)
    regime_second_error: float = Field(ge=0, allow_inf_nan=False)
    maximum_error: float = Field(ge=0, allow_inf_nan=False)
    passed: bool

    @model_validator(mode="after")
    def validate_order_and_status(self) -> Self:
        if self.higher_order <= self.lower_order:
            raise ValueError("higher quadrature order must exceed lower order")
        component_maximum = max(
            self.global_mean_error,
            self.global_second_error,
            self.regime_mean_error,
            self.regime_second_error,
        )
        if not np.isclose(self.maximum_error, component_maximum, rtol=0.0, atol=1e-15):
            raise ValueError("maximum error disagrees with component errors")
        if self.passed != (self.maximum_error <= self.tolerance):
            raise ValueError("quadrature status disagrees with its error")
        return self


class AirRegimeCell(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    values: tuple[str, ...]
    counts: tuple[tuple[int, int, int], tuple[int, int, int]]
    selected_order: int = Field(ge=2)
    convergence: tuple[MomentConvergence, ...] = Field(min_length=1, max_length=3)

    @model_validator(mode="after")
    def validate_cell(self) -> Self:
        flat = [value for regime in self.counts for value in regime]
        if min(flat) < 0 or sum(flat) == 0:
            raise ValueError("fitted cells must contain nonnegative observed counts")
        if not self.convergence[-1].passed:
            raise ValueError("final quadrature comparison must pass")
        if self.selected_order != self.convergence[-1].higher_order:
            raise ValueError("selected order must be the passing higher order")
        if any(item.passed for item in self.convergence[:-1]):
            raise ValueError("quadrature refinement must stop at the first pass")
        expected_pairs = ORDER_PAIRS[: len(self.convergence)]
        actual_pairs = tuple(
            (item.lower_order, item.higher_order) for item in self.convergence
        )
        if actual_pairs != expected_pairs or any(
            item.tolerance != 1e-5 for item in self.convergence
        ):
            raise ValueError("quadrature diagnostics do not follow the fixed schedule")
        return self


class AirRegimeFit(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    model_type: Literal["air_regime_dirichlet_v1"] = "air_regime_dirichlet_v1"
    target_contract: Literal["trajectory-air-standard-v1"] = (
        "trajectory-air-standard-v1"
    )
    experiment_id: str = Field(min_length=1)
    fit_id: str = Field(min_length=1)
    predictor: Predictor
    class_labels: tuple[str, ...] = AIR_CLASSES
    regimes: tuple[str, str] = REGIMES
    concentration: float = Field(gt=0, allow_inf_nan=False)
    draws: int = Field(gt=0)
    seed: int = Field(ge=0)
    cells: tuple[AirRegimeCell, ...]
    represented_regimes: tuple[str, ...]
    training_events: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_fit(self) -> Self:
        if self.class_labels != AIR_CLASSES or self.regimes != REGIMES:
            raise ValueError("unexpected class or regime order")
        expected_width = _cell_width(self.predictor)
        keys = [cell.values for cell in self.cells]
        if any(len(key) != expected_width for key in keys):
            raise ValueError("cell identity does not match predictor arm")
        for key in keys:
            if (
                self.predictor in {"recorded", "recorded_result"}
                and key[0] not in AIR_CLASSES
            ):
                raise ValueError("cell contains an unknown recorded airborne subtype")
            result_index = 0 if self.predictor == "result" else 1
            if (
                self.predictor in {"result", "recorded_result"}
                and not key[result_index]
            ):
                raise ValueError("cell contains an empty result family")
        if len(keys) != len(set(keys)) or keys != sorted(keys):
            raise ValueError("cells must have unique sorted identities")
        if (
            sum(sum(sum(regime) for regime in cell.counts) for cell in self.cells)
            != self.training_events
        ):
            raise ValueError("cell counts do not conserve training events")
        represented = tuple(
            regime
            for index, regime in enumerate(REGIMES)
            if sum(sum(cell.counts[index]) for cell in self.cells) > 0
        )
        if self.represented_regimes != represented:
            raise ValueError("represented regimes disagree with cell counts")
        return self


@dataclass(frozen=True, slots=True)
class SharedCellDraws:
    cell_id: str
    cell_values: tuple[str, ...]
    regime: str
    status: Literal["posterior_cell", "prior_only_cell"]
    probabilities: FloatArray


@dataclass(frozen=True, slots=True)
class AirRegimePrediction:
    events: pl.DataFrame
    cell_draws: tuple[SharedCellDraws, ...]


def _cell_width(predictor: Predictor) -> int:
    return {"marginal": 0, "result": 1, "recorded": 1, "recorded_result": 2}[predictor]


def _cell_values(
    predictor: Predictor, recorded: object, result: object
) -> tuple[str, ...]:
    if predictor == "marginal":
        return ()
    if predictor == "result":
        return (str(result),)
    if predictor == "recorded":
        return (str(recorded),)
    return (str(recorded), str(result))


def _convergence(
    counts: npt.ArrayLike, concentration: float
) -> tuple[int, tuple[MomentConvergence, ...]]:
    diagnostics: list[MomentConvergence] = []
    last_result = None
    for lower, higher in ORDER_PAIRS:
        try:
            result = validate_quadrature_convergence(
                counts, concentration, lower, higher, 1e-5
            )
            passed = True
        except QuadratureConvergenceError as error:
            result = error.report
            passed = False
        last_result = result
        diagnostics.append(
            MomentConvergence(
                lower_order=result.lower_order,
                higher_order=result.higher_order,
                tolerance=result.tolerance,
                global_mean_error=result.global_mean_error,
                global_second_error=result.global_second_error,
                regime_mean_error=result.regime_mean_error,
                regime_second_error=result.regime_second_error,
                maximum_error=result.maximum_error,
                passed=passed,
            )
        )
        if passed:
            return higher, tuple(diagnostics)
    if last_result is None:
        raise RuntimeError("quadrature order schedule is empty")
    raise QuadratureConvergenceError(last_result)


def fit_air_regime(
    frame: pl.DataFrame,
    *,
    predictor: Predictor,
    fit_id: str,
    experiment_id: str = "geometry-air-regime-posterior-v1",
    concentration: float = 30.0,
    draws: int = 4096,
    seed: int = 20260911,
) -> AirRegimeFit:
    required = {
        "event_key",
        "season",
        "recorded_broad_type",
        "recorded_air_subtype",
        "result_family",
        "target_class",
        "known_air_evaluation_eligible",
    }
    if predictor not in ARMS or not required <= set(frame.columns) or frame.is_empty():
        raise ValueError("nonempty eligible airborne training frame required")
    if any(frame[column].null_count() for column in required):
        raise ValueError("training columns must not contain nulls")
    if frame["event_key"].n_unique() != frame.height:
        raise ValueError("training event keys must be unique")
    if (
        frame["known_air_evaluation_eligible"].dtype != pl.Boolean
        or not frame["known_air_evaluation_eligible"].all()
    ):
        raise ValueError("all training rows must be eligible")
    if set(frame["recorded_broad_type"].to_list()) != {"Air"}:
        raise ValueError("training rows must be recorded Air")
    if not set(frame["season"].to_list()) <= set(SEASON_REGIME):
        raise ValueError("training seasons must belong to a declared regime")
    for column in ("recorded_air_subtype", "target_class"):
        if not set(frame[column].to_list()) <= set(AIR_CLASSES):
            raise ValueError(f"invalid airborne class in {column}")
    if any(not isinstance(value, str) or not value for value in frame["result_family"]):
        raise ValueError("result families must be nonempty strings")
    counts: dict[tuple[str, ...], np.ndarray[tuple[int, int], np.dtype[np.int64]]] = (
        defaultdict(lambda: np.zeros((2, 3), dtype=np.int64))
    )
    for season, recorded, result, target in frame.select(
        "season", "recorded_air_subtype", "result_family", "target_class"
    ).iter_rows():
        regime_index = REGIMES.index(SEASON_REGIME[int(season)])
        values = _cell_values(predictor, recorded, result)
        counts[values][regime_index, AIR_CLASSES.index(str(target))] += 1
    cells: list[AirRegimeCell] = []
    for values, matrix in sorted(counts.items()):
        order, diagnostics = _convergence(matrix, concentration)
        cells.append(
            AirRegimeCell(
                values=values,
                counts=cast(
                    tuple[tuple[int, int, int], tuple[int, int, int]],
                    tuple(tuple(int(value) for value in row) for row in matrix),
                ),
                selected_order=order,
                convergence=diagnostics,
            )
        )
    represented = tuple(
        regime
        for index, regime in enumerate(REGIMES)
        if sum(int(matrix[index].sum()) for matrix in counts.values()) > 0
    )
    return AirRegimeFit(
        experiment_id=experiment_id,
        fit_id=fit_id,
        predictor=predictor,
        concentration=concentration,
        draws=draws,
        seed=seed,
        cells=tuple(cells),
        represented_regimes=represented,
        training_events=frame.height,
    )


def cell_identity(fit: AirRegimeFit, values: tuple[str, ...]) -> str:
    payload = json.dumps(
        [fit.experiment_id, fit.fit_id, fit.predictor, fit.concentration, values],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _cell_seed(fit: AirRegimeFit, cell_id: str) -> int:
    payload = json.dumps(
        [
            fit.experiment_id,
            fit.fit_id,
            fit.predictor,
            fit.concentration,
            fit.seed,
            cell_id,
        ],
        separators=(",", ":"),
    )
    return int.from_bytes(hashlib.sha256(payload.encode()).digest()[:8], "big")


def _validated_draws(values: FloatArray, draws: int) -> FloatArray:
    if (
        values.shape != (draws, 2, 3)
        or not np.isfinite(values).all()
        or (values < 0).any()
        or not np.allclose(values.sum(axis=2), 1.0, rtol=0.0, atol=1e-12)
    ):
        raise RuntimeError(
            "posterior cell draws are not finite normalized probabilities"
        )
    output = values.copy()
    output.flags.writeable = False
    return output


def predict_air_regime(fit: AirRegimeFit, frame: pl.DataFrame) -> AirRegimePrediction:
    required = {
        "event_key",
        "season",
        "recorded_broad_type",
        "recorded_air_subtype",
        "result_family",
    }
    if not required <= set(frame.columns):
        raise ValueError("prediction inputs are incomplete")
    if frame["event_key"].null_count() or frame["event_key"].n_unique() != frame.height:
        raise ValueError("prediction event keys must be non-null and unique")
    fitted = {cell.values: cell for cell in fit.cells}
    requested: dict[
        tuple[tuple[str, ...], str], Literal["posterior_cell", "prior_only_cell"]
    ] = {}
    event_rows: list[dict[str, object]] = []
    needs_fine = fit.predictor in {"recorded", "recorded_result"}
    needs_result = fit.predictor in {"result", "recorded_result"}
    for event_key, season, broad, recorded, result in frame.select(
        "event_key",
        "season",
        "recorded_broad_type",
        "recorded_air_subtype",
        "result_family",
    ).iter_rows():
        status: PredictionStatus
        cell_id: str | None = None
        regime: str | None = None
        probabilities: list[float] | None = None
        if broad == "Ground":
            status = "ground_preserved"
            probabilities = [1.0, 0.0, 0.0, 0.0]
        elif broad != "Air":
            status = "broad_unknown_unsupported"
        elif not isinstance(season, int) or season not in SEASON_REGIME:
            status = "regime_unsupported"
        else:
            regime = SEASON_REGIME[season]
            if regime not in fit.represented_regimes:
                status = "regime_unsupported"
            elif needs_fine and recorded is None:
                status = "fine_label_missing_unsupported"
            elif needs_fine and recorded not in AIR_CLASSES:
                status = "fine_label_unknown_unsupported"
            elif needs_result and (not isinstance(result, str) or not result):
                status = "result_family_missing_unsupported"
            else:
                values = _cell_values(fit.predictor, recorded, result)
                cell_id = cell_identity(fit, values)
                status = "posterior_cell" if values in fitted else "prior_only_cell"
                requested[(values, regime)] = status
        event_rows.append(
            {
                "event_key": event_key,
                "probabilities": probabilities,
                "prediction_status": status,
                "cell_id": cell_id,
                "regime": regime,
            }
        )
    shared: list[SharedCellDraws] = []
    means: dict[tuple[str, str], list[float]] = {}
    posterior_cache: dict[tuple[str, ...], tuple[FloatArray, FloatArray]] = {}
    for (values, regime), status in sorted(requested.items()):
        cell = fitted.get(values)
        counts = np.asarray(
            cell.counts if cell else ((0, 0, 0), (0, 0, 0)), dtype=np.int64
        )
        identity = cell_identity(fit, values)
        cached = posterior_cache.get(values)
        if cached is None:
            if cell is None:
                order, _ = _convergence(counts, fit.concentration)
            else:
                order = cell.selected_order
            all_draws = _validated_draws(
                sample_posterior(
                    counts,
                    fit.concentration,
                    order,
                    fit.draws,
                    _cell_seed(fit, identity),
                ),
                fit.draws,
            )
            moments = posterior_moments(
                posterior_grid(counts, fit.concentration, order)
            ).regime_mean
            posterior_cache[values] = (all_draws, moments)
        else:
            all_draws, moments = cached
        regime_index = REGIMES.index(regime)
        probability_draws = all_draws[:, regime_index, :].copy()
        probability_draws.flags.writeable = False
        mean = moments[regime_index]
        means[(identity, regime)] = [0.0, *(float(value) for value in mean)]
        shared.append(
            SharedCellDraws(
                cell_id=identity,
                cell_values=values,
                regime=regime,
                status=status,
                probabilities=probability_draws,
            )
        )
    for row in event_rows:
        identity = row["cell_id"]
        regime = cast(str | None, row["regime"])
        if isinstance(identity, str) and isinstance(regime, str):
            row["probabilities"] = means[(identity, regime)]
    events = pl.DataFrame(
        event_rows,
        schema={
            "event_key": frame.schema["event_key"],
            "probabilities": pl.List(pl.Float64),
            "prediction_status": pl.String,
            "cell_id": pl.String,
            "regime": pl.String,
        },
    )
    return AirRegimePrediction(events=events, cell_draws=tuple(shared))
