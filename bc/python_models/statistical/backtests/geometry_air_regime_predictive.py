from __future__ import annotations

import hashlib
import json
from typing import Literal, cast

import numpy as np
import numpy.typing as npt
import polars as pl

from python_models.statistical.backtests.geometry_air_regime import (
    AIR_CLASSES,
    AirRegimeFit,
    AirRegimePrediction,
    SharedCellDraws,
    cell_identity,
)


IntArray = npt.NDArray[np.int64]
SUPPORTED_STATUSES = frozenset({"posterior_cell", "prior_only_cell"})
COUNT_SCHEMA = {
    "fit_id": pl.String,
    "predictor": pl.String,
    "concentration": pl.Float64,
    "cohort_type": pl.Enum(["game", "season"]),
    "cohort_id": pl.String,
    "season": pl.Int64,
    "events": pl.Int64,
    "observed_counts": pl.List(pl.Int64),
    "replicated_counts": pl.List(pl.List(pl.Int64)),
}
SUMMARY_SCHEMA = {
    "fit_id": pl.String,
    "predictor": pl.String,
    "concentration": pl.Float64,
    "cohort_type": pl.Enum(["game", "season"]),
    "cohort_id": pl.String,
    "season": pl.Int64,
    "events": pl.Int64,
    "class_label": pl.String,
    "interval_level": pl.Float64,
    "observed_count": pl.Int64,
    "lower": pl.Float64,
    "upper": pl.Float64,
    "width": pl.Float64,
    "covered": pl.Boolean,
    "mc_lower_span": pl.Float64,
    "mc_upper_span": pl.Float64,
    "mc_coverage_disagreement_fraction": pl.Float64,
}


def _game_seed(fit: AirRegimeFit, cell_id: str, game_id: str) -> int:
    payload = json.dumps(
        [
            fit.experiment_id,
            fit.fit_id,
            fit.predictor,
            fit.concentration,
            fit.seed,
            cell_id,
            game_id,
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return int.from_bytes(hashlib.sha256(payload.encode()).digest()[:8], "big")


def _draw_map(
    fit: AirRegimeFit, prediction: AirRegimePrediction
) -> dict[tuple[str, str], SharedCellDraws]:
    result: dict[tuple[str, str], SharedCellDraws] = {}
    for shared in prediction.cell_draws:
        expected_id = cell_identity(fit, shared.cell_values)
        if shared.cell_id != expected_id:
            raise ValueError("prediction cell identity does not belong to fit")
        key = (shared.cell_id, shared.regime)
        if key in result:
            raise ValueError("prediction contains duplicate shared cell draws")
        values = np.asarray(shared.probabilities)
        if (
            values.dtype != np.float64
            or values.shape != (fit.draws, 3)
            or not np.isfinite(values).all()
            or (values < 0).any()
            or not np.allclose(values.sum(axis=1), 1.0, rtol=0.0, atol=1e-12)
        ):
            raise ValueError(
                "shared cell draws must be finite normalized probabilities"
            )
        if shared.status not in SUPPORTED_STATUSES:
            raise ValueError("shared cell draw has an unsupported status")
        result[key] = shared
    return result


def _validate_inputs(
    fit: AirRegimeFit, prediction: AirRegimePrediction, evaluation: pl.DataFrame
) -> tuple[pl.DataFrame, dict[tuple[str, str], SharedCellDraws]]:
    evaluation_columns = {"event_key", "game_id", "season", "target_class"}
    prediction_columns = {
        "event_key",
        "cell_id",
        "regime",
        "prediction_status",
    }
    if not evaluation_columns <= set(evaluation.columns):
        raise ValueError("evaluation inputs are incomplete")
    if not prediction_columns <= set(prediction.events.columns):
        raise ValueError("prediction event inputs are incomplete")
    for frame in (evaluation, prediction.events):
        if (
            frame["event_key"].null_count()
            or frame["event_key"].n_unique() != frame.height
        ):
            raise ValueError("event keys must be non-null and unique")
    if evaluation.height != prediction.events.height or set(
        evaluation["event_key"].to_list()
    ) != set(prediction.events["event_key"].to_list()):
        raise ValueError("evaluation and prediction event keys must align exactly")
    if any(
        evaluation[column].null_count()
        for column in ("game_id", "season", "target_class")
    ):
        raise ValueError("evaluation cohort and target columns must not be null")
    if not set(evaluation["target_class"].to_list()) <= set(AIR_CLASSES):
        raise ValueError("evaluation contains an invalid airborne target class")
    statuses = set(prediction.events["prediction_status"].to_list())
    if not statuses <= SUPPORTED_STATUSES:
        raise ValueError("predictive counts require supported airborne predictions")
    if (
        prediction.events["cell_id"].null_count()
        or prediction.events["regime"].null_count()
    ):
        raise ValueError("supported predictions require cell and regime identities")
    joined = evaluation.join(
        prediction.events.select("event_key", "cell_id", "regime", "prediction_status"),
        on="event_key",
        how="inner",
        validate="1:1",
    )
    for column in ("season", "regime"):
        per_game = joined.group_by("game_id").agg(pl.col(column).n_unique().alias("n"))
        if int(cast(int, per_game["n"].max())) > 1:
            raise ValueError(f"each game must belong to exactly one {column}")
    draw_map = _draw_map(fit, prediction)
    requested = set(joined.select("cell_id", "regime").iter_rows())
    if requested != set(draw_map):
        raise ValueError("prediction event cells and shared draws must align exactly")
    return joined, draw_map


def _checked_add(target: IntArray, values: IntArray) -> None:
    maximum = np.iinfo(np.int64).max
    if (values < 0).any() or (target > maximum - values).any():
        raise OverflowError("predictive counts exceed int64 capacity")
    target += values


def predictive_counts(
    fit: AirRegimeFit,
    prediction: AirRegimePrediction,
    evaluation: pl.DataFrame,
) -> pl.DataFrame:
    if (
        evaluation.is_empty()
        and prediction.events.is_empty()
        and not prediction.cell_draws
    ):
        return pl.DataFrame(schema=COUNT_SCHEMA)
    joined, draw_map = _validate_inputs(fit, prediction, evaluation)
    game_draws: dict[str, IntArray] = {}
    game_observed: dict[str, IntArray] = {}
    game_seasons: dict[str, int] = {}
    for group_key, group in joined.group_by(
        "game_id", "season", "cell_id", "regime", maintain_order=False
    ):
        game_id_value, season_value, cell_id_value, regime_value = group_key
        game_id = str(game_id_value)
        season = int(season_value)
        cell_id = str(cell_id_value)
        regime = str(regime_value)
        shared = draw_map[(cell_id, regime)]
        event_count = group.height
        rng = np.random.default_rng(_game_seed(fit, cell_id, game_id))
        replicated = np.asarray(
            rng.multinomial(event_count, shared.probabilities), dtype=np.int64
        )
        if (
            replicated.shape != (fit.draws, 3)
            or not np.equal(replicated.sum(axis=1), event_count).all()
        ):
            raise RuntimeError("game-cell predictive draws do not conserve events")
        target = game_draws.setdefault(
            game_id, np.zeros((fit.draws, 3), dtype=np.int64)
        )
        _checked_add(target, replicated)
        observed = game_observed.setdefault(game_id, np.zeros(3, dtype=np.int64))
        observed_counts = np.bincount(
            np.asarray(
                [AIR_CLASSES.index(str(value)) for value in group["target_class"]],
                dtype=np.int64,
            ),
            minlength=3,
        ).astype(np.int64)
        _checked_add(observed, observed_counts)
        game_seasons[game_id] = season
    rows: list[dict[str, object]] = []
    season_draws: dict[int, IntArray] = {}
    season_observed: dict[int, IntArray] = {}
    for game_id in sorted(game_draws):
        replicated = game_draws[game_id]
        observed = game_observed[game_id]
        season = game_seasons[game_id]
        events = int(observed.sum())
        if not np.equal(replicated.sum(axis=1), events).all():
            raise RuntimeError("game predictive draws do not conserve events")
        rows.append(_count_row(fit, "game", game_id, season, observed, replicated))
        season_target = season_draws.setdefault(
            season, np.zeros((fit.draws, 3), dtype=np.int64)
        )
        _checked_add(season_target, replicated)
        observed_target = season_observed.setdefault(
            season, np.zeros(3, dtype=np.int64)
        )
        _checked_add(observed_target, observed)
    for season in sorted(season_draws):
        rows.append(
            _count_row(
                fit,
                "season",
                str(season),
                season,
                season_observed[season],
                season_draws[season],
            )
        )
    return pl.DataFrame(rows, schema=COUNT_SCHEMA).sort(
        "cohort_type", "season", "cohort_id"
    )


def _count_row(
    fit: AirRegimeFit,
    cohort_type: Literal["game", "season"],
    cohort_id: str,
    season: int,
    observed: IntArray,
    replicated: IntArray,
) -> dict[str, object]:
    events = int(observed.sum())
    if (
        replicated.shape != (fit.draws, 3)
        or not np.equal(replicated.sum(axis=1), events).all()
    ):
        raise RuntimeError("cohort predictive draws do not conserve events")
    return {
        "fit_id": fit.fit_id,
        "predictor": fit.predictor,
        "concentration": fit.concentration,
        "cohort_type": cohort_type,
        "cohort_id": cohort_id,
        "season": season,
        "events": events,
        "observed_counts": observed.tolist(),
        "replicated_counts": replicated.tolist(),
    }


def summarize_counts(counts: pl.DataFrame) -> pl.DataFrame:
    if counts.is_empty():
        if set(counts.columns) != set(COUNT_SCHEMA):
            raise ValueError("empty predictive counts must use the declared schema")
        return pl.DataFrame(schema=SUMMARY_SCHEMA)
    if not set(COUNT_SCHEMA) <= set(counts.columns):
        raise ValueError("predictive count inputs are incomplete")
    identity = counts.select("fit_id", "predictor", "concentration").unique()
    if identity.height != 1:
        raise ValueError("count summaries cannot mix fits")
    rows: list[dict[str, object]] = []
    for row in counts.iter_rows(named=True):
        observed = np.asarray(row["observed_counts"])
        replicated = np.asarray(row["replicated_counts"])
        events = int(cast(int, row["events"]))
        if (
            observed.dtype.kind not in "iu"
            or replicated.dtype.kind not in "iu"
            or observed.shape != (3,)
            or replicated.ndim != 2
            or replicated.shape[1] != 3
            or replicated.shape[0] == 0
            or (observed < 0).any()
            or (replicated < 0).any()
            or int(observed.sum()) != events
            or not np.equal(replicated.sum(axis=1), events).all()
        ):
            raise ValueError(
                "predictive count rows must be nonnegative and conserve events"
            )
        batch_count = min(8, replicated.shape[0])
        for class_index, class_label in enumerate(AIR_CLASSES):
            values = replicated[:, class_index].astype(np.float64)
            observed_count = int(observed[class_index])
            batches = np.array_split(values, batch_count)
            for level in (0.9, 0.95):
                tail = (1.0 - level) / 2.0
                lower, upper = np.quantile(values, [tail, 1.0 - tail])
                batch_bounds = np.asarray(
                    [np.quantile(batch, [tail, 1.0 - tail]) for batch in batches]
                )
                batch_covered = (batch_bounds[:, 0] <= observed_count) & (
                    observed_count <= batch_bounds[:, 1]
                )
                covered = bool(lower <= observed_count <= upper)
                disagreements = int(np.count_nonzero(batch_covered != covered))
                rows.append(
                    {
                        "fit_id": row["fit_id"],
                        "predictor": row["predictor"],
                        "concentration": row["concentration"],
                        "cohort_type": row["cohort_type"],
                        "cohort_id": row["cohort_id"],
                        "season": row["season"],
                        "events": events,
                        "class_label": class_label,
                        "interval_level": level,
                        "observed_count": observed_count,
                        "lower": float(lower),
                        "upper": float(upper),
                        "width": float(upper - lower),
                        "covered": covered,
                        "mc_lower_span": float(
                            batch_bounds[:, 0].max() - batch_bounds[:, 0].min()
                        ),
                        "mc_upper_span": float(
                            batch_bounds[:, 1].max() - batch_bounds[:, 1].min()
                        ),
                        "mc_coverage_disagreement_fraction": disagreements
                        / batch_count,
                    }
                )
    return pl.DataFrame(rows, schema=SUMMARY_SCHEMA).sort(
        "cohort_type", "season", "cohort_id", "class_label", "interval_level"
    )
