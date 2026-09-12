from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import duckdb
import numpy as np
import polars as pl
from pydantic import BaseModel
from scipy.optimize import minimize

from python_models.statistical.backtests.geometry_reliability_data import (
    BASE,
    DATABASE,
    SCHEMA,
    write_json,
)
from python_models.statistical.backtests.geometry_statcast_targets import (
    RECORDED_AIR_SUBTYPES,
    recorded_air_subtype,
    standardize_trajectory,
)
from python_models.statistical.evidence_binding import file_digest

LOGGER = logging.getLogger(__name__)
CLASSES = RECORDED_AIR_SUBTYPES
RESERVE = BASE / "historical_stress/20260911-reserve-v1/reserved_games.parquet"
RECORDED_QUERY = f"""
SELECT season, batter_id, raw_value AS recorded_air_subtype, COUNT(*) AS n
FROM {SCHEMA}.model_input_geometry
WHERE geometry_dimension = 'trajectory'
  AND observed_status = 'observed'
  AND raw_value IN ({", ".join(f"'{name}'" for name in RECORDED_AIR_SUBTYPES)})
  AND game_type = 'RegularSeason'
  AND primary_fold = 'TRAIN'
  AND game_id NOT IN (SELECT game_id FROM reserve_games)
  AND season BETWEEN ? AND ?
GROUP BY 1, 2, 3
"""


class TransportGap(BaseModel):
    batters: int
    recorded_events: int
    true_events: int
    predicted_recorded_share: dict[str, float]
    actual_recorded_share: dict[str, float]
    gap: dict[str, float]
    gap_ci95: dict[str, tuple[float, float]]
    max_abs_gap: float


class TranslationEstimate(BaseModel):
    recorded_given_true: dict[str, dict[str, float]]
    recorded_given_true_ci95: dict[str, dict[str, tuple[float, float]]]
    difference_from_reference: dict[str, dict[str, float]]
    difference_ci95: dict[str, dict[str, tuple[float, float]]]
    max_abs_difference: float
    negative_log_likelihood: float


class TransportResult(BaseModel):
    window: tuple[int, int]
    kind: str
    result: TransportGap
    translation: TranslationEstimate


def standardize_paired(paired: pl.DataFrame) -> pl.DataFrame:
    recorded_classes = cast(list[str | None], paired["recorded_class"].to_list())
    statcast_classes = cast(list[str | None], paired["statcast_bb_type"].to_list())
    angles = cast(list[float | None], paired["launch_angle"].to_list())
    complete_flags = cast(list[bool], paired["game_pairing_complete"].to_list())
    targets = [
        standardize_trajectory(recorded, reference, angle, pairing_complete=complete)
        for recorded, reference, angle, complete in zip(
            recorded_classes, statcast_classes, angles, complete_flags, strict=True
        )
    ]
    return paired.with_columns(
        pl.Series(
            "recorded_air_subtype",
            [recorded_air_subtype(value) for value in recorded_classes],
            dtype=pl.String,
        ),
        pl.Series(
            "target_class", [target.trajectory for target in targets], dtype=pl.String
        ),
        pl.Series(
            "target_status", [target.status for target in targets], dtype=pl.String
        ),
    ).with_columns(
        (
            pl.col("recorded_air_subtype").is_not_null()
            & (pl.col("target_status") == "air_angle_standardized")
        ).alias("eligible")
    )


def translation_matrix(frame: pl.DataFrame) -> np.ndarray:
    counts = np.full((len(CLASSES), len(CLASSES)), 0.5)
    table = (
        frame.filter(pl.col("eligible"))
        .group_by("target_class", "recorded_air_subtype")
        .len()
    )
    for true_class, recorded, n in table.iter_rows():
        counts[CLASSES.index(str(true_class)), CLASSES.index(str(recorded))] += int(n)
    return counts / counts.sum(axis=1, keepdims=True)


def batter_true_profiles(frame: pl.DataFrame) -> pl.DataFrame:
    table = (
        frame.filter(pl.col("eligible"))
        .group_by("batter_id", "target_class")
        .len()
        .pivot(on="target_class", index="batter_id", values="len")
        .fill_null(0)
    )
    for name in CLASSES:
        if name not in table.columns:
            table = table.with_columns(pl.lit(0, dtype=pl.UInt32).alias(name))
    return table.select("batter_id", *CLASSES).with_columns(
        pl.sum_horizontal(CLASSES).alias("true_events")
    )


def recorded_profiles(counts: pl.DataFrame, seasons: Sequence[int]) -> pl.DataFrame:
    table = (
        counts.filter(pl.col("season").is_in(list(seasons)))
        .group_by("batter_id", "recorded_air_subtype")
        .agg(pl.col("n").sum())
        .pivot(on="recorded_air_subtype", index="batter_id", values="n")
        .fill_null(0)
    )
    for name in CLASSES:
        if name not in table.columns:
            table = table.with_columns(pl.lit(0, dtype=pl.Int64).alias(name))
    return table.select(
        "batter_id", *[pl.col(name).alias(f"recorded_{name}") for name in CLASSES]
    ).with_columns(
        pl.sum_horizontal([f"recorded_{name}" for name in CLASSES]).alias(
            "recorded_events"
        )
    )


def cohort_shares(
    true_counts: np.ndarray, recorded_counts: np.ndarray, translation: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    true_shares = true_counts / true_counts.sum(axis=1, keepdims=True)
    weights = recorded_counts.sum(axis=1)
    predicted = (weights[:, None] * (true_shares @ translation)).sum(axis=0)
    actual = recorded_counts.sum(axis=0)
    return predicted / predicted.sum(), actual / actual.sum()


def transport_gap(
    true_counts: np.ndarray,
    recorded_counts: np.ndarray,
    translation: np.ndarray,
    *,
    repetitions: int,
    seed: int,
) -> TransportGap:
    predicted, actual = cohort_shares(true_counts, recorded_counts, translation)
    gap = actual - predicted
    generator = np.random.default_rng(seed)
    draws = np.empty((repetitions, len(CLASSES)))
    batters = true_counts.shape[0]
    for index in range(repetitions):
        sample = cast(np.ndarray, generator.integers(0, batters, size=batters))
        p, a = cohort_shares(true_counts[sample], recorded_counts[sample], translation)
        draws[index] = a - p
    lower, upper = np.quantile(draws, [0.025, 0.975], axis=0)
    return TransportGap(
        batters=int(batters),
        recorded_events=int(recorded_counts.sum()),
        true_events=int(true_counts.sum()),
        predicted_recorded_share={
            name: round(float(predicted[i]), 4) for i, name in enumerate(CLASSES)
        },
        actual_recorded_share={
            name: round(float(actual[i]), 4) for i, name in enumerate(CLASSES)
        },
        gap={name: round(float(gap[i]), 4) for i, name in enumerate(CLASSES)},
        gap_ci95={
            name: (round(float(lower[i]), 4), round(float(upper[i]), 4))
            for i, name in enumerate(CLASSES)
        },
        max_abs_gap=round(float(np.abs(gap).max()), 4),
    )


def _softmax_rows(theta: np.ndarray) -> np.ndarray:
    logits = np.concatenate([np.zeros((len(CLASSES), 1)), theta], axis=1)
    logits -= logits.max(axis=1, keepdims=True)
    weights = np.exp(logits)
    return weights / weights.sum(axis=1, keepdims=True)


def fit_translation(true_counts: np.ndarray, recorded_counts: np.ndarray) -> np.ndarray:
    true_shares = true_counts / true_counts.sum(axis=1, keepdims=True)

    def objective(flat: np.ndarray) -> float:
        matrix = _softmax_rows(flat.reshape(len(CLASSES), len(CLASSES) - 1))
        expected = true_shares @ matrix
        return float(-(recorded_counts * np.log(expected + 1e-12)).sum())

    start = np.zeros(len(CLASSES) * (len(CLASSES) - 1))
    solution = minimize(objective, start, method="L-BFGS-B")
    return _softmax_rows(solution.x.reshape(len(CLASSES), len(CLASSES) - 1))


def estimate_translation(
    true_counts: np.ndarray,
    recorded_counts: np.ndarray,
    reference: np.ndarray,
    *,
    repetitions: int,
    seed: int,
) -> TranslationEstimate:
    estimate = fit_translation(true_counts, recorded_counts)
    generator = np.random.default_rng(seed)
    batters = true_counts.shape[0]
    draws = np.empty((repetitions, len(CLASSES), len(CLASSES)))
    for index in range(repetitions):
        sample = cast(np.ndarray, generator.integers(0, batters, size=batters))
        draws[index] = fit_translation(true_counts[sample], recorded_counts[sample])
    lower, upper = np.quantile(draws, [0.025, 0.975], axis=0)
    difference = estimate - reference
    lower_difference, upper_difference = np.quantile(
        draws - reference, [0.025, 0.975], axis=0
    )
    true_shares = true_counts / true_counts.sum(axis=1, keepdims=True)
    expected = true_shares @ estimate

    def table(matrix: np.ndarray) -> dict[str, dict[str, float]]:
        return {
            true_class: {
                recorded: round(float(matrix[i, j]), 4)
                for j, recorded in enumerate(CLASSES)
            }
            for i, true_class in enumerate(CLASSES)
        }

    def intervals(
        low: np.ndarray, high: np.ndarray
    ) -> dict[str, dict[str, tuple[float, float]]]:
        return {
            true_class: {
                recorded: (round(float(low[i, j]), 4), round(float(high[i, j]), 4))
                for j, recorded in enumerate(CLASSES)
            }
            for i, true_class in enumerate(CLASSES)
        }

    return TranslationEstimate(
        recorded_given_true=table(estimate),
        recorded_given_true_ci95=intervals(lower, upper),
        difference_from_reference=table(difference),
        difference_ci95=intervals(lower_difference, upper_difference),
        max_abs_difference=round(float(np.abs(difference).max()), 4),
        negative_log_likelihood=round(
            float(-(recorded_counts * np.log(expected + 1e-12)).sum()), 3
        ),
    )


def bridge_cohort(
    profiles: pl.DataFrame, recorded: pl.DataFrame, *, minimum_events: int
) -> pl.DataFrame:
    return profiles.join(recorded, on="batter_id", how="inner").filter(
        (pl.col("true_events") >= minimum_events)
        & (pl.col("recorded_events") >= minimum_events)
    )


def cohort_arrays(cohort: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    true_counts = cohort.select(CLASSES).to_numpy().astype(float)
    recorded_counts = (
        cohort.select([f"recorded_{name}" for name in CLASSES]).to_numpy().astype(float)
    )
    return true_counts, recorded_counts


def evaluate_window(
    window: tuple[int, int],
    kind: str,
    profiles: pl.DataFrame,
    recorded: pl.DataFrame,
    reference: np.ndarray,
    *,
    minimum_events: int,
    repetitions: int,
    seed: int,
) -> TransportResult:
    cohort = bridge_cohort(profiles, recorded, minimum_events=minimum_events)
    if cohort.is_empty():
        raise ValueError(
            f"window {window} has no hitter with at least {minimum_events} "
            "reference and recorded events"
        )
    true_counts, recorded_counts = cohort_arrays(cohort)
    LOGGER.info("Window %s: %d bridging batters", window, true_counts.shape[0])
    return TransportResult(
        window=window,
        kind=kind,
        result=transport_gap(
            true_counts, recorded_counts, reference, repetitions=repetitions, seed=seed
        ),
        translation=estimate_translation(
            true_counts, recorded_counts, reference, repetitions=repetitions, seed=seed
        ),
    )


def load_recorded_counts(
    seasons: tuple[int, int], *, database: Path = DATABASE, reserve: Path = RESERVE
) -> pl.DataFrame:
    with duckdb.connect(str(database), read_only=True) as connection:
        connection.register(
            "reserve_games", pl.read_parquet(reserve, columns=["game_id"])
        )
        frame = connection.execute(RECORDED_QUERY, list(seasons)).pl()
    return frame.with_columns(pl.col("n").cast(pl.Int64))


def run(
    *,
    acquisition_root: Path,
    output: Path,
    target_windows: Sequence[tuple[int, int]],
    minimum_events: int,
    repetitions: int,
    seed: int,
    database: Path = DATABASE,
    reserve: Path = RESERVE,
) -> None:
    paired = pl.read_parquet(acquisition_root / "paired_events.parquet")
    reference_seasons = sorted(set(cast(list[int], paired["season"].to_list())))
    frame = standardize_paired(paired)
    translation = translation_matrix(frame)
    profiles = batter_true_profiles(frame)
    bounds = [*target_windows, (reference_seasons[0], reference_seasons[-1])]
    counts = load_recorded_counts(
        (min(start for start, _ in bounds), max(end for _, end in bounds)),
        database=database,
        reserve=reserve,
    )
    results: list[TransportResult] = []
    for season in reference_seasons:
        held_out = frame.filter(pl.col("season") != season)
        results.append(
            evaluate_window(
                (season, season),
                "reference_held_out",
                batter_true_profiles(held_out),
                recorded_profiles(counts, [season]),
                translation_matrix(held_out),
                minimum_events=minimum_events,
                repetitions=repetitions,
                seed=seed,
            )
        )
    for window in target_windows:
        results.append(
            evaluate_window(
                window,
                "transport",
                profiles,
                recorded_profiles(counts, list(range(window[0], window[1] + 1))),
                translation,
                minimum_events=minimum_events,
                repetitions=repetitions,
                seed=seed,
            )
        )
    output.mkdir(parents=True, exist_ok=False)
    frame.write_parquet(output / "standardized_events.parquet")
    profiles.write_parquet(output / "batter_true_profiles.parquet")
    counts.write_parquet(output / "recorded_label_counts.parquet")
    write_json(
        output / "report.json",
        {
            "acquisition_root": str(acquisition_root),
            "acquisition_manifest_sha256": file_digest(
                acquisition_root / "manifest.json"
            ),
            "reserve_sha256": file_digest(reserve),
            "reference_seasons": reference_seasons,
            "eligible_reference_events": int(frame.filter(pl.col("eligible")).height),
            "translation_recorded_given_true": {
                true_class: dict(zip(CLASSES, translation[i].round(4).tolist()))
                for i, true_class in enumerate(CLASSES)
            },
            "minimum_events": minimum_events,
            "bootstrap_repetitions": repetitions,
            "seed": seed,
            "results": [item.model_dump() for item in results],
        },
    )
    write_json(
        output / "manifest.json",
        {
            "files_sha256": {
                path.name: file_digest(path)
                for path in sorted(output.iterdir())
                if path.name != "manifest.json"
            }
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--acquisition-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minimum-events", type=int, default=100)
    parser.add_argument("--repetitions", type=int, default=500)
    parser.add_argument("--seed", type=int, default=20260911)
    parser.add_argument(
        "--window",
        action="append",
        default=[],
        help="target season window as START:END; repeatable",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    windows = [
        (int(start), int(end))
        for start, end in (item.split(":") for item in cast(list[str], args.window))
    ]
    run(
        acquisition_root=cast(Path, args.acquisition_root),
        output=cast(Path, args.output),
        target_windows=windows,
        minimum_events=int(args.minimum_events),
        repetitions=int(args.repetitions),
        seed=int(args.seed),
    )


if __name__ == "__main__":
    main()
