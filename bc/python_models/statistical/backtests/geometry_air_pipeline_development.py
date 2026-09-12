from __future__ import annotations

import argparse
import hashlib
import json
import logging
import platform
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import cast

import numpy as np
import numpy.typing as npt
import polars as pl

from python_models.statistical.backtests.geometry_air_pipeline_bounds import (
    SLACK_FACTOR,
    SLACK_FLOOR,
    IdentifiedSet,
    band_given_recorded,
    clue_slack,
    clue_table,
    feasible,
    identified_set,
    sample_identified_set,
    season_margins,
    validate_bounds,
)
from python_models.statistical.backtests.geometry_air_pipeline_data import (
    ACQUISITION_ROOT,
    CLASSES,
    CLUE_LEVELS,
    COVERAGE_FRAME,
    PIPELINES,
    REFERENCE_SEASONS,
    RESERVE,
    load_historical_counts,
    load_reference_frame,
)
from python_models.statistical.backtests.geometry_air_pipeline_model import (
    ARMS,
    BOUNDARY_MASS_LIMIT,
    EXPERIMENT_ID,
    KAPPA_GRID,
    Arm,
    CellPrediction,
    PipelineFit,
    PipelinePrediction,
    fit_pipeline,
    predict_pipeline,
)
from python_models.statistical.backtests.geometry_air_regime_development import (
    declared_source_paths,
    write_manifest,
)
from python_models.statistical.backtests.geometry_air_regime_report import (
    class_ece,
    gain_matrix,
)
from python_models.statistical.backtests.geometry_reliability_data import (
    DATABASE,
    SCHEMA,
    read_json,
    write_json,
)
from python_models.statistical.backtests.geometry_statcast_bridge_selection import (
    game_digest,
)
from python_models.statistical.evidence_binding import file_digest
from python_models.statistical.splits import game_hash_fold

LOGGER = logging.getLogger(__name__)
FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]

PROTOCOL_PATH = (
    Path(__file__).resolve().parents[4]
    / "docs/geometry-air-pipeline-translation-protocol.md"
)
SOURCE_MODULES = (
    "geometry_air_bridge_check.py",
    "geometry_air_dirichlet.py",
    "geometry_air_pipeline_bounds.py",
    "geometry_air_pipeline_data.py",
    "geometry_air_pipeline_development.py",
    "geometry_air_pipeline_model.py",
    "geometry_air_regime_development.py",
    "geometry_air_regime_report.py",
    "geometry_reliability_data.py",
    "geometry_statcast_bridge_selection.py",
    "geometry_statcast_targets.py",
)
ROOT_SEED = 20260911
FULL_DRAWS = 4096
FULL_REPETITIONS = 500
SMOKE_DRAWS = 256
SMOKE_REPETITIONS = 50
SMOKE_GAME_FOLDS = 10
CANDIDATE: Arm = "recorded_result"
REFERENCE: Arm = "recorded"
CALIBRATION_BINS = 15
CALIBRATION_MIN_EVENTS = 500
ECE_LIMIT = 0.06
BIAS_LIMIT = 0.06
SHARE_CELL_MIN_EVENTS = 200
SHARE_COVERAGE_95_MIN = 0.90
SHARE_COVERAGE_90_MIN = 0.80
GAME_COVERAGE_MIN = 0.90
BOUNDS_DRAWS = 4096
BOUNDS_CLUE_PIPELINE = "B"
SMOKE_STATUS = "smoke_complete"
FULL_STATUS = "development_scoring_complete"
SCREENS = (
    "calibration",
    "share_coverage",
    "game_coverage",
    "gain",
    "concentration",
)


def _seed(*parts: object) -> int:
    payload = json.dumps(list(parts), ensure_ascii=True, separators=(",", ":"))
    return int.from_bytes(hashlib.sha256(payload.encode()).digest()[:8], "big")


def _interval(values: FloatArray, level: float) -> tuple[float, float]:
    tail = (1.0 - level) / 2.0
    lower, upper = np.quantile(values, [tail, 1.0 - tail])
    return float(lower), float(upper)


def _contains(lower: float, upper: float, value: float) -> bool:
    return lower - 1e-12 <= value <= upper + 1e-12


def bindings(
    *,
    smoke: bool,
    draws: int,
    bounds_draws: int,
    repetitions: int,
    protocol: Path,
    coverage_frame: Path,
    acquisition_root: Path,
    database: Path,
    reserve: Path,
) -> dict[str, object]:
    reserve_games = cast(
        list[str], pl.read_parquet(reserve, columns=["game_id"])["game_id"].to_list()
    )
    return {
        "experiment": EXPERIMENT_ID,
        "smoke": smoke,
        "protocol": {"path": str(protocol), "sha256": file_digest(protocol)},
        "sources_sha256": {
            name: file_digest(path)
            for name, path in declared_source_paths(SOURCE_MODULES).items()
        },
        "coverage_frame": {
            "path": str(coverage_frame),
            "sha256": file_digest(coverage_frame),
        },
        "acquisition": {
            "path": str(acquisition_root),
            "manifest_sha256": file_digest(acquisition_root / "manifest.json"),
            "paired_events_sha256": file_digest(
                acquisition_root / "paired_events.parquet"
            ),
        },
        "database": {
            "path": str(database),
            "schema": SCHEMA,
            "bytes": database.stat().st_size,
        },
        "reserve": {
            "path": str(reserve),
            "sha256": file_digest(reserve),
            "games": len(set(reserve_games)),
            "game_digest": game_digest(reserve_games),
        },
        "versions": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "polars": pl.__version__,
            "scipy": version("scipy"),
            "duckdb": version("duckdb"),
            "pydantic": version("pydantic"),
        },
        "seeds": {"root": ROOT_SEED, "bootstrap": ROOT_SEED, "bounds": ROOT_SEED},
        "scale": {
            "draws": draws,
            "bounds_draws": bounds_draws,
            "bootstrap_repetitions": repetitions,
        },
        "limits": {
            "kappa_grid": list(KAPPA_GRID),
            "boundary_mass_limit": BOUNDARY_MASS_LIMIT,
            "ece_limit": ECE_LIMIT,
            "bias_limit": BIAS_LIMIT,
            "calibration_min_events": CALIBRATION_MIN_EVENTS,
            "share_cell_min_events": SHARE_CELL_MIN_EVENTS,
            "share_coverage_95_min": SHARE_COVERAGE_95_MIN,
            "share_coverage_90_min": SHARE_COVERAGE_90_MIN,
            "game_coverage_min": GAME_COVERAGE_MIN,
            "slack_factor": SLACK_FACTOR,
            "slack_floor": SLACK_FLOOR,
            "bounds_clue_pipeline": BOUNDS_CLUE_PIPELINE,
        },
    }


def smoke_subsample(frame: pl.DataFrame, *, fold_count: int) -> pl.DataFrame:
    games = cast(list[str], frame["game_id"].unique().sort().to_list())
    kept = [game for game in games if game_hash_fold(game, fold_count=fold_count) == 0]
    return frame.filter(pl.col("game_id").is_in(kept))


def _labels(frame: pl.DataFrame) -> IntArray:
    return np.array(
        [CLASSES.index(str(value)) for value in frame["target_class"].to_list()],
        dtype=np.int64,
    )


def class_calibration(
    probabilities: FloatArray, labels: IntArray
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, label in enumerate(CLASSES):
        probability = probabilities[:, index]
        target = (labels == index).astype(np.float64)
        rows.append(
            {
                "class_label": label,
                "ece": class_ece(probability, target, CALIBRATION_BINS),
                "class_share_bias": float((probability - target).mean()),
                "actual_share": float(target.mean()),
                "predicted_share": float(probability.mean()),
            }
        )
    return rows


def _cell_counts(
    prediction: PipelinePrediction, mask: npt.NDArray[np.bool_]
) -> IntArray:
    return np.bincount(
        prediction.cell_index[mask], minlength=len(prediction.cells)
    ).astype(np.int64)


def _share_draws(
    cells: tuple[CellPrediction, ...], counts: IntArray, rng: np.random.Generator
) -> FloatArray:
    total = int(counts.sum())
    if total == 0:
        raise ValueError("a share slice needs events")
    draws = cells[0].draws.shape[0]
    accumulated = np.zeros((draws, len(CLASSES)), dtype=np.float64)
    for cell, count in zip(cells, counts, strict=True):
        if count == 0:
            continue
        accumulated += rng.multinomial(int(count), cell.draws)
    return accumulated / total


def share_coverage(
    prediction: PipelinePrediction,
    held: pl.DataFrame,
    labels: IntArray,
    *,
    fit_id: str,
) -> list[dict[str, object]]:
    recorded = held["recorded_air_subtype"].to_numpy()
    slices: list[tuple[str, npt.NDArray[np.bool_]]] = [
        ("all", np.ones(held.height, dtype=np.bool_))
    ]
    for subtype in CLASSES:
        mask = recorded == subtype
        if int(mask.sum()) >= SHARE_CELL_MIN_EVENTS:
            slices.append((subtype, mask))
    rows: list[dict[str, object]] = []
    for name, mask in slices:
        rng = np.random.default_rng(_seed(fit_id, "share", name))
        draws = _share_draws(prediction.cells, _cell_counts(prediction, mask), rng)
        events = int(mask.sum())
        for index, label in enumerate(CLASSES):
            actual = float((labels[mask] == index).mean())
            lower95, upper95 = _interval(draws[:, index], 0.95)
            lower90, upper90 = _interval(draws[:, index], 0.90)
            rows.append(
                {
                    "slice": name,
                    "class_label": label,
                    "events": events,
                    "actual_share": actual,
                    "predicted_share": float(draws[:, index].mean()),
                    "lower_95": lower95,
                    "upper_95": upper95,
                    "covered_95": _contains(lower95, upper95, actual),
                    "lower_90": lower90,
                    "upper_90": upper90,
                    "covered_90": _contains(lower90, upper90, actual),
                }
            )
    return rows


def game_count_coverage(
    prediction: PipelinePrediction,
    held: pl.DataFrame,
    labels: IntArray,
    *,
    fit_id: str,
) -> tuple[dict[str, float], pl.DataFrame]:
    games = held["game_id"].to_numpy()
    unique_games, game_index = np.unique(games, return_inverse=True)
    draws = prediction.cells[0].draws.shape[0]
    simulated = np.zeros((unique_games.size, draws, len(CLASSES)), dtype=np.int32)
    for position, cell in enumerate(prediction.cells):
        selected = prediction.cell_index == position
        counts = np.bincount(game_index[selected], minlength=unique_games.size)
        active = np.flatnonzero(counts)
        if active.size == 0:
            continue
        rng = np.random.default_rng(_seed(fit_id, "game", cell.values))
        simulated[active] += rng.multinomial(
            counts[active][:, None], cell.draws[None, :, :]
        )
    actual = np.zeros((unique_games.size, len(CLASSES)), dtype=np.int64)
    np.add.at(actual, (game_index, labels), 1)
    lower = np.quantile(simulated, 0.025, axis=1)
    upper = np.quantile(simulated, 0.975, axis=1)
    covered = (actual >= lower - 1e-9) & (actual <= upper + 1e-9)
    coverage = {
        label: float(covered[:, index].mean()) for index, label in enumerate(CLASSES)
    }
    table = pl.DataFrame(
        {
            "game_id": unique_games.tolist(),
            **{
                f"actual_{label}": actual[:, index].tolist()
                for index, label in enumerate(CLASSES)
            },
            **{
                f"lower_{label}": lower[:, index].tolist()
                for index, label in enumerate(CLASSES)
            },
            **{
                f"upper_{label}": upper[:, index].tolist()
                for index, label in enumerate(CLASSES)
            },
            **{
                f"covered_{label}": covered[:, index].tolist()
                for index, label in enumerate(CLASSES)
            },
        }
    )
    return coverage, table


def paired_game_gain(
    scores: pl.DataFrame, *, repetitions: int, seed: int
) -> dict[str, dict[str, float]]:
    games = (
        scores.group_by("game_id")
        .agg(
            pl.col("events").sum(),
            pl.col("gain_log_loss").sum(),
            pl.col("gain_brier").sum(),
        )
        .sort("game_id")
    )
    events = games["events"].to_numpy().astype(np.float64)
    rng = np.random.default_rng(seed)
    output: dict[str, dict[str, float]] = {}
    for metric in ("log_loss", "brier"):
        gains = games[f"gain_{metric}"].to_numpy().astype(np.float64)
        samples = np.empty(repetitions, dtype=np.float64)
        for repetition in range(repetitions):
            picks = rng.integers(0, games.height, size=games.height)
            samples[repetition] = gains[picks].sum() / events[picks].sum()
        lower, upper = _interval(samples, 0.95)
        output[metric] = {
            "mean": float(gains.sum() / events.sum()),
            "lower_95": lower,
            "upper_95": upper,
            "games": float(games.height),
        }
    return output


def fit_summary(fit: PipelineFit) -> dict[str, object]:
    return {
        "fit_id": fit.fit_id,
        "pipeline": fit.pipeline,
        "arm": fit.arm,
        "seasons": list(fit.seasons),
        "training_events": fit.training_events,
        "cells": len(fit.cells),
        "kappa_posterior": list(fit.kappa_posterior),
        "kappa_posterior_mean": fit.kappa_posterior_mean,
        "lower_boundary_mass": fit.lower_boundary_mass,
        "upper_boundary_mass": fit.upper_boundary_mass,
        "concentration_pass": fit.lower_boundary_mass <= BOUNDARY_MASS_LIMIT,
        "max_quadrature_order": max(max(cell.orders) for cell in fit.cells),
    }


def training_digest(frame: pl.DataFrame) -> str:
    digest = hashlib.sha256()
    for key in sorted(cast(list[int], frame["event_key"].to_list())):
        digest.update(int(key).to_bytes(8, "big", signed=True))
    return digest.hexdigest()


def load_or_fit(
    checkpoint_dir: Path,
    frame: pl.DataFrame,
    *,
    pipeline: str,
    arm: Arm,
    seasons: tuple[int, ...],
    fit_id: str,
    draws: int,
) -> PipelineFit:
    path = checkpoint_dir / f"{fit_id}.json"
    digest_path = checkpoint_dir / f"{fit_id}.training.json"
    digest = training_digest(frame)
    if path.is_file():
        fit = PipelineFit.model_validate_json(path.read_text())
        stored = read_json(digest_path) if digest_path.is_file() else {}
        if (
            fit.pipeline != pipeline
            or fit.arm != arm
            or fit.seasons != seasons
            or fit.draws != draws
            or fit.seed != ROOT_SEED
            or fit.experiment_id != EXPERIMENT_ID
            or fit.training_events != frame.height
            or stored.get("training_digest") != digest
        ):
            raise ValueError(f"checkpoint {path} does not match the requested fit")
        LOGGER.info("loaded checkpoint %s", fit_id)
        return fit
    LOGGER.info("fitting %s on %d events", fit_id, frame.height)
    fit = fit_pipeline(
        frame,
        pipeline=pipeline,
        arm=arm,
        seasons=seasons,
        fit_id=fit_id,
        draws=draws,
        seed=ROOT_SEED,
    )
    write_json(path, fit.model_dump(mode="json"))
    write_json(digest_path, {"training_digest": digest, "events": frame.height})
    return fit


@dataclass(frozen=True, slots=True)
class HeldOutResult:
    season: int
    summary: dict[str, object]
    predictions: pl.DataFrame
    game_scores: pl.DataFrame
    game_coverage: pl.DataFrame


def evaluate_held_out(
    frame: pl.DataFrame,
    *,
    pipeline: str,
    seasons: tuple[int, ...],
    season: int,
    checkpoint_dir: Path,
    draws: int,
) -> HeldOutResult:
    training = frame.filter(pl.col("season") != season)
    held = frame.filter(pl.col("season") == season).sort("game_id", "event_key")
    remaining = tuple(value for value in seasons if value != season)
    labels = _labels(held)
    fits = {
        arm: load_or_fit(
            checkpoint_dir,
            training,
            pipeline=pipeline,
            arm=arm,
            seasons=remaining,
            fit_id=f"{pipeline}-{arm}-holdout-{season}",
            draws=draws,
        )
        for arm in ARMS
    }
    predictions = {
        arm: predict_pipeline(fit, held, season=None) for arm, fit in fits.items()
    }
    candidate = predictions[CANDIDATE].probabilities
    reference = predictions[REFERENCE].probabilities
    one_hot = np.eye(len(CLASSES))[labels]
    log_gain = gain_matrix(candidate, reference, "log_loss")[
        np.arange(labels.size), labels
    ]
    brier_gain = (gain_matrix(candidate, reference, "brier") * one_hot).sum(axis=1)
    event_frame = held.select(
        "event_key",
        "game_id",
        "season",
        "recorded_air_subtype",
        "result_family",
        "target_class",
    ).with_columns(
        pl.lit(pipeline).alias("pipeline"),
        *[
            pl.Series(f"candidate_{label}", candidate[:, index])
            for index, label in enumerate(CLASSES)
        ],
        *[
            pl.Series(f"reference_{label}", reference[:, index])
            for index, label in enumerate(CLASSES)
        ],
        pl.Series(
            "candidate_cell_status",
            [
                predictions[CANDIDATE].cells[int(index)].status
                for index in predictions[CANDIDATE].cell_index
            ],
        ),
        pl.Series("gain_log_loss", log_gain),
        pl.Series("gain_brier", brier_gain),
    )
    game_scores = (
        event_frame.group_by("game_id")
        .agg(
            pl.len().alias("events"),
            pl.col("gain_log_loss").sum(),
            pl.col("gain_brier").sum(),
        )
        .with_columns(
            pl.lit(pipeline).alias("pipeline"), pl.lit(season).alias("season")
        )
        .sort("game_id")
    )
    fit_id = fits[CANDIDATE].fit_id
    calibration = class_calibration(candidate, labels)
    shares = share_coverage(predictions[CANDIDATE], held, labels, fit_id=fit_id)
    game_coverage, game_table = game_count_coverage(
        predictions[CANDIDATE], held, labels, fit_id=fit_id
    )
    summary: dict[str, object] = {
        "season": season,
        "events": held.height,
        "games": held["game_id"].n_unique(),
        "calibration_supported": held.height >= CALIBRATION_MIN_EVENTS,
        "calibration": calibration,
        "share_coverage": shares,
        "game_coverage": game_coverage,
        "prior_only_events": int(
            (event_frame["candidate_cell_status"] == "prior_only_cell").sum()
        ),
        "mean_gain_log_loss": float(log_gain.mean()),
        "mean_gain_brier": float(brier_gain.mean()),
        "fits": {arm: fit_summary(fit) for arm, fit in fits.items()},
    }
    return HeldOutResult(
        season=season,
        summary=summary,
        predictions=event_frame,
        game_scores=game_scores,
        game_coverage=game_table.with_columns(
            pl.lit(pipeline).alias("pipeline"), pl.lit(season).alias("season")
        ),
    )


def _fraction(flags: list[bool]) -> float:
    return float(np.mean(flags)) if flags else float("nan")


def pipeline_screens(
    held_out: list[HeldOutResult],
    fits: dict[Arm, PipelineFit],
    *,
    repetitions: int,
) -> dict[str, object]:
    calibration_pass = True
    calibration_checks: list[dict[str, object]] = []
    covered_95: list[bool] = []
    covered_90: list[bool] = []
    for result in held_out:
        supported = bool(result.summary["calibration_supported"])
        for row in cast(list[dict[str, object]], result.summary["calibration"]):
            ece = float(cast(float, row["ece"]))
            bias = abs(float(cast(float, row["class_share_bias"])))
            passed = ece <= ECE_LIMIT and bias <= BIAS_LIMIT
            calibration_checks.append(
                {"season": result.season, **row, "supported": supported, "pass": passed}
            )
            if supported and not passed:
                calibration_pass = False
        for row in cast(list[dict[str, object]], result.summary["share_coverage"]):
            covered_95.append(bool(row["covered_95"]))
            covered_90.append(bool(row["covered_90"]))
    share_95 = _fraction(covered_95)
    share_90 = _fraction(covered_90)
    game_coverage = {
        label: _fraction(
            [
                bool(row)
                for result in held_out
                for row in result.game_coverage[f"covered_{label}"].to_list()
            ]
        )
        for label in CLASSES
    }
    gain = paired_game_gain(
        pl.concat([result.game_scores for result in held_out]),
        repetitions=repetitions,
        seed=ROOT_SEED,
    )
    concentration_pass = all(
        fit.lower_boundary_mass <= BOUNDARY_MASS_LIMIT for fit in fits.values()
    )
    holdout_concentration_pass = all(
        bool(cast(dict[str, object], arm_summary)["concentration_pass"])
        for result in held_out
        for arm_summary in cast(dict[str, object], result.summary["fits"]).values()
    )
    screens = {
        "calibration": calibration_pass,
        "share_coverage": share_95 >= SHARE_COVERAGE_95_MIN
        and share_90 >= SHARE_COVERAGE_90_MIN,
        "game_coverage": all(
            value >= GAME_COVERAGE_MIN for value in game_coverage.values()
        ),
        "gain": all(gain[metric]["lower_95"] > 0 for metric in ("log_loss", "brier")),
        "concentration": concentration_pass and holdout_concentration_pass,
    }
    return {
        "calibration_checks": calibration_checks,
        "share_coverage_95": share_95,
        "share_coverage_90": share_90,
        "share_checks": len(covered_95),
        "game_coverage": game_coverage,
        "gain": gain,
        "screens": screens,
        "pass": all(screens[name] for name in SCREENS),
    }


def predictive_tables(fit: PipelineFit, frame: pl.DataFrame) -> dict[str, object]:
    cells_frame = frame.select("recorded_air_subtype", "result_family").unique()
    new_season = predict_pipeline(fit, cells_frame, season=None)
    tables: dict[str, object] = {}
    for cell in new_season.cells:
        entry: dict[str, object] = {
            "status": cell.status,
            "new_season": {
                label: {
                    "mean": float(cell.draws[:, index].mean()),
                    "lower_95": _interval(cell.draws[:, index], 0.95)[0],
                    "upper_95": _interval(cell.draws[:, index], 0.95)[1],
                }
                for index, label in enumerate(CLASSES)
            },
            "seasons": {},
        }
        tables["|".join(cell.values)] = entry
    for season in fit.seasons:
        referenced = predict_pipeline(fit, cells_frame, season=season)
        for cell in referenced.cells:
            entry = cast(dict[str, object], tables["|".join(cell.values)])
            cast(dict[str, object], entry["seasons"])[str(season)] = {
                label: float(cell.draws[:, index].mean())
                for index, label in enumerate(CLASSES)
            }
    return tables


def unreferenced_seasons(pipeline: str) -> list[int]:
    first, last = PIPELINES[pipeline]
    last = min(last, 2025)
    referenced = set(REFERENCE_SEASONS[pipeline])
    return [season for season in range(first, last + 1) if season not in referenced]


def run_pipeline(
    frame: pl.DataFrame,
    *,
    pipeline: str,
    checkpoint_dir: Path,
    draws: int,
    repetitions: int,
) -> tuple[dict[str, object], list[HeldOutResult]]:
    seasons = REFERENCE_SEASONS[pipeline]
    subset = frame.filter(pl.col("pipeline") == pipeline)
    if set(cast(list[int], subset["season"].unique().to_list())) != set(seasons):
        raise ValueError(f"pipeline {pipeline} reference seasons are incomplete")
    fits: dict[Arm, PipelineFit] = {
        arm: load_or_fit(
            checkpoint_dir,
            subset,
            pipeline=pipeline,
            arm=arm,
            seasons=seasons,
            fit_id=f"{pipeline}-{arm}-full",
            draws=draws,
        )
        for arm in ARMS
    }
    held_out = [
        evaluate_held_out(
            subset,
            pipeline=pipeline,
            seasons=seasons,
            season=season,
            checkpoint_dir=checkpoint_dir,
            draws=draws,
        )
        for season in seasons
    ]
    screens = pipeline_screens(held_out, fits, repetitions=repetitions)
    summary: dict[str, object] = {
        "pipeline": pipeline,
        "reference_seasons": list(seasons),
        "events": subset.height,
        "games": subset["game_id"].n_unique(),
        "full_fits": {arm: fit_summary(fit) for arm, fit in fits.items()},
        "held_out": [result.summary for result in held_out],
        **screens,
        "predictive": predictive_tables(fits[CANDIDATE], subset),
        "applies_to_seasons": unreferenced_seasons(pipeline),
    }
    return summary, held_out


def _empty_bounds() -> dict[str, object]:
    return {
        "feasible": False,
        "translation_lower": None,
        "translation_upper": None,
        "band_mix_lower": None,
        "band_mix_upper": None,
        "max_width": None,
    }


def _set_summary(identified: IdentifiedSet) -> dict[str, object]:
    if not identified.feasible:
        return _empty_bounds()
    lower, upper = identified.translation_bounds()
    mix_lower, mix_upper = identified.band_mix_bounds()
    return {
        "feasible": True,
        "translation_lower": lower.tolist(),
        "translation_upper": upper.tolist(),
        "band_mix_lower": mix_lower.tolist(),
        "band_mix_upper": mix_upper.tolist(),
        "max_width": float((upper - lower).max()),
    }


def minimal_slack_multiplier(
    label_shares: FloatArray,
    clue_margin: FloatArray,
    clue_given_band: FloatArray,
    slack: FloatArray,
    *,
    ceiling: float = 64.0,
) -> float | None:
    if feasible(label_shares, clue_margin, clue_given_band, slack):
        return 1.0
    if not feasible(label_shares, clue_margin, clue_given_band, slack * ceiling):
        return None
    low, high = 1.0, ceiling
    while high / low > 1.02:
        middle = float(np.sqrt(low * high))
        if feasible(label_shares, clue_margin, clue_given_band, slack * middle):
            high = middle
        else:
            low = middle
    return high


def bounds_section(
    reference: pl.DataFrame,
    referenced_counts: pl.DataFrame,
    historical_counts: pl.DataFrame,
    *,
    draws: int,
) -> dict[str, object]:
    if set(REFERENCE_SEASONS) != {"B", "C"}:
        raise ValueError("the slack is declared from exactly pipelines B and C")
    tables = {
        pipeline: clue_table(reference.filter(pl.col("pipeline") == pipeline))
        for pipeline in REFERENCE_SEASONS
    }
    slack = clue_slack(tables["B"], tables["C"])
    validation: list[dict[str, object]] = []
    all_inside = True
    for pipeline, seasons in REFERENCE_SEASONS.items():
        other = tables["C" if pipeline == "B" else "B"]
        for season in seasons:
            season_frame = reference.filter(pl.col("season") == season)
            counts = referenced_counts.filter(pl.col("season") == season)
            shares, margin, total = season_margins(counts)
            identified = identified_set(shares, margin, other, slack)
            check_entry: dict[str, object] = {
                "pipeline": pipeline,
                "season": season,
                "clue_table": "C" if pipeline == "B" else "B",
                "recorded_events": total,
                **_set_summary(identified),
            }
            missing = set(CLASSES) - set(season_frame["recorded_air_subtype"].to_list())
            if missing:
                check_entry.update(
                    {
                        "status": "missing_recorded_subtype",
                        "missing_recorded_subtypes": sorted(missing),
                        "truth": None,
                        "inside": False,
                        "max_violation": None,
                    }
                )
                all_inside = False
            else:
                truth = band_given_recorded(season_frame)
                check = validate_bounds(identified, truth)
                inside = identified.feasible and bool(check.inside.all())
                all_inside = all_inside and inside
                check_entry.update(
                    {
                        "status": "checked",
                        "missing_recorded_subtypes": [],
                        "truth": truth.tolist(),
                        "inside": inside,
                        "max_violation": check.max_violation,
                    }
                )
            validation.append(check_entry)
    first, last = PIPELINES["A"]
    table = tables[BOUNDS_CLUE_PIPELINE]
    pipeline_a: dict[str, object] = {}
    for season in range(first, last + 1):
        counts = historical_counts.filter(pl.col("season") == season)
        if counts.is_empty():
            pipeline_a[str(season)] = {
                "status": "no_recorded_labels",
                "recorded_events": 0,
                "label_shares": None,
                "minimal_slack_multiplier": None,
                **_empty_bounds(),
            }
            continue
        shares, margin, total = season_margins(counts)
        identified = identified_set(shares, margin, table, slack)
        entry: dict[str, object] = {
            "status": "identified_set" if identified.feasible else "empty_set",
            "recorded_events": total,
            "label_shares": shares.tolist(),
            **_set_summary(identified),
            "minimal_slack_multiplier": 1.0
            if identified.feasible
            else minimal_slack_multiplier(shares, margin, table, slack),
        }
        if identified.feasible:
            prior = sample_identified_set(identified, draws=draws, seed=ROOT_SEED)
            entry["prior_mean_translation"] = (
                prior[:, : len(CLASSES) ** 2]
                .reshape(draws, len(CLASSES), len(CLASSES))
                .mean(axis=0)
                .tolist()
            )
            entry["prior_mean_band_mix"] = (
                prior[:, len(CLASSES) ** 2 :].mean(axis=0).tolist()
            )
        pipeline_a[str(season)] = entry
    statuses = {
        season: str(cast(dict[str, object], entry)["status"])
        for season, entry in pipeline_a.items()
    }
    bounded = [
        season for season, status in statuses.items() if status == "identified_set"
    ]
    return {
        "clue_levels": list(CLUE_LEVELS),
        "clue_tables": {name: table.tolist() for name, table in tables.items()},
        "slack": slack.tolist(),
        "validation": validation,
        "assumptions_validated": all_inside,
        "pipeline_a": pipeline_a,
        "pipeline_a_bounded_seasons": bounded,
        "pipeline_a_unbounded_seasons": [
            season for season, status in statuses.items() if status == "empty_set"
        ],
        "pipeline_a_untested_seasons": [
            season
            for season, status in statuses.items()
            if status == "no_recorded_labels"
        ],
    }


def run(
    output: Path,
    *,
    smoke: bool,
    draws: int | None = None,
    repetitions: int | None = None,
    protocol: Path = PROTOCOL_PATH,
    coverage_frame: Path = COVERAGE_FRAME,
    acquisition_root: Path = ACQUISITION_ROOT,
    database: Path = DATABASE,
    reserve: Path = RESERVE,
) -> dict[str, object]:
    draws = draws if draws is not None else (SMOKE_DRAWS if smoke else FULL_DRAWS)
    repetitions = (
        repetitions
        if repetitions is not None
        else (SMOKE_REPETITIONS if smoke else FULL_REPETITIONS)
    )
    bounds_draws = SMOKE_DRAWS if smoke else BOUNDS_DRAWS
    output.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = output / "checkpoints"
    checkpoint_dir.mkdir(exist_ok=True)
    bound = bindings(
        smoke=smoke,
        draws=draws,
        bounds_draws=bounds_draws,
        repetitions=repetitions,
        protocol=protocol,
        coverage_frame=coverage_frame,
        acquisition_root=acquisition_root,
        database=database,
        reserve=reserve,
    )
    inputs_identity = {
        "smoke": smoke,
        "coverage_frame": bound["coverage_frame"],
        "acquisition": bound["acquisition"],
        "database": bound["database"],
        "reserve": bound["reserve"],
    }
    identity_path = output / "inputs.json"
    if identity_path.is_file():
        if read_json(identity_path) != inputs_identity:
            raise ValueError(f"{output} was produced from different inputs")
    else:
        write_json(identity_path, inputs_identity)
    write_json(output / "bindings.json", bound)
    reference_path = output / "reference_frame.parquet"
    if reference_path.is_file():
        reference = pl.read_parquet(reference_path)
    else:
        reference = load_reference_frame(
            coverage_frame=coverage_frame,
            acquisition_root=acquisition_root,
            database=database,
        )
        if smoke:
            reference = smoke_subsample(reference, fold_count=SMOKE_GAME_FOLDS)
        reference.write_parquet(reference_path)
    LOGGER.info("reference frame: %d rows", reference.height)
    counts_path = output / "historical_counts.parquet"
    if counts_path.is_file():
        historical = pl.read_parquet(counts_path)
    else:
        historical = load_historical_counts(
            (PIPELINES["A"][0], 2025), database=database, reserve=reserve
        )
        historical.write_parquet(counts_path)
    pipelines: dict[str, object] = {}
    held_out_frames: list[pl.DataFrame] = []
    game_score_frames: list[pl.DataFrame] = []
    game_coverage_frames: list[pl.DataFrame] = []
    for pipeline in REFERENCE_SEASONS:
        summary, held_out = run_pipeline(
            reference,
            pipeline=pipeline,
            checkpoint_dir=checkpoint_dir,
            draws=draws,
            repetitions=repetitions,
        )
        pipelines[pipeline] = summary
        held_out_frames.extend(result.predictions for result in held_out)
        game_score_frames.extend(result.game_scores for result in held_out)
        game_coverage_frames.extend(result.game_coverage for result in held_out)
        write_json(output / f"pipeline_{pipeline}.json", summary)
    pl.concat(held_out_frames).write_parquet(output / "held_out_predictions.parquet")
    pl.concat(game_score_frames).write_parquet(output / "game_scores.parquet")
    pl.concat(game_coverage_frames).write_parquet(output / "game_coverage.parquet")
    referenced_seasons = [
        season for values in REFERENCE_SEASONS.values() for season in values
    ]
    bounds = bounds_section(
        reference,
        historical.filter(pl.col("season").is_in(referenced_seasons)),
        historical.filter(pl.col("season") <= PIPELINES["A"][1]),
        draws=bounds_draws,
    )
    write_json(output / "bounds.json", bounds)
    status = SMOKE_STATUS if smoke else FULL_STATUS
    decision: dict[str, object] = {
        "status": status,
        "makes_decision": not smoke,
        "pipeline_pass": {
            name: bool(cast(dict[str, object], summary)["pass"])
            for name, summary in pipelines.items()
        },
        "bounds_assumptions_validated": bounds["assumptions_validated"],
        "pipeline_a_bounded_seasons": bounds["pipeline_a_bounded_seasons"],
    }
    report: dict[str, object] = {
        "experiment": EXPERIMENT_ID,
        "status": status,
        "bindings": bound,
        "reference_rows": reference.height,
        "reference_games": reference["game_id"].n_unique(),
        "pipelines": pipelines,
        "bounds": bounds,
        "decision": decision,
    }
    write_json(output / "report.json", report)
    write_manifest(output, status, experiment=EXPERIMENT_ID, decision=decision)
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--output", type=Path, required=True)
    _ = parser.add_argument("--smoke", action="store_true")
    _ = parser.add_argument("--draws", type=int, default=None)
    _ = parser.add_argument("--repetitions", type=int, default=None)
    arguments = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    report = run(
        cast(Path, arguments.output),
        smoke=bool(arguments.smoke),
        draws=cast(int | None, arguments.draws),
        repetitions=cast(int | None, arguments.repetitions),
    )
    LOGGER.info("decision %s", json.dumps(report["decision"], sort_keys=True))


if __name__ == "__main__":
    main()
