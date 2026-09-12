from __future__ import annotations

import argparse
import hashlib
import json
import logging
from pathlib import Path
from typing import Literal, cast

import numpy as np
import numpy.typing as npt
import polars as pl

from python_models.statistical.air_trajectory_translation import ESTIMATE_ID
from python_models.statistical.backtests.geometry_air_pipeline_data import (
    CLASSES,
    PIPELINES,
    REFERENCE_SEASONS,
    pipeline_of,
)
from python_models.statistical.backtests.geometry_air_pipeline_model import (
    PipelineFit,
    predict_pipeline,
)
from python_models.statistical.backtests.geometry_reliability_data import write_json
from python_models.statistical.evidence_binding import file_digest

LOGGER = logging.getLogger(__name__)
FloatArray = npt.NDArray[np.float64]
Basis = Literal[
    "referenced_season_posterior",
    "pipeline_new_season_predictive",
    "raked_from_pipeline_c_with_modern_mix",
]

RESULT_FAMILIES = (
    "hit",
    "out_in_play",
    "sacrifice",
    "reached_on_error",
    "fielders_choice",
)
LAST_SEASON = 2025
MODERN_MIX_CONCENTRATION = 2000.0
RAKING_ITERATIONS = 200
RAKING_TOLERANCE = 1e-10
ROOT_SEED = 20260911
SEED_PIPELINE = "C"


def _seed(*parts: object) -> int:
    payload = json.dumps(list(parts), ensure_ascii=True, separators=(",", ":"))
    return int.from_bytes(hashlib.sha256(payload.encode()).digest()[:8], "big")


def cells_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "recorded_air_subtype": [
                recorded for recorded in CLASSES for _ in RESULT_FAMILIES
            ],
            "result_family": [result for _ in CLASSES for result in RESULT_FAMILIES],
        }
    )


def rake(seed: FloatArray, rows: FloatArray, columns: FloatArray) -> FloatArray:
    if seed.shape != (rows.size, columns.size) or (seed < 0).any():
        raise ValueError("raking needs a nonnegative seed matrix matching both margins")
    if (seed.sum(axis=1) <= 0).any() or (seed.sum(axis=0) <= 0).any():
        raise ValueError("every seed row and column needs positive mass")
    if not np.isclose(rows.sum(), columns.sum()):
        raise ValueError("row and column margins must have the same total")
    matrix = seed.copy()
    column_factors = np.ones(columns.size)
    for _ in range(RAKING_ITERATIONS):
        row_scale = rows / matrix.sum(axis=1)
        matrix *= row_scale[:, None]
        column_scale = columns / matrix.sum(axis=0)
        matrix *= column_scale[None, :]
        column_factors *= column_scale
        if np.abs(matrix.sum(axis=1) - rows).max() < RAKING_TOLERANCE:
            break
    return column_factors


def raked_translation(
    seed_translation: FloatArray, label_shares: FloatArray, band_mix: FloatArray
) -> tuple[FloatArray, FloatArray]:
    factors = rake(label_shares[:, None] * seed_translation, label_shares, band_mix)
    adjusted = seed_translation * factors[None, :]
    return adjusted / adjusted.sum(axis=1, keepdims=True), factors


def modern_mix_draws(
    reference: pl.DataFrame, *, draws: int, seed: int
) -> tuple[FloatArray, FloatArray]:
    counts = (
        reference.group_by("season", "target_class")
        .len()
        .pivot("target_class", index="season", values="len")
        .fill_null(0)
        .sort("season")
    )
    shares = counts.select(list(CLASSES)).to_numpy().astype(np.float64)
    shares /= shares.sum(axis=1, keepdims=True)
    mean = shares.mean(axis=0)
    rng = np.random.default_rng(seed)
    return mean, rng.dirichlet(MODERN_MIX_CONCENTRATION * mean, size=draws)


def label_shares_by_season(historical: pl.DataFrame) -> dict[int, FloatArray]:
    totals = (
        historical.group_by("season", "recorded_air_subtype")
        .agg(pl.col("n").sum())
        .pivot("recorded_air_subtype", index="season", values="n")
        .fill_null(0)
        .sort("season")
    )
    output: dict[int, FloatArray] = {}
    for row in totals.iter_rows(named=True):
        counts = np.array([row[label] for label in CLASSES], dtype=np.float64)
        if counts.sum() == 0:
            continue
        output[int(cast(int, row["season"]))] = counts / counts.sum()
    return output


def summarize(draws: FloatArray) -> dict[str, dict[str, float]]:
    lower, upper = np.quantile(draws, [0.025, 0.975], axis=0)
    return {
        label: {
            "mean": float(draws[:, index].mean()),
            "lower_95": float(lower[index]),
            "upper_95": float(upper[index]),
        }
        for index, label in enumerate(CLASSES)
    }


def _cell_key(values: tuple[str, ...]) -> str:
    return "|".join(values)


def referenced_pipeline_estimates(
    fit: PipelineFit, *, first: int, last: int
) -> dict[str, dict[str, object]]:
    frame = cells_frame()
    output: dict[str, dict[str, object]] = {}
    new_season = predict_pipeline(fit, frame, season=None)
    for season in range(first, last + 1):
        if season in fit.seasons:
            prediction = predict_pipeline(fit, frame, season=season)
            basis: Basis = "referenced_season_posterior"
        else:
            prediction = new_season
            basis = "pipeline_new_season_predictive"
        output[str(season)] = {
            "pipeline": fit.pipeline,
            "basis": basis,
            "cells": {
                _cell_key(cell.values): {
                    "status": cell.status,
                    **summarize(cell.draws),
                }
                for cell in prediction.cells
            },
        }
    return output


def raked_pipeline_estimates(
    recorded_fit: PipelineFit,
    cell_fit: PipelineFit,
    *,
    label_shares: dict[int, FloatArray],
    band_mix: FloatArray,
    first: int,
    last: int,
) -> dict[str, dict[str, object]]:
    recorded_frame = pl.DataFrame({"recorded_air_subtype": list(CLASSES)}).with_columns(
        pl.lit("out_in_play").alias("result_family")
    )
    recorded = predict_pipeline(recorded_fit, recorded_frame, season=None)
    by_recorded = {cell.values[0]: cell.draws for cell in recorded.cells}
    seed_draws = np.stack([by_recorded[label] for label in CLASSES], axis=1)
    cells = predict_pipeline(cell_fit, cells_frame(), season=None)
    draws = band_mix.shape[0]
    if seed_draws.shape[0] != draws or any(
        cell.draws.shape[0] != draws for cell in cells.cells
    ):
        raise ValueError("seed fits and mix draws must share the draw count")
    output: dict[str, dict[str, object]] = {}
    for season in range(first, last + 1):
        shares = label_shares.get(season)
        if shares is None:
            LOGGER.warning("season %d has no recorded airborne labels", season)
            continue
        translations = np.empty_like(seed_draws)
        factors = np.empty((draws, len(CLASSES)))
        for draw in range(draws):
            translations[draw], factors[draw] = raked_translation(
                seed_draws[draw], shares, band_mix[draw]
            )
        cell_summaries: dict[str, object] = {}
        for cell in cells.cells:
            adjusted = cell.draws * factors
            adjusted /= adjusted.sum(axis=1, keepdims=True)
            cell_summaries[_cell_key(cell.values)] = {
                "status": cell.status,
                **summarize(adjusted),
            }
        output[str(season)] = {
            "pipeline": "A",
            "basis": "raked_from_pipeline_c_with_modern_mix",
            "label_shares": shares.tolist(),
            "recorded": {
                label: summarize(translations[:, index, :])
                for index, label in enumerate(CLASSES)
            },
            "column_factors": summarize(factors),
            "cells": cell_summaries,
        }
    return output


def load_fit(run_root: Path, fit_id: str) -> PipelineFit:
    return PipelineFit.model_validate_json(
        (run_root / "checkpoints" / f"{fit_id}.json").read_text()
    )


def run(run_root: Path, output: Path) -> dict[str, object]:
    reference = pl.read_parquet(run_root / "reference_frame.parquet")
    historical = pl.read_parquet(run_root / "historical_counts.parquet")
    fits = {
        name: load_fit(run_root, name)
        for name in (
            "B-recorded_result-full",
            "C-recorded_result-full",
            "C-recorded-full",
        )
    }
    draws = fits["C-recorded-full"].draws
    mix_mean, mix_draws = modern_mix_draws(
        reference, draws=draws, seed=_seed(ESTIMATE_ID, "modern_mix", ROOT_SEED)
    )
    seasons: dict[str, dict[str, object]] = {}
    seasons.update(
        raked_pipeline_estimates(
            fits["C-recorded-full"],
            fits["C-recorded_result-full"],
            label_shares=label_shares_by_season(historical),
            band_mix=mix_draws,
            first=PIPELINES["A"][0],
            last=PIPELINES["A"][1],
        )
    )
    seasons.update(
        referenced_pipeline_estimates(
            fits["B-recorded_result-full"],
            first=PIPELINES["B"][0],
            last=PIPELINES["B"][1],
        )
    )
    seasons.update(
        referenced_pipeline_estimates(
            fits["C-recorded_result-full"], first=PIPELINES["C"][0], last=LAST_SEASON
        )
    )
    for season in seasons:
        if pipeline_of(int(season)) != seasons[season]["pipeline"]:
            raise RuntimeError(f"season {season} landed in the wrong pipeline")
    report: dict[str, object] = {
        "estimate": ESTIMATE_ID,
        "run_root": str(run_root),
        "run_manifest_sha256": file_digest(run_root / "manifest.json"),
        "draws": draws,
        "assumptions": {
            "pre_2009_true_band_mix": {
                "mean": mix_mean.tolist(),
                "concentration": MODERN_MIX_CONCENTRATION,
                "source_seasons": sorted(
                    season for values in REFERENCE_SEASONS.values() for season in values
                ),
            },
            "pre_2009_seed_pipeline": SEED_PIPELINE,
            "pre_2009_method": "iterative proportional fitting of the seed translation to the season's recorded label shares and the modern band mix; column factors applied to the seed's recorded-by-result cells",
        },
        "seasons": dict(sorted(seasons.items())),
    }
    output.mkdir(parents=True, exist_ok=True)
    write_json(output / "translation_estimate.json", report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--run-root", type=Path, required=True)
    _ = parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    report = run(cast(Path, arguments.run_root), cast(Path, arguments.output))
    LOGGER.info("wrote %d seasons", len(cast(dict[str, object], report["seasons"])))


if __name__ == "__main__":
    main()
