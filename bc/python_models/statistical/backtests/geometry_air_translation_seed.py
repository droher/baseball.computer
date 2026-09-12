from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import cast

import polars as pl

from python_models.statistical.air_trajectory_translation import (
    BASES,
    CELL_STATUSES,
    ESTIMATE_ID,
    PARTIALLY_IDENTIFIED_BASIS,
    RECORDED_AIR_SUBTYPES,
    RESULT_FAMILIES,
    RUN_MANIFEST_SHA256,
    SEED_COLUMNS,
    STANDARDIZED_AIR_SUBTYPES,
)

LOGGER = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[4]
ESTIMATE_PATH = REPO_ROOT / "docs" / "geometry-air-translation-estimate-2026-09-11.json"
SEED_PATH = (
    REPO_ROOT / "bc" / "seeds" / "batted_ball" / "seed_air_trajectory_translation.csv"
)
SEED_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int16(),
    "recorded_air_subtype": pl.Utf8(),
    "result_family": pl.Utf8(),
    "standardized_air_subtype": pl.Utf8(),
    "probability_mean": pl.Float64(),
    "probability_lower_95": pl.Float64(),
    "probability_upper_95": pl.Float64(),
    "basis": pl.Utf8(),
    "cell_status": pl.Utf8(),
    "partially_identified": pl.Boolean(),
}


def load_report(path: Path = ESTIMATE_PATH) -> dict[str, object]:
    report = cast(dict[str, object], json.loads(path.read_text()))
    if report.get("estimate") != ESTIMATE_ID:
        raise ValueError(f"{path} holds estimate {report.get('estimate')!r}")
    if report.get("run_manifest_sha256") != RUN_MANIFEST_SHA256:
        raise ValueError(f"{path} was built from a different translation run")
    return report


def seed_frame(report: dict[str, object]) -> pl.DataFrame:
    seasons = cast(dict[str, dict[str, object]], report["seasons"])
    rows: list[dict[str, object]] = []
    for season_key, season in seasons.items():
        basis = cast(str, season["basis"])
        if basis not in BASES:
            raise ValueError(f"season {season_key} has unknown basis {basis!r}")
        cells = cast(dict[str, dict[str, object]], season["cells"])
        for recorded in RECORDED_AIR_SUBTYPES:
            for result in RESULT_FAMILIES:
                cell = cells[f"{recorded}|{result}"]
                status = cast(str, cell["status"])
                if status not in CELL_STATUSES:
                    raise ValueError(f"cell {recorded}|{result} has status {status!r}")
                for band in STANDARDIZED_AIR_SUBTYPES:
                    summary = cast(dict[str, float], cell[band])
                    rows.append(
                        {
                            "season": int(season_key),
                            "recorded_air_subtype": recorded,
                            "result_family": result,
                            "standardized_air_subtype": band,
                            "probability_mean": summary["mean"],
                            "probability_lower_95": summary["lower_95"],
                            "probability_upper_95": summary["upper_95"],
                            "basis": basis,
                            "cell_status": status,
                            "partially_identified": basis == PARTIALLY_IDENTIFIED_BASIS,
                        }
                    )
    return (
        pl.DataFrame(rows, schema=SEED_SCHEMA)
        .select(list(SEED_COLUMNS))
        .sort(
            "season",
            "recorded_air_subtype",
            "result_family",
            "standardized_air_subtype",
        )
    )


def read_seed(path: Path = SEED_PATH) -> pl.DataFrame:
    return pl.read_csv(path, schema=SEED_SCHEMA).select(list(SEED_COLUMNS))


def write_seed(report_path: Path = ESTIMATE_PATH, seed_path: Path = SEED_PATH) -> int:
    frame = seed_frame(load_report(report_path))
    frame.write_csv(seed_path, float_precision=12)
    LOGGER.info("wrote %d rows to %s", frame.height, seed_path)
    return frame.height


def main() -> None:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--estimate", type=Path, default=ESTIMATE_PATH)
    _ = parser.add_argument("--seed", type=Path, default=SEED_PATH)
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    _ = write_seed(cast(Path, arguments.estimate), cast(Path, arguments.seed))


if __name__ == "__main__":
    main()
