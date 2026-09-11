from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import shutil
from pathlib import Path
from typing import cast

import duckdb
import polars as pl

from python_models.statistical.evidence_binding import file_digest

LOGGER = logging.getLogger(__name__)
REPOSITORY = Path(__file__).resolve().parents[4]
BASE = REPOSITORY / "artifacts/statistical/backtests"
DATABASE = (
    REPOSITORY / "artifacts/statistical/research/geometry-v2-global-side-20260911/bc.db"
)
SCHEMA = "main_models__geometry_v2_research_20260911"
FOLD_PREFIX = "geometry-reliability-inner-v1:"


def read_json(path: Path) -> dict[str, object]:
    return cast(dict[str, object], json.loads(path.read_text()))


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def inner_fold(game_id: str) -> str:
    value = hashlib.sha256((FOLD_PREFIX + game_id).encode()).digest()
    return (
        "inner_evaluation" if int.from_bytes(value[:8], "big") % 5 == 0 else "inner_fit"
    )


def source_root(target: str) -> Path:
    versions = {
        "trajectory": "20260911-full-v1",
        "location_side": "20260911-global-side-full-v1",
    }
    if target not in versions:
        raise ValueError(f"unsupported target: {target}")
    return BASE / "geometry_reference" / versions[target] / target


def train_query(query: str) -> str:
    boundary = "primary_fold IN ('TRAIN', 'TEST')"
    if query.count(boundary) != 1:
        raise ValueError("source query has no unique known partition boundary")
    query = query.replace(boundary, "primary_fold = 'TRAIN'")
    query, replacements = re.subn(
        r"FROM (?:main_models|main_models__geometry_v2_research_20260911)\.model_input_geometry",
        f"FROM {SCHEMA}.model_input_geometry",
        query,
    )
    if replacements != 1 or "VALIDATE" in query or "TEST" in query:
        raise ValueError("source query is not the declared TRAIN-only geometry query")
    return query


def validate_frames(
    full: pl.DataFrame,
    selected: pl.DataFrame,
    expected_counts: pl.DataFrame,
    reserved_games: set[str],
) -> dict[str, int]:
    if set(full["primary_fold"].unique().to_list()) != {"TRAIN"}:
        raise ValueError("development frame contains non-TRAIN rows")
    if full["event_key"].n_unique() != full.height:
        raise ValueError("development event keys are not unique")
    if set(str(value) for value in full["game_id"]) & reserved_games:
        raise ValueError("development frame overlaps the sealed reserve")
    columns = selected.columns
    missing = selected.join(full.select(columns), on=columns, how="anti")
    if missing.height:
        raise ValueError("selected TRAIN differs from the frozen source")
    keys = ["era", "result_family", "target_class"]
    counts = full.group_by(keys).len(name="n").with_columns(pl.col("n").cast(pl.Int64))
    expected = expected_counts.with_columns(pl.col("n").cast(pl.Int64))
    if (
        counts.join(expected, on=[*keys, "n"], how="anti").height
        or expected.join(counts, on=[*keys, "n"], how="anti").height
    ):
        raise ValueError("full TRAIN class counts differ from the frozen source")
    return {
        "full_train_rows": full.height,
        "full_train_games": full["game_id"].n_unique(),
        "selected_train_rows": selected.height,
        "selected_row_mismatches": missing.height,
        "reserved_game_overlap": 0,
    }


def freeze(target: str, output: Path, *, smoke: bool) -> None:
    root = source_root(target)
    lineage_path = root / "data/lineage.json"
    lineage = read_json(lineage_path)
    data_report = read_json(root / "report.json")
    inventory = cast(dict[str, str], data_report["artifact_files"])
    if file_digest(lineage_path) != inventory["data/lineage.json"]:
        raise ValueError("original lineage changed")
    expected = cast(dict[str, str], lineage["snapshot_files"])
    for name in ("train.parquet", "full_train_counts.parquet"):
        if file_digest(root / "data" / name) != expected[name]:
            raise ValueError("original training inputs changed")
    original_query = str(lineage["source_query"])
    if (
        hashlib.sha256(original_query.encode()).hexdigest()
        != lineage["source_query_sha256"]
    ):
        raise ValueError("original source query changed")
    query = train_query(original_query)
    selected = pl.read_parquet(root / "data/train.parquet")
    counts = pl.read_parquet(root / "data/full_train_counts.parquet")
    inputs = read_json(REPOSITORY / "docs/historical-geometry-stress-inputs.json")
    reservation = read_json(
        BASE / "historical_stress/20260911-reserve-v1/verification.json"
    )
    reserve_path = BASE / "historical_stress/20260911-reserve-v1/reserved_games.parquet"
    if (
        reservation["reserved_digest"] != inputs["reserve_game_digest"]
        or file_digest(reserve_path) != reservation["reserved_game_file_sha256"]
    ):
        raise ValueError("reserve differs from the trusted boundary")
    reserve = set(str(value) for value in pl.read_parquet(reserve_path)["game_id"])
    output.mkdir(parents=True, exist_ok=False)
    (output / "source_query.sql").write_text(query + "\n")
    shutil.copy2(Path(__file__), output / "geometry_reliability_data.py")
    limit = " LIMIT 5000" if smoke else ""
    LOGGER.info("reading %s primary TRAIN smoke=%s", target, smoke)
    with duckdb.connect(str(DATABASE), read_only=True) as con:
        con.execute("SET threads=4")
        full = con.execute(query + limit).pl()
        con.register("development_games", full.select("game_id").unique())
        metadata = con.execute(
            """SELECT game_id, LIST_SORT(LIST(DISTINCT TRIM(cleaned_scorer))) AS cleaned_scorers
                FROM main_models.game_scorekeeping INNER JOIN development_games USING (game_id)
                WHERE NULLIF(TRIM(cleaned_scorer), '') IS NOT NULL
                GROUP BY game_id"""
        ).pl()
    if smoke:
        if set(full["primary_fold"].unique().to_list()) != {"TRAIN"}:
            raise ValueError("smoke includes a forbidden fold")
        verification: dict[str, int] = {"smoke_rows": full.height}
    else:
        verification = validate_frames(full, selected, counts, reserve)
    games = full.select("game_id").unique().sort("game_id")
    games = games.with_columns(
        pl.Series("inner_fold", [inner_fold(str(game)) for game in games["game_id"]])
    ).join(metadata, on="game_id", how="left")
    games = games.with_columns(
        pl.col("cleaned_scorers").fill_null(pl.lit([], dtype=pl.List(pl.String))),
    ).with_columns(
        pl.when(pl.col("cleaned_scorers").list.len() > 0)
        .then(pl.col("cleaned_scorers").list.join("|"))
        .otherwise(pl.lit("__missing__"))
        .alias("scorer")
    )
    full = full.join(games, on="game_id", how="left").sort("event_key")
    if full["event_key"].n_unique() != full.height or full["inner_fold"].null_count():
        raise ValueError("game metadata caused duplicate or unassigned rows")
    full.write_parquet(output / "full_primary_train.parquet")
    selected.join(games, on="game_id", how="inner").sort("event_key").write_parquet(
        output / "selected_train.parquet"
    )
    games.write_parquet(output / "games.parquet")
    report = {
        "target": target,
        "smoke": smoke,
        "source_database": str(DATABASE),
        "source_query_sha256": file_digest(output / "source_query.sql"),
        "original_source_query_sha256": lineage["source_query_sha256"],
        "original_lineage_sha256": file_digest(lineage_path),
        "original_snapshot_files": expected,
        "reservation": reservation,
        "verification": verification,
        "split_rule": FOLD_PREFIX
        + "game_id; sha256 first8 unsigned big-endian mod5 ==0",
        "partition_counts": full.group_by("inner_fold")
        .agg(pl.len().alias("events"), pl.col("game_id").n_unique().alias("games"))
        .sort("inner_fold")
        .to_dicts(),
        "era_counts": full.group_by("era", "inner_fold")
        .len()
        .sort("era", "inner_fold")
        .to_dicts(),
        "uses_validate_labels": False,
        "uses_test_labels": False,
        "artifact_files": {path.name: file_digest(path) for path in output.iterdir()},
    }
    write_json(output / "report.json", report)
    LOGGER.info("saved %s %s", target, verification)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target", choices=("trajectory", "location_side"), required=True
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    freeze(str(args.target), cast(Path, args.output), smoke=bool(args.smoke))


if __name__ == "__main__":
    main()
