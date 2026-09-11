from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import platform
import shutil
from pathlib import Path
from typing import cast

import duckdb
import polars as pl

from python_models.statistical.evidence_binding import file_digest

LOGGER = logging.getLogger(__name__)
REPOSITORY = Path(__file__).resolve().parents[4]
DATABASE = (
    REPOSITORY / "artifacts/statistical/research/geometry-v2-global-side-20260911/bc.db"
)
SCHEMA = "main_models__geometry_v2_research_20260911"
FOLD_PREFIX = "geometry-reliability-inner-v1:"
RESERVE = (
    REPOSITORY
    / "artifacts/statistical/backtests/historical_stress/20260911-reserve-v1/reserved_games.parquet"
)
RESERVE_FILE_DIGEST = "fa6e83509bec2b412781340c4e66cd940159ada4d95de8e4fe5b3fba2d325a35"
RESERVE_GAME_DIGEST = "5ac641fe78b19e98ec5cb576ebb85e652ff289e445e0f6e54e109c523a614dba"
MATERIALIZATION_REPORT = REPOSITORY / "docs/geometry-materialization-2026-09-11.json"
MATERIALIZATION_REPORT_DIGEST = (
    "37f0580195de782ab3c85b6158bcde753e1902a2fd04e6fbb94f3a7c00dee7ad"
)
FORBIDDEN_CANDIDATE_INPUTS = (
    "trajectory",
    "batted_trajectory",
    "recorded_trajectory",
    "trajectory_broad",
    "deduced_value",
    "is_trajectory_deduced",
)

CANDIDATE_SQL = """
WITH putouts AS (
    SELECT
        event_key,
        SUM(assisted_putouts) AS assisted_putouts
    FROM main_models.calc_fielding_play_agg
    WHERE fielding_position != 0
    GROUP BY event_key
), scorer_rows AS (
    SELECT DISTINCT game_id, TRIM(cleaned_scorer) AS scorer
    FROM main_models.game_scorekeeping
    WHERE NULLIF(TRIM(cleaned_scorer), '') IS NOT NULL
), scorer_games AS (
    SELECT
        game_id,
        STRING_AGG(scorer, '|' ORDER BY scorer) AS scorer
    FROM scorer_rows
    GROUP BY game_id
)
SELECT
    e.event_key,
    e.game_id,
    e.season,
    COALESCE(s.scorer, '__missing__') AS scorer,
    COALESCE((
        e.batted_to_fielder BETWEEN 1 AND 6
        AND COALESCE(p.assisted_putouts, 0) > 0
    ), FALSE) AS exact_ground_candidate,
    COALESCE((
        e.plate_appearance_result = 'HomeRun'
        OR (
            l.category_depth = 'Outfield'
            AND e.season NOT BETWEEN 2000 AND 2002
        )
    ), FALSE) AS broad_air_candidate
FROM main_models.stg_events AS e
INNER JOIN selected_games AS g USING (game_id)
LEFT JOIN putouts AS p USING (event_key)
LEFT JOIN main_seeds.seed_hit_location_categories AS l
    USING (batted_location_general)
LEFT JOIN scorer_games AS s USING (game_id)
WHERE e.batted_location_general IS NOT NULL
""".strip()


def inner_fold(game_id: str) -> str:
    value = hashlib.sha256((FOLD_PREFIX + game_id).encode()).digest()
    return (
        "inner_evaluation" if int.from_bytes(value[:8], "big") % 5 == 0 else "inner_fit"
    )


def normalized_recorded_target(raw_value: str) -> str:
    if raw_value.endswith("Bunt"):
        return "Bunt"
    if raw_value not in {"Fly", "GroundBall", "LineDrive", "PopUp"}:
        raise ValueError(f"unknown observed trajectory: {raw_value}")
    return raw_value


def recorded_broad_class(raw_value: str) -> str | None:
    if raw_value in {"GroundBall", "GroundBallBunt"}:
        return "GroundBall"
    if raw_value in {
        "Fly",
        "LineDrive",
        "PopUp",
        "LineDriveBunt",
        "PopUpBunt",
    }:
        return "AirBall"
    if raw_value in {"FoulBunt", "UnspecifiedBunt"}:
        return None
    raise ValueError(f"unknown observed trajectory: {raw_value}")


def wilson_interval(successes: int, total: int) -> list[float] | None:
    if successes < 0 or total < successes:
        raise ValueError("invalid binomial counts")
    if total == 0:
        return None
    z = 1.959963984540054
    proportion = successes / total
    denominator = 1 + z * z / total
    center = (proportion + z * z / (2 * total)) / denominator
    radius = (
        z
        * math.sqrt(proportion * (1 - proportion) / total + z * z / (4 * total * total))
        / denominator
    )
    return [center - radius, center + radius]


def game_digest(games: list[str]) -> str:
    digest = hashlib.sha256()
    for game in sorted(set(games)):
        encoded = game.encode()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def verify_reserve(
    path: Path, expected_file_digest: str, expected_game_digest: str
) -> set[str]:
    if file_digest(path) != expected_file_digest:
        raise ValueError("reserve file digest mismatch")
    frame = pl.read_parquet(path, columns=["game_id"])
    games = cast(list[str], frame["game_id"].cast(pl.String).to_list())
    if len(games) != len(set(games)):
        raise ValueError("reserve contains duplicate game IDs")
    if game_digest(games) != expected_game_digest:
        raise ValueError("reserve game ID digest mismatch")
    return set(games)


def verify_no_reserve_overlap(games: list[str], reserve: set[str]) -> int:
    overlap = sorted(set(games) & reserve)
    if overlap:
        raise ValueError(f"selected games overlap reserve: {overlap[:3]}")
    return 0


def provenance_binding() -> dict[str, object]:
    if file_digest(MATERIALIZATION_REPORT) != MATERIALIZATION_REPORT_DIGEST:
        raise ValueError("materialization report digest mismatch")
    report = cast(dict[str, object], json.loads(MATERIALIZATION_REPORT.read_text()))
    code_identity = cast(dict[str, object], report["code_identity"])
    expected_files = cast(dict[str, str], code_identity["file_sha256"])
    selected = {
        "bc/models/intermediate/coverage/event_observation_geometry.sql": REPOSITORY
        / "bc/models/intermediate/coverage/event_observation_geometry.sql",
        "bc/models/intermediate/modeling_datasets/model_input_geometry.sql": REPOSITORY
        / "bc/models/intermediate/modeling_datasets/model_input_geometry.sql",
        "bc/models/intermediate/modeling_datasets/model_input_observation_batted_ball.sql": REPOSITORY
        / "bc/models/intermediate/modeling_datasets/model_input_observation_batted_ball.sql",
    }
    actual = {name: file_digest(path) for name, path in selected.items()}
    if any(expected_files.get(name) != digest for name, digest in actual.items()):
        raise ValueError("materialized model SQL digest mismatch")
    if report.get("status") != "complete" or report.get("schema") != SCHEMA:
        raise ValueError("materialization report contract mismatch")
    return {
        "status": "partial_materialization_and_selected_sql_binding",
        "materialization_report": str(MATERIALIZATION_REPORT),
        "materialization_report_sha256": MATERIALIZATION_REPORT_DIGEST,
        "selected_model_sql_sha256": actual,
        "missing_for_full_provenance": [
            "full database content digest",
            "content digests for every upstream source relation",
        ],
    }


def binary_summary(
    frame: pl.DataFrame, candidate: str, positive_recorded_label: str
) -> dict[str, object]:
    scored = frame.filter(pl.col("recorded_broad_class").is_not_null())
    candidate_positive = pl.col("candidate_positive")
    recorded_positive = pl.col("recorded_broad_class") == positive_recorded_label
    candidate_positive_recorded_positive = int(
        scored.filter(candidate_positive & recorded_positive)
        .select(pl.col("rows").sum())
        .item()
        or 0
    )
    candidate_positive_recorded_other = int(
        scored.filter(candidate_positive & ~recorded_positive)
        .select(pl.col("rows").sum())
        .item()
        or 0
    )
    candidate_negative_recorded_positive = int(
        scored.filter(~candidate_positive & recorded_positive)
        .select(pl.col("rows").sum())
        .item()
        or 0
    )
    unscorable = int(
        frame.filter(pl.col("recorded_broad_class").is_null())
        .select(pl.col("rows").sum())
        .item()
        or 0
    )
    observed_rows = int(frame.select(pl.col("rows").sum()).item() or 0)
    candidate_positive_rows = int(
        frame.filter(candidate_positive).select(pl.col("rows").sum()).item() or 0
    )
    predicted_positive = (
        candidate_positive_recorded_positive + candidate_positive_recorded_other
    )
    recorded_positive_rows = (
        candidate_positive_recorded_positive + candidate_negative_recorded_positive
    )
    positive_agreement = (
        candidate_positive_recorded_positive / predicted_positive
        if predicted_positive
        else None
    )
    recorded_positive_capture = (
        candidate_positive_recorded_positive / recorded_positive_rows
        if recorded_positive_rows
        else None
    )
    return {
        "candidate": candidate,
        "positive_recorded_label": positive_recorded_label,
        "candidate_positive_recorded_positive_rows": candidate_positive_recorded_positive,
        "candidate_positive_recorded_other_rows": candidate_positive_recorded_other,
        "candidate_negative_recorded_positive_rows": candidate_negative_recorded_positive,
        "unscorable_recorded_rows": unscorable,
        "observed_rows": observed_rows,
        "candidate_positive_rows": candidate_positive_rows,
        "candidate_coverage": (
            candidate_positive_rows / observed_rows if observed_rows else None
        ),
        "recorded_label_positive_agreement": positive_agreement,
        "recorded_label_positive_agreement_wilson95_descriptive_row_iid": wilson_interval(
            candidate_positive_recorded_positive, predicted_positive
        ),
        "recorded_positive_capture": recorded_positive_capture,
        "recorded_positive_capture_wilson95_descriptive_row_iid": wilson_interval(
            candidate_positive_recorded_positive, recorded_positive_rows
        ),
        "interval_scope": "Descriptive row-IID Wilson intervals only; they ignore within-game and shared-source dependence and are not inferential accuracy intervals.",
    }


def slice_summaries(frame: pl.DataFrame) -> list[dict[str, object]]:
    keys = cast(
        list[tuple[str, str, str, str]],
        frame.select("inner_fold", "slice_level", "slice_value", "candidate")
        .unique()
        .sort("inner_fold", "slice_level", "slice_value", "candidate")
        .rows(),
    )
    records: list[dict[str, object]] = []
    for fold, level, value, candidate in keys:
        subset = frame.filter(
            (pl.col("inner_fold") == fold)
            & (pl.col("slice_level") == level)
            & (pl.col("slice_value") == value)
            & (pl.col("candidate") == candidate)
        )
        positive_recorded_label = (
            "GroundBall" if candidate == "exact_ground" else "AirBall"
        )
        metrics = binary_summary(subset, candidate, positive_recorded_label)
        records.append(
            {
                "inner_fold": fold,
                "slice_level": level,
                "slice_value": value,
                **metrics,
            }
        )
    return records


def _write_json(path: Path, value: object) -> None:
    _ = path.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )


def _game_partitions(
    connection: duckdb.DuckDBPyConnection, smoke_games: int | None
) -> pl.DataFrame:
    limit = f" LIMIT {smoke_games}" if smoke_games is not None else ""
    games = connection.execute(
        f"""SELECT DISTINCT game_id
            FROM {SCHEMA}.model_input_geometry
            WHERE primary_fold = 'TRAIN' AND geometry_dimension = 'trajectory'
            ORDER BY game_id{limit}"""
    ).pl()
    game_ids = cast(list[str], games["game_id"].cast(pl.String).to_list())
    return games.with_columns(
        pl.Series("inner_fold", [inner_fold(game) for game in game_ids])
    )


def _game_contribution_query() -> str:
    target_case = """CASE
        WHEN l.raw_value LIKE '%Bunt' THEN 'Bunt'
        ELSE l.raw_value
    END"""
    broad_case = """CASE
        WHEN l.raw_value IN ('GroundBall', 'GroundBallBunt') THEN 'GroundBall'
        WHEN l.raw_value IN ('Fly', 'LineDrive', 'PopUp', 'LineDriveBunt', 'PopUpBunt') THEN 'AirBall'
        WHEN l.raw_value IN ('FoulBunt', 'UnspecifiedBunt') THEN NULL
        ELSE '__INVALID__'
    END"""
    return f"""WITH labeled AS (
        SELECT
            c.*,
            g.inner_fold,
            l.result_family,
            l.raw_value,
            {target_case} AS recorded_target_class,
            {broad_case} AS recorded_broad_class
        FROM candidates AS c
        INNER JOIN game_partitions AS g USING (game_id)
        INNER JOIN {SCHEMA}.model_input_geometry AS l USING (event_key)
        WHERE l.primary_fold = 'TRAIN'
          AND l.geometry_dimension = 'trajectory'
          AND l.observed_status = 'observed'
    ), expanded AS (
        SELECT *, 'exact_ground' AS candidate, exact_ground_candidate AS candidate_positive FROM labeled
        UNION ALL
        SELECT *, 'broad_air' AS candidate, broad_air_candidate AS candidate_positive FROM labeled
    )
    SELECT
        inner_fold,
        game_id,
        season,
        result_family,
        scorer,
        candidate,
        candidate_positive,
        recorded_target_class,
        recorded_broad_class,
        COUNT(*)::BIGINT AS event_rows
    FROM expanded
    GROUP BY ALL
    ORDER BY inner_fold, game_id, candidate,
             candidate_positive, recorded_target_class, recorded_broad_class"""


def _measurement_query() -> str:
    return """WITH slices AS (
        SELECT *, 'overall' AS slice_level, '__all__' AS slice_value
        FROM observed_game_contributions
        UNION ALL
        SELECT *, 'decade', CAST(FLOOR(season / 10) * 10 AS VARCHAR)
        FROM observed_game_contributions
        UNION ALL
        SELECT *, 'result_family', result_family
        FROM observed_game_contributions
        UNION ALL
        SELECT *, 'scorer', scorer
        FROM observed_game_contributions
    )
    SELECT
        inner_fold,
        slice_level,
        slice_value,
        candidate,
        candidate_positive,
        recorded_target_class,
        recorded_broad_class,
        SUM(event_rows)::BIGINT AS rows,
        COUNT(DISTINCT game_id)::BIGINT AS games
    FROM slices
    GROUP BY ALL
    ORDER BY inner_fold, slice_level, slice_value, candidate,
             candidate_positive, recorded_target_class, recorded_broad_class"""


def _population_query() -> str:
    return f"""SELECT
        g.inner_fold,
        CAST(FLOOR(c.season / 10) * 10 AS INTEGER) AS decade,
        l.result_family,
        c.scorer,
        l.observed_status,
        c.exact_ground_candidate,
        c.broad_air_candidate,
        (c.exact_ground_candidate AND c.broad_air_candidate) AS candidate_conflict,
        COUNT(*)::BIGINT AS rows,
        COUNT(DISTINCT c.game_id)::BIGINT AS games
    FROM candidates AS c
    INNER JOIN game_partitions AS g USING (game_id)
    INNER JOIN {SCHEMA}.model_input_geometry AS l USING (event_key)
    WHERE l.primary_fold = 'TRAIN'
      AND l.geometry_dimension = 'trajectory'
    GROUP BY ALL
    ORDER BY inner_fold, decade, result_family, scorer, observed_status,
             exact_ground_candidate, broad_air_candidate"""


def evaluate(
    database: Path, output: Path, *, smoke_games: int | None, reserve_path: Path
) -> None:
    if output.exists():
        raise FileExistsError(output)
    if smoke_games is not None and smoke_games <= 0:
        raise ValueError("smoke_games must be positive")
    lowered = CANDIDATE_SQL.lower()
    forbidden = [name for name in FORBIDDEN_CANDIDATE_INPUTS if name in lowered]
    if forbidden:
        raise ValueError(f"candidate SQL uses target-dependent inputs: {forbidden}")
    reserve = verify_reserve(reserve_path, RESERVE_FILE_DIGEST, RESERVE_GAME_DIGEST)
    source_binding = provenance_binding()
    output.mkdir(parents=True)
    handler = logging.FileHandler(output / "run.log")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    try:
        LOGGER.info("starting weak-measurement evaluation smoke_games=%s", smoke_games)
        with duckdb.connect(str(database), read_only=True) as connection:
            _ = connection.execute("SET threads=4")
            _ = connection.execute("SET memory_limit='16GB'")
            partitions = _game_partitions(connection, smoke_games)
            partition_games = cast(
                list[str], partitions["game_id"].cast(pl.String).to_list()
            )
            reserve_overlap = verify_no_reserve_overlap(partition_games, reserve)
            LOGGER.info(
                "reserve verified games=%s selected_overlap=%s",
                len(reserve),
                reserve_overlap,
            )
            _ = connection.register("selected_games", partitions.select("game_id"))
            _ = connection.register("game_partitions", partitions)
            _ = connection.execute("CREATE TEMP TABLE candidates AS " + CANDIDATE_SQL)
            game_contributions = connection.execute(_game_contribution_query()).pl()
            _ = connection.register("observed_game_contributions", game_contributions)
            measurement = connection.execute(_measurement_query()).pl()
            population = connection.execute(_population_query()).pl()
        if measurement.filter(pl.col("recorded_broad_class") == "__INVALID__").height:
            raise ValueError("observed trajectory outside the scoring contract")
        measurement.write_parquet(output / "observed_measurement_counts.parquet")
        game_contributions.write_parquet(
            output / "observed_game_event_contributions.parquet"
        )
        population.write_parquet(output / "population_clue_counts.parquet")
        _write_json(output / "slice_metrics.json", slice_summaries(measurement))
        _ = (output / "candidate_source.sql").write_text(CANDIDATE_SQL + "\n")
        _ = shutil.copy2(Path(__file__), output / Path(__file__).name)
        overall = measurement.filter(pl.col("slice_level") == "overall")
        summaries: dict[str, object] = {}
        for fold in ("inner_fit", "inner_evaluation"):
            fold_frame = overall.filter(pl.col("inner_fold") == fold)
            summaries[fold] = {
                "exact_ground": binary_summary(
                    fold_frame.filter(pl.col("candidate") == "exact_ground"),
                    "exact_ground",
                    "GroundBall",
                ),
                "broad_air": binary_summary(
                    fold_frame.filter(pl.col("candidate") == "broad_air"),
                    "broad_air",
                    "AirBall",
                ),
                "five_class_candidate_counts": fold_frame.group_by(
                    "candidate", "candidate_positive", "recorded_target_class"
                )
                .agg(pl.col("rows").sum())
                .sort("candidate", "candidate_positive", "recorded_target_class")
                .to_dicts(),
            }
        report = {
            "analysis": "Exploratory label-blind clue agreement with recorded trajectory labels from shared Retrosheet-derived fields",
            "smoke_games": smoke_games,
            "database": str(database),
            "database_size_bytes": database.stat().st_size,
            "database_mtime_ns": database.stat().st_mtime_ns,
            "schema": SCHEMA,
            "runtime": {
                "python": platform.python_version(),
                "duckdb": duckdb.__version__,
                "polars": pl.__version__,
            },
            "fold_rule": FOLD_PREFIX
            + "game_id; sha256 first8 unsigned big-endian modulo5 equals zero",
            "fold_status": "Both inner_fit and inner_evaluation have been inspected and are exploratory/exposed for any feature selection; neither is an untouched confirmation fold.",
            "partition_counts": partitions.group_by("inner_fold")
            .len(name="games")
            .sort("inner_fold")
            .to_dicts(),
            "candidate_inputs": {
                "exact_ground": "raw batted_to_fielder 1-6 and positive assisted putouts",
                "broad_air": "raw HomeRun result or outfield general-location category outside 2000-2002",
            },
            "recorded_broad_class_unscorable": [
                "FoulBunt",
                "UnspecifiedBunt",
            ],
            "selection_limit": "Scoring describes agreement with recorded PRIMARY TRAIN labels only in the selected observed subset; it does not estimate physical classification accuracy.",
            "shared_source_limit": "Candidate clues and recorded trajectory labels are derived from fields in the same Retrosheet event source, so agreement is cross-field consistency rather than independent measurement validation.",
            "unknown_and_derived_limit": "Candidate prevalence is reported for natural derived and unknown rows without treating existing deductions as truth.",
            "uncertainty_limit": "Wilson intervals are descriptive row-IID summaries only. They ignore clustering by game, scorer, park, team, and shared source; clustered inferential uncertainty is unsupported in this artifact.",
            "game_contribution_file": "observed_game_event_contributions.parquet contains event-count sufficient contributions grouped by game and contingency cell for later game-clustered analysis.",
            "summaries": summaries,
            "uses_test_labels": False,
            "uses_validate_labels": False,
            "uses_current_deduced_values_as_candidate_inputs": False,
            "reserve_binding": {
                "path": str(reserve_path),
                "file_sha256": RESERVE_FILE_DIGEST,
                "sorted_game_ids_sha256_length_prefixed_utf8": RESERVE_GAME_DIGEST,
                "games": len(reserve),
                "selected_game_overlap_before_label_read": reserve_overlap,
            },
            "source_provenance": source_binding,
            "upstream_source_files": {
                "calc_batted_ball_type.sql": file_digest(
                    REPOSITORY
                    / "bc/models/intermediate/event_level/calc_batted_ball_type.sql"
                ),
                "calc_fielding_play_agg.sql": file_digest(
                    REPOSITORY
                    / "bc/models/intermediate/event_level/calc_fielding_play_agg.sql"
                ),
                "stg_events.sql": file_digest(
                    REPOSITORY / "bc/models/staging/event/stg_events.sql"
                ),
            },
        }
        _write_json(output / "report.json", report)
        LOGGER.info("completed weak-measurement evaluation")
        handler.flush()
        inventory = {
            path.name: file_digest(path)
            for path in output.iterdir()
            if path.name != "manifest.json"
        }
        _write_json(output / "manifest.json", {"files_sha256": inventory})
    finally:
        LOGGER.removeHandler(handler)
        handler.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--database", type=Path, default=DATABASE)
    _ = parser.add_argument("--output", type=Path, required=True)
    _ = parser.add_argument("--smoke-games", type=int)
    _ = parser.add_argument("--reserve", type=Path, default=RESERVE)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    evaluate(
        cast(Path, args.database),
        cast(Path, args.output),
        smoke_games=cast(int | None, args.smoke_games),
        reserve_path=cast(Path, args.reserve),
    )


if __name__ == "__main__":
    main()
