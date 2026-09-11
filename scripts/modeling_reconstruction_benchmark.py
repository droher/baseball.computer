from __future__ import annotations

import argparse
import hashlib
import json
import logging
import platform
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import duckdb
import numpy as np


TARGETS = ("location_side", "trajectory")
SPLIT_DEFINITION = (
    "main_models.model_input_geometry.primary_fold; TRAIN fits, TEST evaluates, "
    "VALIDATE is untouched; primary_fold is HASH(game_id) mod 100 with "
    "TRAIN=[0,69], VALIDATE=[70,84], TEST=[85,99]"
)
LABEL_SQL = """
CASE
    WHEN geometry_dimension = 'trajectory'
     AND class IN ('FoulBunt', 'GroundBallBunt', 'LineDriveBunt', 'PopUpBunt', 'UnspecifiedBunt')
    THEN 'Bunt'
    ELSE class
END
"""
OBSERVED_SQL = """
SELECT
    event_key,
    geometry_dimension AS target,
    {label_sql} AS label,
    game_id,
    season,
    CAST(FLOOR(season / 10) * 10 AS INTEGER) AS era_start,
    COALESCE(result_family, '__MISSING__') AS result_family,
    primary_fold
FROM main_models.model_input_geometry
WHERE geometry_dimension IN ('location_side', 'trajectory')
  AND is_observed_class
  AND training_weight > 0
  AND game_id IS NOT NULL
  AND primary_fold IN ('TRAIN', 'TEST')
  AND ({sample_predicate})
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_key, geometry_dimension
    ORDER BY label
) = 1
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--db", type=Path, default=Path("bc.db"))
    _ = parser.add_argument("--output", type=Path, required=True)
    _ = parser.add_argument("--checkpoint-log", type=Path, required=True)
    _ = parser.add_argument("--full", action="store_true")
    _ = parser.add_argument("--bootstrap-repetitions", type=int, default=500)
    _ = parser.add_argument("--seed", type=int, default=20260911)
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def configure_logging(path: Path) -> logging.Logger:
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("modeling_reconstruction_benchmark")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(path, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def scalar_rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, object]]:
    result = con.execute(query)
    names = [item[0] for item in result.description]
    return [dict(zip(names, row, strict=True)) for row in result.fetchall()]


def required_cell(con: duckdb.DuckDBPyConnection, query: str) -> object:
    row = con.execute(query).fetchone()
    if row is None:
        raise ValueError("scalar query returned no row")
    return row[0]


def split_game_digest(con: duckdb.DuckDBPyConnection, fold: str) -> tuple[str, int]:
    cursor = con.execute(
        "SELECT DISTINCT game_id FROM observed WHERE primary_fold = ? ORDER BY game_id",
        [fold],
    )
    digest = hashlib.sha256()
    count = 0
    while rows := cursor.fetchmany(10_000):
        for (game_id,) in rows:
            encoded = str(game_id).encode("utf-8")
            digest.update(len(encoded).to_bytes(4, "big"))
            digest.update(encoded)
            count += 1
    return digest.hexdigest(), count


def validate_game_disjoint(con: duckdb.DuckDBPyConnection) -> int:
    overlap = cast(
        int,
        required_cell(
            con,
            """
            SELECT COUNT(*) FROM (
                SELECT game_id FROM observed WHERE primary_fold = 'TRAIN'
                INTERSECT
                SELECT game_id FROM observed WHERE primary_fold = 'TEST'
            )
            """,
        ),
    )
    if overlap != 0:
        raise ValueError(f"train/test game overlap is {overlap}, expected zero")
    return overlap


def create_observed(con: duckdb.DuckDBPyConnection, *, full: bool) -> str:
    sample_predicate = (
        "TRUE" if full else "hash(game_id || ':benchmark-smoke') % 100 = 0"
    )
    rendered = OBSERVED_SQL.format(
        label_sql=LABEL_SQL,
        sample_predicate=sample_predicate,
    )
    conflicts = con.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT event_key, geometry_dimension
            FROM (
                SELECT event_key, geometry_dimension, {LABEL_SQL} AS label
                FROM main_models.model_input_geometry
                WHERE geometry_dimension IN ('location_side', 'trajectory')
                  AND is_observed_class
                  AND training_weight > 0
                  AND game_id IS NOT NULL
                  AND primary_fold IN ('TRAIN', 'TEST')
                  AND ({sample_predicate})
            )
            GROUP BY event_key, geometry_dimension
            HAVING COUNT(DISTINCT label) > 1
        )
        """
    ).fetchone()
    if conflicts is None:
        raise ValueError("observed-label conflict query returned no row")
    conflict_count = int(conflicts[0])
    if conflict_count != 0:
        raise ValueError(
            f"observed labels contain {conflict_count} event-target conflicts"
        )
    con.execute(f"CREATE TEMP TABLE observed AS {rendered}")
    return rendered


def create_predictions(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE class_universe AS
        SELECT target, label, COUNT(*) AS train_count
        FROM observed
        WHERE primary_fold = 'TRAIN'
        GROUP BY target, label
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE predictions AS
        WITH
        target_totals AS (
            SELECT target, SUM(train_count) AS n, COUNT(*) AS k
            FROM class_universe
            GROUP BY target
        ),
        marginal AS (
            SELECT
                c.target,
                c.label,
                (c.train_count + 1.0) / (t.n + t.k) AS probability
            FROM class_universe AS c
            JOIN target_totals AS t USING (target)
        ),
        cells AS (
            SELECT DISTINCT target, era_start, result_family
            FROM observed
            WHERE primary_fold = 'TEST'
        ),
        cell_counts AS (
            SELECT target, era_start, result_family, label, COUNT(*) AS n
            FROM observed
            WHERE primary_fold = 'TRAIN'
            GROUP BY target, era_start, result_family, label
        ),
        cell_totals AS (
            SELECT target, era_start, result_family, SUM(n) AS n
            FROM cell_counts
            GROUP BY target, era_start, result_family
        )
        SELECT
            e.target,
            e.era_start,
            e.result_family,
            c.label,
            m.probability AS marginal_probability,
            (COALESCE(cc.n, 0) + 1.0) /
                (COALESCE(ct.n, 0) + t.k)
                AS conditional_probability,
            COALESCE(ct.n, 0) > 0 AS cell_seen_in_train
        FROM cells AS e
        JOIN class_universe AS c USING (target)
        JOIN target_totals AS t USING (target)
        JOIN marginal AS m USING (target, label)
        LEFT JOIN cell_counts AS cc USING (target, era_start, result_family, label)
        LEFT JOIN cell_totals AS ct USING (target, era_start, result_family)
        """
    )


def create_scores(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE expanded_scores AS
        SELECT
            o.target,
            o.event_key,
            o.game_id,
            o.season,
            o.label AS truth,
            p.label AS candidate,
            p.marginal_probability,
            p.conditional_probability,
            p.cell_seen_in_train,
            (o.label = p.label)::INTEGER AS outcome
        FROM observed AS o
        JOIN predictions AS p USING (target, era_start, result_family)
        WHERE o.primary_fold = 'TEST'
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE event_scores AS
        SELECT
            target,
            event_key,
            game_id,
            season,
            BOOL_OR(cell_seen_in_train) AS cell_seen_in_train,
            -LN(MAX(marginal_probability) FILTER (WHERE outcome = 1))
                AS marginal_log_loss,
            -LN(MAX(conditional_probability) FILTER (WHERE outcome = 1))
                AS conditional_log_loss,
            SUM(POWER(marginal_probability - outcome, 2)) AS marginal_brier,
            SUM(POWER(conditional_probability - outcome, 2)) AS conditional_brier
        FROM expanded_scores
        GROUP BY target, event_key, game_id, season
        """
    )


def metric_rows(con: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    return scalar_rows(
        con,
        """
        WITH slices AS (
            SELECT *, 'all_test' AS slice FROM event_scores
            UNION ALL
            SELECT *, 'pre_1988_observed_test' AS slice
            FROM event_scores
            WHERE season < 1988
        )
        SELECT
            target,
            slice,
            COUNT(*) AS observed_test_rows,
            COUNT(DISTINCT game_id) AS observed_test_games,
            AVG(marginal_log_loss) AS marginal_log_loss,
            AVG(conditional_log_loss) AS conditional_log_loss,
            AVG(marginal_log_loss) - AVG(conditional_log_loss)
                AS conditional_log_loss_improvement,
            AVG(marginal_brier) AS marginal_brier,
            AVG(conditional_brier) AS conditional_brier,
            AVG(marginal_brier) - AVG(conditional_brier)
                AS conditional_brier_improvement,
            AVG(cell_seen_in_train::INTEGER) AS conditional_cell_support_rate
        FROM slices
        GROUP BY target, slice
        ORDER BY target, slice
        """,
    )


def calibration_rows(con: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    return scalar_rows(
        con,
        """
        WITH slices AS (
            SELECT *, 'all_test' AS slice FROM expanded_scores
            UNION ALL
            SELECT *, 'pre_1988_observed_test' AS slice
            FROM expanded_scores
            WHERE season < 1988
        ),
        long AS (
            SELECT target, slice, candidate, outcome, 'marginal' AS baseline,
                   marginal_probability AS probability
            FROM slices
            UNION ALL
            SELECT target, slice, candidate, outcome, 'era_result_family' AS baseline,
                   conditional_probability AS probability
            FROM slices
        ),
        bins AS (
            SELECT
                target,
                slice,
                baseline,
                candidate,
                LEAST(FLOOR(probability * 15), 14)::INTEGER AS bin,
                COUNT(*) AS n,
                AVG(probability) AS mean_probability,
                AVG(outcome) AS observed_rate
            FROM long
            GROUP BY target, slice, baseline, candidate, bin
        )
        SELECT
            target,
            slice,
            baseline,
            SUM(n * ABS(mean_probability - observed_rate)) / SUM(n)
                AS classwise_ece_15_bin,
            COUNT(*) AS populated_class_bins
        FROM bins
        GROUP BY target, slice, baseline
        ORDER BY target, slice, baseline
        """,
    )


def coverage_rows(
    con: duckdb.DuckDBPyConnection, *, full: bool
) -> list[dict[str, object]]:
    sample_predicate = (
        "TRUE" if full else "hash(game_id || ':benchmark-smoke') % 100 = 0"
    )
    return scalar_rows(
        con,
        f"""
        WITH missing AS (
            SELECT
                geometry_dimension AS target,
                season,
                CAST(FLOOR(season / 10) * 10 AS INTEGER) AS era_start,
                result_family
            FROM main_models.model_input_geometry
            WHERE geometry_dimension IN ('location_side', 'trajectory')
              AND observed_status IN ('unknown_code', 'missing')
              AND training_weight > 0
              AND game_id IS NOT NULL
              AND ({sample_predicate})
        ),
        train_cells AS (
            SELECT DISTINCT target, era_start, result_family
            FROM observed
            WHERE primary_fold = 'TRAIN'
        )
        SELECT
            m.target,
            COUNT(*) AS inference_rows,
            AVG((m.season IS NOT NULL)::INTEGER) AS season_coverage,
            AVG((m.result_family IS NOT NULL)::INTEGER) AS result_family_coverage,
            AVG((m.season IS NOT NULL AND m.result_family IS NOT NULL)::INTEGER)
                AS joint_feature_coverage,
            AVG((t.target IS NOT NULL)::INTEGER) AS fitted_cell_coverage
        FROM missing AS m
        LEFT JOIN train_cells AS t
          ON t.target = m.target
         AND t.era_start = m.era_start
         AND t.result_family = COALESCE(m.result_family, '__MISSING__')
        GROUP BY m.target
        ORDER BY m.target
        """,
    )


def bootstrap_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    repetitions: int,
    seed: int,
) -> list[dict[str, object]]:
    rows = con.execute(
        """
        WITH slices AS (
            SELECT *, 'all_test' AS slice FROM event_scores
            UNION ALL
            SELECT *, 'pre_1988_observed_test' AS slice
            FROM event_scores
            WHERE season < 1988
        )
        SELECT
            target,
            slice,
            game_id,
            COUNT(*) AS n,
            SUM(marginal_log_loss - conditional_log_loss) AS log_loss_gain,
            SUM(marginal_brier - conditional_brier) AS brier_gain
        FROM slices
        GROUP BY target, slice, game_id
        ORDER BY target, slice, game_id
        """
    ).fetchall()
    grouped: dict[tuple[str, str], list[tuple[int, float, float]]] = {}
    for target, slice_name, _, n, log_gain, brier_gain in rows:
        key = (str(target), str(slice_name))
        grouped.setdefault(key, []).append((int(n), float(log_gain), float(brier_gain)))
    rng = np.random.default_rng(seed)
    output: list[dict[str, object]] = []
    for (target, slice_name), values in sorted(grouped.items()):
        array = np.asarray(values, dtype=np.float64)
        game_count = array.shape[0]
        draws_log = np.empty(repetitions, dtype=np.float64)
        draws_brier = np.empty(repetitions, dtype=np.float64)
        for index in range(repetitions):
            selected = rng.integers(0, game_count, size=game_count)
            sampled = array[selected]
            denominator = sampled[:, 0].sum()
            draws_log[index] = sampled[:, 1].sum() / denominator
            draws_brier[index] = sampled[:, 2].sum() / denominator
        output.append(
            {
                "target": target,
                "slice": slice_name,
                "cluster_unit": "game_id",
                "repetitions": repetitions,
                "train_fit_during_bootstrap": "fixed",
                "conditional_log_loss_improvement_ci95": [
                    float(np.quantile(draws_log, 0.025)),
                    float(np.quantile(draws_log, 0.975)),
                ],
                "conditional_brier_improvement_ci95": [
                    float(np.quantile(draws_brier, 0.025)),
                    float(np.quantile(draws_brier, 0.975)),
                ],
            }
        )
    return output


def data_fingerprints(con: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    return scalar_rows(
        con,
        """
        SELECT
            target,
            primary_fold,
            COUNT(*) AS row_count,
            COUNT(DISTINCT game_id) AS game_count,
            MIN(season) AS min_season,
            MAX(season) AS max_season,
            PRINTF('%016x', BIT_XOR(HASH(
                event_key, target, label, game_id, season, result_family, primary_fold
            ))) AS duckdb_row_hash_xor64
        FROM observed
        GROUP BY target, primary_fold
        ORDER BY target, primary_fold
        """,
    )


def invariant_results(con: duckdb.DuckDBPyConnection) -> dict[str, object]:
    missing_test_labels = cast(
        int,
        required_cell(
            con,
            """
            SELECT COUNT(*)
            FROM observed AS o
            LEFT JOIN class_universe AS c
              ON c.target = o.target AND c.label = o.label
            WHERE o.primary_fold = 'TEST' AND c.label IS NULL
            """,
        ),
    )
    probability_deviation = cast(
        float,
        required_cell(
            con,
            """
            SELECT MAX(GREATEST(
                ABS(marginal_sum - 1.0), ABS(conditional_sum - 1.0)
            ))
            FROM (
                SELECT
                    target,
                    era_start,
                    result_family,
                    SUM(marginal_probability) AS marginal_sum,
                    SUM(conditional_probability) AS conditional_sum
                FROM predictions
                GROUP BY target, era_start, result_family
            )
            """,
        ),
    )
    score_alignment = scalar_rows(
        con,
        """
        SELECT
            o.target,
            COUNT(*) AS observed_test_rows,
            COUNT(s.event_key) AS scored_test_rows
        FROM observed AS o
        LEFT JOIN event_scores AS s USING (target, event_key)
        WHERE o.primary_fold = 'TEST'
        GROUP BY o.target
        ORDER BY o.target
        """,
    )
    if missing_test_labels != 0:
        raise ValueError(f"TEST has {missing_test_labels} labels absent from TRAIN")
    if probability_deviation > 1e-12:
        raise ValueError(
            f"baseline probability vectors deviate from one by {probability_deviation}"
        )
    if any(
        cast(int, row["observed_test_rows"]) != cast(int, row["scored_test_rows"])
        for row in score_alignment
    ):
        raise ValueError(f"TEST scoring alignment failed: {score_alignment}")
    return {
        "test_labels_absent_from_train": missing_test_labels,
        "max_probability_sum_deviation": probability_deviation,
        "score_alignment": score_alignment,
    }


def run(args: argparse.Namespace) -> dict[str, object]:
    logger = configure_logging(cast(Path, args.checkpoint_log))
    full = bool(args.full)
    logger.info("open read-only database path=%s full=%s", args.db, full)
    con = duckdb.connect(str(args.db), read_only=True)
    try:
        rendered_query = create_observed(con, full=full)
        logger.info("created deduplicated observed frame")
        create_predictions(con)
        logger.info("fit train-only marginal and era-result-family baselines")
        create_scores(con)
        logger.info("scored shared TEST observations")
        train_digest, train_games = split_game_digest(con, "TRAIN")
        test_digest, test_games = split_game_digest(con, "TEST")
        overlap = validate_game_disjoint(con)
        invariants = invariant_results(con)
        view_sql_row = con.execute(
            """
            SELECT sql
            FROM duckdb_views()
            WHERE database_name = current_database()
              AND schema_name = 'main_models'
              AND view_name = 'model_input_geometry'
            """
        ).fetchone()
        if view_sql_row is None:
            raise ValueError("model_input_geometry catalog definition is unavailable")
        payload: dict[str, object] = {
            "benchmark": "categorical_reconstruction_baselines",
            "status": "development_evidence",
            "full_run": full,
            "source_relation": "main_models.model_input_geometry",
            "targets": list(TARGETS),
            "pretraining": "none",
            "upstream_model_artifacts": [],
            "run": {
                "seed": int(args.seed),
                "bootstrap_repetitions": int(args.bootstrap_repetitions),
                "arguments": {
                    "db": str(args.db),
                    "output": str(args.output),
                    "checkpoint_log": str(args.checkpoint_log),
                    "full": full,
                },
                "runtime": {
                    "python": platform.python_version(),
                    "python_implementation": sys.implementation.name,
                    "platform": platform.platform(),
                    "packages": {
                        "duckdb": duckdb.__version__,
                        "numpy": np.__version__,
                    },
                },
            },
            "truth_policy": (
                "is_observed_class AND training_weight > 0; trajectory bunt "
                "variants remapped to Bunt; duplicate event-target rows deduplicated; "
                "derived labels excluded"
            ),
            "feature_policy": (
                "decade from season and result_family, both available before target "
                "reconstruction when their source fields are present"
            ),
            "split": {
                "definition": SPLIT_DEFINITION,
                "group": "game_id",
                "train_game_count": train_games,
                "test_game_count": test_games,
                "train_game_sha256": train_digest,
                "test_game_sha256": test_digest,
                "train_test_game_overlap": overlap,
            },
            "smoothing": {
                "method": "Laplace add-one",
                "conditional_cell": "decade x result_family",
                "unseen_cell_policy": "uniform over train-observed target classes",
            },
            "metric_definitions": {
                "log_loss": "mean negative log probability assigned to the observed class",
                "brier": "mean sum of squared class-probability errors per event-target",
                "calibration": "classwise 15-bin ECE weighted across all class-event pairs",
                "improvement": "marginal metric minus era-result-family metric; positive is better",
            },
            "query_sha256": hashlib.sha256(rendered_query.encode("utf-8")).hexdigest(),
            "source_view_sql_sha256": hashlib.sha256(
                str(view_sql_row[0]).encode("utf-8")
            ).hexdigest(),
            "data_fingerprints": data_fingerprints(con),
            "invariants": invariants,
            "metrics": metric_rows(con),
            "calibration": calibration_rows(con),
            "cluster_bootstrap": bootstrap_rows(
                con,
                repetitions=int(args.bootstrap_repetitions),
                seed=int(args.seed),
            ),
            "inference_feature_coverage": coverage_rows(con, full=full),
            "limitations": [
                "The pre-1988 result is evaluated only where labels were recorded and is not representative of unrecorded historical events.",
                "This benchmark does not identify the MNAR reconstruction distribution or validate transport into games absent from play-by-play.",
                "It does not compare a fitted hierarchy because no fitted model currently shares this untouched split and dependency boundary.",
                "Result-family availability can share source-acquisition mechanisms with geometry labels; coverage is reported rather than assumed.",
                "The 64-bit row fingerprint is an integrity aid, not a cryptographic digest of the full source relation.",
            ],
        }
        logger.info("completed metrics and provenance checks")
        return payload
    finally:
        con.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    payload = run(args)
    output = cast(Path, args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
