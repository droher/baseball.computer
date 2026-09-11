from __future__ import annotations

import argparse
import hashlib
import json
import logging
import platform
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import cast

import duckdb
import numpy as np
import numpy.typing as npt
import polars as pl

from python_models.statistical.evidence_binding import file_digest
from python_models.statistical.geometry_contract import (
    GEOMETRY_TARGET_CONTRACT,
    require_geometry_relation,
)
from python_models.statistical.validate import (
    compute_posterior_diagnostics,
    write_json_atomic,
)

_log = logging.getLogger(__name__)
FEATURES = (
    "era",
    "result_family",
    "base_state_start",
    "outs_start",
    "alignment_regime",
    "batter_hand",
)
TARGETS = ("location_side", "trajectory")
SEED = 20260911
LABEL_SQL = """CASE WHEN geometry_dimension = 'trajectory'
AND class IN ('FoulBunt', 'GroundBallBunt', 'LineDriveBunt', 'PopUpBunt', 'UnspecifiedBunt')
THEN 'Bunt' ELSE class END"""


def write_json(path: Path, value: object) -> None:
    write_json_atomic(path, json.dumps(value, indent=2, allow_nan=False))


def rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, object]]:
    result = con.execute(sql)
    names = [item[0] for item in result.description]
    return [dict(zip(names, row, strict=True)) for row in result.fetchall()]


def game_digest(con: duckdb.DuckDBPyConnection, relation: str) -> str:
    digest = hashlib.sha256()
    for (game_id,) in con.execute(
        f"SELECT DISTINCT game_id FROM {relation} ORDER BY game_id"
    ).fetchall():
        encoded = str(game_id).encode()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def freeze_data(
    con: duckdb.DuckDBPyConnection,
    target: str,
    output_dir: Path,
    *,
    smoke: bool,
    seed: int,
) -> dict[str, object]:
    if target not in TARGETS:
        raise ValueError(f"unsupported target: {target}")
    if target == "location_side":
        require_geometry_relation(
            con,
            "(SELECT * FROM main_models.model_input_geometry "
            "WHERE primary_fold IN ('TRAIN', 'TEST'))",
            dimension_column="geometry_dimension",
            check_classes=True,
        )
    projection = ", ".join(
        f"COALESCE(CAST({field} AS VARCHAR), '__MISSING__') AS {field}"
        for field in FEATURES
        if field != "era"
    )
    query = f"""SELECT event_key, game_id, season, primary_fold,
        {LABEL_SQL} AS target_class,
        CAST(CAST(FLOOR(season / 10) * 10 AS INTEGER) AS VARCHAR) AS era,
        {projection}
        FROM main_models.model_input_geometry
        WHERE geometry_dimension = '{target}' AND is_observed_class
        AND training_weight > 0 AND game_id IS NOT NULL
        AND primary_fold IN ('TRAIN', 'TEST')"""
    con.execute(f"CREATE OR REPLACE TEMP TABLE observed_raw AS {query}")
    conflicts = con.execute(
        "SELECT COUNT(*) FROM (SELECT event_key FROM observed_raw GROUP BY event_key HAVING COUNT(DISTINCT (target_class, game_id, season, primary_fold, era, result_family, base_state_start, outs_start, alignment_regime, batter_hand)) > 1)"
    ).fetchone()
    if conflicts is None or conflicts[0] != 0:
        raise ValueError("contradictory observed event rows")
    con.execute(
        "CREATE OR REPLACE TEMP TABLE observed AS SELECT * FROM observed_raw QUALIFY ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY target_class) = 1"
    )
    con.execute(
        "CREATE OR REPLACE TEMP VIEW full_train AS SELECT * FROM observed WHERE primary_fold='TRAIN'"
    )
    con.execute(
        "CREATE OR REPLACE TEMP VIEW full_test AS SELECT * FROM observed WHERE primary_fold='TEST'"
    )
    overlap = con.execute(
        "SELECT COUNT(*) FROM (SELECT game_id FROM full_train INTERSECT SELECT game_id FROM full_test)"
    ).fetchone()
    if overlap is None or overlap[0] != 0:
        raise ValueError("primary TRAIN and TEST share games")
    budget = 3000 if smoke else 100000
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE selected_train AS SELECT * FROM full_train ORDER BY sha256(CAST(event_key AS VARCHAR) || ':{seed}'), event_key LIMIT {budget}"
    )
    test_limit = "LIMIT 5000" if smoke else ""
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE selected_test AS SELECT * FROM full_test ORDER BY event_key {test_limit}"
    )
    con.execute(
        "CREATE OR REPLACE TEMP TABLE full_train_counts AS SELECT era, result_family, target_class, COUNT(*) AS n FROM full_train GROUP BY ALL"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    for relation, name in (
        ("selected_train", "train"),
        ("selected_test", "test"),
        ("full_train_counts", "full_train_counts"),
    ):
        escaped = str(output_dir / f"{name}.parquet").replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM {relation} ORDER BY ALL) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    populations = rows(
        con,
        "SELECT primary_fold, COUNT(*) AS rows, COUNT(DISTINCT game_id) AS games FROM observed GROUP BY primary_fold ORDER BY primary_fold",
    )
    selected = rows(
        con,
        "SELECT primary_fold, COUNT(*) AS rows, COUNT(DISTINCT game_id) AS games FROM (SELECT * FROM selected_train UNION ALL SELECT * FROM selected_test) GROUP BY primary_fold ORDER BY primary_fold",
    )
    coverage_projection = ", ".join(
        f"COUNT(*) FILTER (WHERE {field} IS NULL) AS {field}_null"
        for field in FEATURES
        if field != "era"
    )
    coverage = rows(
        con,
        f"SELECT COUNT(*) AS rows, {coverage_projection} FROM main_models.model_input_geometry WHERE geometry_dimension='{target}' AND NOT is_observed_class AND training_weight>0 AND observed_status IN ('missing','unknown_code')",
    )
    record: dict[str, object] = {
        "target": target,
        "source_query": query,
        "source_query_sha256": hashlib.sha256(query.encode()).hexdigest(),
        "source_snapshot_labels": rows(
            con,
            f"SELECT DISTINCT source_snapshot_id FROM main_models.model_input_geometry WHERE geometry_dimension='{target}'",
        ),
        "full_populations": populations,
        "selected_populations": selected,
        "requested_train_budget": budget,
        "train_test_game_overlap": int(overlap[0]),
        "full_train_games_sha256": game_digest(con, "full_train"),
        "full_test_games_sha256": game_digest(con, "full_test"),
        "selected_train_games_sha256": game_digest(con, "selected_train"),
        "selected_test_games_sha256": game_digest(con, "selected_test"),
        "inference_feature_missingness": coverage,
        "snapshot_files": {
            path.name: file_digest(path) for path in output_dir.glob("*.parquet")
        },
        "learned_inputs": [],
        "uses_validate_partition": False,
    }
    write_json(output_dir / "lineage.json", record)
    _log.info(
        "froze target=%s populations=%s selected=%s", target, populations, selected
    )
    return record


def baseline_probabilities(
    train_counts: pl.DataFrame,
    test: pl.DataFrame,
    labels: tuple[str, ...],
    *,
    conditional: bool,
) -> npt.NDArray[np.float64]:
    label_index = {label: index for index, label in enumerate(labels)}
    k = len(labels)
    counts: dict[tuple[str, str], npt.NDArray[np.float64]] = {}
    for era, result, label, count in train_counts.select(
        "era", "result_family", "target_class", "n"
    ).iter_rows():
        if label not in label_index:
            raise ValueError(
                f"baseline class {label} absent from model TRAIN vocabulary"
            )
        key = (str(era), str(result)) if conditional else ("", "")
        if key not in counts:
            counts[key] = np.ones(k, dtype=np.float64)
        counts[key][label_index[str(label)]] += float(count)
    probabilities = {key: value / value.sum() for key, value in counts.items()}
    uniform = np.full(k, 1.0 / k)
    if not conditional:
        return np.tile(probabilities.get(("", ""), uniform), (test.height, 1))
    keys = list(test.select("era", "result_family").iter_rows())
    return np.asarray(
        [probabilities.get((str(era), str(result)), uniform) for era, result in keys],
        dtype=np.float64,
    )


def make_prediction_frame(
    train: pl.DataFrame,
    test: pl.DataFrame,
    full_counts: pl.DataFrame,
    model_probabilities: npt.NDArray[np.float64],
    labels: tuple[str, ...],
) -> pl.DataFrame:
    train_games = set(train.get_column("game_id"))
    if train_games.intersection(test.get_column("game_id")):
        raise ValueError("selected TRAIN and TEST share games")
    if set(test.get_column("target_class")) - set(labels):
        raise ValueError("TEST contains classes absent from selected TRAIN")
    if model_probabilities.shape != (test.height, len(labels)):
        raise ValueError("model probabilities do not align to TEST")
    budget_counts = train.group_by("era", "result_family", "target_class").len(name="n")
    return test.select("event_key", "game_id", "season", "target_class").with_columns(
        pl.Series("model_probability", model_probabilities),
        pl.Series(
            "budget_marginal_probability",
            baseline_probabilities(budget_counts, test, labels, conditional=False),
        ),
        pl.Series(
            "budget_contextual_probability",
            baseline_probabilities(budget_counts, test, labels, conditional=True),
        ),
        pl.Series(
            "full_contextual_probability",
            baseline_probabilities(full_counts, test, labels, conditional=True),
        ),
    )


def run_target(
    con: duckdb.DuckDBPyConnection, target: str, root: Path, *, smoke: bool, seed: int
) -> dict[str, object]:
    from python_models.statistical.backtests.geometry_reference_model import (
        fit_reference,
        predict_reference,
    )
    from python_models.statistical.backtests.geometry_reference_evaluation import (
        evaluate_reference,
    )

    output_dir = root / target
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite {output_dir}")
    lineage = freeze_data(con, target, output_dir / "data", smoke=smoke, seed=seed)
    train = pl.read_parquet(output_dir / "data" / "train.parquet")
    test = pl.read_parquet(output_dir / "data" / "test.parquet")
    full_counts = pl.read_parquet(output_dir / "data" / "full_train_counts.parquet")
    idata, levels, labels = fit_reference(
        train, output_dir=output_dir, smoke=smoke, seed=seed
    )
    diagnostics = compute_posterior_diagnostics(idata)
    write_json(output_dir / "diagnostics.json", diagnostics.model_dump())
    _log.info(
        "posterior diagnostics target=%s rhat=%s ess_bulk=%s divergences=%s",
        target,
        diagnostics.rhat_max,
        diagnostics.ess_bulk_min,
        diagnostics.divergences,
    )
    model_probabilities = predict_reference(idata, test, levels, labels)
    predictions = make_prediction_frame(
        train, test, full_counts, model_probabilities, labels
    )
    predictions.write_parquet(output_dir / "test_predictions.parquet")
    _log.info("saved aligned predictions target=%s rows=%s", target, predictions.height)
    evaluated = evaluate_reference(
        predictions, labels, repetitions=20 if smoke else 500, seed=seed
    )
    numerical_pass = (
        diagnostics.rhat_max <= 1.05
        and min(diagnostics.ess_bulk_min, diagnostics.ess_tail_min) >= 100
        and diagnostics.divergences == 0
    )
    predictive_pass = (
        cast(dict[str, object], evaluated["decision_flags"]).get("passed") is True
    )
    report: dict[str, object] = {
        "target": target,
        "status": "smoke" if smoke else "development_evidence",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "class_labels": labels,
        "feature_levels": levels,
        "unseen_test_feature_rows": {
            field: test.filter(~pl.col(field).is_in(vocab)).height
            for field, vocab in levels.items()
        },
        "lineage": lineage,
        "numerical_pass": numerical_pass,
        "protocol_pass": numerical_pass and predictive_pass and not smoke,
        "diagnostics": diagnostics.model_dump(),
        "evaluation": evaluated,
        "publication_mode": "research_only",
        "transport": "unsupported",
        "identification": "unsupported",
        "artifact_files": {
            str(path.relative_to(output_dir)): file_digest(path)
            for path in output_dir.rglob("*")
            if path.is_file()
        },
    }
    write_json(output_dir / "report.json", report)
    for group in idata.groups():
        idata[group].close()
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("bc.db"))
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--target", choices=TARGETS, action="append")
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    root = cast(Path, args.output_root).resolve()
    root.mkdir(parents=True, exist_ok=False)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(root / "run.log")],
    )
    protocol_path = (
        Path(__file__).resolve().parents[4]
        / "docs"
        / "geometry-reference-global-side-protocol.md"
    )
    protocol: dict[str, object] = {
        "geometry_target_contract": GEOMETRY_TARGET_CONTRACT,
        "protocol_text": protocol_path.read_text(),
        "protocol_sha256": file_digest(protocol_path),
        "seed": args.seed,
        "smoke": bool(args.smoke),
        "targets": args.target or TARGETS,
        "packages": {
            name: version(name)
            for name in ("numpy", "polars", "duckdb", "pymc", "arviz", "nutpie")
        },
        "python": platform.python_version(),
        "implementation_hashes": {
            name: file_digest(Path(__file__).with_name(name))
            for name in (
                "geometry_reference.py",
                "geometry_reference_model.py",
                "geometry_reference_evaluation.py",
            )
        },
    }
    write_json(root / "protocol.json", protocol)
    implementation_dir = root / "implementation"
    implementation_dir.mkdir()
    implementation_hashes = cast(dict[str, str], protocol["implementation_hashes"])
    for name, expected in implementation_hashes.items():
        content = Path(__file__).with_name(name).read_bytes()
        if hashlib.sha256(content).hexdigest() != expected:
            raise ValueError(f"implementation changed while freezing protocol: {name}")
        (implementation_dir / name).write_bytes(content)
    (implementation_dir / "geometry-reference-protocol.md").write_text(
        protocol_path.read_text()
    )
    con = duckdb.connect(str(args.db), read_only=True)
    try:
        reports = [
            run_target(con, target, root, smoke=bool(args.smoke), seed=int(args.seed))
            for target in args.target or TARGETS
        ]
        write_json(root / "results.json", {"protocol": protocol, "targets": reports})
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
