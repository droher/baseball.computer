from __future__ import annotations

import argparse
import importlib.util
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType
from typing import Protocol, cast

import duckdb
import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "modeling_reconstruction_benchmark.py"


class BenchmarkModule(Protocol):
    def parse_args(self, argv: Sequence[str] | None = None) -> argparse.Namespace: ...

    def create_observed(self, con: duckdb.DuckDBPyConnection, *, full: bool) -> str: ...

    def create_predictions(self, con: duckdb.DuckDBPyConnection) -> None: ...

    def create_scores(self, con: duckdb.DuckDBPyConnection) -> None: ...

    def invariant_results(
        self, con: duckdb.DuckDBPyConnection
    ) -> dict[str, object]: ...

    def validate_game_disjoint(self, con: duckdb.DuckDBPyConnection) -> int: ...


def load_script() -> BenchmarkModule:
    spec = importlib.util.spec_from_file_location(
        "modeling_reconstruction_benchmark_test_target", SCRIPT_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cast(ModuleType, module))
    return cast(BenchmarkModule, module)


def seed_geometry(
    con: duckdb.DuckDBPyConnection,
    *,
    location_test_labels: tuple[str, str] = ("Default", "Left"),
    trajectory_test_labels: tuple[str, str] = ("Fly", "GroundBall"),
    overlapping_game: bool = False,
) -> None:
    con.execute("CREATE SCHEMA main_models")
    con.execute(
        """
        CREATE TABLE main_models.model_input_geometry (
            event_key INTEGER,
            geometry_dimension VARCHAR,
            class VARCHAR,
            is_observed_class BOOLEAN,
            observed_status VARCHAR,
            training_weight DOUBLE,
            game_id VARCHAR,
            season INTEGER,
            result_family VARCHAR,
            primary_fold VARCHAR
        )
        """
    )
    train_game = "shared-game" if overlapping_game else "train-game"
    test_game = "shared-game" if overlapping_game else "test-game"
    rows = [
        (
            1,
            "location_side",
            "Default",
            True,
            "observed",
            1.0,
            train_game,
            1991,
            "hit",
            "TRAIN",
        ),
        (
            2,
            "location_side",
            "Left",
            True,
            "observed",
            1.0,
            train_game,
            1991,
            "out",
            "TRAIN",
        ),
        (
            3,
            "trajectory",
            "Fly",
            True,
            "observed",
            1.0,
            train_game,
            1991,
            "hit",
            "TRAIN",
        ),
        (
            4,
            "trajectory",
            "GroundBall",
            True,
            "observed",
            1.0,
            train_game,
            1991,
            "out",
            "TRAIN",
        ),
        (
            5,
            "location_side",
            location_test_labels[0],
            True,
            "observed",
            1.0,
            test_game,
            1992,
            "hit",
            "TEST",
        ),
        (
            6,
            "location_side",
            location_test_labels[1],
            True,
            "observed",
            1.0,
            test_game,
            1992,
            "out",
            "TEST",
        ),
        (
            7,
            "trajectory",
            trajectory_test_labels[0],
            True,
            "observed",
            1.0,
            test_game,
            1992,
            "hit",
            "TEST",
        ),
        (
            8,
            "trajectory",
            trajectory_test_labels[1],
            True,
            "observed",
            1.0,
            test_game,
            1992,
            "out",
            "TEST",
        ),
        (9, "trajectory", "Fly", False, "derived", 1.0, test_game, 1992, "hit", "TEST"),
    ]
    con.executemany(
        "INSERT INTO main_models.model_input_geometry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def prediction_rows(
    module: BenchmarkModule,
    *,
    location_test_labels: tuple[str, str],
    trajectory_test_labels: tuple[str, str],
) -> list[tuple[object, ...]]:
    con = duckdb.connect(":memory:")
    try:
        seed_geometry(
            con,
            location_test_labels=location_test_labels,
            trajectory_test_labels=trajectory_test_labels,
        )
        _ = module.create_observed(con, full=True)
        module.create_predictions(con)
        return con.execute(
            """
            SELECT * FROM predictions
            ORDER BY target, era_start, result_family, label
            """
        ).fetchall()
    finally:
        con.close()


def test_test_label_changes_do_not_change_fitted_probabilities() -> None:
    module = load_script()
    original = prediction_rows(
        module,
        location_test_labels=("Default", "Left"),
        trajectory_test_labels=("Fly", "GroundBall"),
    )
    changed = prediction_rows(
        module,
        location_test_labels=("Left", "Default"),
        trajectory_test_labels=("GroundBall", "Fly"),
    )

    assert changed == original


def test_split_probability_and_score_invariants() -> None:
    module = load_script()
    con = duckdb.connect(":memory:")
    try:
        seed_geometry(con)
        _ = module.create_observed(con, full=True)
        assert module.validate_game_disjoint(con) == 0
        module.create_predictions(con)
        module.create_scores(con)
        result = module.invariant_results(con)

        assert result["test_labels_absent_from_train"] == 0
        assert cast(float, result["max_probability_sum_deviation"]) < 1e-12
        assert result["score_alignment"] == [
            {
                "target": "location_side",
                "observed_test_rows": 2,
                "scored_test_rows": 2,
            },
            {
                "target": "trajectory",
                "observed_test_rows": 2,
                "scored_test_rows": 2,
            },
        ]

        con.execute("DELETE FROM event_scores WHERE event_key = 5")
        with pytest.raises(ValueError, match="scoring alignment failed"):
            _ = module.invariant_results(con)
    finally:
        con.close()


def test_game_overlap_is_rejected() -> None:
    module = load_script()
    con = duckdb.connect(":memory:")
    try:
        seed_geometry(con, overlapping_game=True)
        _ = module.create_observed(con, full=True)

        with pytest.raises(ValueError, match="train/test game overlap is 1"):
            _ = module.validate_game_disjoint(con)
    finally:
        con.close()


def test_duplicate_observed_label_conflict_is_rejected() -> None:
    module = load_script()
    con = duckdb.connect(":memory:")
    try:
        seed_geometry(con)
        con.execute(
            """
            INSERT INTO main_models.model_input_geometry
            VALUES (1, 'location_side', 'Right', TRUE, 'observed', 1.0,
                    'train-game', 1991, 'hit', 'TRAIN')
            """
        )

        with pytest.raises(ValueError, match="event-target conflicts"):
            _ = module.create_observed(con, full=True)
    finally:
        con.close()


def test_remap_equivalent_bunt_rows_deduplicate() -> None:
    module = load_script()
    con = duckdb.connect(":memory:")
    try:
        seed_geometry(con)
        con.execute(
            """
            INSERT INTO main_models.model_input_geometry
            VALUES (10, 'trajectory', 'GroundBallBunt', TRUE, 'observed', 1.0,
                    'train-game', 1991, 'out', 'TRAIN'),
                   (10, 'trajectory', 'PopUpBunt', TRUE, 'observed', 1.0,
                    'train-game', 1991, 'out', 'TRAIN')
            """
        )

        _ = module.create_observed(con, full=True)

        assert con.execute(
            "SELECT label, COUNT(*) FROM observed WHERE event_key = 10 GROUP BY label"
        ).fetchall() == [("Bunt", 1)]
    finally:
        con.close()


def test_output_path_is_required() -> None:
    module = load_script()

    with pytest.raises(SystemExit):
        _ = module.parse_args(["--checkpoint-log", "/private/tmp/benchmark.log"])
