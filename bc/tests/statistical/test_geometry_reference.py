from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import polars as pl
import pytest

from python_models.statistical.backtests.geometry_reference import (
    baseline_probabilities,
    freeze_data,
    make_prediction_frame,
)


def source() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE SCHEMA main_models")
    con.execute("""CREATE TABLE main_models.model_input_geometry (
        event_key INTEGER, geometry_dimension VARCHAR, class VARCHAR,
        is_observed_class BOOLEAN, training_weight DOUBLE, game_id VARCHAR,
        season INTEGER, primary_fold VARCHAR, result_family VARCHAR,
        base_state_start VARCHAR, outs_start VARCHAR, alignment_regime VARCHAR,
        batter_hand VARCHAR, source_snapshot_id VARCHAR, observed_status VARCHAR
    )""")
    for key, fold, label in [
        (1, "TRAIN", "Left"),
        (2, "TRAIN", "Right"),
        (3, "TEST", "Left"),
        (4, "VALIDATE", "DoNotRead"),
    ]:
        con.execute(
            "INSERT INTO main_models.model_input_geometry VALUES (?, 'location_side', ?, TRUE, 1.0, ?, 1990, ?, 'hit', '0', '0', 'standard', NULL, 'dev', 'observed')",
            [key, label, f"game-{key}", fold],
        )
    con.execute(
        "ALTER TABLE main_models.model_input_geometry ADD COLUMN "
        "geometry_target_contract VARCHAR DEFAULT 'geometry-v2-global-side'"
    )
    return con


def test_freeze_obeys_partition_and_train_vocabulary_boundary(tmp_path: Path) -> None:
    with source() as con:
        record = freeze_data(
            con, "location_side", tmp_path / "first", smoke=True, seed=31
        )
        assert record["train_test_game_overlap"] == 0
        train = pl.read_parquet(tmp_path / "first" / "train.parquet")
        assert set(train["event_key"]) == {1, 2}
        assert set(train["batter_hand"]) == {"__MISSING__"}
        assert set(
            pl.read_parquet(tmp_path / "first" / "test.parquet")["event_key"]
        ) == {3}
        con.execute(
            "UPDATE main_models.model_input_geometry SET class='Right' WHERE primary_fold='TEST'"
        )
        freeze_data(con, "location_side", tmp_path / "second", smoke=True, seed=31)
        assert train.equals(pl.read_parquet(tmp_path / "second" / "train.parquet"))
        assert pl.read_parquet(tmp_path / "first" / "full_train_counts.parquet").equals(
            pl.read_parquet(tmp_path / "second" / "full_train_counts.parquet")
        )


@pytest.mark.parametrize("fault", ["shared_game", "conflicting_event"])
def test_freeze_rejects_population_ambiguities(tmp_path: Path, fault: str) -> None:
    with source() as con:
        if fault == "shared_game":
            con.execute(
                "UPDATE main_models.model_input_geometry SET game_id='game-1' WHERE primary_fold='TEST'"
            )
        else:
            con.execute(
                "INSERT INTO main_models.model_input_geometry SELECT * REPLACE ('Right' AS class) FROM main_models.model_input_geometry WHERE event_key=1"
            )
        with pytest.raises(ValueError, match="share games|contradictory"):
            freeze_data(con, "location_side", tmp_path / "bad", smoke=True, seed=31)


def test_baseline_normalization_and_unseen_cell_policy() -> None:
    counts = pl.DataFrame(
        {
            "era": ["1990", "1990"],
            "result_family": ["hit", "hit"],
            "target_class": ["Left", "Right"],
            "n": [9, 1],
        }
    )
    test = pl.DataFrame({"era": ["1990", "2000"], "result_family": ["hit", "new"]})
    result = baseline_probabilities(counts, test, ("Left", "Right"), conditional=True)
    np.testing.assert_allclose(result.sum(axis=1), 1)
    np.testing.assert_allclose(result[1], [0.5, 0.5])
    assert result[0, 0] > result[0, 1]


def test_prediction_frame_rejects_misaligned_scores(tmp_path: Path) -> None:
    with source() as con:
        freeze_data(con, "location_side", tmp_path, smoke=True, seed=31)
    train = pl.read_parquet(tmp_path / "train.parquet")
    test = pl.read_parquet(tmp_path / "test.parquet")
    counts = pl.read_parquet(tmp_path / "full_train_counts.parquet")
    with pytest.raises(ValueError, match="align"):
        make_prediction_frame(train, test, counts, np.ones((0, 2)), ("Left", "Right"))
    predicted = make_prediction_frame(
        train, test, counts, np.asarray([[0.6, 0.4]]), ("Left", "Right")
    )
    assert predicted["event_key"].to_list() == test["event_key"].to_list()


def test_freeze_targets_named_development_schema(tmp_path: Path) -> None:
    with source() as con:
        con.execute("CREATE SCHEMA isolated")
        con.execute(
            "CREATE VIEW isolated.model_input_geometry AS SELECT * FROM main_models.model_input_geometry"
        )
        lineage = freeze_data(
            con,
            "location_side",
            tmp_path / "named",
            smoke=True,
            seed=31,
            ledger_schema="isolated",
        )
        assert "FROM isolated.model_input_geometry" in str(lineage["source_query"])
        assert pl.read_parquet(tmp_path / "named/test.parquet")[
            "event_key"
        ].to_list() == [3]
        with pytest.raises(ValueError, match="SQL identifier"):
            freeze_data(
                con,
                "location_side",
                tmp_path / "bad",
                smoke=True,
                seed=31,
                ledger_schema="isolated; DROP SCHEMA main_models",
            )
