"""Export-time partition / fold-provenance invariants on the OOF export frame."""

from __future__ import annotations

import polars as pl
import pytest

from python_models.statistical.deep.io import (
    FOLD_ID_COLUMN,
    add_kfold_id,
    assert_export_partition_invariants,
)
from python_models.statistical.splits import game_hash_fold

FOLD_COUNT = 3
N_GAMES = 12
ROWS_PER_GAME = 3


def _train_df() -> pl.DataFrame:
    rows = [
        {
            "event_key": g_idx * 100 + r,
            "game_id": f"GAME{g_idx:04d}",
        }
        for g_idx in range(N_GAMES)
        for r in range(ROWS_PER_GAME)
    ]
    return add_kfold_id(
        pl.DataFrame(rows), game_id_column="game_id", fold_count=FOLD_COUNT
    )


def _export_frame(train_df: pl.DataFrame) -> pl.DataFrame:
    oof = train_df.select(
        pl.col("event_key"),
        pl.lit("OOF").alias("partition"),
        pl.col("kfold_id").cast(pl.Int32).alias(FOLD_ID_COLUMN),
    )
    other = pl.DataFrame(
        {
            "event_key": [990_001, 990_002],
            "partition": ["VALIDATE", "TEST"],
            FOLD_ID_COLUMN: pl.Series([None, None], dtype=pl.Int32),
        }
    )
    return pl.concat([oof, other])


def test_honest_export_passes() -> None:
    train_df = _train_df()
    assert_export_partition_invariants(
        _export_frame(train_df),
        train_df=train_df,
        grain_column="event_key",
        game_id_column="game_id",
        fold_count=FOLD_COUNT,
    )


def test_cross_partition_duplicate_raises() -> None:
    train_df = _train_df()
    export = _export_frame(train_df)
    duplicate = export.filter(pl.col("partition") == "OOF").head(1).with_columns(
        pl.lit("TEST").alias("partition"),
        pl.lit(None, dtype=pl.Int32).alias(FOLD_ID_COLUMN),
    )
    with pytest.raises(RuntimeError, match="multiple"):
        assert_export_partition_invariants(
            pl.concat([export, duplicate]),
            train_df=train_df,
            grain_column="event_key",
            game_id_column="game_id",
            fold_count=FOLD_COUNT,
        )


def test_wrong_fold_id_raises() -> None:
    train_df = _train_df()
    first_key = int(train_df["event_key"][0])
    corrupted = _export_frame(train_df).with_columns(
        pl.when(pl.col("event_key") == first_key)
        .then((pl.col(FOLD_ID_COLUMN) + 1) % FOLD_COUNT)
        .otherwise(pl.col(FOLD_ID_COLUMN))
        .cast(pl.Int32)
        .alias(FOLD_ID_COLUMN)
    )
    with pytest.raises(RuntimeError, match="does not match"):
        assert_export_partition_invariants(
            corrupted,
            train_df=train_df,
            grain_column="event_key",
            game_id_column="game_id",
            fold_count=FOLD_COUNT,
        )


def test_null_fold_id_on_oof_raises() -> None:
    train_df = _train_df()
    first_key = int(train_df["event_key"][0])
    nulled = _export_frame(train_df).with_columns(
        pl.when(pl.col("event_key") == first_key)
        .then(pl.lit(None, dtype=pl.Int32))
        .otherwise(pl.col(FOLD_ID_COLUMN))
        .alias(FOLD_ID_COLUMN)
    )
    with pytest.raises(RuntimeError, match="null"):
        assert_export_partition_invariants(
            nulled,
            train_df=train_df,
            grain_column="event_key",
            game_id_column="game_id",
            fold_count=FOLD_COUNT,
        )


def test_fold_assignment_matches_splits_hash() -> None:
    train_df = _train_df()
    for game_id, kfold in train_df.select("game_id", "kfold_id").iter_rows():
        assert kfold == game_hash_fold(game_id, fold_count=FOLD_COUNT)
