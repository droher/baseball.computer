"""Dataset Parquet loading + in-memory k-fold assignment.

The deep fold runner consumes a single ``model_input_*`` Parquet snapshot
(as produced by ``bc-stats prepare-dataset``) and assigns each TRAIN row
to one of ``fold_count`` out-of-fold buckets by hashing the row's
``game_id``. ``kfold_id`` is computed per run and never written back to
the SQLMesh view.
"""

from __future__ import annotations

import hashlib
import logging

import polars as pl

_log = logging.getLogger(__name__)

KFOLD_COLUMN: str = "kfold_id"


def load_dataset_parquet(parquet_path: str | bytes) -> pl.DataFrame:
    return pl.read_parquet(parquet_path)


def _game_hash_kfold(game_id: str, fold_count: int) -> int:
    digest = hashlib.blake2s(game_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % fold_count


def add_kfold_id(
    df: pl.DataFrame,
    *,
    game_id_column: str,
    fold_count: int,
    out_column: str = KFOLD_COLUMN,
) -> pl.DataFrame:
    if fold_count <= 1:
        raise ValueError(f"fold_count must be > 1, got {fold_count}")
    if game_id_column not in df.columns:
        raise KeyError(f"game_id column {game_id_column!r} not in frame")
    folds = [_game_hash_kfold(str(g), fold_count) for g in df[game_id_column].to_list()]
    return df.with_columns(pl.Series(out_column, folds, dtype=pl.Int32))


def assert_game_group_invariant(
    df: pl.DataFrame,
    *,
    game_id_column: str,
    kfold_column: str = KFOLD_COLUMN,
) -> None:
    grouped = (
        df.group_by(game_id_column)
        .agg(pl.col(kfold_column).n_unique().alias("n_folds"))
        .filter(pl.col("n_folds") > 1)
    )
    offenders = grouped[game_id_column].to_list()
    if offenders:
        sample = offenders[:10]
        raise ValueError(
            f"{len(offenders)} game_ids span multiple kfold_id values; sample: {sample}"
        )


def partition_by_split(
    df: pl.DataFrame,
    *,
    split_column: str,
    train_label: str,
    validate_label: str,
    test_label: str,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    if split_column not in df.columns:
        raise KeyError(f"split column {split_column!r} not in frame")
    train_df = df.filter(pl.col(split_column) == train_label)
    validate_df = df.filter(pl.col(split_column) == validate_label)
    test_df = df.filter(pl.col(split_column) == test_label)
    _log.info(
        "partition: train=%d validate=%d test=%d (split_column=%s)",
        train_df.height,
        validate_df.height,
        test_df.height,
        split_column,
    )
    return train_df, validate_df, test_df
