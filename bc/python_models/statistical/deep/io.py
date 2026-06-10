"""Dataset Parquet loading + in-memory k-fold assignment.

The deep fold runner consumes a single ``model_input_*`` Parquet snapshot
(as produced by ``bc-stats prepare-dataset``) and assigns each TRAIN row
to one of ``fold_count`` out-of-fold buckets by hashing the row's
``game_id``. ``kfold_id`` is computed per run and never written back to
the SQLMesh view.
"""

from __future__ import annotations

import logging

import polars as pl

from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

KFOLD_COLUMN: str = "kfold_id"
FOLD_ID_COLUMN: str = "fold_id"
PARTITION_COLUMN: str = "partition"
OOF_PARTITION_LABEL: str = "OOF"


def load_dataset_parquet(parquet_path: str | bytes) -> pl.DataFrame:
    return pl.read_parquet(parquet_path)


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
    folds = [
        game_hash_fold(str(g), fold_count=fold_count)
        for g in df[game_id_column].to_list()
    ]
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


def assert_export_partition_invariants(
    probabilities: pl.DataFrame,
    *,
    train_df: pl.DataFrame,
    grain_column: str,
    game_id_column: str,
    fold_count: int,
    partition_column: str = PARTITION_COLUMN,
    fold_id_column: str = FOLD_ID_COLUMN,
) -> None:
    """Raise unless the export frame keeps partitions disjoint and OOF fold provenance honest."""
    multi_partition = (
        probabilities.group_by(grain_column)
        .agg(pl.col(partition_column).n_unique().alias("n_partitions"))
        .filter(pl.col("n_partitions") > 1)
    )
    if multi_partition.height > 0:
        sample = multi_partition[grain_column].to_list()[:10]
        raise RuntimeError(
            f"{multi_partition.height} {grain_column} values appear in multiple "
            f"export partitions; sample: {sample}"
        )

    oof = probabilities.filter(pl.col(partition_column) == OOF_PARTITION_LABEL)
    if oof.height == 0:
        return
    null_folds = int(oof[fold_id_column].null_count())
    if null_folds > 0:
        raise RuntimeError(
            f"{null_folds} OOF rows have a null {fold_id_column}; OOF export "
            "requires per-fold provenance"
        )
    games = train_df.select(grain_column, game_id_column).unique(subset=grain_column)
    joined = oof.select(grain_column, fold_id_column).join(
        games, on=grain_column, how="left"
    )
    missing_games = int(joined[game_id_column].null_count())
    if missing_games > 0:
        raise RuntimeError(
            f"{missing_games} OOF rows have no matching TRAIN {game_id_column}; "
            "cannot verify fold provenance"
        )
    expected = pl.Series(
        "expected_fold",
        [
            game_hash_fold(str(g), fold_count=fold_count)
            for g in joined[game_id_column].to_list()
        ],
        dtype=pl.Int32,
    )
    mismatched = joined.with_columns(expected).filter(
        pl.col(fold_id_column).cast(pl.Int32) != pl.col("expected_fold")
    )
    if mismatched.height > 0:
        sample = mismatched[grain_column].to_list()[:10]
        raise RuntimeError(
            f"{mismatched.height} OOF rows carry a {fold_id_column} that does not "
            f"match game_hash_fold({game_id_column}) % {fold_count}; sample "
            f"{grain_column}s: {sample}"
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
