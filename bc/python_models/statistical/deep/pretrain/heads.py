"""Per-head encoding + NULL-masking for pretext targets."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import polars as pl
from numpy.typing import NDArray

from python_models.statistical.deep.pretrain.spec import HeadSpec

_log = logging.getLogger(__name__)


def apply_target_remap(df: pl.DataFrame, head: HeadSpec) -> pl.DataFrame:
    if not head.target_remap:
        return df
    remap = {src: dst for src, dst in head.target_remap}
    return df.with_columns(
        pl.col(head.target_column)
        .cast(pl.Utf8)
        .replace(remap)
        .alias(head.target_column)
    )


def build_class_labels(
    train_df: pl.DataFrame,
    head: HeadSpec,
) -> tuple[str, ...]:
    if head.class_universe_source == "configured":
        if not head.configured_class_labels:
            raise ValueError(
                f"head {head.name!r} class_universe_source='configured' but configured_class_labels is empty"
            )
        return tuple(head.configured_class_labels)
    labels = (
        train_df[head.target_column]
        .cast(pl.Utf8)
        .drop_nulls()
        .unique()
        .sort()
        .to_list()
    )
    return tuple(str(v) for v in labels)


def encode_multiclass_head(
    df: pl.DataFrame,
    head: HeadSpec,
    class_index: dict[str, int],
) -> tuple[NDArray[np.int64], NDArray[np.float32]]:
    codes = (
        df[head.target_column]
        .cast(pl.Utf8)
        .replace_strict(class_index, default=-1)
        .cast(pl.Int64)
        .to_numpy()
        .astype(np.int64)
    )
    valid = codes >= 0
    weights = valid.astype(np.float32)
    safe_codes = np.where(valid, codes, 0).astype(np.int64)
    return safe_codes, weights


def encode_binary_head(
    df: pl.DataFrame,
    head: HeadSpec,
) -> tuple[NDArray[np.float32], NDArray[np.float32]]:
    series = df[head.target_column]
    if series.dtype != pl.Boolean:
        series = series.cast(pl.Boolean, strict=False)
    arr = series.to_numpy()
    valid = ~series.is_null().to_numpy()
    y = np.zeros_like(arr, dtype=np.float32)
    y[valid] = arr[valid].astype(np.float32)
    weights = valid.astype(np.float32)
    return y.reshape(-1, 1), weights


def encode_head(
    df: pl.DataFrame,
    head: HeadSpec,
    class_index: dict[str, int] | None,
) -> tuple[NDArray[Any], NDArray[np.float32]]:
    if head.kind == "multiclass":
        if class_index is None:
            raise ValueError(
                f"multiclass head {head.name!r} requires class_index"
            )
        return encode_multiclass_head(df, head, class_index)
    if head.kind == "binary":
        return encode_binary_head(df, head)
    raise ValueError(f"unsupported head kind {head.kind!r}")
