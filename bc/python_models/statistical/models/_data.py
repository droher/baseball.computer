"""Observation-model dataset prep.

Loads the frozen ``model_input_observation_batted_ball`` Parquet, filters
to a single dimension's training rows, and builds contiguous integer
indexers for season / scorer / source_family. The returned
``ObservationModelInputs`` is the only input shape ``build_*_model``
constructors accept.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import ClassVar

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict

_log = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]
FloatArray = npt.NDArray[np.float64]
ByteArray = npt.NDArray[np.int8]

DEFAULT_SMOKE_LIMIT: int = 100_000
DEFAULT_SEED: int = 20260513


class ObservationModelInputs(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    y: ByteArray
    season_idx: IntArray
    scorer_idx: IntArray
    source_idx: IntArray
    dl_logit: FloatArray
    coords: dict[str, list[str]]


def _category_index(
    df: pl.DataFrame, column: str, fill_value: str = "__unknown__"
) -> tuple[IntArray, list[str]]:
    series = df.get_column(column).fill_null(fill_value).cast(pl.Utf8)
    raw_values: list[object] = list(series.unique().to_list())
    categories: list[str] = sorted(str(v) for v in raw_values)
    mapping: dict[str, int] = {c: i for i, c in enumerate(categories)}
    codes = (
        series.replace_strict(mapping, return_dtype=pl.Int64)
        .to_numpy()
        .astype(np.int64)
    )
    return codes, categories


def prepare_observation_inputs(
    parquet_path: Path,
    *,
    dimension: str,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
) -> ObservationModelInputs:
    """Read the observation Parquet, filter to ``dimension`` training rows.

    Builds contiguous integer indexers for season / scorer / source_family
    and asserts each index covers ``range(len(category_labels))`` — guards
    against off-by-one bugs that would silently mis-index hierarchical
    effects.
    """
    df = pl.read_parquet(parquet_path)
    df = df.filter(
        (pl.col("dimension") == dimension) & (pl.col("training_weight") > 0.0)
    )
    if df.height == 0:
        raise ValueError(
            f"no rows for dimension={dimension!r} with training_weight>0 in {parquet_path}"
        )
    if smoke_limit is not None and df.height > smoke_limit:
        df = df.sample(n=smoke_limit, seed=seed)
        _log.info(
            "prepare_observation_inputs subsampled to %d rows (seed=%d)",
            smoke_limit,
            seed,
        )

    season_idx, season_labels = _category_index(df, "season")
    scorer_idx, scorer_labels = _category_index(df, "scorer")
    source_idx, source_labels = _category_index(df, "source_family")

    for name, idx, labels in (
        ("season", season_idx, season_labels),
        ("scorer", scorer_idx, scorer_labels),
        ("source", source_idx, source_labels),
    ):
        expected = set(range(len(labels)))
        actual: set[int] = set(
            pl.Series("u", np.unique(idx).astype(np.int64)).cast(pl.Int64).to_list()
        )
        if actual != expected:
            raise AssertionError(
                f"{name}_idx contiguity check failed: expected {expected}, got {actual}"
            )

    y = df.get_column("is_observed").fill_null(False).cast(pl.Int8).to_numpy()
    n = df.height
    dl_logit = np.zeros(n, dtype=np.float64)

    return ObservationModelInputs(
        y=y.astype(np.int8),
        season_idx=season_idx,
        scorer_idx=scorer_idx,
        source_idx=source_idx,
        dl_logit=dl_logit,
        coords={
            "season": list(season_labels),
            "scorer": list(scorer_labels),
            "source": list(source_labels),
        },
    )
