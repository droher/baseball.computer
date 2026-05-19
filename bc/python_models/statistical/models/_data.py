"""Observation-model dataset prep.

Loads the frozen ``model_input_observation_batted_ball`` Parquet, filters
to a single dimension's training rows, and builds contiguous integer
indexers for season / scorer / source_family. The returned
``ObservationModelInputs`` is the only input shape ``build_*_model``
constructors accept.
"""

from __future__ import annotations

import json
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
_LOGIT_CLIP: float = 1e-6


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


def _compute_dl_logits(
    df: pl.DataFrame,
    *,
    dl_artifact_dir: Path,
    collapse_positive_classes: tuple[str, ...],
) -> FloatArray:
    """Join published DL probabilities by ``event_key`` and return logits.

    When ``collapse_positive_classes`` is empty, take ``logit(max p)`` per
    row (DL confidence). When non-empty, sum the listed class
    probabilities and take ``logit`` of the sum (e.g., broad_contact's
    AirBall collapse). Rows with no DL match get logit(0.5)=0.
    """
    probabilities_path = dl_artifact_dir / "exports" / "probabilities.parquet"
    class_labels_path = dl_artifact_dir / "exports" / "class_labels.json"
    if not probabilities_path.exists():
        raise FileNotFoundError(
            f"DL probabilities parquet missing at {probabilities_path}"
        )
    if not class_labels_path.exists():
        raise FileNotFoundError(f"DL class labels missing at {class_labels_path}")

    labels_payload = json.loads(class_labels_path.read_text(encoding="utf-8"))
    if "labels" in labels_payload:
        labels: list[str] = list(labels_payload["labels"])
    else:
        raise ValueError(
            f"unsupported class_labels.json shape at {class_labels_path}: single-head 'labels' key required"
        )

    if collapse_positive_classes:
        missing = [c for c in collapse_positive_classes if c not in labels]
        if missing:
            raise ValueError(
                f"collapse classes {missing!r} not in DL labels {labels!r} (artifact {dl_artifact_dir})"
            )
        positive_indices = tuple(labels.index(c) for c in collapse_positive_classes)
    else:
        positive_indices = ()

    probs = pl.read_parquet(probabilities_path).select(["event_key", "dl_p_class"])
    if positive_indices:
        idx_list = list(positive_indices)
        probs = probs.with_columns(
            pl.col("dl_p_class")
            .list.gather(idx_list, null_on_oob=False)
            .list.sum()
            .alias("_p")
        )
    else:
        probs = probs.with_columns(pl.col("dl_p_class").list.max().alias("_p"))
    probs = probs.select(["event_key", "_p"])

    df_keys = df.select(["event_key"]).with_row_index(name="_row")
    joined = df_keys.join(probs, on="event_key", how="left")
    joined = joined.sort("_row")
    unmatched = int(joined.get_column("_p").null_count())
    if unmatched:
        _log.info(
            "_compute_dl_logits unmatched event_keys=%d / %d (filled with logit=0)",
            unmatched,
            df.height,
        )
    p_arr = joined.get_column("_p").fill_null(0.5).to_numpy().astype(np.float64)
    p_clipped = np.clip(p_arr, _LOGIT_CLIP, 1.0 - _LOGIT_CLIP)
    return np.log(p_clipped / (1.0 - p_clipped))


def prepare_observation_inputs(
    parquet_path: Path,
    *,
    dimension: str,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
    dl_artifact_dir: Path | None = None,
    dl_class_collapse_positive: tuple[str, ...] = (),
) -> ObservationModelInputs:
    """Read the observation Parquet, filter to ``dimension`` training rows.

    Builds contiguous integer indexers for season / scorer / source_family
    and asserts each index covers ``range(len(category_labels))`` — guards
    against off-by-one bugs that would silently mis-index hierarchical
    effects. When ``dl_artifact_dir`` is provided, joins published DL
    probabilities by ``event_key`` and emits ``dl_logit`` (logit of max
    class probability, or of the summed collapse classes).
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
    if dl_artifact_dir is not None:
        dl_logit = _compute_dl_logits(
            df,
            dl_artifact_dir=dl_artifact_dir,
            collapse_positive_classes=dl_class_collapse_positive,
        )
    else:
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
