"""Cell-grain prep for the assist-count submodel.

Reads ``model_input_fielding_credit`` and aggregates the realized
assist count ``M`` per event (sum of ``known_credit`` over the 9 fielder
positions for ``credit_type='assist'``) on the well-attributed slice
(``personnel_hard_mask_available=TRUE``). Buckets ``M`` into the
spec's class set ``{1, 2, 3, 4}`` (counts above the cap fold into the
top class), restricted to events with at least one assist (the count
model conditions on an assist having occurred — ``M=0`` is the NONE
class handled by the K=10 allocation softmax). Rolls events up to
``(event_class, base_out)`` cells where ``event_class`` is the
``result_family`` proxy and ``base_out`` pairs ``base_state_start`` with
``outs_start``. Each cell carries a length-``M_MAX`` count vector — the
aggregate of the per-event one-hot assist-count labels — so the
likelihood collapses to one cell-grain Multinomial.

A deterministic per-game held-out split (``HOLDOUT_FOLD_COUNT=10`` via
``game_hash_fold``, fold 0) is removed before training so the
cell-grain count vectors are computed on the training games only.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
from pathlib import Path
from typing import ClassVar

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict

from python_models.statistical.models._credit_data import (
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    MIN_EVENTS_PER_SEASON,
    UNKNOWN_LEVEL,
)
from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]

DEFAULT_SEED: int = 20260513

ASSIST_COUNT_MAX: int = 4
ASSIST_COUNT_CLASSES: tuple[str, ...] = tuple(
    str(m) for m in range(1, ASSIST_COUNT_MAX + 1)
)

EVENT_CLASS_COLUMN: str = "result_family"
BASE_OUT_COLUMNS: tuple[str, ...] = ("base_state_start", "outs_start")

MIN_EVENTS_PER_CELL: int = 25


class AssistCountInputs(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    counts: IntArray
    cell_event_class_idx: IntArray
    coords: dict[str, list[str]]

    @property
    def n_cells(self) -> int:
        return int(self.counts.shape[0])

    @property
    def n_classes(self) -> int:
        return int(self.counts.shape[1])

    @property
    def n_events(self) -> int:
        return int(self.counts.sum())


def _bucket_assist_count(total: int) -> int:
    if total < 1:
        return -1
    return min(total, ASSIST_COUNT_MAX) - 1


def prepare_assist_count_inputs(
    parquet_path: Path,
    *,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
    min_events_per_season: int = MIN_EVENTS_PER_SEASON,
    min_events_per_cell: int = MIN_EVENTS_PER_CELL,
    held_out_fold_id: int = HOLDOUT_FOLD_ID,
    held_out_fold_count: int = HOLDOUT_FOLD_COUNT,
) -> AssistCountInputs:
    """Aggregate per-event assist counts to ``(event_class, base_out)`` cells."""
    per_event = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col("credit_type") == "assist")
            & (pl.col("personnel_hard_mask_available") == True)  # noqa: E712
        )
        .group_by("event_key")
        .agg(
            pl.col("known_credit").sum().round(0).cast(pl.Int64).alias("assist_total"),
            pl.first("game_id").alias("game_id"),
            pl.first("season").alias("season"),
            pl.first(EVENT_CLASS_COLUMN).alias(EVENT_CLASS_COLUMN),
            pl.first("base_state_start").alias("base_state_start"),
            pl.first("outs_start").alias("outs_start"),
        )
        .filter(pl.col("assist_total") >= 1)
        .collect()
    )
    if per_event.height == 0:
        raise ValueError(
            f"no assist events with M>=1 on the hard-mask slice in {parquet_path}"
        )

    season_counts = per_event.group_by("season").agg(pl.len().alias("_n"))
    informative = season_counts.filter(
        pl.col("_n") >= min_events_per_season
    ).get_column("season")
    dropped = season_counts.height - informative.len()
    if dropped:
        _log.info(
            "prepare_assist_count_inputs dropped %d seasons with <%d events",
            dropped,
            min_events_per_season,
        )
        per_event = per_event.filter(pl.col("season").is_in(informative.implode()))
        if per_event.height == 0:
            raise ValueError(
                f"every season fell below min_events_per_season={min_events_per_season}"
            )

    distinct_game_ids = per_event.get_column("game_id").unique().to_list()
    holdout_game_ids = [
        g
        for g in distinct_game_ids
        if game_hash_fold(g, fold_count=held_out_fold_count) == held_out_fold_id
    ]
    per_event = per_event.filter(~pl.col("game_id").is_in(holdout_game_ids))
    _log.info(
        "prepare_assist_count_inputs held-out %d/%d games via fold %d/%d",
        len(holdout_game_ids),
        len(distinct_game_ids),
        held_out_fold_id,
        held_out_fold_count,
    )
    if per_event.height == 0:
        raise ValueError("every game landed in the held-out fold; nothing to fit")

    if smoke_limit is not None and per_event.height > smoke_limit:
        per_event = per_event.sample(n=smoke_limit, seed=seed, shuffle=True)
        _log.info(
            "prepare_assist_count_inputs subsampled to %d events (budget=%d, seed=%d)",
            per_event.height,
            smoke_limit,
            seed,
        )

    class_idx = np.fromiter(
        (
            _bucket_assist_count(int(t))
            for t in per_event.get_column("assist_total").to_list()
        ),
        dtype=np.int64,
        count=per_event.height,
    )

    event_class = (
        per_event.get_column(EVENT_CLASS_COLUMN)
        .fill_null(UNKNOWN_LEVEL)
        .cast(pl.Utf8)
        .to_list()
    )
    base_out = [
        f"{b}|{o}"
        for b, o in zip(
            per_event.get_column("base_state_start").fill_null(-1).to_list(),
            per_event.get_column("outs_start").fill_null(-1).to_list(),
            strict=True,
        )
    ]
    cell_keys = [f"{ec}||{bo}" for ec, bo in zip(event_class, base_out, strict=True)]

    cell_df = pl.DataFrame(
        {
            "cell_key": cell_keys,
            "event_class": event_class,
            "class_idx": class_idx,
        }
    )
    cell_sizes = cell_df.group_by("cell_key").agg(pl.len().alias("_n"))
    kept_cells = cell_sizes.filter(
        pl.col("_n") >= min_events_per_cell
    ).get_column("cell_key")
    dropped_cells = cell_sizes.height - kept_cells.len()
    if dropped_cells:
        _log.info(
            "prepare_assist_count_inputs dropped %d cells with <%d events",
            dropped_cells,
            min_events_per_cell,
        )
        cell_df = cell_df.filter(pl.col("cell_key").is_in(kept_cells.implode()))
    if cell_df.height == 0:
        raise ValueError(
            f"every cell fell below min_events_per_cell={min_events_per_cell}"
        )

    cell_labels = sorted(cell_df.get_column("cell_key").unique().to_list())
    cell_to_idx = {c: i for i, c in enumerate(cell_labels)}
    event_class_labels = sorted(cell_df.get_column("event_class").unique().to_list())
    event_class_to_idx = {c: i for i, c in enumerate(event_class_labels)}

    n_cells = len(cell_labels)
    n_classes = ASSIST_COUNT_MAX
    counts = np.zeros((n_cells, n_classes), dtype=np.int64)
    cell_event_class_idx = np.zeros(n_cells, dtype=np.int64)

    grouped = cell_df.group_by("cell_key").agg(
        pl.col("class_idx"),
        pl.first("event_class").alias("event_class"),
    )
    for row in grouped.iter_rows(named=True):
        ci = cell_to_idx[row["cell_key"]]
        cell_event_class_idx[ci] = event_class_to_idx[row["event_class"]]
        for k in row["class_idx"]:
            counts[ci, int(k)] += 1

    coords: dict[str, list[str]] = {
        "cell": cell_labels,
        "event_class": event_class_labels,
        "assist_count_class": list(ASSIST_COUNT_CLASSES),
        "assist_count_nonref": list(ASSIST_COUNT_CLASSES[1:]),
    }

    _log.info(
        "prepare_assist_count_inputs cells=%d event_classes=%d classes=%d events=%d",
        n_cells,
        len(event_class_labels),
        n_classes,
        int(counts.sum()),
    )

    return AssistCountInputs(
        counts=counts,
        cell_event_class_idx=cell_event_class_idx,
        coords=coords,
    )
