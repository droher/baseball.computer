"""Event-grain prep for the pitch-summary ``has_count`` coverage arm.

Reads the per-event ``model_input_pitch_summary`` Parquet at the full
``event_level`` grain (both the ``has_count`` and the not-``has_count``
population) and shapes a Bernoulli design over whether the plate
appearance's final ball-strike count was observed. The outcome is the
``has_count`` flag; the linear predictor pools partially over a
``season|league`` cell (the dense observation regime, mirroring the
season random effect in the obs-propensity model) and the ``scorer``,
with sum-to-zero fixed effects for ``result_family`` and
``alignment_regime``. A deterministic per-game held-out split
(``HOLDOUT_FOLD_COUNT=10`` via ``game_hash_fold``, fold 0) is removed
before training and ships alongside for OOS scoring.
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

from python_models.statistical.models._credit_data import FixedEffectDesign
from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]
FloatArray = npt.NDArray[np.float64]

DEFAULT_SEED: int = 20260513

HOLDOUT_FOLD_COUNT: int = 10
HOLDOUT_FOLD_ID: int = 0

UNKNOWN_LEVEL: str = "__unknown__"

UNSEEN_LEVEL_CODE: int = -1

DIMENSION: str = "has_count"

CONTEXT_FIXED_EFFECT_COLUMNS: tuple[str, ...] = (
    "result_family",
    "alignment_regime",
)


class PitchCoverageHeldOutSet(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    y: IntArray
    cell_idx: IntArray
    scorer_idx: IntArray
    fixed_effect_codes: dict[str, IntArray]

    @property
    def n_events(self) -> int:
        return int(self.y.shape[0])


class PitchCoverageScoringFrame(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    event_keys: IntArray
    cell_idx: IntArray
    scorer_idx: IntArray
    fixed_effect_codes: dict[str, IntArray]

    @property
    def n_events(self) -> int:
        return int(self.event_keys.shape[0])


class PitchCoverageInputs(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    y: IntArray
    cell_idx: IntArray
    scorer_idx: IntArray
    fixed_effects: dict[str, FixedEffectDesign]

    cell_labels: list[str]
    scorer_labels: list[str]
    coords: dict[str, list[str]]
    outcome: str
    dimension: str
    held_out: PitchCoverageHeldOutSet

    @property
    def n_events(self) -> int:
        return int(self.y.shape[0])

    @property
    def n_cells(self) -> int:
        return len(self.cell_labels)


def _coverage_frame(parquet_path: Path) -> pl.DataFrame:
    return (
        pl.scan_parquet(parquet_path)
        .filter(pl.col("game_id").is_not_null())
        .with_columns(
            pl.col("has_count").cast(pl.Int64).alias("y"),
            (
                pl.col("season").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("league").cast(pl.Utf8).fill_null(UNKNOWN_LEVEL)
            ).alias("season_league"),
            pl.col("scorer").fill_null(UNKNOWN_LEVEL).alias("scorer"),
            pl.col("result_family").fill_null(UNKNOWN_LEVEL).alias("result_family"),
            pl.col("alignment_regime")
            .fill_null(UNKNOWN_LEVEL)
            .alias("alignment_regime"),
        )
        .select(
            [
                "event_key",
                "game_id",
                "y",
                "season_league",
                "scorer",
                *CONTEXT_FIXED_EFFECT_COLUMNS,
            ]
        )
        .collect()
    )


def _index_for(values: list[str], vocab: dict[str, int], default: int) -> IntArray:
    return np.fromiter(
        (vocab.get(v, default) for v in values),
        dtype=np.int64,
        count=len(values),
    )


def _fixed_effect_design(
    train: pl.DataFrame, column: str
) -> tuple[FixedEffectDesign, dict[str, int]]:
    levels = sorted(train.get_column(column).unique().to_list())
    vocab = {lvl: i for i, lvl in enumerate(levels)}
    codes = _index_for(train.get_column(column).to_list(), vocab, default=0)
    design = FixedEffectDesign(codes=codes, levels=tuple(levels))
    return design, vocab


def prepare_pitch_coverage_inputs(
    parquet_path: Path,
    *,
    dimension: str | None = None,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
) -> PitchCoverageInputs:
    """Shape the event-grain ``has_count`` Bernoulli design.

    The ``dimension`` kwarg is accepted for interface parity and ignored.
    ``smoke_limit`` deterministically subsamples whole events to the budget
    after the game holdout split (full fits pass a budget above the corpus
    and never subsample).
    """
    _ = dimension

    frame = _coverage_frame(parquet_path)
    if frame.height == 0:
        raise ValueError(f"no event_level rows with a game_id in {parquet_path}")

    holdout_mask = [
        game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
        for g in frame.get_column("game_id").to_list()
    ]
    frame = frame.with_columns(pl.Series("_held_out", holdout_mask))
    held = frame.filter(pl.col("_held_out"))
    train = frame.filter(~pl.col("_held_out"))
    if train.height == 0:
        raise ValueError("every event landed in the held-out fold; nothing to fit")

    if smoke_limit is not None and train.height > smoke_limit:
        train = train.sample(n=smoke_limit, seed=seed, shuffle=True)
        _log.info(
            "prepare_pitch_coverage_inputs smoke-subsampled to %d events (seed=%d)",
            smoke_limit,
            seed,
        )

    cell_labels = sorted(train.get_column("season_league").unique().to_list())
    cell_vocab = {c: i for i, c in enumerate(cell_labels)}
    scorer_labels = sorted(train.get_column("scorer").unique().to_list())
    scorer_vocab = {s: i for i, s in enumerate(scorer_labels)}

    cell_idx = _index_for(train.get_column("season_league").to_list(), cell_vocab, 0)
    scorer_idx = _index_for(train.get_column("scorer").to_list(), scorer_vocab, 0)

    fixed_effects: dict[str, FixedEffectDesign] = {}
    fe_vocabs: dict[str, dict[str, int]] = {}
    for column in CONTEXT_FIXED_EFFECT_COLUMNS:
        design, vocab = _fixed_effect_design(train, column)
        fixed_effects[column] = design
        fe_vocabs[column] = vocab

    y = train.get_column("y").to_numpy().astype(np.int64)

    coords: dict[str, list[str]] = {
        "source": [UNKNOWN_LEVEL],
        "season_league": list(cell_labels),
        "scorer": list(scorer_labels),
    }
    for column, design in fixed_effects.items():
        coords[f"{column}_levels"] = list(design.levels)

    held_out = _build_held_out(
        held,
        cell_vocab=cell_vocab,
        scorer_vocab=scorer_vocab,
        fe_vocabs=fe_vocabs,
    )

    _log.info(
        "prepare_pitch_coverage_inputs train_events=%d held_events=%d cells=%d "
        "scorers=%d coverage_rate=%.4f",
        train.height,
        held_out.n_events,
        len(cell_labels),
        len(scorer_labels),
        float(y.mean()) if y.size else 0.0,
    )

    return PitchCoverageInputs(
        y=y,
        cell_idx=cell_idx,
        scorer_idx=scorer_idx,
        fixed_effects=fixed_effects,
        cell_labels=list(cell_labels),
        scorer_labels=list(scorer_labels),
        coords=coords,
        outcome="has_count",
        dimension=DIMENSION,
        held_out=held_out,
    )


def build_pitch_coverage_scoring_frame(
    parquet_path: Path,
    *,
    inputs: PitchCoverageInputs,
) -> PitchCoverageScoringFrame:
    """Encode the full ``event_level`` population against the training vocab.

    Every event of the corpus (observed and unobserved) is scored for
    ``P(count observed)``. Cell / scorer / FE codes encode against the
    training labels carried on ``inputs``; unseen levels map to ``-1``,
    which the posterior reconstruction masks to the prior mean. Rows come
    back sorted by ``event_key``.
    """
    frame = (
        pl.scan_parquet(parquet_path)
        .filter(pl.col("game_id").is_not_null())
        .with_columns(
            (
                pl.col("season").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("league").cast(pl.Utf8).fill_null(UNKNOWN_LEVEL)
            ).alias("season_league"),
            pl.col("scorer").fill_null(UNKNOWN_LEVEL).alias("scorer"),
            pl.col("result_family").fill_null(UNKNOWN_LEVEL).alias("result_family"),
            pl.col("alignment_regime")
            .fill_null(UNKNOWN_LEVEL)
            .alias("alignment_regime"),
        )
        .select(
            [
                "event_key",
                "season_league",
                "scorer",
                *CONTEXT_FIXED_EFFECT_COLUMNS,
            ]
        )
        .unique(subset=["event_key"])
        .sort("event_key")
        .collect()
    )
    if frame.height == 0:
        raise ValueError(f"no event_level rows with a game_id in {parquet_path}")

    cell_vocab = {c: i for i, c in enumerate(inputs.cell_labels)}
    scorer_vocab = {s: i for i, s in enumerate(inputs.scorer_labels)}

    event_keys = (
        frame.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )
    cell_idx = _index_for(
        frame.get_column("season_league").to_list(), cell_vocab, UNSEEN_LEVEL_CODE
    )
    scorer_idx = _index_for(
        frame.get_column("scorer").to_list(), scorer_vocab, UNSEEN_LEVEL_CODE
    )
    fixed_effect_codes = {
        column: _index_for(
            frame.get_column(column).to_list(),
            {lvl: i for i, lvl in enumerate(design.levels)},
            UNSEEN_LEVEL_CODE,
        )
        for column, design in inputs.fixed_effects.items()
    }

    _log.info(
        "build_pitch_coverage_scoring_frame events=%d",
        frame.height,
    )
    return PitchCoverageScoringFrame(
        event_keys=event_keys,
        cell_idx=cell_idx,
        scorer_idx=scorer_idx,
        fixed_effect_codes=fixed_effect_codes,
    )


def _build_held_out(
    held: pl.DataFrame,
    *,
    cell_vocab: dict[str, int],
    scorer_vocab: dict[str, int],
    fe_vocabs: dict[str, dict[str, int]],
) -> PitchCoverageHeldOutSet:
    if held.height == 0:
        empty = np.zeros(0, dtype=np.int64)
        return PitchCoverageHeldOutSet(
            y=empty,
            cell_idx=empty,
            scorer_idx=empty,
            fixed_effect_codes={c: empty for c in CONTEXT_FIXED_EFFECT_COLUMNS},
        )
    y = held.get_column("y").to_numpy().astype(np.int64)
    cell_idx = _index_for(
        held.get_column("season_league").to_list(), cell_vocab, UNSEEN_LEVEL_CODE
    )
    scorer_idx = _index_for(
        held.get_column("scorer").to_list(), scorer_vocab, UNSEEN_LEVEL_CODE
    )
    fixed_effect_codes = {
        column: _index_for(
            held.get_column(column).to_list(), fe_vocabs[column], UNSEEN_LEVEL_CODE
        )
        for column in CONTEXT_FIXED_EFFECT_COLUMNS
    }
    return PitchCoverageHeldOutSet(
        y=y,
        cell_idx=cell_idx,
        scorer_idx=scorer_idx,
        fixed_effect_codes=fixed_effect_codes,
    )
