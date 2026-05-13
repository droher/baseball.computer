"""Grouped split registry + leakage checks."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterable

import polars as pl

from python_models.statistical.schemas import SplitAssignment

_log = logging.getLogger(__name__)

DEFAULT_FOLD_COUNT: int = 100


def game_hash_fold(game_id: str, *, fold_count: int = DEFAULT_FOLD_COUNT) -> int:
    """Stable HASH(game_id) % fold_count.

    Uses BLAKE2s so the partition is deterministic across processes and
    platforms (Python's built-in ``hash`` is salted per interpreter).
    """
    digest = hashlib.blake2s(game_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % fold_count


def assign_game_hash_split(
    game_ids: Iterable[str],
    *,
    fold_count: int = DEFAULT_FOLD_COUNT,
    split_registry_id: str,
) -> list[SplitAssignment]:
    return [
        SplitAssignment(
            split_registry_id=split_registry_id,
            unit_type="game_id",
            unit_id=gid,
            fold_id=game_hash_fold(gid, fold_count=fold_count),
            split_family="game_hash",
            holdout_regime="primary",
        )
        for gid in game_ids
    ]


def detect_leakage(
    assignments: pl.DataFrame,
    *,
    unit_col: str,
    fold_col: str = "fold_id",
) -> list[str]:
    """Return ``unit_col`` values that appear under more than one fold."""
    grouped = (
        assignments.group_by(unit_col)
        .agg(pl.col(fold_col).n_unique().alias("n_folds"))
        .filter(pl.col("n_folds") > 1)
    )
    return grouped[unit_col].to_list()
