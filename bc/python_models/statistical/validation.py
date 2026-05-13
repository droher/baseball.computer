"""Conservation + calibration blocking finding container."""

from __future__ import annotations

import logging
from collections.abc import Sequence

import polars as pl

from python_models.statistical.schemas import ValidationFinding

_log = logging.getLogger(__name__)


def probability_normalization_check(
    df: pl.DataFrame,
    *,
    key_cols: Sequence[str],
    prob_col: str,
    tol: float = 1e-3,
) -> list[ValidationFinding]:
    sums = df.group_by(list(key_cols)).agg(pl.col(prob_col).sum().alias("p_sum"))
    bad = sums.filter((pl.col("p_sum") < 1.0 - tol) | (pl.col("p_sum") > 1.0 + tol))
    if bad.height == 0:
        return []
    return [
        ValidationFinding(
            severity="block",
            code="probability_not_normalized",
            message=f"{bad.height} probability groups do not sum to 1 within {tol}",
        )
    ]


def conservation_residual_check(
    df: pl.DataFrame,
    *,
    expected_col: str,
    actual_col: str,
    group_cols: Sequence[str],
    tol: float = 1e-6,
) -> list[ValidationFinding]:
    residual = df.group_by(list(group_cols)).agg(
        (pl.col(actual_col) - pl.col(expected_col)).abs().sum().alias("resid")
    )
    bad = residual.filter(pl.col("resid") > tol)
    if bad.height == 0:
        return []
    return [
        ValidationFinding(
            severity="block",
            code="conservation_residual",
            message=f"{bad.height} groups violate conservation between {expected_col} and {actual_col}",
        )
    ]
