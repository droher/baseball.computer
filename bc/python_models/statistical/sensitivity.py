"""MNAR pattern-mixture sensitivity ribbon for imputation class shares.

The per-class selection offset of `mnar-selection-offset-design.md` corrects an
observed-only imputation by `softmax(eta_c + delta_c)`. `delta_c` is not
identified from the data, so the honest deliverable is a sensitivity band: how
the imputed population class mix moves as each class's selection log-odds shifts
over a grid, holding the others fixed. `delta = 0` reproduces the published
(MAR) shares exactly.

The correction is a post-hoc reweight of an existing per-event share export
(`shares_c * exp(delta_c)` renormalized), so the ribbon needs no refit.

The grid is in selection log-odds (nats), the interpretable unit of `delta_c`:
a class under-recorded with a selection log-odds of `+0.5` means its recording
odds are `exp(0.5) ~ 1.6x` the average. The masked backtest's recovered oracle
offsets land near `+-0.4..1.0`, so a grid out to `+-1.0` spans realistic MNAR.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

import numpy as np
import polars as pl

DEFAULT_GRID: tuple[float, ...] = (-1.0, -0.5, -0.25, 0.0, 0.25, 0.5, 1.0)


def marginal_shares(
    export_df: pl.DataFrame,
    *,
    offset: Mapping[str, float],
    class_column: str = "class_label",
    share_column: str = "expected_share",
    event_column: str = "event_key",
) -> dict[str, float]:
    """Population mean of the per-event reweighted shares under `offset`.

    `offset` maps class label -> additive log-odds shift; missing labels are 0.
    An all-zero offset returns the unweighted marginal shares.
    """
    weight_by_label = {
        str(row[class_column]): math.exp(offset.get(str(row[class_column]), 0.0))
        for row in export_df.select(class_column).unique().iter_rows(named=True)
    }
    reweighted = (
        export_df.with_columns(
            pl.col(class_column)
            .cast(pl.Utf8)
            .replace_strict(weight_by_label, default=1.0, return_dtype=pl.Float64)
            .alias("_w")
        )
        .with_columns((pl.col(share_column) * pl.col("_w")).alias("_num"))
    )
    denom = reweighted.group_by(event_column).agg(pl.col("_num").sum().alias("_denom"))
    per_event = (
        reweighted.join(denom, on=event_column)
        .with_columns((pl.col("_num") / pl.col("_denom")).alias("_share"))
        .group_by(class_column)
        .agg(pl.col("_share").mean().alias("_marginal"))
    )
    n_events = export_df.select(pl.col(event_column).n_unique()).item()
    out: dict[str, float] = {}
    for row in per_event.iter_rows(named=True):
        out[str(row[class_column])] = float(row["_marginal"])
    total = sum(out.values())
    if not math.isclose(total, 1.0, abs_tol=1e-6):
        raise ValueError(f"marginal shares sum to {total:.6f}, not 1 (n={n_events})")
    return out


def sensitivity_ribbon(
    export_df: pl.DataFrame,
    *,
    dimension: str,
    grid: Sequence[float] = DEFAULT_GRID,
    class_column: str = "class_label",
    share_column: str = "expected_share",
    event_column: str = "event_key",
) -> pl.DataFrame:
    """Per-class imputed marginal share as each class's selection offset sweeps `grid`.

    For grid value `g` (a selection log-odds, nats) and class `c`, the offset is
    `g` on `c` alone (others 0). Each row is one `(class, delta_logodds)` point;
    `delta_logodds = 0` reproduces the baseline marginal.

    Pivots to one row per event once, then each single-class sweep is column
    arithmetic: a class-`c` offset only rescales `c`, so the per-event renormalizer
    is `total + share_c * (exp(g) - 1)` with no join or per-point group-by.
    """
    wide = (
        export_df.select(event_column, class_column, share_column)
        .pivot(values=share_column, index=event_column, on=class_column)
        .fill_null(0.0)
    )
    labels = sorted(c for c in wide.columns if c != event_column)
    total = pl.sum_horizontal(*labels)
    rows: list[dict[str, object]] = []
    for label in labels:
        baseline = wide.select((pl.col(label) / total).mean()).item()
        for g in grid:
            scale = math.exp(g)
            denom = total + pl.col(label) * (scale - 1.0)
            marginal = wide.select((pl.col(label) * scale / denom).mean()).item()
            rows.append(
                {
                    "geometry_dimension": dimension,
                    "class_label": label,
                    "delta_logodds": float(g),
                    "marginal_share": float(marginal),
                    "baseline_share": float(baseline),
                }
            )
    return pl.DataFrame(rows).sort(["class_label", "delta_logodds"])


def ribbon_band(ribbon: pl.DataFrame) -> pl.DataFrame:
    """Collapse a ribbon to one `[low, high]` band row per class."""
    return (
        ribbon.group_by("geometry_dimension", "class_label")
        .agg(
            pl.col("baseline_share").first().alias("baseline_share"),
            pl.col("marginal_share").min().alias("share_low"),
            pl.col("marginal_share").max().alias("share_high"),
        )
        .sort("class_label")
    )


def offset_recovers_target(
    export_df: pl.DataFrame,
    *,
    offset: Mapping[str, float],
    target: Mapping[str, float],
    class_column: str = "class_label",
    share_column: str = "expected_share",
    event_column: str = "event_key",
) -> float:
    """Total-variation distance between the offset-corrected marginal and a target.

    Used to check a chosen / oracle offset against a known truth (the backtest
    case) or an external anchor.
    """
    corrected = marginal_shares(
        export_df,
        offset=offset,
        class_column=class_column,
        share_column=share_column,
        event_column=event_column,
    )
    labels = set(corrected) | set(target)
    return 0.5 * float(
        np.sum([abs(corrected.get(c, 0.0) - target.get(c, 0.0)) for c in labels])
    )
