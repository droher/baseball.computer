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

Where an external lower bound on a class's unrecorded-slice share exists (the
derived-slice bound of `mnar_anchor.py`), `offset_reaching_share` inverts the
single-class sweep to find the smallest offset at which the corrected marginal
reaches it, so a reader can see whether the grid contains the bound.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

import numpy as np
import numpy.typing as npt
import polars as pl

FloatArray = npt.NDArray[np.float64]

DEFAULT_GRID: tuple[float, ...] = (-1.0, -0.5, -0.25, 0.0, 0.25, 0.5, 1.0)

OFFSET_SEARCH_LIMIT_NATS: float = 64.0
OFFSET_SEARCH_TOLERANCE_NATS: float = 1e-10

BOUND_OFFSET_COLUMNS: tuple[str, ...] = (
    "era_bucket",
    "class_label",
    "baseline_share",
    "grid_low_delta",
    "grid_low_share",
    "grid_high_delta",
    "grid_high_share",
    "target_share",
    "delta_at_target",
    "target_in_grid",
)

_BOUND_OFFSET_SCHEMA: dict[str, pl.DataType] = {
    "era_bucket": pl.Utf8(),
    "class_label": pl.Utf8(),
    "baseline_share": pl.Float64(),
    "grid_low_delta": pl.Float64(),
    "grid_low_share": pl.Float64(),
    "grid_high_delta": pl.Float64(),
    "grid_high_share": pl.Float64(),
    "target_share": pl.Float64(),
    "delta_at_target": pl.Float64(),
    "target_in_grid": pl.Boolean(),
}


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
    reweighted = export_df.with_columns(
        pl.col(class_column)
        .cast(pl.Utf8)
        .replace_strict(weight_by_label, default=1.0, return_dtype=pl.Float64)
        .alias("_w")
    ).with_columns((pl.col(share_column) * pl.col("_w")).alias("_num"))
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


def _pivot_wide(
    export_df: pl.DataFrame,
    *,
    class_column: str,
    share_column: str,
    event_column: str,
) -> tuple[list[str], pl.DataFrame]:
    wide = (
        export_df.select(event_column, class_column, share_column)
        .pivot(values=share_column, index=event_column, on=class_column)
        .fill_null(0.0)
    )
    labels = sorted(c for c in wide.columns if c != event_column)
    return labels, wide


def _single_class_marginal(share: FloatArray, total: FloatArray, delta: float) -> float:
    """Population mean of one class's share after shifting only that class by `delta`.

    A single-class offset only rescales that class, so the per-event
    renormalizer is `total + share * (exp(delta) - 1)`.
    """
    scale = math.exp(delta)
    return float(np.mean(share * scale / (total + share * (scale - 1.0))))


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
    """
    labels, wide = _pivot_wide(
        export_df,
        class_column=class_column,
        share_column=share_column,
        event_column=event_column,
    )
    mat = np.asarray(wide.select(labels).to_numpy(), dtype=np.float64)
    total = mat.sum(axis=1)
    rows: list[dict[str, object]] = []
    for k, label in enumerate(labels):
        share = mat[:, k]
        baseline = _single_class_marginal(share, total, 0.0)
        for g in grid:
            rows.append(
                {
                    "geometry_dimension": dimension,
                    "class_label": label,
                    "delta_logodds": float(g),
                    "marginal_share": _single_class_marginal(share, total, float(g)),
                    "baseline_share": baseline,
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


def _bracket(
    share: FloatArray, total: FloatArray, target: float
) -> tuple[float, float] | None:
    lo, hi = -1.0, 1.0
    while _single_class_marginal(share, total, lo) > target:
        lo *= 2.0
        if lo < -OFFSET_SEARCH_LIMIT_NATS:
            return None
    while _single_class_marginal(share, total, hi) < target:
        hi *= 2.0
        if hi > OFFSET_SEARCH_LIMIT_NATS:
            return None
    return lo, hi


def _offset_reaching_share(
    share: FloatArray, total: FloatArray, target: float
) -> float | None:
    baseline = _single_class_marginal(share, total, 0.0)
    if math.isclose(baseline, target, abs_tol=OFFSET_SEARCH_TOLERANCE_NATS):
        return 0.0
    supremum = float(np.mean(share > 0.0))
    if target <= 0.0 or target >= supremum:
        return None
    bracket = _bracket(share, total, target)
    if bracket is None:
        return None
    lo, hi = bracket
    while hi - lo > OFFSET_SEARCH_TOLERANCE_NATS:
        mid = 0.5 * (lo + hi)
        if _single_class_marginal(share, total, mid) < target:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def _class_share_and_total(
    export_df: pl.DataFrame,
    *,
    class_label: str,
    class_column: str,
    share_column: str,
    event_column: str,
) -> tuple[FloatArray, FloatArray]:
    labels, wide = _pivot_wide(
        export_df,
        class_column=class_column,
        share_column=share_column,
        event_column=event_column,
    )
    if class_label not in labels:
        raise ValueError(f"{class_label!r} not in export classes {labels}")
    mat = np.asarray(wide.select(labels).to_numpy(), dtype=np.float64)
    return mat[:, labels.index(class_label)], mat.sum(axis=1)


def offset_reaching_share(
    export_df: pl.DataFrame,
    *,
    class_label: str,
    target_share: float,
    class_column: str = "class_label",
    share_column: str = "expected_share",
    event_column: str = "event_key",
) -> float | None:
    """Smallest single-class offset at which `class_label`'s marginal reaches `target_share`.

    The marginal is continuous and strictly increasing in the class's own
    offset, so the answer is unique; it is found by bisection to
    `OFFSET_SEARCH_TOLERANCE_NATS`. Returns `None` when the target is at or
    below zero, at or above the sweep's supremum (the share of events with any
    mass on the class), or otherwise not reached within
    `+-OFFSET_SEARCH_LIMIT_NATS`.
    """
    share, total = _class_share_and_total(
        export_df,
        class_label=class_label,
        class_column=class_column,
        share_column=share_column,
        event_column=event_column,
    )
    return _offset_reaching_share(share, total, float(target_share))


def era_sensitivity_ribbon(
    export_df: pl.DataFrame,
    *,
    dimension: str,
    grid: Sequence[float] = DEFAULT_GRID,
    era_column: str = "era_bucket",
    class_column: str = "class_label",
    share_column: str = "expected_share",
    event_column: str = "event_key",
) -> pl.DataFrame:
    """`sensitivity_ribbon` computed within each era bucket, tagged with the era."""
    frames: list[pl.DataFrame] = []
    eras = sorted(
        str(era) for era in export_df.select(era_column).unique().to_series().to_list()
    )
    for era in eras:
        ribbon = sensitivity_ribbon(
            export_df.filter(pl.col(era_column) == era),
            dimension=dimension,
            grid=grid,
            class_column=class_column,
            share_column=share_column,
            event_column=event_column,
        )
        frames.append(ribbon.with_columns(pl.lit(era).alias("era_bucket")))
    if not frames:
        return pl.DataFrame(
            schema={
                "geometry_dimension": pl.Utf8,
                "class_label": pl.Utf8,
                "delta_logodds": pl.Float64,
                "marginal_share": pl.Float64,
                "baseline_share": pl.Float64,
                "era_bucket": pl.Utf8,
            }
        )
    return pl.concat(frames).select(
        "era_bucket",
        "geometry_dimension",
        "class_label",
        "delta_logodds",
        "marginal_share",
        "baseline_share",
    )


def bound_offset_table(
    export_df: pl.DataFrame,
    *,
    class_label: str,
    target_by_era: Mapping[str, float],
    grid: Sequence[float] = DEFAULT_GRID,
    era_column: str = "era_bucket",
    class_column: str = "class_label",
    share_column: str = "expected_share",
    event_column: str = "event_key",
) -> pl.DataFrame:
    """Per era: the class's MAR share, its share at the grid ends, the target, and
    the offset at which the corrected marginal reaches the target.

    `target_in_grid` says whether that offset lies within `[min(grid), max(grid)]`,
    so a reader can see whether the published band contains the external bound.
    Eras absent from `target_by_era` are skipped.
    """
    grid_low, grid_high = float(min(grid)), float(max(grid))
    rows: list[dict[str, object]] = []
    eras = sorted(
        str(era) for era in export_df.select(era_column).unique().to_series().to_list()
    )
    for era in eras:
        if era not in target_by_era:
            continue
        share, total = _class_share_and_total(
            export_df.filter(pl.col(era_column) == era),
            class_label=class_label,
            class_column=class_column,
            share_column=share_column,
            event_column=event_column,
        )
        target = float(target_by_era[era])
        delta = _offset_reaching_share(share, total, target)
        rows.append(
            {
                "era_bucket": era,
                "class_label": class_label,
                "baseline_share": _single_class_marginal(share, total, 0.0),
                "grid_low_delta": grid_low,
                "grid_low_share": _single_class_marginal(share, total, grid_low),
                "grid_high_delta": grid_high,
                "grid_high_share": _single_class_marginal(share, total, grid_high),
                "target_share": target,
                "delta_at_target": delta,
                "target_in_grid": delta is not None and grid_low <= delta <= grid_high,
            }
        )
    return pl.DataFrame(rows, schema=_BOUND_OFFSET_SCHEMA).sort("era_bucket")
