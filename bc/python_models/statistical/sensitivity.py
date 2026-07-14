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
import numpy.typing as npt
import polars as pl

FloatArray = npt.NDArray[np.float64]

DEFAULT_GRID: tuple[float, ...] = (-1.0, -0.5, -0.25, 0.0, 0.25, 0.5, 1.0)

JOINT_T_GRID: tuple[float, ...] = (0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5)
JOINT_PERTURBATION_NATS: float = 0.25
JOINT_ANCHOR_T: float = 1.0

SWEEP_MARGINAL: str = "marginal"
SWEEP_JOINT_ANCHOR: str = "joint_anchor"
SWEEP_JOINT_ANCHOR_PERTURBED: str = "joint_anchor_perturbed"

_JOINT_RIBBON_SCHEMA: dict[str, pl.DataType] = {
    "geometry_dimension": pl.Utf8(),
    "era_bucket": pl.Utf8(),
    "sweep_kind": pl.Utf8(),
    "class_label": pl.Utf8(),
    "delta_logodds": pl.Float64(),
    "t": pl.Float64(),
    "perturbed_class": pl.Utf8(),
    "perturbation_nats": pl.Float64(),
    "marginal_share": pl.Float64(),
    "baseline_share": pl.Float64(),
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


def _pivot_wide_matrix(
    export_df: pl.DataFrame,
    *,
    class_column: str,
    share_column: str,
    event_column: str,
) -> tuple[list[str], FloatArray]:
    """One row per event, one column per class, as a dense float matrix.

    Returns the sorted class labels aligned to the matrix columns. Missing
    per-event class rows fill to a zero share.
    """
    wide = (
        export_df.select(event_column, class_column, share_column)
        .pivot(values=share_column, index=event_column, on=class_column)
        .fill_null(0.0)
    )
    labels = sorted(c for c in wide.columns if c != event_column)
    mat = np.asarray(wide.select(labels).to_numpy(), dtype=np.float64)
    return labels, mat


def _marginal_under_offset(mat: FloatArray, offset_vec: FloatArray) -> FloatArray:
    """Population mean of the per-event softmax reweight `shares · exp(offset)`.

    Closed form, no per-point group-by: reweight, renormalize per event, average
    over events. An all-zero `offset_vec` returns the unweighted marginal.
    """
    weighted = mat * np.exp(offset_vec)
    denom = weighted.sum(axis=1, keepdims=True)
    return np.asarray((weighted / denom).mean(axis=0), dtype=np.float64)


def joint_sensitivity_ribbon(
    export_df: pl.DataFrame,
    *,
    dimension: str,
    anchor_offset_by_era: Mapping[str, Mapping[str, float]],
    focal_class: str,
    t_grid: Sequence[float] = JOINT_T_GRID,
    perturbation_nats: float = JOINT_PERTURBATION_NATS,
    grid: Sequence[float] = DEFAULT_GRID,
    class_column: str = "class_label",
    share_column: str = "expected_share",
    event_column: str = "event_key",
    era_column: str = "era_bucket",
) -> pl.DataFrame:
    """Joint anchored sensitivity ribbon, grouped per era bucket.

    Extends the single-class marginal sweep with an anchored-direction sweep.
    Per era bucket (from `era_column`) three families of rows are produced:

    - `marginal`: the existing per-class single-class sweep over `grid`
      (`sensitivity_ribbon`), unchanged in value, tagged with its era.
    - `joint_anchor`: the population class mix under the offset vector
      `t · delta_anchor` for each `t` in `t_grid`, where `delta_anchor` is the
      era's anchored offset (`anchor_offset_by_era[era]`, a per-class map;
      missing classes are 0). `t = 0` reproduces the MAR marginal exactly.
    - `joint_anchor_perturbed`: at `t = 1`, each non-focal class perturbed by
      `±perturbation_nats` one at a time on top of the anchor, exposing joint
      reallocation uncertainty among the remaining classes. These rows bracket
      the unperturbed `t = 1` `joint_anchor` row.

    Each offset config is a closed-form per-event renormalization; no refit.
    """
    eras = sorted(
        str(era) for era in export_df.select(era_column).unique().to_series().to_list()
    )
    frames: list[pl.DataFrame] = []
    for era in eras:
        era_df = export_df.filter(pl.col(era_column) == era)
        labels, mat = _pivot_wide_matrix(
            era_df,
            class_column=class_column,
            share_column=share_column,
            event_column=event_column,
        )
        anchor = anchor_offset_by_era.get(era, {})
        anchor_vec = np.asarray(
            [anchor.get(label, 0.0) for label in labels], dtype=np.float64
        )
        baseline = _marginal_under_offset(mat, np.zeros(len(labels), dtype=np.float64))
        baseline_by_label = {label: float(baseline[k]) for k, label in enumerate(labels)}

        marginal_frame = (
            sensitivity_ribbon(
                era_df,
                dimension=dimension,
                grid=grid,
                class_column=class_column,
                share_column=share_column,
                event_column=event_column,
            )
            .with_columns(
                pl.lit(era).alias("era_bucket"),
                pl.lit(SWEEP_MARGINAL).alias("sweep_kind"),
                pl.lit(None, dtype=pl.Float64).alias("t"),
                pl.lit(None, dtype=pl.Utf8).alias("perturbed_class"),
                pl.lit(None, dtype=pl.Float64).alias("perturbation_nats"),
            )
            .select(list(_JOINT_RIBBON_SCHEMA.keys()))
        )
        frames.append(marginal_frame)

        rows: list[dict[str, object]] = []
        for t in t_grid:
            marginal_t = _marginal_under_offset(mat, anchor_vec * float(t))
            for k, label in enumerate(labels):
                rows.append(
                    {
                        "geometry_dimension": dimension,
                        "era_bucket": era,
                        "sweep_kind": SWEEP_JOINT_ANCHOR,
                        "class_label": label,
                        "delta_logodds": None,
                        "t": float(t),
                        "perturbed_class": None,
                        "perturbation_nats": None,
                        "marginal_share": float(marginal_t[k]),
                        "baseline_share": baseline_by_label[label],
                    }
                )
        for j, perturbed_label in enumerate(labels):
            if perturbed_label == focal_class:
                continue
            for signed in (perturbation_nats, -perturbation_nats):
                offset_vec = anchor_vec * JOINT_ANCHOR_T
                offset_vec[j] += signed
                marginal_p = _marginal_under_offset(mat, offset_vec)
                for k, label in enumerate(labels):
                    rows.append(
                        {
                            "geometry_dimension": dimension,
                            "era_bucket": era,
                            "sweep_kind": SWEEP_JOINT_ANCHOR_PERTURBED,
                            "class_label": label,
                            "delta_logodds": None,
                            "t": JOINT_ANCHOR_T,
                            "perturbed_class": perturbed_label,
                            "perturbation_nats": float(signed),
                            "marginal_share": float(marginal_p[k]),
                            "baseline_share": baseline_by_label[label],
                        }
                    )
        frames.append(pl.DataFrame(rows, schema=_JOINT_RIBBON_SCHEMA))

    if not frames:
        return pl.DataFrame(schema=_JOINT_RIBBON_SCHEMA)
    return pl.concat(frames).sort(
        "era_bucket", "sweep_kind", "class_label", "t", "delta_logodds"
    )


def joint_ribbon_band(ribbon: pl.DataFrame) -> pl.DataFrame:
    """Per-(era, class) perturbation band around the anchored `t = 1` mix.

    Collapses the `joint_anchor_perturbed` rows to `[low, high]` over the
    perturbations, paired with the unperturbed `joint_anchor` `t = 1` share.
    """
    anchored = (
        ribbon.filter(
            (pl.col("sweep_kind") == SWEEP_JOINT_ANCHOR)
            & (pl.col("t") == JOINT_ANCHOR_T)
        )
        .group_by("era_bucket", "class_label")
        .agg(pl.col("marginal_share").first().alias("anchor_share"))
    )
    perturbed = (
        ribbon.filter(pl.col("sweep_kind") == SWEEP_JOINT_ANCHOR_PERTURBED)
        .group_by("era_bucket", "class_label")
        .agg(
            pl.col("marginal_share").min().alias("share_low"),
            pl.col("marginal_share").max().alias("share_high"),
        )
    )
    return anchored.join(perturbed, on=["era_bucket", "class_label"], how="left").sort(
        "era_bucket", "class_label"
    )
