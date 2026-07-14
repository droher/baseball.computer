"""Anchored MNAR selection-offset estimator for the geometry imputations.

The observed-only imputation softmax can be corrected by a per-class selection
log-odds offset ``delta_c`` (see ``sensitivity.py`` and the offset design note),
but ``delta_c`` is not identified from the observed slice alone. This module
supplies an *anchored* estimate by contrasting two slices of a geometry
dimension against each other, per season era:

- the **observed** slice (``observed_status='observed'``, class ``raw_value``) —
  what scorers directly recorded;
- the **derived** slice (``observed_status='derived'``, class ``deduced_value``)
  — the masked-slice classes recoverable by deduction (e.g. ground balls read
  off a fielding string).

The offset is ``delta_c = log(p_masked_c / p_obs_c)`` where ``p_masked`` is the
derived-slice class share and ``p_obs`` the observed-slice share, centered to
zero mean over the classes covered in both slices per era (offsets are
identified only up to an additive constant). Classes present in one slice but
not the other receive an explicit ``NaN`` offset and a ``covered=False`` flag —
never ``±inf``.

PARTIAL TRUTH. The derived slice recovers only deduced-recoverable classes, so
``p_masked`` is a partial-truth estimate of the masked distribution, not full
truth. ``PARTIAL_TRUTH_ASSUMPTION`` states this and is published with the
artifact.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

import polars as pl

_log = logging.getLogger(__name__)

MODULE_VERSION: str = "1.0.0"

OBSERVED_STATUS: str = "observed"
DERIVED_STATUS: str = "derived"

PARTIAL_TRUTH_ASSUMPTION: str = (
    "The derived slice recovers only deduced-recoverable classes (for trajectory, "
    "ground balls deduced from fielding strings), so p_masked is the derived slice "
    "alone and is a partial-truth estimate of the masked-slice distribution, not "
    "full truth. Classes never produced by deduction sit at p_masked=0, receive an "
    "explicit NaN offset, and are flagged covered=False rather than taking an "
    "infinite log-ratio. Rows with observed_status outside {'observed','derived'} "
    "(e.g. 'unknown_code', 'missing') remain uncharacterized by this anchor. "
    "Offsets are identified only up to an additive constant and are centered to "
    "zero mean over the covered classes within each era."
)

PAPER_BUCKET_PRE_1950: str = "pre-1950"
PAPER_BUCKET_1950_1987: str = "1950-1987"
PAPER_BUCKET_1988_PLUS: str = "1988+"

OFFSET_COLUMNS: tuple[str, ...] = (
    "era_bucket",
    "class_label",
    "n_obs",
    "n_masked",
    "p_obs",
    "p_masked",
    "delta_raw",
    "delta",
    "covered",
)

_OFFSET_SCHEMA: dict[str, pl.DataType] = {
    "era_bucket": pl.Utf8(),
    "class_label": pl.Utf8(),
    "n_obs": pl.Int64(),
    "n_masked": pl.Int64(),
    "p_obs": pl.Float64(),
    "p_masked": pl.Float64(),
    "delta_raw": pl.Float64(),
    "delta": pl.Float64(),
    "covered": pl.Boolean(),
}


def paper_era_bucket(season: int) -> str:
    """Paper era bucket: ``pre-1950`` / ``1950-1987`` / ``1988+``."""
    if season < 1950:
        return PAPER_BUCKET_PRE_1950
    if season <= 1987:
        return PAPER_BUCKET_1950_1987
    return PAPER_BUCKET_1988_PLUS


def decade_bucket(season: int) -> str:
    """Decade bucket, e.g. ``1910s`` for any season in ``1910..1919``."""
    return f"{(season // 10) * 10}s"


ERA_BUCKETINGS: dict[str, Callable[[int], str]] = {
    "paper": paper_era_bucket,
    "decade": decade_bucket,
}


@dataclass(frozen=True)
class AnchorSummary:
    """Provenance and slice sizes for one anchor run under one bucketing."""

    module_version: str
    dimension: str
    era_bucketing: str
    partial_truth_assumption: str
    buckets: list[str]
    classes: list[str]
    slice_row_counts: dict[str, dict[str, int]]
    n_observed: int
    n_derived: int


@dataclass(frozen=True)
class AnchorResult:
    """Per-(era, class) offsets plus their provenance summary."""

    offsets: pl.DataFrame
    summary: AnchorSummary


def _tag_slices(
    frame: pl.DataFrame,
    *,
    era_bucketing: Callable[[int], str],
    season_column: str,
    status_column: str,
    raw_value_column: str,
    deduced_value_column: str,
) -> pl.DataFrame:
    seasons = (
        frame.select(pl.col(season_column).cast(pl.Int64))
        .to_series()
        .unique()
        .to_list()
    )
    bucket_map = {int(s): era_bucketing(int(s)) for s in seasons}
    return (
        frame.select(
            season_column, status_column, raw_value_column, deduced_value_column
        )
        .filter(pl.col(status_column).is_in([OBSERVED_STATUS, DERIVED_STATUS]))
        .with_columns(
            pl.col(season_column)
            .cast(pl.Int64)
            .replace_strict(bucket_map, return_dtype=pl.Utf8)
            .alias("era_bucket"),
            pl.when(pl.col(status_column) == OBSERVED_STATUS)
            .then(pl.col(raw_value_column))
            .otherwise(pl.col(deduced_value_column))
            .cast(pl.Utf8)
            .alias("class_label"),
        )
    )


def anchor_offsets(
    frame: pl.DataFrame,
    *,
    era_bucketing: Callable[[int], str] = paper_era_bucket,
    season_column: str = "season",
    status_column: str = "observed_status",
    raw_value_column: str = "raw_value",
    deduced_value_column: str = "deduced_value",
) -> pl.DataFrame:
    """Per-(era, class) anchored selection offsets from a row-grain slice frame.

    ``frame`` carries one row per event with ``season``, ``observed_status`` and
    the two class columns. Returns one row per ``(era_bucket, class_label)`` with
    the observed / masked shares, the raw log-ratio, the era-centered offset, and
    a coverage flag. Uncovered classes carry ``NaN`` for ``delta_raw`` / ``delta``.
    """
    if frame.height == 0:
        return pl.DataFrame(schema=_OFFSET_SCHEMA)

    tagged = _tag_slices(
        frame,
        era_bucketing=era_bucketing,
        season_column=season_column,
        status_column=status_column,
        raw_value_column=raw_value_column,
        deduced_value_column=deduced_value_column,
    )

    observed = (
        tagged.filter(pl.col(status_column) == OBSERVED_STATUS)
        .group_by("era_bucket", "class_label")
        .agg(pl.len().alias("n_obs"))
        .with_columns(
            (pl.col("n_obs") / pl.col("n_obs").sum().over("era_bucket")).alias("p_obs")
        )
    )
    masked = (
        tagged.filter(pl.col(status_column) == DERIVED_STATUS)
        .group_by("era_bucket", "class_label")
        .agg(pl.len().alias("n_masked"))
        .with_columns(
            (pl.col("n_masked") / pl.col("n_masked").sum().over("era_bucket")).alias(
                "p_masked"
            )
        )
    )

    joined = (
        observed.join(
            masked, on=["era_bucket", "class_label"], how="full", coalesce=True
        )
        .with_columns(
            pl.col("n_obs").fill_null(0),
            pl.col("n_masked").fill_null(0),
            pl.col("p_obs").fill_null(0.0),
            pl.col("p_masked").fill_null(0.0),
        )
        .with_columns(
            ((pl.col("p_obs") > 0.0) & (pl.col("p_masked") > 0.0)).alias("covered")
        )
        .with_columns(
            pl.when(pl.col("covered"))
            .then((pl.col("p_masked") / pl.col("p_obs")).log())
            .otherwise(float("nan"))
            .alias("delta_raw")
        )
    )

    center = (
        pl.when(pl.col("covered"))
        .then(pl.col("delta_raw"))
        .otherwise(None)
        .mean()
        .over("era_bucket")
    )
    return (
        joined.with_columns(
            pl.when(pl.col("covered"))
            .then(pl.col("delta_raw") - center)
            .otherwise(float("nan"))
            .alias("delta")
        )
        .select(*OFFSET_COLUMNS)
        .cast({"n_obs": pl.Int64, "n_masked": pl.Int64})
        .sort("era_bucket", "class_label")
    )


def _summarize(
    frame: pl.DataFrame,
    offsets: pl.DataFrame,
    *,
    dimension: str,
    era_bucketing_name: str,
    era_bucketing: Callable[[int], str],
    season_column: str,
    status_column: str,
    raw_value_column: str,
    deduced_value_column: str,
) -> AnchorSummary:
    tagged = _tag_slices(
        frame,
        era_bucketing=era_bucketing,
        season_column=season_column,
        status_column=status_column,
        raw_value_column=raw_value_column,
        deduced_value_column=deduced_value_column,
    )
    counts = tagged.group_by("era_bucket", status_column).agg(pl.len().alias("n"))
    slice_row_counts: dict[str, dict[str, int]] = {}
    for row in counts.iter_rows(named=True):
        bucket = str(row["era_bucket"])
        slice_row_counts.setdefault(bucket, {})[str(row[status_column])] = int(row["n"])

    n_observed = int(tagged.filter(pl.col(status_column) == OBSERVED_STATUS).height)
    n_derived = int(tagged.filter(pl.col(status_column) == DERIVED_STATUS).height)
    buckets = sorted(slice_row_counts)
    classes = (
        sorted(offsets.select("class_label").to_series().unique().to_list())
        if offsets.height
        else []
    )
    return AnchorSummary(
        module_version=MODULE_VERSION,
        dimension=dimension,
        era_bucketing=era_bucketing_name,
        partial_truth_assumption=PARTIAL_TRUTH_ASSUMPTION,
        buckets=buckets,
        classes=classes,
        slice_row_counts=slice_row_counts,
        n_observed=n_observed,
        n_derived=n_derived,
    )


def compute_anchor(
    frame: pl.DataFrame,
    *,
    dimension: str,
    era_bucketing_name: str,
    era_bucketing: Callable[[int], str] | None = None,
    season_column: str = "season",
    status_column: str = "observed_status",
    raw_value_column: str = "raw_value",
    deduced_value_column: str = "deduced_value",
) -> AnchorResult:
    """Offsets + summary for one dimension under one named bucketing."""
    fn = era_bucketing or ERA_BUCKETINGS[era_bucketing_name]
    offsets = anchor_offsets(
        frame,
        era_bucketing=fn,
        season_column=season_column,
        status_column=status_column,
        raw_value_column=raw_value_column,
        deduced_value_column=deduced_value_column,
    )
    summary = _summarize(
        frame,
        offsets,
        dimension=dimension,
        era_bucketing_name=era_bucketing_name,
        era_bucketing=fn,
        season_column=season_column,
        status_column=status_column,
        raw_value_column=raw_value_column,
        deduced_value_column=deduced_value_column,
    )
    _log.debug(
        "anchor %s/%s: %d observed, %d derived, %d offset rows",
        dimension,
        era_bucketing_name,
        summary.n_observed,
        summary.n_derived,
        offsets.height,
    )
    return AnchorResult(offsets=offsets, summary=summary)
