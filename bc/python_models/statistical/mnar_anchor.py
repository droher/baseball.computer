"""Derived-slice bounds on the unrecorded class mix of a geometry dimension.

The observed-only imputation softmax scores every event whose class was not
recorded (``observed_status != 'observed'``) under a missing-at-random
assumption. That unrecorded slice splits in two:

- the **derived** slice (``observed_status='derived'``), whose class is known
  from deduction (for trajectory, ground balls read off a fielding string);
- the **unknown** remainder (every other non-observed status), whose class is
  genuinely unknown.

The derived slice is a partial truth about the unrecorded population, and this
module turns it into three per-(era, class) quantities:

1. ``share_lower_bound = n_derived_class / n_unrecorded``: a hard lower bound
   on P(class | unrecorded), because every derived event of that class is an
   unrecorded event of that class.
2. ``share_point_estimate = (n_derived_class + sum of the MAR share over the
   unknown rows) / n_unrecorded``: truth on the derived rows, MAR only on the
   rows whose class is unknown.
3. ``mar_share_on_derived`` / ``mar_log_loss_on_derived``: the MAR export's
   mean share and log loss for the derived rows whose true class is this
   class (truth 1.0), a direct calibration check of the MAR shares on a
   known-truth subslice.

None of these is a selection offset. The derived slice never contains a
non-deduced class, so contrasting it against the observed slice measures only
how much of the observed slice is that class; it says nothing about the
unknown remainder. ``IDENTIFICATION_STATEMENT`` says this in plain English and
is published with the artifact.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from python_models.statistical import config as cfg
from python_models.statistical.duckdb_io import open_bc_db
from python_models.statistical.manifests import (
    find_published_manifest,
    read_published_pointer,
)

_log = logging.getLogger(__name__)

MODULE_VERSION: str = "2.0.0"

OBSERVED_STATUS: str = "observed"
DERIVED_STATUS: str = "derived"

LOG_LOSS_CLIP: float = 1e-12

IDENTIFICATION_STATEMENT: str = (
    "The derived slice (observed_status='derived') is the part of the unrecorded "
    "slice whose class is known by deduction; for trajectory it is entirely "
    "GroundBall. It identifies a lower bound on the unrecorded-slice share of each "
    "deduced class (n_derived_class / n_unrecorded) and supplies a known-truth "
    "subslice on which the MAR shares can be checked. It does not identify the "
    "class mix of the unknown remainder, and contrasting it against the observed "
    "slice does not identify a selection offset: because the derived slice holds "
    "only the deduced class, that contrast is a function of the observed slice "
    "alone. The point estimate applies the MAR shares only to the unknown "
    "remainder and is therefore a partial-truth estimate, not a correction."
)

PAPER_BUCKET_PRE_1950: str = "pre-1950"
PAPER_BUCKET_1950_1987: str = "1950-1987"
PAPER_BUCKET_1988_PLUS: str = "1988+"

BOUND_COLUMNS: tuple[str, ...] = (
    "era_bucket",
    "class_label",
    "n_observed",
    "observed_share",
    "n_unrecorded",
    "n_derived",
    "n_unknown",
    "n_derived_class",
    "share_lower_bound",
    "mar_share_unrecorded",
    "mar_share_unknown",
    "share_point_estimate",
    "mar_share_on_derived",
    "mar_log_loss_on_derived",
    "covered",
)

_BOUND_SCHEMA: dict[str, pl.DataType] = {
    "era_bucket": pl.Utf8(),
    "class_label": pl.Utf8(),
    "n_observed": pl.Int64(),
    "observed_share": pl.Float64(),
    "n_unrecorded": pl.Int64(),
    "n_derived": pl.Int64(),
    "n_unknown": pl.Int64(),
    "n_derived_class": pl.Int64(),
    "share_lower_bound": pl.Float64(),
    "mar_share_unrecorded": pl.Float64(),
    "mar_share_unknown": pl.Float64(),
    "share_point_estimate": pl.Float64(),
    "mar_share_on_derived": pl.Float64(),
    "mar_log_loss_on_derived": pl.Float64(),
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
class BoundSummary:
    """Provenance and slice sizes for one bounds run under one bucketing."""

    module_version: str
    dimension: str
    era_bucketing: str
    identification_statement: str
    buckets: list[str]
    classes: list[str]
    slice_row_counts: dict[str, dict[str, int]]
    n_observed: int
    n_unrecorded: int
    n_derived: int
    n_unknown: int


@dataclass(frozen=True)
class BoundResult:
    """Per-(era, class) bounds plus their provenance summary."""

    bounds: pl.DataFrame
    summary: BoundSummary


@dataclass(frozen=True)
class PublishedGeometryInputs:
    """The frozen dataset and MAR export behind a published geometry fit."""

    model_name: str
    artifact_id: str
    dataset_artifact_id: str
    dataset_path: Path
    export_path: Path


def resolve_published_geometry_inputs(
    model_name: str,
    *,
    dataset_name: str = "model_input_geometry",
    require_dataset: bool = True,
    require_export: bool = True,
) -> PublishedGeometryInputs:
    """Locate the dataset parquet and class-share export of a published fit.

    The manifest's ``dataset_artifact_id`` names the frozen dataset the fit was
    trained on, so bounds computed from it reproduce regardless of what the
    prod database holds today. Both paths are always returned; only the
    required ones must exist on disk, so a caller that overrides one input
    is not blocked by the other being absent.
    """
    pointer_path = find_published_manifest(model_name)
    if pointer_path is None:
        raise FileNotFoundError(f"no published pointer for {model_name}")
    manifest_path = read_published_pointer(pointer_path).manifest_path
    manifest = json.loads(manifest_path.read_text())
    dataset_artifact_id = str(manifest["dataset_artifact_id"])
    dataset_path = (
        cfg.DATASETS_ROOT / dataset_name / dataset_artifact_id / "dataset.parquet"
    )
    export_path = manifest_path.parent / "exports" / "geometry_probabilities.parquet"
    required = (
        *((dataset_path,) if require_dataset else ()),
        *((export_path,) if require_export else ()),
    )
    for path in required:
        if not path.exists():
            raise FileNotFoundError(f"{model_name}: missing {path}")
    return PublishedGeometryInputs(
        model_name=model_name,
        artifact_id=str(manifest["artifact_id"]),
        dataset_artifact_id=dataset_artifact_id,
        dataset_path=dataset_path,
        export_path=export_path,
    )


SLICE_COLUMNS: tuple[str, ...] = (
    "event_key",
    "season",
    "observed_status",
    "raw_value",
    "deduced_value",
)

_SLICE_QUERY = """
    SELECT event_key, season, observed_status, raw_value, deduced_value
    FROM main_models.model_input_geometry
    WHERE geometry_dimension = '{dimension}'
"""


def load_dimension_slice(
    dimension: str, *, dataset_path: Path | None = None, db_path: Path | None = None
) -> pl.DataFrame:
    """One row per event of `dimension` with season, status and class columns.

    Reads the frozen dataset parquet when `dataset_path` is given (the
    reproducible choice: it is what the published fit was trained on), else the
    prod database read-only.
    """
    if (dataset_path is None) == (db_path is None):
        raise ValueError("pass exactly one of dataset_path or db_path")
    if dataset_path is not None:
        frame = (
            pl.scan_parquet(dataset_path)
            .filter(pl.col("geometry_dimension") == dimension)
            .select(*SLICE_COLUMNS)
            .collect()
        )
    else:
        assert db_path is not None
        with open_bc_db(db_path, read_only=True) as con:
            _ = con.execute("PRAGMA disable_progress_bar")
            frame = con.execute(_SLICE_QUERY.format(dimension=dimension)).pl()
    _log.info(
        "loaded %d %s rows from %s", frame.height, dimension, dataset_path or db_path
    )
    return frame.with_columns(
        pl.col("event_key").cast(pl.Int64), pl.col("season").cast(pl.Int64)
    )


def _era_column(
    frame: pl.DataFrame, *, era_bucketing: Callable[[int], str], season_column: str
) -> pl.Expr:
    seasons = frame.select(pl.col(season_column).cast(pl.Int64)).to_series().unique()
    bucket_map = {int(s): era_bucketing(int(s)) for s in seasons.to_list()}
    return (
        pl.col(season_column)
        .cast(pl.Int64)
        .replace_strict(bucket_map, return_dtype=pl.Utf8)
        .alias("era_bucket")
    )


def _remap_expr(column: str, class_remap: Mapping[str, str]) -> pl.Expr:
    labels = pl.col(column).cast(pl.Utf8)
    if not class_remap:
        return labels
    return labels.replace(dict(class_remap))


def derived_slice_bounds(
    slice_frame: pl.DataFrame,
    export_frame: pl.DataFrame,
    *,
    era_bucketing: Callable[[int], str] = paper_era_bucket,
    class_remap: Mapping[str, str] | None = None,
    event_column: str = "event_key",
    season_column: str = "season",
    status_column: str = "observed_status",
    raw_value_column: str = "raw_value",
    deduced_value_column: str = "deduced_value",
    class_column: str = "class_label",
    share_column: str = "expected_share",
) -> pl.DataFrame:
    """Per-(era, class) derived-slice bound, point estimate and MAR diagnostic.

    ``slice_frame`` holds one row per event of the dimension (observed rows
    included, for the observed-share context column). ``export_frame`` is the
    MAR export, one row per (event, class), and must cover every unrecorded
    event for every class. ``class_remap`` aligns raw / deduced labels to the
    export vocabulary; observed rows whose remapped label is outside the
    vocabulary are excluded from the observed share.

    An era with observed rows but no unrecorded rows is still emitted, one row
    per class, with every unrecorded count at zero, ``covered = False``,
    ``share_lower_bound = 0.0`` (the vacuous floor), and a NULL
    ``share_point_estimate`` and MAR diagnostics, since there is no unrecorded
    population to estimate.
    """
    if slice_frame.height == 0:
        return pl.DataFrame(schema=_BOUND_SCHEMA)
    remap = dict(class_remap or {})
    vocabulary = sorted(
        str(c) for c in export_frame.select(class_column).unique().to_series().to_list()
    )

    tagged = slice_frame.select(
        pl.col(event_column).cast(pl.Int64).alias("_event"),
        _era_column(
            slice_frame, era_bucketing=era_bucketing, season_column=season_column
        ),
        pl.col(status_column).cast(pl.Utf8).alias("_status"),
        _remap_expr(raw_value_column, remap).alias("_raw"),
        _remap_expr(deduced_value_column, remap).alias("_deduced"),
    )
    eras = tagged.select("era_bucket").unique().to_series().to_list()
    grid = pl.DataFrame(
        {
            "era_bucket": [era for era in eras for _ in vocabulary],
            "class_label": [label for _ in eras for label in vocabulary],
        },
        schema={"era_bucket": pl.Utf8, "class_label": pl.Utf8},
    )

    observed = tagged.filter(
        (pl.col("_status") == OBSERVED_STATUS) & pl.col("_raw").is_in(vocabulary)
    )
    observed_by_class = (
        observed.group_by("era_bucket", pl.col("_raw").alias("class_label"))
        .agg(pl.len().alias("_n_obs_class"))
        .with_columns(
            pl.col("_n_obs_class").sum().over("era_bucket").alias("n_observed")
        )
        .with_columns(
            (pl.col("_n_obs_class") / pl.col("n_observed")).alias("observed_share")
        )
        .drop("_n_obs_class")
    )

    unrecorded = tagged.filter(pl.col("_status") != OBSERVED_STATUS).with_columns(
        (pl.col("_status") == DERIVED_STATUS).alias("_derived")
    )
    bad_derived = unrecorded.filter(
        pl.col("_derived") & ~pl.col("_deduced").is_in(vocabulary)
    )
    if bad_derived.height:
        labels = bad_derived.select("_deduced").unique().to_series().to_list()
        raise ValueError(
            f"{bad_derived.height} derived rows carry a deduced class outside the "
            f"export vocabulary {vocabulary}: {labels}"
        )
    era_counts = unrecorded.group_by("era_bucket").agg(
        pl.len().alias("n_unrecorded"),
        pl.col("_derived").sum().alias("n_derived"),
        (~pl.col("_derived")).sum().alias("n_unknown"),
    )

    export = export_frame.select(
        pl.col(event_column).cast(pl.Int64).alias("_event"),
        pl.col(class_column).cast(pl.Utf8).alias("class_label"),
        pl.col(share_column).cast(pl.Float64).alias("_p"),
    )
    joined = unrecorded.join(export, on="_event", how="left")
    unscored = joined.filter(pl.col("_p").is_null()).select("_event").n_unique()
    if unscored:
        raise ValueError(f"{unscored} unrecorded events have no row in the MAR export")

    per_class = joined.group_by("era_bucket", "class_label").agg(
        pl.len().alias("_n_rows"),
        (pl.col("_derived") & (pl.col("_deduced") == pl.col("class_label")))
        .sum()
        .alias("n_derived_class"),
        pl.col("_p").mean().alias("mar_share_unrecorded"),
        pl.col("_p").filter(~pl.col("_derived")).mean().alias("mar_share_unknown"),
        pl.col("_p").filter(~pl.col("_derived")).sum().alias("_unknown_mass"),
        pl.col("_p")
        .filter(pl.col("_derived") & (pl.col("_deduced") == pl.col("class_label")))
        .mean()
        .alias("mar_share_on_derived"),
        (-pl.col("_p").clip(LOG_LOSS_CLIP, 1.0).log())
        .filter(pl.col("_derived") & (pl.col("_deduced") == pl.col("class_label")))
        .mean()
        .alias("mar_log_loss_on_derived"),
    )

    out = (
        grid.join(era_counts, on="era_bucket", how="left")
        .join(per_class, on=["era_bucket", "class_label"], how="left")
        .join(observed_by_class, on=["era_bucket", "class_label"], how="left")
        .with_columns(
            pl.col("n_observed")
            .fill_null(pl.col("n_observed").max().over("era_bucket"))
            .fill_null(0),
            pl.col("observed_share").fill_null(0.0),
            pl.col("_n_rows").fill_null(0),
            pl.col("n_unrecorded").fill_null(0),
            pl.col("n_derived").fill_null(0),
            pl.col("n_unknown").fill_null(0),
            pl.col("n_derived_class").fill_null(0),
        )
    )
    incomplete = out.filter(pl.col("_n_rows") != pl.col("n_unrecorded"))
    if incomplete.height:
        rows = incomplete.select("era_bucket", "class_label", "_n_rows", "n_unrecorded")
        raise ValueError(
            f"MAR export does not score every class for every event: {rows}"
        )

    has_unrecorded = pl.col("n_unrecorded") > 0
    return (
        out.with_columns(
            pl.when(has_unrecorded)
            .then(pl.col("n_derived_class") / pl.col("n_unrecorded"))
            .otherwise(pl.lit(0.0))
            .alias("share_lower_bound"),
            pl.when(has_unrecorded)
            .then(
                (pl.col("n_derived_class") + pl.col("_unknown_mass").fill_null(0.0))
                / pl.col("n_unrecorded")
            )
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("share_point_estimate"),
            (pl.col("n_derived_class") > 0).alias("covered"),
        )
        .select(*BOUND_COLUMNS)
        .cast(
            {
                "n_observed": pl.Int64,
                "n_unrecorded": pl.Int64,
                "n_derived": pl.Int64,
                "n_unknown": pl.Int64,
                "n_derived_class": pl.Int64,
            }
        )
        .sort("era_bucket", "class_label")
    )


def _summarize(
    slice_frame: pl.DataFrame,
    bounds: pl.DataFrame,
    *,
    dimension: str,
    era_bucketing_name: str,
    era_bucketing: Callable[[int], str],
    season_column: str,
    status_column: str,
) -> BoundSummary:
    tagged = slice_frame.select(
        _era_column(
            slice_frame, era_bucketing=era_bucketing, season_column=season_column
        ),
        pl.col(status_column).cast(pl.Utf8).alias("_status"),
    )
    counts = tagged.group_by("era_bucket", "_status").agg(pl.len().alias("n"))
    slice_row_counts: dict[str, dict[str, int]] = {}
    for row in counts.iter_rows(named=True):
        bucket = str(row["era_bucket"])
        slice_row_counts.setdefault(bucket, {})[str(row["_status"])] = int(row["n"])
    n_observed = int(tagged.filter(pl.col("_status") == OBSERVED_STATUS).height)
    n_derived = int(tagged.filter(pl.col("_status") == DERIVED_STATUS).height)
    n_unrecorded = int(tagged.filter(pl.col("_status") != OBSERVED_STATUS).height)
    classes = (
        sorted(bounds.select("class_label").to_series().unique().to_list())
        if bounds.height
        else []
    )
    return BoundSummary(
        module_version=MODULE_VERSION,
        dimension=dimension,
        era_bucketing=era_bucketing_name,
        identification_statement=IDENTIFICATION_STATEMENT,
        buckets=sorted(slice_row_counts),
        classes=classes,
        slice_row_counts=slice_row_counts,
        n_observed=n_observed,
        n_unrecorded=n_unrecorded,
        n_derived=n_derived,
        n_unknown=n_unrecorded - n_derived,
    )


def compute_bounds(
    slice_frame: pl.DataFrame,
    export_frame: pl.DataFrame,
    *,
    dimension: str,
    era_bucketing_name: str,
    era_bucketing: Callable[[int], str] | None = None,
    class_remap: Mapping[str, str] | None = None,
    event_column: str = "event_key",
    season_column: str = "season",
    status_column: str = "observed_status",
    raw_value_column: str = "raw_value",
    deduced_value_column: str = "deduced_value",
    class_column: str = "class_label",
    share_column: str = "expected_share",
) -> BoundResult:
    """Bounds + summary for one dimension under one named bucketing."""
    fn = era_bucketing or ERA_BUCKETINGS[era_bucketing_name]
    bounds = derived_slice_bounds(
        slice_frame,
        export_frame,
        era_bucketing=fn,
        class_remap=class_remap,
        event_column=event_column,
        season_column=season_column,
        status_column=status_column,
        raw_value_column=raw_value_column,
        deduced_value_column=deduced_value_column,
        class_column=class_column,
        share_column=share_column,
    )
    summary = _summarize(
        slice_frame,
        bounds,
        dimension=dimension,
        era_bucketing_name=era_bucketing_name,
        era_bucketing=fn,
        season_column=season_column,
        status_column=status_column,
    )
    _log.debug(
        "bounds %s/%s: %d observed, %d unrecorded (%d derived), %d rows",
        dimension,
        era_bucketing_name,
        summary.n_observed,
        summary.n_unrecorded,
        summary.n_derived,
        bounds.height,
    )
    return BoundResult(bounds=bounds, summary=summary)
