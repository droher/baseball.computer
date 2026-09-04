"""MNAR sensitivity ribbon for the published geometry imputations.

Three modes:

``--publish`` (default): for each published ``geometry_*`` pointer, reweight its
per-event class shares over the selection-log-odds grid and write
``exports/sensitivity_ribbon.parquet`` + a ``[low, high]`` band beside the fit.
The ribbon is the honest band on each imputed class mix under plausible MNAR;
``delta = 0`` reproduces the published (MAR) marginal.

``--bound <anchor-run-dir>``: for the trajectory dimension only, split the
published export by paper era, sweep the marginal ribbon within each era, and
add one derived number per era: the smallest GroundBall offset at which the
corrected marginal GroundBall share on the unrecorded slice reaches the
derived-slice lower bound from ``scripts/mnar_anchor.py``. Writes
``exports/sensitivity_ribbon_by_era.parquet``,
``exports/trajectory_bound_offset.parquet`` and
``exports/trajectory_bound_offset_summary.json`` beside the trajectory fit, so a
reader can see whether the +-1.0 grid contains the bound. The other four
geometry dimensions have no derived slice; ``--publish`` covers them.

The bound is a floor, not an anchor: the derived slice is 100% GroundBall, so
contrasting it against the observed slice would only restate the observed
GroundBall share. The offset reported here is the offset a reader would need
to assume to make the MAR marginal respect the floor.

``--validate-backtest <run-dir>``: on the masked backtest, the real correction
is known (the per-class mask probabilities recover the true maskable-slice
mix). Confirms the default grid spans that correction — the oracle per-class
offset lands inside the grid and the focal-class band brackets the truth — so
the published ribbon is not too narrow to contain a realistic MNAR shift.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.statistical import mnar_anchor  # noqa: E402
from python_models.statistical.manifests import (  # noqa: E402
    find_published_manifest,
    read_published_pointer,
)
from python_models.statistical.sensitivity import (  # noqa: E402
    DEFAULT_GRID,
    bound_offset_table,
    era_sensitivity_ribbon,
    offset_recovers_target,
    ribbon_band,
    sensitivity_ribbon,
)

log = logging.getLogger("sensitivity_ribbon")

GEOMETRY_MODELS: tuple[str, ...] = (
    "geometry_trajectory",
    "geometry_location_side",
    "geometry_location_depth",
    "geometry_location_edge",
    "geometry_general_location",
)
_LOGIT_CLIP = 1e-6

BOUND_DIMENSION = "trajectory"
BOUND_MODEL = "geometry_trajectory"
FOCAL_CLASS = "GroundBall"
BOUND_BUCKETING = "paper"


def _published_export(model_name: str) -> tuple[str, Path] | None:
    pointer_path = find_published_manifest(model_name)
    if pointer_path is None:
        log.warning("no published pointer for %s", model_name)
        return None
    pointer = read_published_pointer(pointer_path)
    export = pointer.manifest_path.parent / "exports" / "geometry_probabilities.parquet"
    if not export.exists():
        log.warning("%s pointer has no geometry export at %s", model_name, export)
        return None
    return pointer.artifact_id, export


def _dimension_of(export_df: pl.DataFrame) -> str:
    return str(export_df.select("geometry_dimension").unique().item())


def _publish_marginal(model_name: str) -> bool:
    resolved = _published_export(model_name)
    if resolved is None:
        return False
    _artifact_id, export_path = resolved
    export_df = pl.read_parquet(export_path)
    dimension = _dimension_of(export_df)
    ribbon = sensitivity_ribbon(export_df, dimension=dimension)
    out_path = export_path.parent / "sensitivity_ribbon.parquet"
    ribbon.write_parquet(out_path)
    band = ribbon_band(ribbon)
    log.info("%s -> %s", model_name, out_path)
    for row in band.iter_rows(named=True):
        log.info(
            "  %-12s baseline=%.4f band=[%.4f, %.4f]",
            row["class_label"],
            row["baseline_share"],
            row["share_low"],
            row["share_high"],
        )
    return True


def publish_ribbons() -> int:
    written = sum(_publish_marginal(model_name) for model_name in GEOMETRY_MODELS)
    if written == 0:
        log.error("no published geometry exports resolved; nothing written")
        return 1
    return 0


def _bounds_by_era(anchor_dir: Path) -> pl.DataFrame:
    bounds = pl.read_parquet(anchor_dir / "derived_slice_bounds.parquet").filter(
        (pl.col("bucketing") == BOUND_BUCKETING)
        & (pl.col("class_label") == FOCAL_CLASS)
    )
    if bounds.height == 0:
        raise ValueError(
            f"{anchor_dir} has no {BOUND_BUCKETING}/{FOCAL_CLASS} rows in derived_slice_bounds.parquet"
        )
    return bounds.select(
        "era_bucket",
        "share_lower_bound",
        "share_point_estimate",
        "mar_share_unrecorded",
        "mar_share_on_derived",
        "n_unrecorded",
        "n_derived_class",
    ).sort("era_bucket")


def _attach_era(
    export_df: pl.DataFrame, *, source: str, dataset_path: Path, db_path: Path
) -> pl.DataFrame:
    if source == "dataset":
        slice_frame = mnar_anchor.load_dimension_slice(
            BOUND_DIMENSION, dataset_path=dataset_path
        )
    else:
        slice_frame = mnar_anchor.load_dimension_slice(BOUND_DIMENSION, db_path=db_path)
    seasons = slice_frame.select("event_key", "season")
    joined = export_df.with_columns(pl.col("event_key").cast(pl.Int64)).join(
        seasons, on="event_key", how="left"
    )
    missing = (
        joined.filter(pl.col("season").is_null()).get_column("event_key").n_unique()
    )
    if missing:
        raise ValueError(f"{missing} export events have no season in the slice source")
    return joined.with_columns(
        pl.when(pl.col("season") < 1950)
        .then(pl.lit(mnar_anchor.PAPER_BUCKET_PRE_1950))
        .when(pl.col("season") <= 1987)
        .then(pl.lit(mnar_anchor.PAPER_BUCKET_1950_1987))
        .otherwise(pl.lit(mnar_anchor.PAPER_BUCKET_1988_PLUS))
        .alias("era_bucket")
    )


def _log_bound_table(table: pl.DataFrame, ribbon: pl.DataFrame) -> None:
    for row in table.iter_rows(named=True):
        era = row["era_bucket"]
        focal = (
            ribbon.filter(
                (pl.col("era_bucket") == era) & (pl.col("class_label") == FOCAL_CLASS)
            )
            .sort("delta_logodds")
            .select("delta_logodds", "marginal_share")
        )
        grid_text = "  ".join(f"d={d:+.2f}:{s:.4f}" for d, s in focal.iter_rows())
        delta = row["delta_at_target"]
        log.info(
            "[%s] %s MAR=%.4f  %s  bound=%.4f  delta_at_bound=%s  in_grid=%s",
            era,
            FOCAL_CLASS,
            row["baseline_share"],
            grid_text,
            row["target_share"],
            "unreachable" if delta is None else f"{delta:+.4f}",
            row["target_in_grid"],
        )


ANCHOR_SUMMARY_FILENAME = "summary.json"
ANCHOR_ARTIFACT_KEY = "trajectory_artifact_id"


def anchor_run_artifact_id(anchor_dir: Path) -> str:
    """The published fit artifact id an anchor run computed its bounds against."""
    summary_path = anchor_dir / ANCHOR_SUMMARY_FILENAME
    if not summary_path.exists():
        raise FileNotFoundError(f"{anchor_dir} has no {ANCHOR_SUMMARY_FILENAME}")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    artifact_id = (
        summary.get(ANCHOR_ARTIFACT_KEY) if isinstance(summary, dict) else None
    )
    if not isinstance(artifact_id, str) or not artifact_id:
        raise ValueError(
            f"{summary_path} carries no {ANCHOR_ARTIFACT_KEY}; the bounds cannot be "
            "matched to a published fit"
        )
    return artifact_id


def assert_anchor_matches_published(
    anchor_dir: Path, published_artifact_id: str
) -> None:
    """Raise unless the anchor run's bounds were computed against the published fit."""
    anchor_artifact_id = anchor_run_artifact_id(anchor_dir)
    if anchor_artifact_id != published_artifact_id:
        raise ValueError(
            f"{anchor_dir} bounds were computed against {BOUND_MODEL} artifact "
            f"{anchor_artifact_id!r}, but the published pointer now names "
            f"{published_artifact_id!r}; rerun scripts/mnar_anchor.py against the "
            "published fit before deriving its bound offsets"
        )


def publish_bound_ribbons(
    anchor_dir: Path, *, source: str, dataset_path: Path | None, db_path: Path
) -> int:
    started = time.perf_counter()
    inputs = mnar_anchor.resolve_published_geometry_inputs(
        BOUND_MODEL, require_dataset=source == "dataset" and dataset_path is None
    )
    assert_anchor_matches_published(anchor_dir, inputs.artifact_id)
    bounds = _bounds_by_era(anchor_dir)
    resolved_dataset = dataset_path if dataset_path is not None else inputs.dataset_path
    export_df = pl.read_parquet(inputs.export_path)
    dimension = _dimension_of(export_df)
    if dimension != BOUND_DIMENSION:
        raise ValueError(
            f"{BOUND_MODEL} export is dimension {dimension!r}, not {BOUND_DIMENSION!r}"
        )
    export_df = _attach_era(
        export_df, source=source, dataset_path=resolved_dataset, db_path=db_path
    )

    ribbon = era_sensitivity_ribbon(export_df, dimension=dimension)
    target_by_era = {
        str(row["era_bucket"]): float(row["share_lower_bound"])
        for row in bounds.iter_rows(named=True)
    }
    table = bound_offset_table(
        export_df, class_label=FOCAL_CLASS, target_by_era=target_by_era
    )
    table = table.join(
        bounds.select(
            "era_bucket",
            "share_point_estimate",
            "mar_share_on_derived",
            "n_unrecorded",
            "n_derived_class",
        ),
        on="era_bucket",
        how="left",
    )

    ribbon_path = inputs.export_path.parent / "sensitivity_ribbon_by_era.parquet"
    ribbon.write_parquet(ribbon_path)
    table_path = inputs.export_path.parent / "trajectory_bound_offset.parquet"
    table.write_parquet(table_path)
    log.info(
        "%s -> %s (%d rows), %s (%d rows)",
        BOUND_MODEL,
        ribbon_path,
        ribbon.height,
        table_path,
        table.height,
    )
    _log_bound_table(table, ribbon)

    elapsed = time.perf_counter() - started
    summary = {
        "focal_class": FOCAL_CLASS,
        "anchor_run_dir": str(anchor_dir),
        "trajectory_artifact_id": inputs.artifact_id,
        "dataset_artifact_id": inputs.dataset_artifact_id,
        "slice_source": source,
        "slice_path": str(resolved_dataset if source == "dataset" else db_path),
        "grid": list(DEFAULT_GRID),
        "identification_statement": mnar_anchor.IDENTIFICATION_STATEMENT,
        "elapsed_seconds": round(elapsed, 3),
        "eras": table.to_dicts(),
    }
    summary_path = inputs.export_path.parent / "trajectory_bound_offset_summary.json"
    _ = summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    log.info("wrote %s", summary_path)
    log.info("elapsed %.2fs", elapsed)
    return 0


def _logit(p: float) -> float:
    p = min(max(p, _LOGIT_CLIP), 1.0 - _LOGIT_CLIP)
    return math.log(p / (1.0 - p))


def validate_backtest(run_dir: Path) -> int:
    mask_summary = json.loads((run_dir / "mask_summary.json").read_text())
    truth = mask_summary["truth_shares_maskable"]
    mask_prob = mask_summary["class_mask_probabilities"]
    focal = mask_summary["focal_class"]

    uncorrected = next(
        run_dir.glob("bayes/*/*-uncorrected/exports/geometry_probabilities.parquet"),
        None,
    )
    if uncorrected is None:
        log.error("no uncorrected export under %s", run_dir)
        return 1
    export_df = pl.read_parquet(uncorrected)

    raw_offset = {c: _logit(p) for c, p in mask_prob.items()}
    centre = sum(raw_offset.values()) / len(raw_offset)
    oracle = {c: v - centre for c, v in raw_offset.items()}

    grid_lo, grid_hi = min(DEFAULT_GRID), max(DEFAULT_GRID)
    in_grid = {c: grid_lo <= d <= grid_hi for c, d in oracle.items()}

    baseline_tv = offset_recovers_target(export_df, offset={}, target=truth)
    corrected_tv = offset_recovers_target(export_df, offset=oracle, target=truth)

    ribbon = sensitivity_ribbon(export_df, dimension=_dimension_of(export_df))
    band = ribbon_band(ribbon)
    focal_band = next(
        r for r in band.iter_rows(named=True) if r["class_label"] == focal
    )
    focal_truth = truth[focal]
    brackets = focal_band["share_low"] <= focal_truth <= focal_band["share_high"]

    log.info("focal class: %s (truth share %.4f)", focal, focal_truth)
    log.info("oracle per-class offset (centered, nats):")
    for c in sorted(oracle):
        log.info("  %-12s %+.3f  in-grid=%s", c, oracle[c], in_grid[c])
    log.info(
        "TV vs truth: baseline(MAR)=%.4f  oracle-corrected=%.4f",
        baseline_tv,
        corrected_tv,
    )
    log.info(
        "focal band [%.4f, %.4f] brackets truth %.4f: %s",
        focal_band["share_low"],
        focal_band["share_high"],
        focal_truth,
        brackets,
    )

    oracle_in_grid = all(in_grid.values())
    corrected_better = corrected_tv < baseline_tv
    ok = oracle_in_grid and corrected_better and brackets
    log.info(
        "VALIDATION %s (oracle-in-grid=%s, corrected<baseline=%s, brackets=%s)",
        "PASS" if ok else "FAIL",
        oracle_in_grid,
        corrected_better,
        brackets,
    )
    return 0 if ok else 1


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--validate-backtest",
        type=Path,
        default=None,
        metavar="RUN_DIR",
        help="validate grid coverage against a masked-backtest run dir instead of publishing",
    )
    _ = parser.add_argument(
        "--bound",
        type=Path,
        default=None,
        metavar="ANCHOR_RUN_DIR",
        help="per-era trajectory ribbon plus the GroundBall offset at the derived-slice bound",
    )
    _ = parser.add_argument(
        "--source",
        choices=("dataset", "db"),
        default="dataset",
        help="era source for --bound: the fit's frozen dataset parquet (default) or prod bc.db",
    )
    _ = parser.add_argument(
        "--dataset",
        type=Path,
        default=None,
        help="override the dataset parquet for --bound (default: from the published manifest)",
    )
    _ = parser.add_argument(
        "--db",
        type=Path,
        default=REPO_ROOT / "bc.db",
        help="prod DuckDB for --source db (default: bc.db at repo root); READ-ONLY",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()
    if args.validate_backtest is not None:
        return validate_backtest(args.validate_backtest)
    if args.bound is not None:
        return publish_bound_ribbons(
            args.bound, source=args.source, dataset_path=args.dataset, db_path=args.db
        )
    return publish_ribbons()


if __name__ == "__main__":
    raise SystemExit(main())
