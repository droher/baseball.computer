"""Derived-slice bounds for the geometry trajectory imputation.

Joins the trajectory slice of the frozen ``model_input_geometry`` dataset the
published ``geometry_trajectory`` fit was trained on (resolved from its
manifest's ``dataset_artifact_id``; ``--source db`` reads the prod ``bc.db``
read-only instead) to that fit's MAR class-share export, and writes per-(era,
class) rows under ``artifacts/statistical/anchors/trajectory/<run-id>/``:

- ``share_lower_bound``: P(class | unrecorded) >= n_derived_class / n_unrecorded;
- ``share_point_estimate``: truth on the derived rows, MAR on the unknown rows;
- ``mar_share_on_derived`` / ``mar_log_loss_on_derived``: the MAR export
  scored against the derived rows' known class.

See ``bc/python_models/statistical/mnar_anchor.py`` for the estimator and the
published identification statement.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.statistical import mnar_anchor  # noqa: E402
from python_models.statistical.models._geometry_data import GEOMETRY_DIMENSIONS  # noqa: E402

log = logging.getLogger("mnar_anchor")

DIMENSION = "trajectory"
MODEL_NAME = "geometry_trajectory"
BUCKETINGS: tuple[str, ...] = ("paper", "decade")


def _log_bounds(bucketing: str, bounds: pl.DataFrame) -> None:
    for row in bounds.filter(pl.col("covered")).iter_rows(named=True):
        log.info(
            "  [%s] %-10s %-11s n_unrecorded=%d n_derived=%d bound=%.4f "
            "mar=%.4f point=%.4f mar_on_derived=%.4f logloss=%.4f observed_share=%.4f",
            bucketing,
            row["era_bucket"],
            row["class_label"],
            row["n_unrecorded"],
            row["n_derived_class"],
            row["share_lower_bound"],
            row["mar_share_unrecorded"],
            row["share_point_estimate"],
            row["mar_share_on_derived"],
            row["mar_log_loss_on_derived"],
            row["observed_share"],
        )


def _published_inputs(
    *, need_dataset: bool, need_export: bool
) -> mnar_anchor.PublishedGeometryInputs:
    """Resolve the published fit, requiring only the files the run will read.

    The pointer and manifest are always read so the run records which
    published fit its bounds describe; the frozen dataset and the export
    must exist only when no override replaces them.
    """
    return mnar_anchor.resolve_published_geometry_inputs(
        MODEL_NAME, require_dataset=need_dataset, require_export=need_export
    )


def run(
    *,
    source: str,
    dataset_path: Path | None,
    db_path: Path,
    export_path: Path | None,
    out_root: Path,
    run_id: str,
) -> Path:
    started = time.perf_counter()
    need_published_dataset = source == "dataset" and dataset_path is None
    need_published_export = export_path is None
    inputs = _published_inputs(
        need_dataset=need_published_dataset, need_export=need_published_export
    )
    if source == "dataset":
        resolved_dataset = (
            dataset_path if dataset_path is not None else inputs.dataset_path
        )
        slice_frame = mnar_anchor.load_dimension_slice(
            DIMENSION, dataset_path=resolved_dataset
        )
        slice_source = str(resolved_dataset)
    else:
        slice_frame = mnar_anchor.load_dimension_slice(DIMENSION, db_path=db_path)
        slice_source = str(db_path)
    resolved_export = export_path if export_path is not None else inputs.export_path
    export_frame = pl.read_parquet(resolved_export)
    log.info("loaded %d export rows from %s", export_frame.height, resolved_export)

    run_dir = out_root / DIMENSION / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    remap = dict(GEOMETRY_DIMENSIONS[DIMENSION].remap)

    frames: list[pl.DataFrame] = []
    bucketing_summaries: dict[str, object] = {}
    for bucketing in BUCKETINGS:
        result = mnar_anchor.compute_bounds(
            slice_frame,
            export_frame,
            dimension=DIMENSION,
            era_bucketing_name=bucketing,
            class_remap=remap,
        )
        log.info(
            "bucketing=%s: %d observed / %d unrecorded (%d derived, %d unknown), %d rows",
            bucketing,
            result.summary.n_observed,
            result.summary.n_unrecorded,
            result.summary.n_derived,
            result.summary.n_unknown,
            result.bounds.height,
        )
        _log_bounds(bucketing, result.bounds)
        frames.append(result.bounds.with_columns(pl.lit(bucketing).alias("bucketing")))
        bucketing_summaries[bucketing] = {
            "buckets": result.summary.buckets,
            "classes": result.summary.classes,
            "slice_row_counts": result.summary.slice_row_counts,
            "n_observed": result.summary.n_observed,
            "n_unrecorded": result.summary.n_unrecorded,
            "n_derived": result.summary.n_derived,
            "n_unknown": result.summary.n_unknown,
        }

    combined = pl.concat(frames).select("bucketing", *mnar_anchor.BOUND_COLUMNS)
    bounds_path = run_dir / "derived_slice_bounds.parquet"
    combined.write_parquet(bounds_path)

    elapsed = time.perf_counter() - started
    summary = {
        "module_version": mnar_anchor.MODULE_VERSION,
        "run_id": run_id,
        "dimension": DIMENSION,
        "model_name": MODEL_NAME,
        "trajectory_artifact_id": inputs.artifact_id,
        "dataset_artifact_id": inputs.dataset_artifact_id,
        "slice_source": source,
        "slice_path": slice_source,
        "export_path": str(resolved_export),
        "class_remap": remap,
        "identification_statement": mnar_anchor.IDENTIFICATION_STATEMENT,
        "total_slice_rows": slice_frame.height,
        "total_export_rows": export_frame.height,
        "elapsed_seconds": round(elapsed, 3),
        "bucketings": bucketing_summaries,
    }
    summary_path = run_dir / "summary.json"
    _ = summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))

    log.info("wrote %s (%d rows)", bounds_path, combined.height)
    log.info("wrote %s", summary_path)
    log.info("elapsed %.2fs", elapsed)
    return run_dir


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--source",
        choices=("dataset", "db"),
        default="dataset",
        help="slice source: the published fit's frozen dataset parquet (default) or prod bc.db",
    )
    _ = parser.add_argument(
        "--dataset",
        type=Path,
        default=None,
        help="override the dataset parquet (default: resolved from the published manifest)",
    )
    _ = parser.add_argument(
        "--db",
        type=Path,
        default=REPO_ROOT / "bc.db",
        help="prod DuckDB for --source db (default: bc.db at repo root); opened READ-ONLY",
    )
    _ = parser.add_argument(
        "--export",
        type=Path,
        default=None,
        help="override the MAR class-share export (default: the published fit's)",
    )
    _ = parser.add_argument(
        "--out-root",
        type=Path,
        default=REPO_ROOT / "artifacts" / "statistical" / "anchors",
        help="artifact root; run dir is <out-root>/trajectory/<run-id>/",
    )
    _ = parser.add_argument(
        "--run-id",
        type=str,
        default=datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
        help="run id (default: UTC timestamp)",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()
    run(
        source=args.source,
        dataset_path=args.dataset,
        db_path=args.db,
        export_path=args.export,
        out_root=args.out_root,
        run_id=args.run_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
