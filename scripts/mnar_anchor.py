"""Anchored MNAR offset driver for the geometry trajectory dimension.

Reads the observed and derived slices of ``main_models.model_input_geometry``
(trajectory dimension) read-only from the prod ``bc.db`` at the repo root,
computes anchored per-(era, class) selection offsets under both the paper era
buckets and per-decade buckets, and writes the offsets parquet plus a provenance
summary under ``artifacts/statistical/anchors/trajectory/<run-id>/``.

The offsets are a partial-truth anchor for the observed-only geometry
imputation softmax: ``delta_c = log(p_masked_c / p_obs_c)`` centered per era. See
``bc/python_models/statistical/mnar_anchor.py`` for the estimator and the
published partial-truth assumption.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.statistical import mnar_anchor  # noqa: E402
from python_models.statistical.duckdb_io import open_bc_db  # noqa: E402

log = logging.getLogger("mnar_anchor")

DIMENSION = "trajectory"
BUCKETINGS: tuple[str, ...] = ("paper", "decade")
_SLICE_QUERY = """
    SELECT season, observed_status, raw_value, deduced_value
    FROM main_models.model_input_geometry
    WHERE geometry_dimension = '{dimension}'
      AND observed_status IN ('observed', 'derived')
"""


def _load_slice_frame(db_path: Path) -> pl.DataFrame:
    with open_bc_db(db_path, read_only=True) as con:
        _ = con.execute("PRAGMA disable_progress_bar")
        frame = con.execute(_SLICE_QUERY.format(dimension=DIMENSION)).pl()
    log.info(
        "loaded %d observed/derived rows for dimension=%s", frame.height, DIMENSION
    )
    return frame


def _log_offsets(bucketing: str, offsets: pl.DataFrame) -> None:
    for row in offsets.iter_rows(named=True):
        log.info(
            "  [%s] %-14s %-16s p_obs=%.4f p_masked=%.4f delta=%s covered=%s",
            bucketing,
            row["era_bucket"],
            row["class_label"],
            row["p_obs"],
            row["p_masked"],
            "nan" if row["delta"] != row["delta"] else f"{row['delta']:+.4f}",
            row["covered"],
        )


def run(db_path: Path, out_root: Path, run_id: str) -> Path:
    frame = _load_slice_frame(db_path)
    run_dir = out_root / DIMENSION / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    offset_frames: list[pl.DataFrame] = []
    bucketing_summaries: dict[str, object] = {}
    for bucketing in BUCKETINGS:
        result = mnar_anchor.compute_anchor(
            frame, dimension=DIMENSION, era_bucketing_name=bucketing
        )
        log.info(
            "bucketing=%s: %d observed / %d derived rows, %d offset rows",
            bucketing,
            result.summary.n_observed,
            result.summary.n_derived,
            result.offsets.height,
        )
        _log_offsets(bucketing, result.offsets)
        offset_frames.append(
            result.offsets.with_columns(pl.lit(bucketing).alias("bucketing"))
        )
        bucketing_summaries[bucketing] = {
            "buckets": result.summary.buckets,
            "classes": result.summary.classes,
            "slice_row_counts": result.summary.slice_row_counts,
            "n_observed": result.summary.n_observed,
            "n_derived": result.summary.n_derived,
        }

    combined = pl.concat(offset_frames).select("bucketing", *mnar_anchor.OFFSET_COLUMNS)
    offsets_path = run_dir / "anchor_offsets.parquet"
    combined.write_parquet(offsets_path)

    summary = {
        "module_version": mnar_anchor.MODULE_VERSION,
        "run_id": run_id,
        "dimension": DIMENSION,
        "db_path": str(db_path),
        "partial_truth_assumption": mnar_anchor.PARTIAL_TRUTH_ASSUMPTION,
        "total_rows": frame.height,
        "bucketings": bucketing_summaries,
    }
    summary_path = run_dir / "summary.json"
    _ = summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))

    log.info("wrote %s (%d rows)", offsets_path, combined.height)
    log.info("wrote %s", summary_path)
    return run_dir


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--db",
        type=Path,
        default=REPO_ROOT / "bc.db",
        help="prod DuckDB to read (default: bc.db at repo root); opened READ-ONLY",
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
    run(args.db, args.out_root, args.run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
