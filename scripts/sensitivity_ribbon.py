"""MNAR sensitivity ribbon for the published geometry imputations.

Two modes:

``--publish`` (default): for each published ``geometry_*`` pointer, reweight its
per-event class shares over the selection-log-odds grid and write
``exports/sensitivity_ribbon.parquet`` + a ``[low, high]`` band beside the fit.
The ribbon is the honest band on each imputed class mix under plausible MNAR;
``delta = 0`` reproduces the published (MAR) marginal.

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
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.statistical.manifests import find_published_manifest  # noqa: E402
from python_models.statistical.sensitivity import (  # noqa: E402
    DEFAULT_GRID,
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


def _published_export(model_name: str) -> tuple[str, Path] | None:
    pointer = find_published_manifest(model_name)
    if pointer is None:
        log.warning("no published pointer for %s", model_name)
        return None
    manifest = json.loads(pointer.read_text())
    export = (
        Path(manifest["manifest_path"]).parent / "exports" / "geometry_probabilities.parquet"
    )
    if not export.exists():
        log.warning("%s pointer has no geometry export at %s", model_name, export)
        return None
    return manifest["artifact_id"], export


def _dimension_of(export_df: pl.DataFrame) -> str:
    return str(export_df.select("geometry_dimension").unique().item())


def publish_ribbons() -> int:
    written = 0
    for model_name in GEOMETRY_MODELS:
        resolved = _published_export(model_name)
        if resolved is None:
            continue
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
        written += 1
    if written == 0:
        log.error("no published geometry exports resolved; nothing written")
        return 1
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
    focal_band = next(r for r in band.iter_rows(named=True) if r["class_label"] == focal)
    focal_truth = truth[focal]
    brackets = focal_band["share_low"] <= focal_truth <= focal_band["share_high"]

    log.info("focal class: %s (truth share %.4f)", focal, focal_truth)
    log.info("oracle per-class offset (centered, nats):")
    for c in sorted(oracle):
        log.info("  %-12s %+.3f  in-grid=%s", c, oracle[c], in_grid[c])
    log.info("TV vs truth: baseline(MAR)=%.4f  oracle-corrected=%.4f", baseline_tv, corrected_tv)
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
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()
    if args.validate_backtest is not None:
        return validate_backtest(args.validate_backtest)
    return publish_ribbons()


if __name__ == "__main__":
    raise SystemExit(main())
