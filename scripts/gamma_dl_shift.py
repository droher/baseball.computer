"""Report the gamma_dl publication-tier posterior shift for a geometry dimension.

Loads a ``gamma_dl_zero`` fit and its published ``gamma_dl_shrunk`` sibling
and prints, per publication-tier effect block (``alpha_class`` and each
``delta_<fe>``), the distribution of ``|mean_shrunk - mean_zero| /
posterior_sd`` across cells plus the share of cells shifted beyond the
threshold. Emits a JSON summary when ``--json <path>`` is given.

Read-only: touches only ``artifacts/statistical/bayes/*`` posteriors and
advances no published pointer.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.statistical.config import BAYES_ROOT  # noqa: E402
from python_models.statistical.gamma_dl_shift import (  # noqa: E402
    DEFAULT_SHIFT_THRESHOLD_SD,
    report_from_artifact_dirs,
)

log = logging.getLogger("gamma_dl_shift")


def _resolve_dir(model: str, artifact_id: str, root: Path) -> Path:
    path = root / model / artifact_id
    if not path.exists():
        raise FileNotFoundError(f"artifact dir missing: {path}")
    return path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--model", required=True, help="Geometry model name.")
    _ = parser.add_argument("--zero", required=True, help="gamma_dl_zero artifact id.")
    _ = parser.add_argument("--shrunk", required=True, help="gamma_dl_shrunk artifact id.")
    _ = parser.add_argument(
        "--threshold-sd",
        type=float,
        default=DEFAULT_SHIFT_THRESHOLD_SD,
        help="Per-cell shift threshold in posterior-SD units (default 0.25).",
    )
    _ = parser.add_argument(
        "--bayes-root",
        type=Path,
        default=BAYES_ROOT,
        help="Root of bayes artifacts (default artifacts/statistical/bayes).",
    )
    _ = parser.add_argument(
        "--json",
        type=Path,
        default=None,
        help="Optional path to write the report JSON.",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()
    zero_dir = _resolve_dir(str(args.model), str(args.zero), args.bayes_root)
    shrunk_dir = _resolve_dir(str(args.model), str(args.shrunk), args.bayes_root)
    report = report_from_artifact_dirs(
        zero_dir, shrunk_dir, threshold_sd=float(args.threshold_sd)
    )

    log.info(
        "%s  zero=%s  shrunk=%s  threshold=%.2f SD",
        args.model,
        report.zero_artifact,
        report.shrunk_artifact,
        report.threshold_sd,
    )
    log.info(
        "%-26s %6s %8s %8s %8s %8s %10s",
        "block",
        "cells",
        "mean",
        "median",
        "p90",
        "max",
        "share>thr",
    )
    for block in report.blocks:
        log.info(
            "%-26s %6d %8.3f %8.3f %8.3f %8.3f %10.4f",
            block.block,
            block.n_cells,
            block.mean_abs_shift_sd,
            block.median_abs_shift_sd,
            block.p90_abs_shift_sd,
            block.max_abs_shift_sd,
            block.share_over_threshold,
        )
    log.info(
        "OVERALL cells=%d mean=%.3f max=%.3f share>%.2fSD=%.4f most_cells_over=%s",
        report.overall_n_cells,
        report.overall_mean_abs_shift_sd,
        report.overall_max_abs_shift_sd,
        report.threshold_sd,
        report.overall_share_over_threshold,
        report.most_cells_over_threshold,
    )
    verdict = (
        "FLAG: >0.25 SD on most cells — shrunk vs zero materially differ"
        if report.most_cells_over_threshold
        else "shrunk flavor stands: shift < 0.25 SD on most cells"
    )
    log.info("VERDICT: %s", verdict)

    if args.json is not None:
        _ = args.json.write_text(json.dumps(report.model_dump(), indent=2, sort_keys=True))
        log.info("wrote %s", args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
