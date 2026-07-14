"""Run the MNAR masked backtest validating the gamma_propensity covariate.

Thin argparse wrapper over
``python_models.statistical.backtests.mnar_masked.run_backtest``. Exits 0
when every pass criterion holds, 1 otherwise. Outputs (mask_summary.json,
metrics.json, temp dataset + fit artifacts) land under
``artifacts/statistical/backtests/mnar/<run-id>/``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.statistical.backtests.mnar_masked import (  # noqa: E402
    DEFAULT_BUDGET,
    DEFAULT_MIN_SEASON,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_SEED,
    SMOKE_BUDGET,
    MaskConfig,
    run_backtest,
)

log = logging.getLogger("mnar_masked_backtest")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--model",
        choices=("geometry", "ball_handler"),
        default="geometry",
        help="which imputation model to backtest",
    )
    _ = parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="seed for the synthetic mask and every fit",
    )
    _ = parser.add_argument(
        "--budget",
        type=int,
        default=None,
        help=(
            f"training row budget per fit (default {DEFAULT_BUDGET}, "
            f"{SMOKE_BUDGET} with --smoke)"
        ),
    )
    _ = parser.add_argument(
        "--smoke",
        action="store_true",
        help="tiny budget + smoke sampler so the whole run finishes in minutes",
    )
    _ = parser.add_argument(
        "--run-id",
        default=None,
        help="output directory name (default derived from model+seed+budget)",
    )
    _ = parser.add_argument(
        "--dataset-parquet",
        type=Path,
        default=None,
        help=(
            "frozen dataset parquet for the truth universe (default resolves "
            "the published pointer's dataset)"
        ),
    )
    _ = parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="root directory for run outputs",
    )
    _ = parser.add_argument(
        "--min-season",
        type=int,
        default=DEFAULT_MIN_SEASON,
        help="earliest season included in the truth universe",
    )
    _ = parser.add_argument(
        "--mask-design",
        choices=(
            "w_class_intensity",
            "covariate_joint",
            "scorer_blocked",
            "era_graded",
        ),
        default="w_class_intensity",
        help="synthetic MNAR mask design to run (see MaskConfig.design)",
    )
    _ = parser.add_argument(
        "--verbose",
        action="store_true",
        help="DEBUG-level logging",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    mask_config = MaskConfig(design=args.mask_design)
    result = run_backtest(
        model=args.model,
        seed=args.seed,
        budget=args.budget,
        smoke=args.smoke,
        run_id=args.run_id,
        dataset_parquet=args.dataset_parquet,
        output_root=args.output_root,
        min_season=args.min_season,
        mask_config=mask_config,
    )
    criteria = result.metrics["criteria"]
    log.info(
        "backtest %s: criteria=%s metrics=%s",
        "PASS" if result.overall_pass else "FAIL",
        criteria,
        result.metrics_path,
    )
    return 0 if result.overall_pass else 1


if __name__ == "__main__":
    sys.exit(main())
