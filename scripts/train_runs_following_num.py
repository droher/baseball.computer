"""Train the Phase 6 runs-following regression model and pin the run id.

Run via:
    uv run --group migration-ml python scripts/train_runs_following_num.py \
        [--epochs 3] [--rows-per-batch 250000] [--db bc/bc.db]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BC_DIR = REPO_ROOT / "bc"
if str(BC_DIR) not in sys.path:
    sys.path.insert(0, str(BC_DIR))

from python_models.ml.features import RUNS_FOLLOWING_NUM  # noqa: E402
from python_models.ml.training import (  # noqa: E402
    DEFAULT_BATCH_ROWS,
    DEFAULT_DB,
    DEFAULT_EPOCHS,
    DEFAULT_SCHEMA,
    train,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--db", default=DEFAULT_DB, help="Path to bc.db")
    _ = parser.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        help="DuckDB schema for ml_features (main_models or main_models__dev)",
    )
    _ = parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    _ = parser.add_argument(
        "--rows-per-batch", type=int, default=DEFAULT_BATCH_ROWS
    )
    _ = parser.add_argument(
        "--rebuild-vocabs",
        action="store_true",
        help="Force vocabulary rebuild even if parquet cache is present",
    )
    _ = parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=str(args.log_level).upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    run_id = train(
        target_spec=RUNS_FOLLOWING_NUM,
        db_path=str(args.db),
        schema=str(args.schema),
        epochs=int(args.epochs),
        rows_per_batch=int(args.rows_per_batch),
        rebuild_vocabs=bool(args.rebuild_vocabs),
    )
    logging.info("training complete: run_id=%s", run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
