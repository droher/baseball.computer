"""One iteration of grid_search.py: open Context, plan dev, exit.

Stub-runs SnapshotEvaluator.audit to skip cross-model audit references
that fail on a wiped state (same trick as perf_run.py).
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

from sqlmesh import Context
from sqlmesh.core.snapshot.evaluator import SnapshotEvaluator

REPO_ROOT = Path(__file__).resolve().parent.parent
BC_DIR = REPO_ROOT / "bc"

logger = logging.getLogger("grid_one")


def install_audit_skip() -> None:
    def noop_audit(self: SnapshotEvaluator, snapshot: Any, **kwargs: Any) -> list[Any]:
        del self, snapshot, kwargs
        return []

    SnapshotEvaluator.audit = noop_audit  # type: ignore[method-assign]


def main() -> int:
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    install_audit_skip()
    os.chdir(BC_DIR)

    workers = int(os.environ.get("BC_CONCURRENT_TASKS", "2"))
    threads = int(os.environ.get("BC_DUCKDB_THREADS", "7"))
    logger.info("grid iter starting: threads=%d workers=%d", threads, workers)

    ctx = Context(paths=[BC_DIR], concurrent_tasks=workers)
    t0 = time.monotonic()
    plan = ctx.plan(
        environment="dev",
        auto_apply=True,
        no_prompts=True,
        skip_tests=True,
        skip_linter=True,
    )
    elapsed = time.monotonic() - t0
    logger.info(
        "plan complete: missing_intervals=%d directly_modified=%d in %.1fs",
        len(plan.missing_intervals or []),
        len(plan.directly_modified or []),
        elapsed,
    )

    try:
        ctx.engine_adapter.execute("CHECKPOINT bc")
        logger.info("final CHECKPOINT bc done")
    except Exception as exc:
        logger.warning("final checkpoint failed: %r", exc)

    print(f"ELAPSED_S={elapsed:.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
