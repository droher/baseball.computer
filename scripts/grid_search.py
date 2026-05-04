"""Grid-search build wall-clock vs (DuckDB threads, SQLMesh concurrent_tasks).

Sources/ENUMs/seeds stay in bc.db across iterations; only the model
snapshot schemas + the SQLMesh state DB are dropped, so each iteration
measures the model-build cost from a clean slate against pre-loaded
sources.

Each iteration spawns a subprocess (env vars are read by config.py at
module load — in-process re-import would not pick up the new values).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
BC_DIR = REPO_ROOT / "bc"
DB_PATH = REPO_ROOT / "bc.db"
STATE_DB_PATH = BC_DIR / "bc_state.db"
LOG_DIR = REPO_ROOT / "logs" / "perf" / "grid"
LOG_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_COMBOS: list[tuple[int, int]] = [
    (14, 1),
    (7, 2),
    (4, 4),
    (4, 6),
    (2, 8),
    (7, 4),
    (14, 2),
]

logger = logging.getLogger("grid_search")


def reset_build_artifacts() -> None:
    """Drop model snapshot schemas + state DB. Sources/ENUMs/seeds stay.

    Also nukes a stale `bc.db.wal` if present. Interrupted iterations
    leave a partial WAL that triggers a DuckDB internal error on the
    next open (`Failure while replaying WAL file`). Sources are
    committed before the model build starts, so dropping the WAL only
    discards the post-commit snapshot writes we'd drop anyway.
    """
    bc_wal = DB_PATH.with_suffix(".db.wal")
    if bc_wal.exists():
        bc_wal.unlink()
        logger.info("removed stale %s", bc_wal)

    for p in (STATE_DB_PATH, STATE_DB_PATH.with_suffix(".db.wal")):
        if p.exists():
            p.unlink()
            logger.info("removed %s", p)

    if not DB_PATH.exists():
        logger.warning("bc.db missing; nothing to drop. Run preload first.")
        return

    db = duckdb.connect(str(DB_PATH))
    try:
        rows = db.execute(
            """
            SELECT schema_name FROM information_schema.schemata
            WHERE catalog_name = 'bc'
              AND (schema_name LIKE 'sqlmesh%'
                OR schema_name = 'main_models'
                OR schema_name LIKE 'main_models\\_\\_%' ESCAPE '\\')
            """
        ).fetchall()
        for (s,) in rows:
            logger.info("dropping schema bc.%s", s)
            db.execute(f'DROP SCHEMA IF EXISTS bc."{s}" CASCADE')
        db.execute("CHECKPOINT bc")
    finally:
        db.close()


def run_one(threads: int, workers: int) -> dict[str, object]:
    reset_build_artifacts()

    env = os.environ.copy()
    env["BC_DUCKDB_THREADS"] = str(threads)
    env["BC_CONCURRENT_TASKS"] = str(workers)
    env.pop("BC_PERF_MODE", None)

    log_path = LOG_DIR / f"grid_t{threads}_w{workers}.log"
    cmd = [
        "uv", "run", "--group", "build",
        "python", str(REPO_ROOT / "scripts" / "_grid_one.py"),
    ]
    logger.info("[t=%d, w=%d] starting -> %s", threads, workers, log_path)
    t0 = time.monotonic()
    with log_path.open("w") as f:
        proc = subprocess.run(cmd, cwd=REPO_ROOT, env=env, stdout=f, stderr=subprocess.STDOUT)
    elapsed = time.monotonic() - t0

    inner_elapsed: float | None = None
    if log_path.exists():
        for line in reversed(log_path.read_text().splitlines()):
            if line.startswith("ELAPSED_S="):
                try:
                    inner_elapsed = float(line.split("=", 1)[1])
                except ValueError:
                    pass
                break

    logger.info(
        "[t=%d, w=%d] %s wall=%.1fs inner=%s (rc=%d)",
        threads,
        workers,
        "OK" if proc.returncode == 0 else "FAIL",
        elapsed,
        f"{inner_elapsed:.1f}s" if inner_elapsed else "?",
        proc.returncode,
    )
    return {
        "threads": threads,
        "workers": workers,
        "wall_s": round(elapsed, 2),
        "plan_s": round(inner_elapsed, 2) if inner_elapsed else None,
        "rc": proc.returncode,
        "log": str(log_path),
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--combos",
        default=None,
        help='JSON list of [threads, workers] pairs. Default grid: '
             f'{DEFAULT_COMBOS}',
    )
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    combos = (
        [tuple(c) for c in json.loads(args.combos)]
        if args.combos
        else DEFAULT_COMBOS
    )

    results: list[dict[str, object]] = []
    for t, w in combos:
        results.append(run_one(int(t), int(w)))

    out_path = LOG_DIR / "grid_results.json"
    out_path.write_text(json.dumps(results, indent=2))
    logger.info("wrote %s", out_path)

    print()
    print(f"{'threads':>8} {'workers':>8} {'wall_s':>10} {'plan_s':>10} {'rc':>4}")
    print("-" * 50)
    for r in results:
        plan_s = r["plan_s"]
        plan_disp = f"{plan_s:.1f}" if isinstance(plan_s, (int, float)) else "?"
        print(
            f"{r['threads']:>8} {r['workers']:>8} {r['wall_s']:>10.1f} {plan_disp:>10} {r['rc']:>4}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
