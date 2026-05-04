# pyright: reportAny=false, reportExplicitAny=false, reportUnusedCallResult=false
"""Capture per-model DuckDB JSON profiles for the slowest snapshots.

The perf-run profile JSONs in ``logs/perf/profiles/`` capture the *last*
query DuckDB ran during each ``SnapshotEvaluator.evaluate`` call. For
SQL models that's a trailing ``COMMENT ON COLUMN`` DDL, not the CTAS —
useless for finding hot operators. So this script re-renders the
top-N slowest models from the latest perf JSONL via ``Context.render``,
runs each rendered SELECT against the already-built ``bc.db`` as a
``CREATE OR REPLACE TEMPORARY TABLE _profile_tmp AS <select>`` (which
forces full execution while staying read-only), and writes DuckDB's
per-query JSON profile to ``logs/perf/profiles_analyze/<model>.json``.

Analysis happens out-of-band — read the JSONs directly (or via
subagents) and write findings to ``notes/perf-profile-report.md``.

Usage::

    uv run --group build python scripts/profile_hotspots.py [--top N]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BC_DIR = PROJECT_ROOT / "bc"
DB_PATH = PROJECT_ROOT / "bc.db"
PERF_DIR = PROJECT_ROOT / "logs" / "perf"
ANALYZE_DIR = PERF_DIR / "profiles_analyze"

ANALYZE_DIR.mkdir(parents=True, exist_ok=True)

_ = os.environ.setdefault("BC_PERF_MODE", "0")

import duckdb  # noqa: E402

logger = logging.getLogger("profile_hotspots")


@dataclass
class HotModel:
    model: str
    duration_s: float
    rss_peak_mb: float
    db_growth_mb: float


def latest_jsonl() -> Path:
    candidates = sorted(PERF_DIR.glob("perf_*.jsonl"))
    if not candidates:
        raise SystemExit("no perf_*.jsonl found in logs/perf/")
    return candidates[-1]


def load_top_models(jsonl: Path, top: int) -> list[HotModel]:
    rows: list[HotModel] = []
    with jsonl.open() as f:
        for line in f:
            r: dict[str, Any] = json.loads(line)
            if r.get("error"):
                continue
            name = str(r["model"]).strip('"').replace('"."', ".")
            rows.append(
                HotModel(
                    model=name,
                    duration_s=float(r["duration_s"]),
                    rss_peak_mb=float(r.get("rss_peak_mb", 0.0)),
                    db_growth_mb=float(r.get("db_growth_mb", 0.0)),
                )
            )
    rows.sort(key=lambda r: r.duration_s, reverse=True)
    return rows[:top]


_PHYSICAL_RE = re.compile(
    r'"sqlmesh__main_models"\."main_models__([a-zA-Z0-9_]+?)__\d+__dev"'
)


def rewrite_to_virtual(sql: str) -> str:
    """Rewrite physical snapshot references to virtual layer views.

    Snapshot IDs in ``ctx.render`` output can drift from what's on disk
    (state file refers to a different snapshot ID than the one the
    perf-run build materialized). The virtual layer at
    ``main_models__dev.<model>`` is stable across snapshot churn.
    """
    return _PHYSICAL_RE.sub(r'"main_models__dev"."\1"', sql)


def render_select(ctx: Any, model_name: str) -> str:
    expr = ctx.render(model_name)
    sql: str = expr.sql(dialect="duckdb")
    return rewrite_to_virtual(sql)


def configure_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("SET memory_limit = '48GB'")
    con.execute("SET threads = 7")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET enable_fsst_vectors = true")
    tmp_dir = (PROJECT_ROOT / "bc.db.tmp").as_posix()
    con.execute(f"SET temp_directory = '{tmp_dir}'")
    con.execute("SET enable_profiling = 'json'")
    con.execute("SET profiling_mode = 'detailed'")
    con.execute("SET profiling_coverage = 'ALL'")


def capture_profile(
    con: duckdb.DuckDBPyConnection, sql: str, profile_path: Path
) -> tuple[float, str | None]:
    """Run the SELECT body via a TEMPORARY CTAS and capture its JSON profile.

    Approach choices:

    * ``EXPLAIN ANALYZE`` doesn't write ``profile_output`` — probably
      because EXPLAIN itself is the profiling mechanism.
    * ``COPY (sql) TO '/dev/null'`` works but pays full CSV
      serialization cost (~5s vs ~1s for the same input as a CTAS),
      polluting per-operator timings.
    * ``CREATE OR REPLACE TEMPORARY TABLE`` works in read-only mode
      (DuckDB temp tables go to ``temp_directory``, not the database
      file), forces full execution, and writes the SELECT plan tree
      cleanly.

    DuckDB writes ``profile_output`` immediately on query completion to
    whatever path was set when the query *started*, so the SET-then-CTAS
    pattern below routes each query's plan to its own file.
    """
    con.execute(f"SET profile_output = '{profile_path.as_posix()}'")
    t0 = time.perf_counter()
    err: str | None = None
    try:
        con.execute(f"CREATE OR REPLACE TEMPORARY TABLE _profile_tmp AS {sql}")
    except Exception as exc:
        err = repr(exc)
    elapsed = time.perf_counter() - t0
    return elapsed, err


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--top",
        type=int,
        default=15,
        help="Number of slowest models to capture profiles for",
    )
    parser.add_argument("--jsonl", type=Path, default=None, help="Override JSONL path")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if not DB_PATH.exists():
        logger.error("bc.db not found at %s — run perf_run.py first", DB_PATH)
        return 1

    jsonl = args.jsonl or latest_jsonl()
    logger.info("using jsonl=%s", jsonl)
    hot = load_top_models(jsonl, args.top)
    logger.info("loaded %d hot models", len(hot))
    for h in hot:
        logger.info(
            "  %s — %.2fs (rss=%.0fMB db=%.0fMB)",
            h.model,
            h.duration_s,
            h.rss_peak_mb,
            h.db_growth_mb,
        )

    sys.path.insert(0, str(BC_DIR))
    os.chdir(BC_DIR)
    from sqlmesh import Context

    ctx = Context(paths=[BC_DIR], concurrent_tasks=1)
    logger.info("Context loaded")

    con = duckdb.connect(str(DB_PATH), read_only=True)
    configure_duckdb(con)
    logger.info("connected read-only to %s", DB_PATH)

    failed: list[tuple[str, str]] = []
    for h in hot:
        logger.info("rendering %s", h.model)
        try:
            sql = render_select(ctx, h.model)
        except Exception as exc:
            logger.exception("render failed for %s", h.model)
            failed.append((h.model, f"render: {exc!r}"))
            continue

        safe = h.model.replace(".", "_").replace('"', "")
        profile_path = ANALYZE_DIR / f"{safe}.json"
        elapsed, err = capture_profile(con, sql, profile_path)
        if err:
            logger.warning("  capture FAILED %.2fs: %s", elapsed, err)
            failed.append((h.model, err))
        else:
            logger.info("  captured %.2fs → %s", elapsed, profile_path)

    logger.info(
        "done; %d profiles in %s; %d failures",
        len(hot) - len(failed),
        ANALYZE_DIR,
        len(failed),
    )
    for model, err in failed:
        logger.warning("  %s: %s", model, err)
    return 0


if __name__ == "__main__":
    sys.exit(main())
