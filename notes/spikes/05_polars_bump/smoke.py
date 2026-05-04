"""Smoke test for Polars 1.40 against existing scratch.ipynb usage surface.

scratch.ipynb cells use only:
  - import polars as pl
  - duckdb -> .df() (pandas, not polars)
  - a CHUNK_SIZE constant

The notebook itself doesn't actively call polars APIs in the saved cells. To
exercise the upgrade meaningfully, this smoke test exercises the polars APIs
that production code paths (bc/, scripts/) and the planned axis-D Phase 5
spikes will rely on:
  - duckdb -> arrow -> polars zero-copy
  - lazy frame ops with .over()
  - forward_fill (used by Spike 4)
  - .collect(streaming=True)

Anything raising DeprecationWarning is captured.
"""
from __future__ import annotations

import logging
import sys
import warnings
from pathlib import Path

import duckdb
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("polars_smoke")

DB_PATH = Path("/Users/davidroher/Repos/baseball.computer/bc.db")


def _capture_warnings() -> list[warnings.WarningMessage]:
    captured: list[warnings.WarningMessage] = []

    def _showwarning(message, category, filename, lineno, file=None, line=None):
        captured.append(
            warnings.WarningMessage(message, category, filename, lineno, file, line)
        )

    warnings.simplefilter("always")
    warnings.showwarning = _showwarning
    return captured


def main() -> int:
    captured = _capture_warnings()
    log.info("polars version: %s", pl.__version__)

    if not DB_PATH.exists():
        log.error("bc.db missing at %s", DB_PATH)
        return 1

    con = duckdb.connect(str(DB_PATH), read_only=True)

    # 1. duckdb -> arrow -> polars zero-copy
    arrow_tbl = con.sql(
        "SELECT * FROM main_models.player_team_season_offense_stats LIMIT 50000"
    ).arrow()
    df = pl.from_arrow(arrow_tbl)
    assert isinstance(df, pl.DataFrame)
    log.info("zero-copy from arrow: %s rows, %s cols", df.height, df.width)

    # 2. lazy + .over() (canonical pattern for axis-D)
    lf = df.lazy()
    if "season" in df.columns and "player_id" in df.columns:
        out = lf.select(
            pl.col("player_id"),
            pl.col("season"),
            pl.col("plate_appearances")
            .cum_sum()
            .over("player_id", order_by="season")
            .alias("career_pa"),
        ).collect()
        log.info(".over() lazy collect: %s rows", out.height)

    # 3. forward_fill (Spike 4 dependency)
    sample = pl.DataFrame(
        {
            "g": [1, 1, 1, 1, 1, 1],
            "i": [1, 2, 3, 4, 5, 6],
            "v": [None, "save", None, None, "blown", None],
        }
    )
    ff = sample.lazy().with_columns(
        pl.col("v").forward_fill().over("g", order_by="i").alias("v_fill")
    ).collect()
    assert ff["v_fill"].to_list() == [None, "save", "save", "save", "blown", "blown"]
    log.info("forward_fill().over() OK")

    # 4. streaming collect on a larger query
    n_rows = con.sql(
        "SELECT COUNT(*) AS n FROM main_models.event_offense_stats"
    ).fetchone()[0]
    log.info("event_offense_stats has %s rows", n_rows)

    # 5. summary
    by_cat = sorted({(w.category.__name__, str(w.message)[:120]) for w in captured})
    log.info("warnings captured: %s", len(by_cat))
    for cat, msg in by_cat[:20]:
        log.warning("  [%s] %s", cat, msg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
