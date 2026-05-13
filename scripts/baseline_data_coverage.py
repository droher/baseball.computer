"""Phase 0 baseline: snapshot row counts for data-coverage planning.

Read-only DuckDB script. Records each query, its result, the DB path,
the source snapshot ID, git SHA, env, and timestamp. Output JSON lives
under ``artifacts/statistical/baseline/`` so later validation reports
can reference a stable baseline ID.

Imports stdlib + duckdb + the new statistical package's ``duckdb_io``
helpers; never imports PyMC, Keras, Torch, or MLflow.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from python_models.statistical.config import BASELINE_ROOT, resolve_db_path
from python_models.statistical.logging import configure as configure_logging
from python_models.statistical.manifests import new_artifact_id

_log = logging.getLogger("baseline_data_coverage")


SCALAR_QUERIES: dict[str, str] = {
    "event_states_full_event_count": "SELECT COUNT(*) FROM main_models.event_states_full",
    "event_states_full_game_count": "SELECT COUNT(DISTINCT game_id) FROM main_models.event_states_full",
    "calc_batted_ball_row_count": "SELECT COUNT(*) FROM main_models.calc_batted_ball_type",
    "calc_batted_ball_unknown_final_trajectory": (
        "SELECT COUNT(*) FROM main_models.calc_batted_ball_type WHERE trajectory IS NULL"
    ),
    "calc_batted_ball_unknown_recorded_trajectory": (
        "SELECT COUNT(*) FROM main_models.calc_batted_ball_type WHERE recorded_trajectory IS NULL"
    ),
    "calc_batted_ball_unknown_recorded_location": (
        "SELECT COUNT(*) FROM main_models.calc_batted_ball_type WHERE recorded_location IS NULL"
    ),
    "calc_fielding_play_unknown_putouts_total": (
        "SELECT COALESCE(SUM(unknown_putouts), 0) FROM main_models.calc_fielding_play_agg"
    ),
    "calc_fielding_play_incomplete_events": (
        "SELECT COALESCE(SUM(incomplete_events), 0) FROM main_models.calc_fielding_play_agg"
    ),
    "player_position_game_residual_pos_putouts": (
        "SELECT COUNT(*) FROM main_models.player_position_game_fielding_stats WHERE surplus_box_putouts > 0"
    ),
    "player_position_game_residual_neg_putouts": (
        "SELECT COUNT(*) FROM main_models.player_position_game_fielding_stats WHERE surplus_box_putouts < 0"
    ),
    "player_position_game_residual_pos_assists": (
        "SELECT COUNT(*) FROM main_models.player_position_game_fielding_stats WHERE surplus_box_assists > 0"
    ),
    "player_position_game_residual_neg_assists": (
        "SELECT COUNT(*) FROM main_models.player_position_game_fielding_stats WHERE surplus_box_assists < 0"
    ),
    "unknown_fielding_play_shares_row_count": (
        "SELECT COUNT(*) FROM main_models.unknown_fielding_play_shares"
    ),
    "park_factors_row_count": "SELECT COUNT(*) FROM main_models.park_factors",
    "calc_park_factors_basic_row_count": "SELECT COUNT(*) FROM main_models.calc_park_factors_basic",
    "calc_park_factors_advanced_row_count": "SELECT COUNT(*) FROM main_models.calc_park_factors_advanced",
    "linear_weights_row_count": "SELECT COUNT(*) FROM main_models.linear_weights",
    "season_team_coverage_row_count": "SELECT COUNT(*) FROM main_models.season_team_coverage",
    "season_team_coverage_min_season": "SELECT MIN(season) FROM main_models.season_team_coverage",
    "season_team_coverage_max_season": "SELECT MAX(season) FROM main_models.season_team_coverage",
}

GROUPED_QUERIES: dict[str, str] = {
    "game_start_info_count_by_source_type": (
        "SELECT source_type, COUNT(*) AS game_count "
        "FROM main_models.game_start_info "
        "GROUP BY 1 ORDER BY source_type"
    ),
    "season_team_coverage_by_least_granular_source_type": (
        "SELECT least_granular_source_type, COUNT(*) AS team_season_count "
        "FROM main_models.season_team_coverage "
        "GROUP BY 1 ORDER BY least_granular_source_type"
    ),
    "linear_weights_sparse_cell_count_per_season": (
        "SELECT season, COUNT(*) AS row_count "
        "FROM main_models.linear_weights "
        "GROUP BY 1 ORDER BY season"
    ),
}


def _git_sha_short() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"
    return result.stdout.strip()


def _git_branch() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"
    return result.stdout.strip()


def _scalar(con: duckdb.DuckDBPyConnection, query: str) -> int | float | str | None:
    row = con.execute(query).fetchone()
    if row is None:
        return None
    value = row[0]
    if hasattr(value, "item"):
        value = value.item()
    return value


def _rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    cursor = con.execute(query)
    columns = [d[0] for d in cursor.description]
    out: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        record: dict[str, Any] = {}
        for col, val in zip(columns, row):
            if hasattr(val, "item"):
                val = val.item()
            record[col] = val
        out.append(record)
    return out


def _table_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    row = con.execute(
        "SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?",
        [schema, name],
    ).fetchone()
    return bool(row and row[0])


def _safe_run(label: str, con: duckdb.DuckDBPyConnection, query: str, *, scalar: bool) -> dict[str, Any]:
    try:
        result = _scalar(con, query) if scalar else _rows(con, query)
        return {"query": query, "result": result, "error": None}
    except duckdb.Error as exc:
        _log.warning("query %s failed: %s", label, exc)
        return {"query": query, "result": None, "error": str(exc)}


def collect_baseline(db_path: Path) -> dict[str, Any]:
    out: dict[str, Any] = {"scalar_queries": {}, "grouped_queries": {}}
    with duckdb.connect(str(db_path), read_only=True) as con:
        for label, q in SCALAR_QUERIES.items():
            out["scalar_queries"][label] = _safe_run(label, con, q, scalar=True)
        for label, q in GROUPED_QUERIES.items():
            out["grouped_queries"][label] = _safe_run(label, con, q, scalar=False)
    return out


def build_report(*, output_dir: Path) -> Path:
    db_path = resolve_db_path()
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found at {db_path}")
    _log.info("baseline_start db_path=%s", db_path)
    sha = _git_sha_short()
    branch = _git_branch()
    timestamp = datetime.now(tz=timezone.utc)
    iso_date = timestamp.strftime("%Y%m%d")
    baseline_id = new_artifact_id()
    queries = collect_baseline(db_path)
    payload: dict[str, Any] = {
        "baseline_id": baseline_id,
        "generated_at": timestamp.isoformat(),
        "db_path": str(db_path),
        "git_sha_short": sha,
        "git_branch": branch,
        "env": {
            "BC_DB_PATH": os.environ.get("BC_DB_PATH"),
            "BC_STATE_DB_PATH": os.environ.get("BC_STATE_DB_PATH"),
            "BC_STATS_PUBLISHED_ROOT": os.environ.get("BC_STATS_PUBLISHED_ROOT"),
        },
        "queries": queries,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"baseline_{iso_date}_{sha}.json"
    out_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    _log.info("baseline_done path=%s baseline_id=%s", out_path, baseline_id)
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Phase 0 data-coverage baseline.")
    _ = parser.add_argument(
        "--output-dir",
        default=str(BASELINE_ROOT),
        help="Output directory (default: artifacts/statistical/baseline).",
    )
    _ = parser.add_argument("--log-level", default="INFO", help="stdlib logging level name.")
    args = parser.parse_args(argv)
    configure_logging(getattr(logging, args.log_level.upper(), logging.INFO))
    path = build_report(output_dir=Path(args.output_dir))
    print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
