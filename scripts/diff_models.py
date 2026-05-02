"""Phase 2 row-by-row diff harness.

Compares main_models.<table> (prod) vs main_models__dev.<table> (dev) for
the 17 tables touched in Phase 2. Per-column tolerances:

    int counters         -> 0 (byte-exact)
    rate / percentage    -> 1e-9 absolute
    *_park_factor        -> 1e-2 absolute  (ROUND-2 at source)
    sqrt_sample_size     -> 1                (ROUND-0 at source)

Drift bucketing:

    clean        - no drift                         exit 0
    known-flaky  - drift is in scripts/diff_known_flaky.json   exit 0 (warning)
    BLOCKING     - any other drift                  exit 1

Capture mode (--capture-baseline) writes the *current* drift set to
scripts/diff_known_flaky.json. Run on the *unchanged* Phase 1.6 codebase
to seed the allowlist before Phase 2 work begins.

Usage:

    uv run --group migration python scripts/diff_models.py
    uv run --group migration python scripts/diff_models.py --capture-baseline
    uv run --group migration python scripts/diff_models.py --models metrics_player_career_offense
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "bc.db"
BASELINE_PATH = REPO_ROOT / "scripts" / "diff_known_flaky.json"

PROD_SCHEMA = "main_models"
DEV_SCHEMA = "main_models__dev"

PHASE_2_MODELS: list[str] = [
    "metrics_player_career_offense",
    "metrics_player_career_pitching",
    "metrics_player_career_fielding",
    "metrics_player_season_league_offense",
    "metrics_player_season_league_pitching",
    "metrics_player_season_league_fielding",
    "metrics_team_season_offense",
    "metrics_team_season_pitching",
    "metrics_team_season_fielding",
    "calc_park_factors_advanced",
    "calc_park_factors_basic",
    "calc_park_factor_in_play",
    "calc_park_factor_outs",
    "calc_park_factor_plate_appearances",
    "calc_park_factor_trajectory_outs",
    "calc_park_factor_hit_location",
    "calc_park_factor_out_location",
]

GRAINS: dict[str, list[str]] = {
    "metrics_player_career_offense": ["player_id"],
    "metrics_player_career_pitching": ["player_id"],
    "metrics_player_career_fielding": ["player_id"],
    "metrics_player_season_league_offense": ["player_id", "season", "league"],
    "metrics_player_season_league_pitching": ["player_id", "season", "league"],
    "metrics_player_season_league_fielding": ["player_id", "season", "league"],
    "metrics_team_season_offense": ["team_id", "season"],
    "metrics_team_season_pitching": ["team_id", "season"],
    "metrics_team_season_fielding": ["team_id", "season"],
    "calc_park_factors_advanced": ["park_id", "season", "league"],
    "calc_park_factors_basic": ["park_id", "season", "league"],
    "calc_park_factor_in_play": ["park_id", "season", "league"],
    "calc_park_factor_outs": ["park_id", "season", "league"],
    "calc_park_factor_plate_appearances": ["park_id", "season", "league"],
    "calc_park_factor_trajectory_outs": ["park_id", "season", "league"],
    "calc_park_factor_hit_location": ["park_id", "season", "league", "batter_hand"],
    "calc_park_factor_out_location": ["park_id", "season", "league", "batter_hand"],
}

INT_TOL = 0.0
RATE_TOL = 1e-9
PARK_FACTOR_TOL = 1e-2
SQRT_SAMPLE_TOL = 1.0

log = logging.getLogger("diff_models")


@dataclass
class ColumnDrift:
    column: str
    rows_diff: int
    max_abs: float
    examples: list[tuple[Any, ...]] = field(default_factory=list)


@dataclass
class ModelDrift:
    model: str
    prod_row_count: int
    dev_row_count: int
    columns: list[ColumnDrift] = field(default_factory=list)
    missing_in_dev: int = 0
    missing_in_prod: int = 0


def column_tolerance(table: str, column: str, sql_type: str) -> float:
    if column == "sqrt_sample_size":
        return SQRT_SAMPLE_TOL
    if column.endswith("_park_factor"):
        return PARK_FACTOR_TOL
    sql_type_upper = sql_type.upper()
    if (
        "INT" in sql_type_upper
        or "SMALLINT" in sql_type_upper
        or "BIGINT" in sql_type_upper
    ):
        return INT_TOL
    return RATE_TOL


def _connect(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def _scalar_int(row: tuple[Any, ...] | None) -> int:
    if row is None:
        return 0
    return int(cast(int, row[0]))


def _columns(
    con: duckdb.DuckDBPyConnection, schema: str, table: str
) -> list[tuple[str, str]]:
    rows = con.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    ).fetchall()
    return [(name, dtype) for name, dtype in rows]


def _quote(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _qualify(schema: str, table: str) -> str:
    return f"{_quote(schema)}.{_quote(table)}"


def _diff_one_model(
    con: duckdb.DuckDBPyConnection,
    table: str,
    grain: list[str],
) -> ModelDrift | None:
    prod_cols = _columns(con, PROD_SCHEMA, table)
    dev_cols = _columns(con, DEV_SCHEMA, table)
    if not prod_cols:
        # Some models (e.g. the calc_park_factor_* analyses) are new
        # views in Phase 2 with no prod-side counterpart. Skip them.
        return None
    if not dev_cols:
        raise RuntimeError(f"dev table {DEV_SCHEMA}.{table} not found")
    prod_col_set = {c for c, _ in prod_cols}
    dev_col_set = {c for c, _ in dev_cols}
    if prod_col_set != dev_col_set:
        only_prod = prod_col_set - dev_col_set
        only_dev = dev_col_set - prod_col_set
        raise RuntimeError(
            f"column-set mismatch on {table}: only_prod={sorted(only_prod)} only_dev={sorted(only_dev)}"
        )
    prod_q = _qualify(PROD_SCHEMA, table)
    dev_q = _qualify(DEV_SCHEMA, table)
    prod_rows = _scalar_int(con.execute(f"SELECT COUNT(*) FROM {prod_q}").fetchone())
    dev_rows = _scalar_int(con.execute(f"SELECT COUNT(*) FROM {dev_q}").fetchone())
    drift = ModelDrift(model=table, prod_row_count=prod_rows, dev_row_count=dev_rows)

    grain_cols_csv = ", ".join(_quote(c) for c in grain)
    drift.missing_in_dev = _scalar_int(
        con.execute(
            f"""
            SELECT COUNT(*)
            FROM {prod_q} AS p
            ANTI JOIN {dev_q} AS d USING ({grain_cols_csv})
            """
        ).fetchone()
    )
    drift.missing_in_prod = _scalar_int(
        con.execute(
            f"""
            SELECT COUNT(*)
            FROM {dev_q} AS d
            ANTI JOIN {prod_q} AS p USING ({grain_cols_csv})
            """
        ).fetchone()
    )

    grain_set = set(grain)
    for col, dtype in prod_cols:
        if col in grain_set:
            continue
        tol = column_tolerance(table, col, dtype)
        col_q = _quote(col)
        if tol == 0.0:
            mismatch_clause = f"p.{col_q} IS DISTINCT FROM d.{col_q}"
            max_clause = f"MAX(ABS(COALESCE(p.{col_q}, 0) - COALESCE(d.{col_q}, 0)))"
        else:
            # IS DISTINCT FROM correctly treats NaN/NaN and inf/inf as
            # equal. Tolerance only applies to finite-vs-finite pairs;
            # for NULL/NaN/inf differences, trust IS DISTINCT FROM.
            distinct = f"p.{col_q} IS DISTINCT FROM d.{col_q}"
            non_finite = (
                f"(p.{col_q} IS NULL OR d.{col_q} IS NULL "
                f"OR isnan(p.{col_q}) OR isnan(d.{col_q}) "
                f"OR isinf(p.{col_q}) OR isinf(d.{col_q}))"
            )
            mismatch_clause = (
                f"({distinct} "
                f"AND ({non_finite} OR ABS(p.{col_q} - d.{col_q}) > {tol}))"
            )
            # Only finite-vs-finite contributes to max_abs; non-finite
            # mismatches still count in rows_diff.
            both_finite = (
                f"p.{col_q} IS NOT NULL AND d.{col_q} IS NOT NULL "
                f"AND NOT isnan(p.{col_q}) AND NOT isnan(d.{col_q}) "
                f"AND NOT isinf(p.{col_q}) AND NOT isinf(d.{col_q})"
            )
            max_clause = (
                f"MAX(CASE WHEN {both_finite} "
                f"THEN ABS(p.{col_q} - d.{col_q}) ELSE 0 END)"
            )
        sql = f"""
            SELECT
                COUNT(*) FILTER (WHERE {mismatch_clause}) AS rows_diff,
                COALESCE({max_clause} FILTER (WHERE {mismatch_clause}), 0) AS max_abs
            FROM {prod_q} AS p
            INNER JOIN {dev_q} AS d USING ({grain_cols_csv})
        """
        row = con.execute(sql).fetchone()
        if row is None:
            continue
        rows_diff = int(cast(int, row[0]))
        max_abs = float(cast(float, row[1] or 0.0))
        if rows_diff > 0:
            ex_sql = f"""
                SELECT {grain_cols_csv}, p.{col_q} AS prod_val, d.{col_q} AS dev_val
                FROM {prod_q} AS p
                INNER JOIN {dev_q} AS d USING ({grain_cols_csv})
                WHERE {mismatch_clause}
                LIMIT 3
            """
            examples = [tuple(r) for r in con.execute(ex_sql).fetchall()]
            drift.columns.append(
                ColumnDrift(
                    column=col, rows_diff=rows_diff, max_abs=max_abs, examples=examples
                )
            )
    return drift


def _load_baseline() -> set[tuple[str, str]]:
    if not BASELINE_PATH.exists():
        return set()
    data = json.loads(BASELINE_PATH.read_text())
    return {(row["model"], row["column"]) for row in data["pairs"]}


def _write_baseline(pairs: Iterable[tuple[str, str]]) -> None:
    payload = {
        "comment": (
            "Phase 1.6 baseline — captured pre-Phase-2 to distinguish carried-over "
            "nondeterminism from Phase-2-introduced drift. See notes/phase-1-followups.md."
        ),
        "pairs": [{"model": m, "column": c} for m, c in sorted(pairs)],
    }
    BASELINE_PATH.write_text(json.dumps(payload, indent=2) + "\n")


def _format_example(ex: tuple[Any, ...], grain: list[str]) -> str:
    grain_kv = ", ".join(f"{g}={v!r}" for g, v in zip(grain, ex[:-2]))
    prod_val, dev_val = ex[-2], ex[-1]
    return f"{grain_kv} prod={prod_val} dev={dev_val}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models", nargs="*", default=None, help="subset of models")
    parser.add_argument(
        "--capture-baseline",
        action="store_true",
        help="overwrite scripts/diff_known_flaky.json with current drift set",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    targets = args.models or PHASE_2_MODELS
    unknown = [m for m in targets if m not in GRAINS]
    if unknown:
        log.error("unknown model(s): %s", unknown)
        return 2

    baseline = set() if args.capture_baseline else _load_baseline()
    log.info(
        "baseline: %d (model, column) pair(s) %s",
        len(baseline),
        f"from {BASELINE_PATH.name}" if baseline else "(empty)",
    )

    con = _connect(read_only=True)
    drifts: list[ModelDrift] = []
    captured: set[tuple[str, str]] = set()
    blocking: list[tuple[str, ColumnDrift]] = []

    for table in targets:
        log.info("diffing %s...", table)
        try:
            drift = _diff_one_model(con, table, GRAINS[table])
        except Exception:
            log.exception("diff failed for %s", table)
            return 3
        if drift is None:
            log.info("  SKIP %s (no prod counterpart — new view)", table)
            continue
        drifts.append(drift)
        if drift.prod_row_count != drift.dev_row_count:
            log.warning(
                "%s: row count differs prod=%d dev=%d missing_in_dev=%d missing_in_prod=%d",
                table,
                drift.prod_row_count,
                drift.dev_row_count,
                drift.missing_in_dev,
                drift.missing_in_prod,
            )
        for col_drift in drift.columns:
            captured.add((table, col_drift.column))
            tol_label = (
                "exact"
                if column_tolerance(table, col_drift.column, "INTEGER") == 0
                else f"tol>{_short(column_tolerance(table, col_drift.column, 'DOUBLE'))}"
            )
            severity = "FLAKY" if (table, col_drift.column) in baseline else "BLOCKING"
            log.info(
                "  %-13s %-40s rows_diff=%d max_abs=%.6g (%s)",
                severity,
                col_drift.column,
                col_drift.rows_diff,
                col_drift.max_abs,
                tol_label,
            )
            for ex in col_drift.examples:
                log.debug("    e.g. %s", _format_example(ex, GRAINS[table]))
            if severity == "BLOCKING":
                blocking.append((table, col_drift))

    if args.capture_baseline:
        _write_baseline(captured)
        log.info(
            "wrote baseline: %d (model, column) pair(s) -> %s",
            len(captured),
            BASELINE_PATH,
        )
        return 0

    flaky_seen = {
        (d.model, c.column)
        for d in drifts
        for c in d.columns
        if (d.model, c.column) in baseline
    }
    blocking_pairs = sorted({(t, cd.column) for t, cd in blocking})

    log.info("---- summary ----")
    log.info("clean models      : %d", sum(1 for d in drifts if not d.columns))
    log.info("flaky col drifts  : %d (allowlisted)", len(flaky_seen))
    log.info("BLOCKING drifts   : %d", len(blocking_pairs))
    if blocking_pairs:
        for t, c in blocking_pairs:
            log.error("BLOCKING %s.%s", t, c)
        return 1
    return 0


def _short(x: float) -> str:
    if x == 0:
        return "0"
    if x >= 1:
        return f"{x:g}"
    if x < 1e-3:
        return f"{x:.0e}"
    return f"{x:g}"


if __name__ == "__main__":
    sys.exit(main())
