"""End-to-end driver for the synthetic lineup optimizer.

Opens the local ``bc.db`` read-only and runs the optimizer + report
against existing dev/prod tables, so the optimizer can be iterated on
real data without re-running ``sqlmesh plan``. Mirrors the SQL in
``bc/models/synthetic_box_score/lineup_assignments.py`` and
``lineup_optimization_report.py``.

Reads the new stinted-appearances + new modal-lineups outputs from the
``main_models__dev`` schema (which the codex agent already materialized);
all other upstream tables come from ``main_models``.

Usage:

    uv run --group build python scripts/run_lineup_optimizer.py \
        --seasons 1900-1909 \
        [--db bc.db] \
        [--out-dir bc/.lineup_cache]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import duckdb
import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.synthetic_box_scores import (  # noqa: E402
    build_synthetic_lineup_assignments,
    build_synthetic_lineup_report_from_assignments,
    compute_modal_lineups,
)
from python_models.synthetic_box_scores.game_lineups import (  # noqa: E402
    CANDIDATE_INPUT_COLUMNS,
    GAME_INPUT_COLUMNS,
    LINEUP_INPUT_COLUMNS,
)

_LOG = logging.getLogger("run_lineup_optimizer")


_BUILD_LOCAL_APPEARANCES_SQL = """
CREATE OR REPLACE TEMP TABLE _local_appearances AS
WITH games_total AS (
    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        team_id,
        lg_id AS league_id,
        g_all AS games_total
    FROM baseballdatabank.appearances
),

fielding_source AS (
    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        team_id,
        lg_id AS league_id,
        stint,
        CASE pos
            WHEN 'P' THEN 1
            WHEN 'C' THEN 2
            WHEN '1B' THEN 3
            WHEN '2B' THEN 4
            WHEN '3B' THEN 5
            WHEN 'SS' THEN 6
        END::UTINYINT AS fielding_position,
        g AS games_at_position
    FROM baseballdatabank.fielding
    WHERE pos IN ('P', 'C', '1B', '2B', '3B', 'SS')
),

fielding_outfield AS (
    SELECT
        player_id,
        year_id,
        team_id,
        lg_id,
        stint,
        g AS stint_of_g,
        SUM(g) OVER (PARTITION BY player_id, year_id, team_id) AS team_of_g
    FROM baseballdatabank.fielding
    WHERE pos = 'OF'
),

outfield_source AS (
    SELECT
        fo.player_id AS databank_player_id,
        fo.year_id AS season,
        fo.team_id,
        fo.lg_id AS league_id,
        fo.stint,
        ROUND(a.g_lf::DOUBLE * fo.stint_of_g / NULLIF(fo.team_of_g, 0))::INTEGER AS games_left_field,
        ROUND(a.g_cf::DOUBLE * fo.stint_of_g / NULLIF(fo.team_of_g, 0))::INTEGER AS games_center_field,
        ROUND(a.g_rf::DOUBLE * fo.stint_of_g / NULLIF(fo.team_of_g, 0))::INTEGER AS games_right_field
    FROM fielding_outfield AS fo
    INNER JOIN baseballdatabank.appearances AS a
        ON fo.player_id = a.player_id
        AND fo.year_id = a.year_id
        AND fo.team_id = a.team_id
),

outfield_unpivoted AS (
    SELECT * FROM outfield_source
    UNPIVOT (
        games_at_position FOR position_label IN (
            games_left_field,
            games_center_field,
            games_right_field
        )
    )
),

outfield_with_position AS (
    SELECT
        databank_player_id,
        season,
        team_id,
        league_id,
        stint,
        CASE position_label
            WHEN 'games_left_field' THEN 7
            WHEN 'games_center_field' THEN 8
            WHEN 'games_right_field' THEN 9
        END::UTINYINT AS fielding_position,
        games_at_position
    FROM outfield_unpivoted
),

position_games AS (
    SELECT * FROM fielding_source
    UNION ALL
    SELECT * FROM outfield_with_position
),

non_pitcher_outs_played AS (
    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        team_id,
        stint,
        SUM(inn_outs)::INTEGER AS outs_played
    FROM baseballdatabank.fielding
    WHERE pos != 'P'
      AND inn_outs IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

SELECT
    people.retrosheet_player_id AS player_id,
    position_games.databank_player_id,
    position_games.season::SMALLINT AS season,
    position_games.stint::SMALLINT AS stint,
    position_games.team_id,
    COALESCE(games_total.league_id, position_games.league_id) AS league_id,
    position_games.fielding_position,
    position_games.games_at_position::USMALLINT AS games_at_position,
    games_total.games_total::USMALLINT AS games_total,
    COALESCE(outs.outs_played, 0)::INTEGER AS outs_played
FROM position_games
INNER JOIN main_models.stg_people AS people USING (databank_player_id)
LEFT JOIN games_total USING (databank_player_id, season, team_id)
LEFT JOIN non_pitcher_outs_played AS outs
    USING (databank_player_id, season, team_id, stint)
WHERE people.retrosheet_player_id IS NOT NULL
  AND position_games.games_at_position IS NOT NULL
  AND position_games.games_at_position > 0
  AND games_total.games_total IS NOT NULL
"""


_GAMES_SQL = """
SELECT
    g.game_id::VARCHAR AS game_id,
    g.date,
    g.season,
    g.use_dh,
    g.home_team_id::VARCHAR AS home_team_id,
    g.away_team_id::VARCHAR AS away_team_id,
    gl.home_starting_pitcher_id::VARCHAR AS home_starting_pitcher_id,
    gl.away_starting_pitcher_id::VARCHAR AS away_starting_pitcher_id
FROM {games} AS g
INNER JOIN {gamelog} AS gl USING (game_id)
{where}
"""

_LINEUPS_SQL = """
SELECT
    season,
    team_id::VARCHAR AS team_id,
    lineup_position,
    fielding_position,
    player_id::VARCHAR AS player_id
FROM {lineups}
{where}
"""

_CANDIDATES_SQL = """
WITH valid_team_seasons AS (
    SELECT DISTINCT season, away_team_id::VARCHAR AS team_id
    FROM {games}
    UNION
    SELECT DISTINCT season, home_team_id::VARCHAR AS team_id
    FROM {games}
),

appearances AS (
    SELECT
        a.season,
        a.team_id::VARCHAR AS team_id,
        a.player_id::VARCHAR AS player_id,
        a.stint,
        a.fielding_position,
        SUM(a.games_at_position)::INTEGER AS games_at_position,
        MAX(a.games_total)::INTEGER AS games_total,
        MAX(a.outs_played)::INTEGER AS outs_played
    FROM {appearances} AS a
    INNER JOIN valid_team_seasons AS valid USING (season, team_id)
    WHERE a.fielding_position BETWEEN 1 AND 9
      AND a.games_at_position > 0
    GROUP BY 1, 2, 3, 4, 5
),

batting AS (
    SELECT
        b.season,
        b.team_id::VARCHAR AS team_id,
        people.retrosheet_player_id::VARCHAR AS player_id,
        b.stint,
        SUM(COALESCE(b.plate_appearances, 0))::INTEGER AS plate_appearances,
        SUM(COALESCE(b.games, 0))::INTEGER AS games_played
    FROM {batting} AS b
    INNER JOIN {people} AS people USING (databank_player_id)
    INNER JOIN valid_team_seasons AS valid USING (season, team_id)
    WHERE people.retrosheet_player_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

SELECT
    a.season,
    a.team_id,
    a.player_id,
    a.stint,
    a.fielding_position,
    a.games_at_position,
    a.games_total,
    a.outs_played,
    COALESCE(b.plate_appearances, 0)::INTEGER AS plate_appearances,
    COALESCE(b.games_played, 0)::INTEGER AS games_played
FROM appearances AS a
LEFT JOIN batting AS b USING (season, team_id, player_id, stint)
"""


def _parse_seasons(spec: str | None) -> tuple[int, int] | None:
    if spec is None:
        return None
    parts = spec.replace("..", "-").split("-")
    if len(parts) == 1:
        year = int(parts[0])
        return (year, year)
    if len(parts) == 2:
        start, end = int(parts[0]), int(parts[1])
        if start > end:
            raise ValueError(f"invalid season range: {spec!r}")
        return (start, end)
    raise ValueError(f"unrecognized seasons spec: {spec!r}")


def _season_where(seasons: tuple[int, int] | None, alias: str) -> str:
    if seasons is None:
        return ""
    return f"WHERE {alias}.season BETWEEN {seasons[0]} AND {seasons[1]}"


def _filter_seasons(
    frame: pl.DataFrame, seasons: tuple[int, int] | None
) -> pl.DataFrame:
    if seasons is None or "season" not in frame.columns:
        return frame
    return frame.filter(pl.col("season").is_between(seasons[0], seasons[1]))


_LOCAL_MODAL_BATTING_SQL = """
SELECT
    b.season,
    b.team_id::VARCHAR AS team_id,
    people.retrosheet_player_id::VARCHAR AS player_id,
    SUM(COALESCE(b.plate_appearances, 0))::INTEGER AS plate_appearances,
    SUM(COALESCE(b.games, 0))::INTEGER AS games_played
FROM {batting} AS b
INNER JOIN {people} AS people USING (databank_player_id)
WHERE people.retrosheet_player_id IS NOT NULL
{where}
GROUP BY 1, 2, 3
"""


def _compute_local_modal_lineups(
    con: duckdb.DuckDBPyConnection,
    candidates: pl.DataFrame,
    args: argparse.Namespace,
    seasons: tuple[int, int] | None,
) -> pl.DataFrame:
    season_filter = ""
    if seasons is not None:
        season_filter = f"AND b.season BETWEEN {seasons[0]} AND {seasons[1]}"
    batting_sql = _LOCAL_MODAL_BATTING_SQL.format(
        batting=args.batting_table,
        people=args.people_table,
        where=season_filter,
    )
    batting = con.sql(batting_sql).pl()
    appearances = (
        candidates.group_by(["season", "team_id", "player_id", "fielding_position"])
        .agg(pl.sum("games_at_position").alias("games_at_position"))
        .with_columns(
            pl.col("season").cast(pl.Int16),
            pl.col("games_at_position").cast(pl.UInt32),
            pl.col("fielding_position").cast(pl.UInt8),
        )
    )
    batting = batting.with_columns(
        pl.col("season").cast(pl.Int16),
        pl.col("plate_appearances").cast(pl.UInt32),
        pl.col("games_played").cast(pl.UInt32),
    )
    return compute_modal_lineups(appearances, batting)


def _validate_input(name: str, frame: pl.DataFrame, expected: tuple[str, ...]) -> None:
    missing = set(expected) - set(frame.columns)
    if missing:
        raise RuntimeError(f"{name} missing columns: {sorted(missing)}")


_ABS_ERROR_BUCKETS: tuple[tuple[str, float, float], ...] = (
    ("exact", 0.0, 0.0),
    ("(0,1]", 0.0, 1.0),
    ("(1,2]", 1.0, 2.0),
    ("(2,5]", 2.0, 5.0),
    ("(5,10]", 5.0, 10.0),
    (">10", 10.0, float("inf")),
)


_PCT_ERROR_BUCKETS: tuple[tuple[str, float, float], ...] = (
    ("0%", 0.0, 0.0),
    ("(0,1%]", 0.0, 0.01),
    ("(1,5%]", 0.01, 0.05),
    ("(5,10%]", 0.05, 0.10),
    ("(10,25%]", 0.10, 0.25),
    (">25%", 0.25, float("inf")),
)


def _bucket_label(
    column: str, buckets: tuple[tuple[str, float, float], ...]
) -> pl.Expr:
    expr = pl.lit(buckets[-1][0])
    for label, low, high in reversed(buckets[:-1]):
        if low == high:
            cond = pl.col(column) == low
        else:
            cond = (pl.col(column) > low) & (pl.col(column) <= high)
        expr = pl.when(cond).then(pl.lit(label)).otherwise(expr)
    return expr


def _bucket_order(
    buckets: tuple[tuple[str, float, float], ...],
) -> dict[str, int]:
    return {label: index for index, (label, _, _) in enumerate(buckets)}


def _format_frame(frame: pl.DataFrame) -> str:
    with pl.Config(
        tbl_rows=200,
        tbl_cols=20,
        tbl_width_chars=160,
        fmt_str_lengths=40,
    ):
        return repr(frame)


def _quality_report(
    report: pl.DataFrame,
    *,
    games: pl.DataFrame,
    assignments: pl.DataFrame,
    candidates: pl.DataFrame,
    runtime_seconds: float,
    out_dir: Path,
) -> None:
    sections: list[tuple[str, pl.DataFrame]] = []

    headline = _headline_metrics(
        report=report,
        games=games,
        assignments=assignments,
        candidates=candidates,
        runtime_seconds=runtime_seconds,
    )
    sections.append(("headline", headline))

    if report.is_empty():
        for name, frame in sections:
            _LOG.info("%s:\n%s", name, _format_frame(frame))
        return

    sections.append(("by metric_type", _by_metric(report)))
    sections.append(("abs_error distribution", _abs_error_distribution(report)))
    sections.append(("pct_error distribution", _pct_error_distribution(report)))
    sections.append(("signed bias", _signed_bias(report)))
    sections.append(("by season", _by_season(report)))
    sections.append(("by fielding_position", _by_fielding_position(report)))
    sections.append(
        ("worst 15 team-seasons by total_abs_error", _worst_team_seasons(report))
    )
    sections.append(("top 25 rows by abs_error", _worst_rows(report)))
    sections.append(("missed candidates (target>0, realized=0)", _missed(report)))

    for name, frame in sections:
        _LOG.info("%s:\n%s", name, _format_frame(frame))

    summary_path = out_dir / "report_summary.md"
    _write_markdown(summary_path, sections)
    _LOG.info("wrote %s", summary_path)


def _headline_metrics(
    *,
    report: pl.DataFrame,
    games: pl.DataFrame,
    assignments: pl.DataFrame,
    candidates: pl.DataFrame,
    runtime_seconds: float,
) -> pl.DataFrame:
    expected_assignments = games.shape[0] * 2 * 9
    expected_non_pitcher = games.shape[0] * 2 * 8
    non_pitcher = assignments.filter(
        pl.col("fielding_position").is_in([2, 3, 4, 5, 6, 7, 8, 9])
    )
    pitcher = assignments.filter(pl.col("fielding_position") == 1)

    candidate_targets = candidates.filter(
        pl.col("fielding_position").is_in([2, 3, 4, 5, 6, 7, 8, 9])
        & (pl.col("games_at_position") > 0)
    )
    target_player_stints = candidate_targets.select(
        ["season", "team_id", "player_id", "stint"]
    ).unique()
    realized_player_stints = non_pitcher.select(
        ["season", "team_id", "player_id", "stint"]
    ).unique()
    placed = target_player_stints.join(
        realized_player_stints,
        on=["season", "team_id", "player_id", "stint"],
        how="inner",
    )

    rows: list[dict[str, object]] = [
        {"metric": "runtime_seconds", "value": round(runtime_seconds, 1)},
        {"metric": "games_in_scope", "value": games.shape[0]},
        {"metric": "game_sides", "value": games.shape[0] * 2},
        {
            "metric": "assignments_total",
            "value": assignments.shape[0],
        },
        {
            "metric": "assignments_expected",
            "value": expected_assignments,
        },
        {
            "metric": "non_pitcher_assignments",
            "value": non_pitcher.shape[0],
        },
        {
            "metric": "non_pitcher_assignments_expected",
            "value": expected_non_pitcher,
        },
        {"metric": "pitcher_assignments", "value": pitcher.shape[0]},
        {
            "metric": "candidate_player_stints_with_target",
            "value": target_player_stints.shape[0],
        },
        {
            "metric": "candidate_player_stints_placed",
            "value": placed.shape[0],
        },
        {
            "metric": "candidate_player_stints_unplaced",
            "value": target_player_stints.shape[0] - placed.shape[0],
        },
        {"metric": "report_rows", "value": report.shape[0]},
        {
            "metric": "exact_match_rows",
            "value": (
                int(report.filter(pl.col("abs_error") == 0).shape[0])
                if not report.is_empty()
                else 0
            ),
        },
    ]
    return pl.DataFrame(rows)


def _by_metric(report: pl.DataFrame) -> pl.DataFrame:
    return (
        report.group_by("metric_type")
        .agg(
            pl.len().alias("rows"),
            pl.col("abs_error").sum().alias("total_abs_error"),
            pl.col("abs_error").mean().round(3).alias("mean_abs_error"),
            pl.col("abs_error").max().alias("max_abs_error"),
            pl.col("pct_error").mean().round(4).alias("mean_pct_error"),
            (pl.col("abs_error") == 0).sum().alias("exact_rows"),
        )
        .sort("metric_type")
    )


def _abs_error_distribution(report: pl.DataFrame) -> pl.DataFrame:
    order = _bucket_order(_ABS_ERROR_BUCKETS)
    return (
        report.with_columns(
            _bucket_label("abs_error", _ABS_ERROR_BUCKETS).alias("bucket")
        )
        .group_by(["metric_type", "bucket"])
        .agg(pl.len().alias("rows"))
        .with_columns(
            pl.col("bucket")
            .replace_strict(order, return_dtype=pl.Int8)
            .alias("bucket_rank")
        )
        .sort(["metric_type", "bucket_rank"])
        .drop("bucket_rank")
    )


def _pct_error_distribution(report: pl.DataFrame) -> pl.DataFrame:
    order = _bucket_order(_PCT_ERROR_BUCKETS)
    return (
        report.with_columns(
            _bucket_label("pct_error", _PCT_ERROR_BUCKETS).alias("bucket")
        )
        .group_by(["metric_type", "bucket"])
        .agg(pl.len().alias("rows"))
        .with_columns(
            pl.col("bucket")
            .replace_strict(order, return_dtype=pl.Int8)
            .alias("bucket_rank")
        )
        .sort(["metric_type", "bucket_rank"])
        .drop("bucket_rank")
    )


def _signed_bias(report: pl.DataFrame) -> pl.DataFrame:
    return (
        report.group_by("metric_type")
        .agg(
            pl.col("signed_error").sum().round(3).alias("total_signed_error"),
            pl.col("signed_error").mean().round(3).alias("mean_signed_error"),
            (pl.col("signed_error") > 0).sum().alias("over_assigned_rows"),
            (pl.col("signed_error") < 0).sum().alias("under_assigned_rows"),
            (pl.col("signed_error") == 0).sum().alias("exact_rows"),
        )
        .sort("metric_type")
    )


def _by_season(report: pl.DataFrame) -> pl.DataFrame:
    return (
        report.group_by(["season", "metric_type"])
        .agg(
            pl.len().alias("rows"),
            pl.col("abs_error").sum().round(2).alias("total_abs_error"),
            pl.col("abs_error").mean().round(3).alias("mean_abs_error"),
            pl.col("abs_error").max().alias("max_abs_error"),
            pl.col("pct_error").mean().round(4).alias("mean_pct_error"),
            (pl.col("abs_error") == 0).sum().alias("exact_rows"),
        )
        .sort(["season", "metric_type"])
    )


def _by_fielding_position(report: pl.DataFrame) -> pl.DataFrame:
    return (
        report.filter(pl.col("metric_type") == "Position")
        .group_by("fielding_position")
        .agg(
            pl.len().alias("rows"),
            pl.col("abs_error").sum().round(2).alias("total_abs_error"),
            pl.col("abs_error").mean().round(3).alias("mean_abs_error"),
            pl.col("abs_error").max().alias("max_abs_error"),
            pl.col("pct_error").mean().round(4).alias("mean_pct_error"),
            (pl.col("abs_error") == 0).sum().alias("exact_rows"),
        )
        .sort("fielding_position")
    )


def _worst_team_seasons(report: pl.DataFrame) -> pl.DataFrame:
    return (
        report.group_by(["season", "team_id"])
        .agg(
            pl.len().alias("rows"),
            pl.col("abs_error").sum().round(2).alias("total_abs_error"),
            pl.col("abs_error").max().alias("max_abs_error"),
            pl.col("pct_error").mean().round(4).alias("mean_pct_error"),
        )
        .sort("total_abs_error", descending=True)
        .head(15)
    )


def _worst_rows(report: pl.DataFrame) -> pl.DataFrame:
    return (
        report.sort(
            ["abs_error", "pct_error"],
            descending=[True, True],
            nulls_last=True,
        )
        .head(25)
        .select(
            "season",
            "team_id",
            "player_id",
            "stint",
            "metric_type",
            "fielding_position",
            "actual_games",
            "realized_games",
            "signed_error",
            "abs_error",
            "pct_error",
        )
    )


def _missed(report: pl.DataFrame) -> pl.DataFrame:
    return (
        report.filter(
            (pl.col("metric_type") == "Total")
            & (pl.col("realized_games") == 0)
            & (pl.col("actual_games") > 0)
        )
        .sort("actual_games", descending=True)
        .head(25)
        .select(
            "season",
            "team_id",
            "player_id",
            "stint",
            "actual_games",
            "abs_error",
            "pct_error",
        )
    )


def _write_markdown(path: Path, sections: list[tuple[str, pl.DataFrame]]) -> None:
    lines: list[str] = ["# Synthetic lineup optimizer accuracy report", ""]
    for name, frame in sections:
        lines.append(f"## {name}")
        lines.append("")
        lines.append("```")
        lines.append(_format_frame(frame))
        lines.append("```")
        lines.append("")
    path.write_text("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--seasons", default=None)
    _ = parser.add_argument(
        "--db",
        default=str(REPO_ROOT / "bc.db"),
        help="Path to bc.db (default: ./bc.db).",
    )
    _ = parser.add_argument(
        "--games-table",
        default="synthetic_box_score__dev.box_score_games",
    )
    _ = parser.add_argument(
        "--gamelog-table",
        default="main_models.stg_gamelog",
    )
    _ = parser.add_argument(
        "--lineups-table",
        default="_local_modal_lineups",
        help=(
            "Source for modal lineups. Default '_local_modal_lineups' "
            "computes them inline from local appearances + batting via "
            "compute_modal_lineups so the runner reflects this branch's "
            "modal-lineup logic. Override to read a materialized table."
        ),
    )
    _ = parser.add_argument(
        "--appearances-table",
        default="_local_appearances",
        help=(
            "Source for stinted appearances. Default '_local_appearances' "
            "rebuilds the table in a TEMP table from baseballdatabank.* "
            "to match the shape on this branch. Override to read a "
            "materialized table from bc.db."
        ),
    )
    _ = parser.add_argument(
        "--batting-table",
        default="main_models.stg_databank_batting",
    )
    _ = parser.add_argument(
        "--people-table",
        default="main_models.stg_people",
    )
    _ = parser.add_argument(
        "--out-dir",
        default=str(REPO_ROOT / "bc" / ".lineup_cache"),
    )
    _ = parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=str(args.log_level).upper(),
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    seasons = _parse_seasons(args.seasons)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"bc.db not found at {db_path}")
    _LOG.info("opening %s read-only", db_path)
    con = duckdb.connect(str(db_path), read_only=True)

    if args.appearances_table == "_local_appearances":
        _LOG.info(
            "rebuilding stinted appearances locally (TEMP table, no writes to bc.db)"
        )
        started_apps = time.monotonic()
        _ = con.sql(_BUILD_LOCAL_APPEARANCES_SQL)
        rows = con.sql("SELECT COUNT(*) FROM _local_appearances").fetchone()
        local_count = 0 if rows is None else int(rows[0])
        _LOG.info(
            "_local_appearances: %d rows in %.1fs",
            local_count,
            time.monotonic() - started_apps,
        )

    games_where = _season_where(seasons, "g")
    lineups_where = ""
    if seasons:
        lineups_where = f"WHERE season BETWEEN {seasons[0]} AND {seasons[1]}"
    games_sql = _GAMES_SQL.format(
        games=args.games_table,
        gamelog=args.gamelog_table,
        where=games_where,
    )
    lineups_sql = _LINEUPS_SQL.format(
        lineups=args.lineups_table,
        where=lineups_where,
    )
    candidates_sql = _CANDIDATES_SQL.format(
        games=args.games_table,
        appearances=args.appearances_table,
        batting=args.batting_table,
        people=args.people_table,
    )

    started = time.monotonic()
    games: pl.DataFrame = con.sql(games_sql).pl()
    candidates: pl.DataFrame = con.sql(candidates_sql).pl()
    candidates = _filter_seasons(candidates, seasons)
    if args.lineups_table == "_local_modal_lineups":
        lineups = _compute_local_modal_lineups(con, candidates, args, seasons)
    else:
        lineups = con.sql(lineups_sql).pl()
    _LOG.info(
        "loaded inputs in %.1fs: games=%d lineups=%d candidates=%d",
        time.monotonic() - started,
        games.shape[0],
        lineups.shape[0],
        candidates.shape[0],
    )

    _validate_input("games", games, GAME_INPUT_COLUMNS)
    _validate_input("lineups", lineups, LINEUP_INPUT_COLUMNS)
    _validate_input("candidates", candidates, CANDIDATE_INPUT_COLUMNS)

    started = time.monotonic()
    assignments = build_synthetic_lineup_assignments(games, lineups, candidates)
    runtime = time.monotonic() - started
    _LOG.info(
        "optimizer produced %d rows in %.1fs",
        assignments.shape[0],
        runtime,
    )

    report = build_synthetic_lineup_report_from_assignments(assignments, candidates)

    assignments_path = out_dir / "assignments.parquet"
    report_path = out_dir / "report.parquet"
    assignments.write_parquet(assignments_path)
    report.write_parquet(report_path)
    _LOG.info("wrote %s (%d rows)", assignments_path, assignments.shape[0])
    _LOG.info("wrote %s (%d rows)", report_path, report.shape[0])

    _quality_report(
        report,
        games=games,
        assignments=assignments,
        candidates=candidates,
        runtime_seconds=runtime,
        out_dir=out_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
