"""Row-level parity checks: existing completeness model vs ledger reproduction.

For each pair, runs ``EXCEPT`` in both directions on a shared (key, flags)
projection and reports how many rows fall out on each side. Used as the
Phase-1 exit-gate "completeness models can be reproduced from ledgers"
audit (item 250) — a nonzero diff means either the ledger reproduction
needs work, the existing view drifted, or the new ledger is canonical and
the existing view is stale.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bc"))

from python_models.statistical.config import resolve_db_path
from python_models.statistical.logging import configure as configure_logging

_log = logging.getLogger("rollup_parity_checks")

ENV_LEDGER_SCHEMA: str = "BC_LEDGER_SCHEMA"
DEFAULT_LEDGER_SCHEMA: str = "main_models"

LEDGER_TABLES: frozenset[str] = frozenset(
    {
        "source_acquisition_ledger",
        "source_data_error_risk_ledger",
        "official_aggregate_availability",
        "personnel_state_reliability",
        "entity_link_reliability",
        "game_context_observation_ledger",
        "game_exposure_ledger",
        "event_observation_geometry",
        "event_observation_pitch",
        "event_observation_credit",
        "event_observation_context",
        "fielding_credit_gaps",
        "official_credit_authority",
    }
)


def _retarget_ledgers(sql: str) -> str:
    schema = os.environ.get(ENV_LEDGER_SCHEMA, DEFAULT_LEDGER_SCHEMA)
    if schema == DEFAULT_LEDGER_SCHEMA:
        return sql
    out = sql
    for table in LEDGER_TABLES:
        out = out.replace(f"main_models.{table}", f"{schema}.{table}")
    return out


@dataclass(frozen=True)
class ParityCheck:
    name: str
    description: str
    existing_select: str
    ledger_select: str


CHECKS: dict[str, ParityCheck] = {
    "event_completeness_pitches": ParityCheck(
        name="event_completeness_pitches",
        description="event_key flags pivoted from event_observation_pitch.",
        existing_select="""
            SELECT
                event_key,
                has_count_balls,
                has_count_strikes,
                has_count,
                has_pitches,
                has_pitch_results,
                has_strike_types
            FROM main_models.event_completeness_pitches
        """,
        ledger_select="""
            WITH p AS (
                SELECT
                    event_key,
                    BOOL_OR(dimension = 'count_balls' AND observed_status = 'observed') AS has_count_balls,
                    BOOL_OR(dimension = 'count_strikes' AND observed_status = 'observed') AS has_count_strikes,
                    BOOL_OR(dimension = 'pitch_sequence' AND observed_status = 'observed') AS has_pitches,
                    BOOL_OR(dimension = 'pitch_results' AND observed_status = 'observed') AS has_pitch_results,
                    BOOL_OR(dimension = 'strike_types' AND observed_status NOT IN ('missing', 'unknown_code')) AS has_strike_types
                FROM main_models.event_observation_pitch
                GROUP BY 1
            ),
            ev AS (
                SELECT event_key, plate_appearance_result FROM main_models.stg_events
            )
            SELECT
                p.event_key,
                p.has_count_balls,
                p.has_count_strikes,
                (p.has_count_balls AND p.has_count_strikes) AS has_count,
                p.has_pitches,
                p.has_pitch_results,
                p.has_strike_types
            FROM p
            INNER JOIN ev USING (event_key)
            WHERE ev.plate_appearance_result IS NOT NULL OR p.has_pitches
        """,
    ),
    "event_completeness_fielding_credit": ParityCheck(
        name="event_completeness_fielding_credit",
        description="event_key fielder-credit flags pivoted from event_observation_credit.",
        existing_select="""
            SELECT
                event_key,
                has_fielder_putouts,
                has_fielder_assists,
                has_fielder_errors
            FROM main_models.event_completeness_fielding_credit
        """,
        ledger_select="""
            WITH c AS (
                SELECT
                    event_key,
                    BOOL_OR(dimension = 'putout_credit' AND observed_status = 'unknown_code') AS unk_putouts,
                    BOOL_OR(dimension = 'assist_credit' AND observed_status = 'unknown_code') AS unk_assists,
                    BOOL_OR(dimension = 'error_credit' AND observed_status = 'unknown_code') AS unk_errors
                FROM main_models.event_observation_credit
                GROUP BY 1
            ),
            ev AS (
                SELECT event_key, plate_appearance_result FROM main_models.stg_events
            )
            SELECT
                ev.event_key,
                NOT COALESCE(c.unk_putouts, FALSE) AS has_fielder_putouts,
                NOT COALESCE(c.unk_assists OR c.unk_putouts, FALSE) AS has_fielder_assists,
                NOT COALESCE(c.unk_errors, FALSE) AS has_fielder_errors
            FROM ev
            LEFT JOIN c USING (event_key)
            WHERE ev.plate_appearance_result IS NOT NULL
        """,
    ),
    "event_completeness_batted_balls": ParityCheck(
        name="event_completeness_batted_balls",
        description="event_key batted-ball geometry flags pivoted from event_observation_geometry.",
        existing_select="""
            SELECT
                event_key,
                has_trajectory,
                has_general_location,
                has_batted_to_fielder,
                has_any_location
            FROM main_models.event_completeness_batted_balls
        """,
        ledger_select="""
            WITH g AS (
                SELECT
                    event_key,
                    BOOL_OR(dimension = 'trajectory' AND observed_status = 'observed') AS has_trajectory,
                    BOOL_OR(dimension = 'general_location' AND observed_status = 'observed') AS has_general_location,
                    BOOL_OR(dimension = 'ball_handler_position' AND observed_status = 'observed') AS has_batted_to_fielder
                FROM main_models.event_observation_geometry
                GROUP BY 1
            ),
            ev AS (
                SELECT
                    event_key,
                    plate_appearance_result
                FROM main_models.stg_events
            )
            SELECT
                ev.event_key,
                COALESCE(g.has_trajectory, FALSE) AS has_trajectory,
                COALESCE(g.has_general_location, FALSE) AS has_general_location,
                COALESCE(g.has_batted_to_fielder, FALSE) AS has_batted_to_fielder,
                COALESCE(g.has_batted_to_fielder OR g.has_general_location, FALSE) AS has_any_location
            FROM ev
            INNER JOIN main_seeds.seed_plate_appearance_result_types AS rt USING (plate_appearance_result)
            LEFT JOIN g USING (event_key)
        """,
    ),
    "player_game_data_completeness": ParityCheck(
        name="player_game_data_completeness",
        description="(game_id, player_id, player_type) rollup of event_observation_* keyed via event_observation_context.",
        existing_select="""
            SELECT
                game_id,
                player_id,
                player_type,
                has_count_balls,
                has_count_strikes,
                has_count,
                has_pitches
            FROM main_models.player_game_data_completeness
        """,
        ledger_select="""
            WITH ctx AS (
                SELECT event_key, game_id, batter_id, pitcher_id
                FROM main_models.event_observation_context
            ),
            pitch_flags AS (
                SELECT
                    event_key,
                    BOOL_OR(dimension = 'count_balls' AND observed_status = 'observed') AS has_count_balls,
                    BOOL_OR(dimension = 'count_strikes' AND observed_status = 'observed') AS has_count_strikes,
                    BOOL_OR(dimension = 'pitch_sequence' AND observed_status = 'observed') AS has_pitches
                FROM main_models.event_observation_pitch
                GROUP BY 1
            ),
            joined AS (
                SELECT
                    ctx.game_id,
                    ctx.batter_id AS player_id,
                    'BATTING' AS player_type,
                    pitch_flags.has_count_balls,
                    pitch_flags.has_count_strikes,
                    pitch_flags.has_pitches
                FROM ctx
                INNER JOIN pitch_flags USING (event_key)
                WHERE ctx.batter_id IS NOT NULL
                UNION ALL
                SELECT
                    ctx.game_id,
                    ctx.pitcher_id AS player_id,
                    'PITCHING' AS player_type,
                    pitch_flags.has_count_balls,
                    pitch_flags.has_count_strikes,
                    pitch_flags.has_pitches
                FROM ctx
                INNER JOIN pitch_flags USING (event_key)
                WHERE ctx.pitcher_id IS NOT NULL
            )
            SELECT
                game_id,
                player_id,
                player_type,
                BOOL_AND(has_count_balls) AS has_count_balls,
                BOOL_AND(has_count_strikes) AS has_count_strikes,
                BOOL_AND(has_count_balls AND has_count_strikes) AS has_count,
                BOOL_AND(has_pitches) AS has_pitches
            FROM joined
            GROUP BY 1, 2, 3
        """,
    ),
    "player_completeness": ParityCheck(
        name="player_completeness",
        description="(player_id, player_type) rollup of player_game_data_completeness reproduction.",
        existing_select="""
            SELECT
                player_id,
                player_type,
                total_games,
                count_balls,
                count_strikes,
                count,
                pitches
            FROM main_models.player_completeness
        """,
        ledger_select="""
            WITH ctx AS (
                SELECT event_key, game_id, batter_id, pitcher_id
                FROM main_models.event_observation_context
            ),
            pitch_flags AS (
                SELECT
                    event_key,
                    BOOL_OR(dimension = 'count_balls' AND observed_status = 'observed') AS has_count_balls,
                    BOOL_OR(dimension = 'count_strikes' AND observed_status = 'observed') AS has_count_strikes,
                    BOOL_OR(dimension = 'pitch_sequence' AND observed_status = 'observed') AS has_pitches
                FROM main_models.event_observation_pitch
                GROUP BY 1
            ),
            per_event AS (
                SELECT
                    ctx.game_id,
                    ctx.batter_id AS player_id,
                    'BATTING' AS player_type,
                    pitch_flags.has_count_balls,
                    pitch_flags.has_count_strikes,
                    pitch_flags.has_pitches
                FROM ctx
                INNER JOIN pitch_flags USING (event_key)
                WHERE ctx.batter_id IS NOT NULL
                UNION ALL
                SELECT
                    ctx.game_id,
                    ctx.pitcher_id AS player_id,
                    'PITCHING' AS player_type,
                    pitch_flags.has_count_balls,
                    pitch_flags.has_count_strikes,
                    pitch_flags.has_pitches
                FROM ctx
                INNER JOIN pitch_flags USING (event_key)
                WHERE ctx.pitcher_id IS NOT NULL
            ),
            per_game AS (
                SELECT
                    game_id,
                    player_id,
                    player_type,
                    BOOL_AND(has_count_balls) AS has_count_balls,
                    BOOL_AND(has_count_strikes) AS has_count_strikes,
                    BOOL_AND(has_pitches) AS has_pitches
                FROM per_event
                GROUP BY 1, 2, 3
            )
            SELECT
                player_id,
                player_type,
                COUNT(*) AS total_games,
                COUNT_IF(has_count_balls) AS count_balls,
                COUNT_IF(has_count_strikes) AS count_strikes,
                COUNT_IF(has_count_balls AND has_count_strikes) AS count,
                COUNT_IF(has_pitches) AS pitches
            FROM per_game
            GROUP BY 1, 2
        """,
    ),
    "game_data_completeness": ParityCheck(
        name="game_data_completeness",
        description="game_id source/has_* flags rollup via event_observation_context + game_context_observation_ledger + source_acquisition_ledger.",
        existing_select="""
            SELECT
                game_id,
                has_play_by_play,
                has_box_score
            FROM main_models.game_data_completeness
        """,
        ledger_select="""
            WITH src AS (
                SELECT
                    game_id,
                    BOOL_OR(source_type = 'PlayByPlay') AS has_play_by_play,
                    BOOL_OR(source_type IN ('Event', 'BoxScore')) AS has_box_score
                FROM main_models.source_acquisition_ledger
                WHERE team_id IS NULL
                GROUP BY 1
            )
            SELECT game_id, has_play_by_play, has_box_score FROM src
        """,
    ),
    "season_team_coverage": ParityCheck(
        name="season_team_coverage",
        description="(season, team_id) least_granular_source_type derived from source_acquisition_ledger.",
        existing_select="""
            SELECT season, team_id, least_granular_source_type
            FROM main_models.season_team_coverage
        """,
        ledger_select="""
            WITH per_team_game AS (
                SELECT
                    gsi.season,
                    gsi.team_id,
                    sal.source_type
                FROM main_models.source_acquisition_ledger AS sal
                INNER JOIN main_models.team_game_start_info AS gsi
                    ON sal.game_id = gsi.game_id
                    AND (sal.team_id IS NULL OR sal.team_id = gsi.team_id)
                WHERE gsi.game_id NOT IN (SELECT game_id FROM main_models.game_forfeits)
                    AND (gsi.game_type != 'Exhibition' OR gsi.league IS NULL)
            )
            SELECT
                season,
                team_id,
                CASE
                    WHEN BOOL_AND(source_type = 'PlayByPlay') THEN 'PlayByPlay'
                    WHEN BOOL_AND(source_type IN ('PlayByPlay', 'BoxScore')) THEN 'BoxScore'
                    ELSE 'GameLog'
                END AS least_granular_source_type
            FROM per_team_game
            GROUP BY 1, 2
        """,
    ),
}


def _diff_sql(existing_select: str, ledger_select: str) -> str:
    return f"""
        WITH existing AS ({existing_select}),
             from_ledger AS ({ledger_select}),
             in_existing_not_ledger AS (
                 SELECT * FROM existing
                 EXCEPT
                 SELECT * FROM from_ledger
             ),
             in_ledger_not_existing AS (
                 SELECT * FROM from_ledger
                 EXCEPT
                 SELECT * FROM existing
             )
        SELECT 'in_existing_not_ledger' AS side, COUNT(*) AS row_count FROM in_existing_not_ledger
        UNION ALL
        SELECT 'in_ledger_not_existing', COUNT(*) FROM in_ledger_not_existing
    """


def _run_check(con: duckdb.DuckDBPyConnection, check: ParityCheck) -> dict[str, Any]:
    sql = _retarget_ledgers(_diff_sql(check.existing_select, check.ledger_select))
    _log.info("rollup_parity_run name=%s", check.name)
    try:
        rows = con.execute(sql).fetchall()
    except duckdb.Error as exc:
        _log.error("rollup_parity_failed name=%s err=%s", check.name, exc)
        return {"name": check.name, "error": str(exc), "diffs": None}
    diffs = {side: count for side, count in rows}
    return {"name": check.name, "error": None, "diffs": diffs}


def _print_summary(results: list[dict[str, Any]]) -> None:
    sys.stdout.write("# Rollup parity report\n\n")
    sys.stdout.write(
        "| model | in_existing_not_ledger | in_ledger_not_existing | error |\n"
    )
    sys.stdout.write(
        "|-------|------------------------|------------------------|-------|\n"
    )
    for r in results:
        if r["error"]:
            sys.stdout.write(f"| `{r['name']}` | - | - | {r['error']} |\n")
            continue
        diffs = r["diffs"] or {}
        sys.stdout.write(
            f"| `{r['name']}` | {diffs.get('in_existing_not_ledger', 0)} | {diffs.get('in_ledger_not_existing', 0)} |  |\n"
        )
    sys.stdout.write("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Rollup parity checks vs ledger reproductions."
    )
    subparsers = parser.add_subparsers(dest="cmd")
    chk_parser = subparsers.add_parser("check", help="Run parity checks (default).")
    for p in (parser, chk_parser):
        mode = p.add_mutually_exclusive_group()
        _ = mode.add_argument(
            "--all", action="store_true", help="Run every check (default)."
        )
        _ = mode.add_argument("--model", help="Run a single check by model name.")
        _ = p.add_argument(
            "--allow-mismatch",
            action="append",
            default=[],
            help="Model name to ignore when computing exit code. Repeatable.",
        )
        _ = p.add_argument(
            "--log-level", default="INFO", help="stdlib logging level name."
        )
    args = parser.parse_args(argv)
    configure_logging(getattr(logging, args.log_level.upper(), logging.INFO))

    if args.model:
        if args.model not in CHECKS:
            _log.error("unknown_model name=%s known=%s", args.model, sorted(CHECKS))
            return 2
        selected = [CHECKS[args.model]]
    else:
        selected = list(CHECKS.values())

    db_path = resolve_db_path()
    if not db_path.exists():
        _log.error("db_not_found path=%s", db_path)
        return 2
    _log.info("rollup_parity_start db_path=%s checks=%d", db_path, len(selected))

    con = duckdb.connect(":memory:")
    con.execute(f"ATTACH '{db_path}' AS bc (READ_ONLY)")
    con.execute("USE bc")
    results: list[dict[str, Any]] = []
    try:
        for check in selected:
            results.append(_run_check(con, check))
    finally:
        con.close()

    _print_summary(results)

    allow = set(args.allow_mismatch)
    has_failure = False
    for r in results:
        if r["name"] in allow:
            continue
        if r["error"]:
            has_failure = True
            continue
        diffs = r["diffs"] or {}
        if any(c > 0 for c in diffs.values()):
            has_failure = True
    return 1 if has_failure else 0


if __name__ == "__main__":
    sys.exit(main())
