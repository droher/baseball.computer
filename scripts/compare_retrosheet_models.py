from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import cast

import duckdb
from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter
from sqlmesh.core.table_diff import TableDiff

from scripts.build_retrosheet_candidate import schema_digest
from scripts.compare_retrosheet_sources import (
    CsvWriter,
    digest,
    export,
    identifier,
    known_expression,
    literal,
    save_json,
    stat_identity,
    validate_output,
)

LOGGER = logging.getLogger(__name__)
MODEL_SCHEMA = "main_models"
GAME_KEYS: dict[str, list[str]] = {
    "game_start_info": ["game_id"],
    "game_results": ["game_id"],
    "game_line_scores": ["game_id"],
    "game_starting_lineups": ["game_id"],
    "game_data_completeness": ["game_id"],
    "team_game_start_info": ["game_id", "team_id"],
    "team_game_results": ["game_id", "team_id"],
    "team_game_offense_stats": ["game_id", "team_id"],
    "team_game_pitching_stats": ["game_id", "team_id"],
    "team_game_fielding_stats": ["game_id", "team_id"],
    "team_game_data_issues": ["game_id", "team_id", "issue_type"],
    "player_game_appearances": ["game_id", "player_id"],
    "player_game_offense_stats": ["game_id", "team_id", "player_id"],
    "player_game_pitching_stats": ["game_id", "player_id"],
    "player_position_game_fielding_stats": [
        "game_id",
        "player_id",
        "fielding_position",
    ],
    "player_game_data_completeness": ["game_id", "player_type", "player_id"],
}
EVENT_KEYS: dict[str, list[str]] = {
    name: ["game_id", "event_id"]
    for name in (
        "event_states_full",
        "event_batting_stats",
        "event_pitching_stats",
        "event_fielding_stats",
        "event_batted_ball_stats",
        "event_pitch_sequence_stats",
        "event_transition_values",
        "event_base_out_states",
        "event_score_states",
        "event_fielders_flat",
        "event_completeness_batted_balls",
        "event_completeness_fielding_credit",
        "event_completeness_pitches",
    )
}
EVENT_KEYS["event_pitching_stats"] = ["game_id", "event_id", "player_id"]
EVENT_KEYS["event_offense_stats"] = ["game_id", "event_id", "baserunner"]
EVENT_KEYS["event_player_fielding_stats"] = ["game_id", "event_id", "fielding_position"]
DEFAULT_TABLES = list(GAME_KEYS) + [
    "event_completeness_pitches",
    "event_completeness_batted_balls",
    "event_completeness_fielding_credit",
    "event_pitch_sequence_stats",
    "event_batted_ball_stats",
    "event_states_full",
    "event_transition_values",
]
EXCLUDED = {"filename", "line_number"}
EXAMPLES = ("BOS197405290", "ATL197905192", "PHI197109041")


def columns(connection: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    return {
        str(row[0]): str(row[1])
        for row in connection.execute(f"DESCRIBE {table}").fetchall()
    }


def inventory(
    connection: duckdb.DuckDBPyConnection, catalog: str, schema: str
) -> list[str]:
    return [
        str(row[0])
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog=? AND table_schema=? ORDER BY table_name",
            [catalog, schema],
        ).fetchall()
    ]


def resolve_side(candidates: dict[str, object], role: str) -> dict[str, object]:
    sides = cast(dict[str, dict[str, object]], candidates["sides"])
    side = sides[role]
    same_as = side.get("same_as")
    return sides[cast(str, same_as)] if same_as else side


def verify_identity(side: dict[str, object]) -> Path:
    path = Path(cast(str, side["db"]))
    if stat_identity(path) != side["identity"]:
        raise ValueError(f"Database changed since the candidates were built: {path}")
    return path


def attribution_message(old: dict[str, object], new: dict[str, object]) -> str:
    code_equal = all(
        old[key] == new[key] for key in ("fingerprints_sha256", "seeds_sha256")
    )
    sources_equal = all(
        old[key] == new[key]
        for key in ("retrosheet_sources_sha256", "other_sources_sha256")
    )
    if code_equal and sources_equal:
        return "No change: model code, seeds and sources are identical."
    if code_equal:
        return "Source change only: model code and seeds are identical; differences come from the sources."
    if sources_equal:
        return "Code change only: sources are identical; differences come from model code or seeds."
    return "Code and source change: differences combine model code, seeds and sources."


def validate_alignment(
    candidates: dict[str, object],
    old: dict[str, object],
    new: dict[str, object],
    alignment: Path | None,
) -> Path | None:
    if old["retrosheet_sources_sha256"] == new["retrosheet_sources_sha256"]:
        return None
    if alignment is None:
        raise ValueError("Sides read different Retrosheet sources; pass --alignment")
    prod = resolve_side(candidates, "prod")
    release = resolve_side(candidates, "B")
    if (
        old["retrosheet_sources_sha256"] != prod["retrosheet_sources_sha256"]
        or new["retrosheet_sources_sha256"] != release["retrosheet_sources_sha256"]
    ):
        raise ValueError("Event alignment pairs prod sources with the release only")
    manifest_path = alignment.parent.parent / "source-manifest.json"
    for path in (alignment, alignment.parent / "summary.json", manifest_path):
        if not path.is_file():
            raise ValueError(f"Event alignment input does not exist: {path}")
    summary = cast(
        dict[str, object], json.loads((alignment.parent / "summary.json").read_text())
    )
    integrity = cast(dict[str, object], summary.get("supplemental_integrity", {}))
    if summary.get("baseline_stat_after") != prod["identity"]:
        raise ValueError(
            "Alignment baseline differs from the candidates' prod database"
        )
    if integrity.get("source_manifest_sha256") != digest(manifest_path):
        raise ValueError("Alignment was computed from a different source manifest")
    manifest = cast(dict[str, object], json.loads(manifest_path.read_text()))
    files = {
        name: cast(str, item["sha256"])
        for name, item in cast(dict[str, dict[str, object]], manifest["files"]).items()
    }
    if schema_digest(files, retrosheet=True) != release["retrosheet_sources_sha256"]:
        raise ValueError(
            "Alignment release sources differ from the candidates' B sources"
        )
    return alignment


def validate_keys(
    connection: duckdb.DuckDBPyConnection, table: str, keys: list[str]
) -> None:
    names = ",".join(identifier(key) for key in keys)
    nulls = " OR ".join(f"{identifier(key)} IS NULL" for key in keys)
    invalid = connection.execute(
        f"SELECT EXISTS(SELECT 1 FROM {table} WHERE {nulls}) OR EXISTS(SELECT 1 FROM {table} GROUP BY {names} HAVING count(*)>1)"
    ).fetchone()
    if invalid is None or invalid[0]:
        raise ValueError(f"Nonunique or null stable key in {table}: {keys}")


def build_cohort(
    connection: duckdb.DuckDBPyConnection, alignment: Path | None, limit: int
) -> None:
    if alignment is None:
        connection.execute(
            "CREATE TABLE alignment AS SELECT game_id, TRUE AS aligned FROM bc.game.games"
        )
    else:
        connection.execute(
            f"CREATE TABLE alignment AS SELECT * FROM read_csv_auto({literal(str(alignment))})"
        )
    validate_keys(connection, "alignment", ["game_id"])
    connection.execute(
        "CREATE TABLE cohort AS SELECT o.game_id,o.season,coalesce(a.aligned,FALSE) aligned "
        "FROM bc.main_models.game_start_info o "
        "LEFT JOIN candidate_game_info n USING(game_id) "
        "LEFT JOIN alignment a USING(game_id) "
        "WHERE NOT (coalesce(o.is_negro_leagues,FALSE) OR coalesce(n.is_negro_leagues,FALSE)) "
        "AND EXISTS(SELECT 1 FROM bc.game.games g WHERE g.game_id=o.game_id AND CAST(g.account_type AS VARCHAR)='PlayByPlay') AND EXISTS(SELECT 1 FROM bc.event.events e WHERE e.game_id=o.game_id) "
        "ORDER BY o.game_id" + (f" LIMIT {limit}" if limit else "")
    )
    validate_keys(connection, "cohort", ["game_id"])


def scoped_view(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    name: str,
    event: bool,
    schema: str,
    side: str,
) -> dict[str, str]:
    fields = columns(connection, source)
    select = ["t.*"]
    join = ""
    if event and ("game_id" not in fields or "event_id" not in fields):
        lookup = f"{'bc.main_models' if side == 'old' else 'bc.' + identifier(schema)}.event_states_full"
        join = f" JOIN {lookup} e USING(event_key)"
        select += [
            f"e.{identifier(key)}"
            for key in ("game_id", "event_id")
            if key not in fields
        ]
    connection.execute(
        f"CREATE VIEW {name} AS SELECT {','.join(select)} FROM {source} t{join} "
        f"JOIN cohort c ON c.game_id={'t' if 'game_id' in fields else 'e'}.game_id"
    )
    return columns(connection, name)


def coverage_expression(field: str) -> str:
    name = identifier(field)
    if field.endswith("_known") or field.startswith(("known_", "has_")):
        return f"try_cast({name} AS DOUBLE)>0"
    if field in {"count_balls", "count_strikes"}:
        return f"{name} BETWEEN 0 AND {3 if field == 'count_balls' else 2}"
    return known_expression(field)


def player_season_changes(
    connection: duckdb.DuckDBPyConnection, table: str, fields: list[str], output: Path
) -> None:
    if table not in {
        "player_game_offense_stats",
        "player_game_pitching_stats",
        "player_position_game_fielding_stats",
    }:
        return
    additive = [
        field
        for field in fields
        if field
        in {
            "plate_appearances",
            "at_bats",
            "hits",
            "singles",
            "doubles",
            "triples",
            "home_runs",
            "total_bases",
            "runs",
            "earned_runs",
            "team_unearned_runs",
            "runs_batted_in",
            "walks",
            "intentional_walks",
            "strikeouts",
            "pitches",
            "balls",
            "strikes",
            "outs_recorded",
            "batting_outs",
            "putouts",
            "assists",
            "errors",
            "trajectory_known",
            "batted_location_known",
            "fielded_by_known",
        }
    ]
    if not additive:
        return
    for side in ("old", "new"):
        sums = ",".join(
            f"sum(t.{identifier(field)}) AS {identifier(field)}" for field in additive
        )
        connection.execute(
            f"CREATE TABLE {side}_player_season AS SELECT t.player_id,c.season,{sums} FROM {side}_scoped t JOIN cohort c USING(game_id) GROUP BY t.player_id,c.season"
        )
    values = ",".join(
        f"({literal(field)},o.{identifier(field)},n.{identifier(field)})"
        for field in additive
    )
    export(
        connection,
        f"SELECT coalesce(o.player_id,n.player_id) player_id,coalesce(o.season,n.season) season,v.field,coalesce(v.old_value,0) old_value,coalesce(v.new_value,0) new_value,coalesce(v.new_value,0)-coalesce(v.old_value,0) delta FROM old_player_season o FULL JOIN new_player_season n USING(player_id,season) CROSS JOIN LATERAL (VALUES {values}) v(field,old_value,new_value) WHERE coalesce(v.old_value,0) IS DISTINCT FROM coalesce(v.new_value,0) ORDER BY abs(delta) DESC,player_id,season,field",
        output / "player-season-count-changes.csv",
    )
    for side in ("old", "new"):
        connection.execute(f"DROP TABLE {side}_player_season")


def compare_table(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    schema: str,
    output: Path,
    sample: int,
    candidate_connection: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, object]:
    event = table in EVENT_KEYS
    keys = (EVENT_KEYS if event else GAME_KEYS)[table]
    fields: dict[str, dict[str, str]] = {}
    for side, catalog in [
        ("old", "bc.main_models"),
        ("new", "bc." + identifier(schema)),
    ]:
        reader = (
            candidate_connection
            if side == "new" and candidate_connection is not None
            else connection
        )
        fields[side] = scoped_view(
            reader,
            f"{catalog}.{identifier(table)}",
            f"{side}_scoped",
            event,
            schema,
            side,
        )
        if side == "new" and candidate_connection is not None:
            scoped_path = output / "candidate-scoped.parquet"
            transport = ",".join(
                f"CAST({identifier(field)} AS VARCHAR) AS {identifier(field)}"
                if "HUGEINT" in kind
                else identifier(field)
                for field, kind in fields[side].items()
            )
            reader.execute(
                f"COPY (SELECT {transport} FROM new_scoped) TO {literal(str(scoped_path))} (FORMAT PARQUET)"
            )
            restored = ",".join(
                f"CAST({identifier(field)} AS {kind}) AS {identifier(field)}"
                if "HUGEINT" in kind
                else identifier(field)
                for field, kind in fields[side].items()
            )
            connection.execute(
                f"CREATE VIEW new_scoped AS SELECT {restored} FROM read_parquet({literal(str(scoped_path))})"
            )
        if side == "old":
            connection.execute("CREATE TABLE old_cached AS SELECT * FROM old_scoped")
            connection.execute("DROP VIEW old_scoped")
            connection.execute("CREATE VIEW old_scoped AS SELECT * FROM old_cached")
        validate_keys(connection, f"{side}_scoped", keys)
    adapter = DuckDBEngineAdapter(lambda: connection)
    schema_diff = TableDiff(
        adapter=adapter, source="old_scoped", target="new_scoped", on=keys
    ).schema_diff()
    save_json(
        output / "schema.json",
        {
            "old": fields["old"],
            "new": fields["new"],
            "transport_restored_schema": columns(connection, "new_scoped"),
            "native_projection_old": {
                key: str(value) for key, value in schema_diff.source_schema.items()
            },
            "native_projection_new": {
                key: str(value) for key, value in schema_diff.target_schema.items()
            },
        },
    )
    shared = sorted(set(fields["old"]) & set(fields["new"]))
    shared = [
        field
        for field in shared
        if not field.endswith("_key") and field not in EXCLUDED
    ]
    if not set(keys) <= set(shared):
        raise ValueError(f"Stable keys missing from {table}")
    for side in ("old", "new"):
        projection = ",".join(
            f"CAST({identifier(field)} AS VARCHAR) AS {identifier(field)}"
            for field in shared
        )
        connection.execute(
            f"CREATE TABLE {side}_normalized AS SELECT {projection} FROM {side}_scoped"
            + (
                " WHERE game_id IN (SELECT game_id FROM cohort WHERE aligned)"
                if event
                else ""
            )
        )
    rows = TableDiff(
        adapter=adapter,
        source="old_normalized",
        target="new_normalized",
        on=keys,
        limit=sample,
    ).row_diff(temp_schema="sqlmesh_temp")
    save_json(output / "native.json", {"keys": keys, "stats": rows.stats})
    for name, frame in [
        ("columns", rows.column_stats),
        ("changed", rows.joined_sample),
        ("removed", rows.s_sample),
        ("added", rows.t_sample),
    ]:
        cast(CsvWriter, frame).to_csv(output / f"{name}.csv", index=name == "columns")
    coverage_fields = sorted(
        (set(fields["old"]) | set(fields["new"])) - set(keys) - EXCLUDED
    )
    coverage_fields = [field for field in coverage_fields if not field.endswith("_key")]
    known_names = {
        field: f"known_{index}" for index, field in enumerate(coverage_fields)
    }
    for side in ("old", "new"):
        projection = ",".join(
            f"coalesce(({coverage_expression(field) if field in fields[side] else 'FALSE'}),FALSE) {known_names[field]}"
            for field in coverage_fields
        )
        connection.execute(
            f"CREATE VIEW {side}_known AS SELECT *,{projection} FROM {side}_scoped"
        )
    totals: dict[str, tuple[object, ...]] = {}
    for side in ("old", "new"):
        totals_expressions = ",".join(
            f"count(*) FILTER(WHERE {known_names[field]})" for field in coverage_fields
        )
        row = connection.execute(
            f"SELECT count(*),{totals_expressions} FROM {side}_known"
        ).fetchone()
        if row is None:
            raise ValueError("Missing coverage totals")
        totals[side] = row
        export(
            connection,
            f"SELECT game_id,count(*) row_count,{','.join(f'count(*) FILTER(WHERE {known_names[field]}) AS {identifier(field)}' for field in coverage_fields)} FROM {side}_known GROUP BY game_id ORDER BY game_id",
            output / f"{side}-coverage-game.csv",
        )
    join = " AND ".join(
        f"o.{identifier(key)} IS NOT DISTINCT FROM n.{identifier(key)}" for key in keys
    )
    expressions: list[str] = []
    for field in coverage_fields:
        known = known_names[field]
        expressions.extend(
            [
                f"count(*) FILTER(WHERE o.{known})",
                f"count(*) FILTER(WHERE n.{known})",
                f"count(*) FILTER(WHERE NOT o.{known} AND n.{known})",
                f"count(*) FILTER(WHERE o.{known} AND NOT n.{known})",
                f"count(*) FILTER(WHERE {'CAST(o.' + identifier(field) + ' AS VARCHAR)' if field in fields['old'] else 'NULL'} IS DISTINCT FROM {'CAST(n.' + identifier(field) + ' AS VARCHAR)' if field in fields['new'] else 'NULL'})",
            ]
        )
    counts = connection.execute(
        f"SELECT {','.join(expressions)} FROM old_known o JOIN new_known n ON {join}"
        + (
            " WHERE o.game_id IN (SELECT game_id FROM cohort WHERE aligned)"
            if event
            else ""
        )
    ).fetchone()
    if counts is None:
        raise ValueError("Missing matched coverage totals")
    coverage = [
        {
            "field": field,
            "old_rows": totals["old"][0],
            "new_rows": totals["new"][0],
            "old_known": totals["old"][index + 1],
            "new_known": totals["new"][index + 1],
            "matched_old_known": counts[index * 5],
            "matched_new_known": counts[index * 5 + 1],
            "gained": counts[index * 5 + 2],
            "lost": counts[index * 5 + 3],
            "changed": counts[index * 5 + 4],
            "predicate": coverage_expression(field),
            "old_column_present": field in fields["old"],
            "new_column_present": field in fields["new"],
        }
        for index, field in enumerate(coverage_fields)
    ]
    save_json(output / "coverage.json", coverage)
    for side in ("old", "new"):
        connection.execute(f"DROP VIEW {side}_known")
    numeric_fields = [
        field
        for field in coverage_fields
        if field in fields["old"]
        and field in fields["new"]
        and fields["old"][field].split("(")[0]
        in {
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "DOUBLE",
            "FLOAT",
            "DECIMAL",
        }
    ]
    if numeric_fields and not event:
        values = ",".join(
            f"({literal(field)},try_cast(o.{identifier(field)} AS DOUBLE),try_cast(n.{identifier(field)} AS DOUBLE))"
            for field in numeric_fields
        )
        stable = ",".join(identifier(key) + ":=o." + identifier(key) for key in keys)
        connection.execute(
            f"CREATE TABLE numeric_changes AS SELECT to_json(struct_pack({stable})) stable_key,v.field,v.old_value,v.new_value,v.new_value-v.old_value delta FROM old_scoped o JOIN new_scoped n ON {join} CROSS JOIN LATERAL (VALUES {values}) v(field,old_value,new_value) WHERE v.old_value IS DISTINCT FROM v.new_value"
        )
        export(
            connection,
            f"SELECT * FROM numeric_changes ORDER BY abs(delta) DESC NULLS LAST,stable_key,field LIMIT {sample}",
            output / "largest-numeric-changes.csv",
        )
        export(
            connection,
            "SELECT field,count(*) exact_changes,count(*) FILTER(WHERE delta IS NULL) null_transitions,count(*) FILTER(WHERE abs(delta)<=1e-9) changes_at_most_1e_9,count(*) FILTER(WHERE abs(delta)>1e-9) changes_above_1e_9,max(abs(delta)) max_absolute_delta FROM numeric_changes GROUP BY field ORDER BY changes_above_1e_9 DESC,field",
            output / "numeric-magnitude.csv",
        )
        connection.execute("DROP TABLE numeric_changes")
    player_season_changes(connection, table, shared, output)
    examples = ",".join(literal(game) for game in EXAMPLES)
    for side in ("old", "new"):
        export(
            connection,
            f"SELECT * FROM {side}_scoped WHERE game_id IN ({examples})",
            output / f"{side}-example-games.csv",
        )
    result: dict[str, object] = {
        "table": table,
        "keys": keys,
        "scope": "aligned_existing_pbp" if event else "existing_pbp",
        "stats": rows.stats,
    }
    for side in ("old", "new"):
        connection.execute(f"DROP TABLE {side}_normalized")
        connection.execute(f"DROP VIEW {side}_scoped")
    connection.execute("DROP TABLE old_cached")
    if candidate_connection is not None:
        candidate_connection.execute("DROP VIEW new_scoped")
        (output / "candidate-scoped.parquet").unlink()
    return result


def run(
    candidates_path: Path,
    old_role: str,
    new_role: str,
    alignment: Path | None,
    output: Path,
    tables: list[str],
    limit: int = 0,
    sample: int = 100,
) -> dict[str, object]:
    if old_role not in ("prod", "A") or new_role not in ("A", "B"):
        raise ValueError("Old side must be prod or A; new side must be A or B")
    candidates = cast(dict[str, object], json.loads(candidates_path.read_text()))
    old = resolve_side(candidates, old_role)
    new = resolve_side(candidates, new_role)
    baseline = verify_identity(old)
    candidate = verify_identity(new)
    if baseline.samefile(candidate):
        raise ValueError("Old and new sides resolve to the same database")
    for path in (
        baseline,
        candidate,
        candidates_path,
        *([alignment] if alignment else []),
    ):
        validate_output(path, output)
    alignment = validate_alignment(candidates, old, new, alignment)
    if output.exists():
        raise ValueError(
            "Use a fresh output directory; completed comparisons are immutable"
        )
    output.mkdir(parents=True)
    identities = {
        name: stat_identity(path)
        for name, path in [("baseline", baseline), ("candidate", candidate)]
    }
    schema = MODEL_SCHEMA
    metadata: dict[str, object] = {
        "candidates": str(candidates_path.resolve()),
        "old": old_role,
        "new": new_role,
        "baseline": str(baseline),
        "candidate": str(candidate),
        "input_identity_before": identities,
        "alignment_sha256": digest(alignment) if alignment else None,
        "limit_games": limit,
        "sample_rows": sample,
        "attribution": attribution_message(old, new),
    }
    save_json(output / "inputs.json", metadata)
    connection = duckdb.connect(str(output / "comparison.duckdb"))
    candidate_connection = duckdb.connect()
    try:
        connection.execute("SET threads=4")
        connection.execute("SET memory_limit='8GB'")
        connection.execute(f"ATTACH {literal(str(baseline))} AS bc (READ_ONLY)")
        candidate_connection.execute(
            f"ATTACH {literal(str(candidate))} AS bc (READ_ONLY)"
        )
        candidate_connection.execute("SET threads=4")
        candidate_connection.execute("SET memory_limit='8GB'")
        connection.execute("CREATE SCHEMA sqlmesh_temp")
        old_inventory = inventory(connection, "bc", "main_models")
        new_inventory = inventory(candidate_connection, "bc", schema)
        selected = tables or DEFAULT_TABLES
        if set(selected) - (set(GAME_KEYS) | set(EVENT_KEYS)):
            raise ValueError(
                "Requested table lacks a reviewed stable-key and cohort strategy"
            )
        save_json(
            output / "inventory.json",
            [
                {
                    "table": table,
                    "baseline": table in old_inventory,
                    "candidate": table in new_inventory,
                    "selected": table in selected,
                    "reason": "game-scoped modeled result"
                    if table in selected
                    else (
                        "optional event detail; request --table explicitly"
                        if table in EVENT_KEYS
                        else "outside existing-PBP scope or no reviewed stable-key/cohort strategy; season/career metrics include added games and potentially NLB"
                    ),
                }
                for table in sorted(set(old_inventory) | set(new_inventory))
            ],
        )
        candidate_games = output / "candidate-games.parquet"
        candidate_connection.execute(
            f"COPY (SELECT game_id,is_negro_leagues FROM bc.{identifier(schema)}.game_start_info) TO {literal(str(candidate_games))} (FORMAT PARQUET)"
        )
        connection.execute(
            f"CREATE VIEW candidate_game_info AS SELECT * FROM read_parquet({literal(str(candidate_games))})"
        )
        build_cohort(connection, alignment, limit)
        cohort_parquet = output / "cohort.parquet"
        connection.execute(
            f"COPY cohort TO {literal(str(cohort_parquet))} (FORMAT PARQUET)"
        )
        candidate_connection.execute(
            f"CREATE TABLE cohort AS SELECT * FROM read_parquet({literal(str(cohort_parquet))})"
        )
        export(
            connection, "SELECT * FROM cohort ORDER BY game_id", output / "cohort.csv"
        )
        export(
            connection,
            "SELECT season,aligned,count(*) games FROM cohort GROUP BY ALL ORDER BY 1,2",
            output / "cohort-year.csv",
        )
        summaries: list[dict[str, object]] = []
        for table in selected:
            if table not in old_inventory or table not in new_inventory:
                summaries.append(
                    {
                        "table": table,
                        "status": "missing_table",
                        "baseline": table in old_inventory,
                        "candidate": table in new_inventory,
                    }
                )
                continue
            LOGGER.info("Comparing %s", table)
            directory = output / table
            directory.mkdir()
            summaries.append(
                compare_table(
                    connection, table, schema, directory, sample, candidate_connection
                )
            )
            save_json(output / "checkpoint.json", summaries)
            connection.execute("CHECKPOINT")
        metadata["tables"] = summaries
        metadata["cohort"] = connection.execute(
            "SELECT aligned,count(*) FROM cohort GROUP BY aligned ORDER BY aligned"
        ).fetchall()
    finally:
        candidate_connection.close()
        connection.close()
        after = {
            name: stat_identity(path)
            for name, path in [("baseline", baseline), ("candidate", candidate)]
        }
        save_json(
            output / "integrity.json",
            {"before": identities, "after": after, "unchanged": identities == after},
        )
        if identities != after:
            raise ValueError("Input database identity changed during comparison")
    save_json(output / "summary.json", metadata)
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--old", choices=("prod", "A"), required=True)
    parser.add_argument("--new", choices=("A", "B"), required=True)
    parser.add_argument("--alignment", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--table", action="append", default=[])
    parser.add_argument("--limit-games", type=int, default=0)
    parser.add_argument("--sample-rows", type=int, default=100)
    args = parser.parse_args()
    if args.limit_games < 0 or args.sample_rows < 1:
        parser.error("limit-games must be nonnegative and sample-rows positive")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    run(
        args.candidates,
        args.old,
        args.new,
        args.alignment,
        args.output,
        args.table,
        args.limit_games,
        args.sample_rows,
    )


if __name__ == "__main__":
    main()
