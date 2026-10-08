from __future__ import annotations

import argparse
import hashlib
import json
import logging
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Protocol, cast

import duckdb
from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter
from sqlmesh.core.table_diff import TableDiff

LOGGER = logging.getLogger(__name__)
EXCLUDED = {"event_key", "game_key", "filename", "line_number"}
CORE = [
    "event_id",
    "inning",
    "frame",
    "batter_id",
    "pitcher_id",
    "plate_appearance_result",
    "no_play_flag",
    "outs",
    "base_state",
]


class CsvWriter(Protocol):
    def to_csv(self, path_or_buf: Path, *, index: bool = True) -> None: ...


def normalized_columns(columns: list[str], alias: str = "e") -> str:
    return ",".join(
        f"CAST({alias}.{identifier(col)} AS VARCHAR) AS {identifier(col)}"
        for col in columns
    )


def validate_output(baseline: Path, output: Path) -> None:
    if baseline.resolve().is_relative_to(output.resolve()):
        raise ValueError("Baseline database must be outside the output directory")
    if (
        output.exists()
        and baseline.exists()
        and any(
            path.is_file() and path.samefile(baseline) for path in output.rglob("*")
        )
    ):
        raise ValueError("Output artifacts must not hardlink baseline")
    if output.is_symlink() or (
        output.exists() and any(path.is_symlink() for path in output.rglob("*"))
    ):
        raise ValueError("Output artifacts must not contain symlinks")


def build_alignment(connection: duckdb.DuckDBPyConnection) -> None:
    for side in ["old", "new"]:
        connection.execute(
            f"CREATE TABLE {side}_streams AS SELECT game_id,count(*) events,count(DISTINCT event_id)=count(*) unique_ids FROM {side}_events GROUP BY game_id"
        )
    equal = " AND ".join(
        f"CAST(o.{identifier(field)} AS VARCHAR) IS NOT DISTINCT FROM CAST(n.{identifier(field)} AS VARCHAR)"
        for field in CORE
    )
    connection.execute(
        f"CREATE TABLE core_alignment AS SELECT coalesce(o.game_id,n.game_id) game_id,bool_and(o.game_id IS NOT NULL AND n.game_id IS NOT NULL AND ({equal})) core_equal FROM old_events o FULL JOIN new_events n USING(game_id,event_id) GROUP BY coalesce(o.game_id,n.game_id)"
    )
    connection.execute(
        "CREATE TABLE alignment AS SELECT game_id,o.events old_events,n.events new_events,a.core_equal AND coalesce(o.unique_ids,FALSE) AND coalesce(n.unique_ids,FALSE) aligned FROM old_streams o FULL JOIN new_streams n USING(game_id) JOIN core_alignment a USING(game_id)"
    )


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def save_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, default=str) + "\n")


def stat_identity(path: Path) -> dict[str, int]:
    info = path.stat()
    return {"size": info.st_size, "mtime_ns": info.st_mtime_ns, "inode": info.st_ino}


def export(connection: duckdb.DuckDBPyConnection, query: str, path: Path) -> None:
    connection.execute(f"COPY ({query}) TO {literal(str(path))} (HEADER, FORMAT CSV)")


def freeze_sources(
    tables: list[tuple[str, str]], root: str, version: str, output: Path, offline: bool
) -> None:
    cache = output / "new"
    cache.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "source-manifest.json"
    previous = (
        cast(dict[str, object], json.loads(manifest_path.read_text()))
        if manifest_path.exists()
        else {}
    )
    if previous and (
        previous["source_root"] != root or previous["source_version"] != version
    ):
        raise ValueError(
            "Cache source root/version differs; choose a fresh output directory"
        )
    previous_files = cast(dict[str, dict[str, object]], previous.get("files", {}))

    if previous_files and set(previous_files) != {
        f"{schema}.{name}" for schema, name in tables
    }:
        raise ValueError(
            "Frozen manifest inventory differs; use a fresh output directory"
        )

    def remote_identity(url: str) -> dict[str, str]:
        result = subprocess.run(
            ["curl", "--fail", "--location", "--silent", "--show-error", "--head", url],
            check=True,
            capture_output=True,
            text=True,
        )
        return {
            key.strip().lower(): value.strip()
            for line in result.stdout.splitlines()
            if ":" in line
            for key, value in [line.split(":", 1)]
            if key.strip().lower() in {"etag", "last-modified", "content-length"}
        }

    def acquire(table: tuple[str, str]) -> tuple[str, dict[str, object]]:
        schema, name = table
        key = f"{schema}.{name}"
        path = cache / f"{name}.parquet"
        headers = cache / f"{name}.headers"
        url = f"{root.rstrip('/')}/{name}.parquet"
        if version:
            url += f"?v={version}"
        if key in previous_files:
            if not path.exists() or digest(path) != previous_files[key]["sha256"]:
                raise ValueError(f"Frozen cache altered: {key}")
        else:
            if offline:
                raise ValueError(f"No frozen offline cache for {key}")
            identity_before = remote_identity(url)
            temp = path.with_suffix(".download")
            subprocess.run(
                [
                    "curl",
                    "--fail",
                    "--location",
                    "--retry",
                    "2",
                    "--silent",
                    "--show-error",
                    "--dump-header",
                    str(headers),
                    "--output",
                    str(temp),
                    url,
                ],
                check=True,
            )
            if identity_before != remote_identity(url):
                raise ValueError(f"Remote source changed during capture: {key}")
            temp.replace(path)
        LOGGER.info("Frozen %s (%s bytes)", key, path.stat().st_size)
        return key, {
            "url": url,
            "sha256": digest(path),
            "bytes": path.stat().st_size,
            "response_headers": headers.read_text(),
        }

    with ThreadPoolExecutor(max_workers=3) as pool:
        files = dict(pool.map(acquire, tables))
    save_json(
        manifest_path, {"source_root": root, "source_version": version, "files": files}
    )


def validate_frozen(output: Path) -> None:
    manifest = cast(
        dict[str, object], json.loads((output / "source-manifest.json").read_text())
    )
    files = cast(dict[str, dict[str, object]], manifest["files"])
    for key, item in files.items():
        path = output / "new" / (key.split(".", 1)[1] + ".parquet")
        if digest(path) != item["sha256"]:
            raise ValueError(f"Frozen cache altered: {key}")


def known_expression(field: str) -> str:
    name = identifier(field)
    if field == "baserunner_known_outcome":
        return "is_out=TRUE OR run_scored_flag=TRUE OR base_end IS NOT NULL"
    if field == "batted_to_fielder":
        return f"{name} BETWEEN 1 AND 9"
    if field in {"attendance", "time_of_game_minutes"}:
        return f"{name} > 0"
    if field == "count_complete":
        return "count_balls BETWEEN 0 AND 3 AND count_strikes BETWEEN 0 AND 2"
    return f"{name} IS NOT NULL AND CAST({name} AS VARCHAR) NOT IN ('Unknown', 'Default', '?', '')"


def validate_completed(
    baseline: Path, run: Path, seed: Path | None = None
) -> dict[str, object]:
    summary_path = run / "summary.json"
    if not summary_path.exists():
        raise ValueError("Supplement requires a completed comparison summary")
    summary = cast(dict[str, object], json.loads(summary_path.read_text()))
    if summary["baseline_db"] != str(baseline.resolve()) or summary[
        "baseline_stat_after"
    ] != stat_identity(baseline):
        raise ValueError("Supplement baseline differs from completed comparison")
    if (
        seed is not None
        and "seed_sha256" in summary
        and summary["seed_sha256"] != digest(seed)
    ):
        raise ValueError("Supplement seed differs from completed comparison")
    return summary


def supplemental_reports(connection: duckdb.DuckDBPyConnection, run: Path) -> None:
    connection.execute("DELETE FROM coverage WHERE field='baserunner_known_outcome'")
    for side, prefix in [("old", "baseline.event"), ("new", "fresh")]:
        connection.execute(
            f"INSERT INTO coverage SELECT '{side}',game_id,'baserunner_known_outcome',count(*),count(*) FILTER(WHERE is_out=TRUE OR run_scored_flag=TRUE OR base_end IS NOT NULL) FROM {prefix}.event_baserunners JOIN selected USING(game_id) GROUP BY game_id"
        )
    connection.execute(
        "CREATE OR REPLACE TABLE coverage_game AS SELECT coalesce(o.game_id,n.game_id) game_id,coalesce(o.field,n.field) field,o.eligible old_eligible,n.eligible new_eligible,o.known old_known,n.known new_known,n.known-o.known known_delta,a.aligned FROM (SELECT * FROM coverage WHERE side='old') o FULL JOIN (SELECT * FROM coverage WHERE side='new') n USING(game_id,field) LEFT JOIN alignment a ON a.game_id=coalesce(o.game_id,n.game_id)"
    )
    export(
        connection,
        "SELECT * FROM coverage_game ORDER BY game_id,field",
        run / "coverage-game.csv",
    )
    export(
        connection,
        "SELECT year(g.date) AS year,field,aligned,sum(old_eligible) old_eligible,sum(new_eligible) new_eligible,sum(old_known) old_known,sum(new_known) new_known FROM coverage_game JOIN baseline.game.games g USING(game_id) GROUP BY ALL ORDER BY ALL",
        run / "coverage-year.csv",
    )
    fields = [
        "batted_trajectory",
        "batted_to_fielder",
        "batted_location_general",
        "batted_location_depth",
        "batted_location_angle",
        "batted_contact_strength",
        "count_complete",
    ]
    adapter = DuckDBEngineAdapter(lambda: connection)
    seed = Path("bc/seeds/misc/seed_plate_appearance_result_types.csv")
    connection.execute(
        f"CREATE OR REPLACE VIEW current_result_types AS SELECT * FROM read_csv_auto({literal(str(seed.resolve()))})"
    )
    seed_rows = TableDiff(
        adapter=adapter,
        source="result_types",
        target="current_result_types",
        on=["plate_appearance_result"],
    ).row_diff(temp_schema="sqlmesh_temp")
    if (
        seed_rows.stats["full_match_count"] != seed_rows.stats["s_count"]
        or seed_rows.stats["full_match_count"] != seed_rows.stats["t_count"]
    ):
        raise ValueError("Staged result seed differs from current seed")
    save_json(
        run / "native-seed-compatibility.json",
        {"stats": seed_rows.stats, "current_seed_sha256": digest(seed)},
    )
    for side in ["old", "new"]:
        projection: list[str] = []
        for field in fields:
            eligible = (
                "e.plate_appearance_result IS NOT NULL AND NOT coalesce(e.no_play_flag,FALSE)"
                if field == "count_complete"
                else "coalesce(r.is_batted,FALSE) AND NOT coalesce(e.no_play_flag,FALSE)"
            )
            projection.append(
                f"coalesce(({eligible}) AND ({known_expression(field)}),FALSE) {identifier(field)}"
            )
        connection.execute(
            f"CREATE OR REPLACE VIEW known_{side} AS SELECT game_id,event_id,{','.join(projection)} FROM {side}_events e JOIN alignment a USING(game_id) LEFT JOIN result_types r USING(plate_appearance_result) WHERE a.aligned"
        )
    rows = TableDiff(
        adapter=adapter,
        source="known_old",
        target="known_new",
        on=["game_id", "event_id"],
        limit=200,
    ).row_diff(temp_schema="sqlmesh_temp")
    save_json(run / "native-aligned-known.json", {"stats": rows.stats})
    cast(CsvWriter, rows.joined_sample).to_csv(
        run / "native-aligned-known-changed.csv", index=False
    )
    changes: list[dict[str, object]] = []
    for field in fields:
        old_row = connection.execute(
            f"SELECT count(*) FILTER(WHERE {identifier(field)}) FROM known_old"
        ).fetchone()
        new_row = connection.execute(
            f"SELECT count(*) FILTER(WHERE {identifier(field)}) FROM known_new"
        ).fetchone()
        old_known = int(old_row[0]) if old_row else 0
        new_known = int(new_row[0]) if new_row else 0
        changed = int(rows.stats["join_count"] - rows.stats[field + "_matches"])
        changes.append(
            {
                "field": field,
                "old_known": old_known,
                "new_known": new_known,
                "gains": (changed + new_known - old_known) // 2,
                "losses": (changed - new_known + old_known) // 2,
            }
        )
    save_json(run / "aligned-coverage-changes.json", changes)
    connection.execute(
        "CREATE OR REPLACE TABLE observation_status(side VARCHAR,game_id VARCHAR,field VARCHAR,eligible BIGINT,known BIGINT,missing BIGINT,unknown BIGINT,default_detail BIGINT)"
    )
    for side in ["old", "new"]:
        for field in fields:
            eligible = (
                "e.plate_appearance_result IS NOT NULL AND NOT coalesce(e.no_play_flag,FALSE)"
                if field == "count_complete"
                else "coalesce(r.is_batted,FALSE) AND NOT coalesce(e.no_play_flag,FALSE)"
            )
            missing = (
                "count_balls IS NULL OR count_strikes IS NULL"
                if field == "count_complete"
                else f"{identifier(field)} IS NULL"
            )
            default = (
                "FALSE"
                if field in {"count_complete", "batted_to_fielder"}
                else f"CAST({identifier(field)} AS VARCHAR)='Default'"
            )
            known = f"coalesce(({known_expression(field)}),FALSE)"
            connection.execute(
                f"INSERT INTO observation_status SELECT '{side}',game_id,{literal(field)},count(*),count(*) FILTER(WHERE {known}),count(*) FILTER(WHERE {missing}),count(*) FILTER(WHERE NOT ({known}) AND NOT ({missing}) AND NOT coalesce(({default}),FALSE)),count(*) FILTER(WHERE {default}) FROM {side}_events e LEFT JOIN result_types r USING(plate_appearance_result) WHERE {eligible} GROUP BY game_id"
            )
    invalid = connection.execute(
        "SELECT count(*) FROM observation_status WHERE eligible<>known+missing+unknown+default_detail"
    ).fetchone()
    if invalid != (0,):
        raise RuntimeError("Observation status partition failed")
    export(
        connection,
        "SELECT * FROM observation_status ORDER BY side,game_id,field",
        run / "observation-status-game.csv",
    )
    export(
        connection,
        "SELECT side,year(g.date) AS year,field,sum(eligible) eligible,sum(known) known,sum(missing) missing,sum(unknown) AS unknown,sum(default_detail) default_detail FROM observation_status JOIN baseline.game.games g USING(game_id) GROUP BY ALL ORDER BY ALL",
        run / "observation-status-year.csv",
    )
    export(
        connection,
        "SELECT o.game_id,count(*) losses,count(*) FILTER(WHERE e.count_balls IS NULL OR e.count_strikes IS NULL) new_null,count(*) FILTER(WHERE e.count_balls NOT BETWEEN 0 AND 3 OR e.count_strikes NOT BETWEEN 0 AND 2) new_invalid FROM known_old o JOIN known_new n USING(game_id,event_id) JOIN new_events e USING(game_id,event_id) WHERE o.count_complete AND NOT n.count_complete GROUP BY o.game_id ORDER BY losses DESC",
        run / "count-loss-games.csv",
    )
    export(
        connection,
        "SELECT o.game_id,o.event_id,o.status old_status,n.status new_status,o.raw_pitch_sequence old_sequence,n.raw_pitch_sequence new_sequence FROM baseline.event.event_pitch_sequence_status o JOIN fresh.event_pitch_sequence_status n USING(game_id,event_id) JOIN alignment a USING(game_id) WHERE a.aligned AND o.status<>n.status ORDER BY o.game_id,o.event_id",
        run / "pitch-status-transition-events.csv",
    )
    export(
        connection,
        "SELECT game_id,old_status,new_status,count(*) events FROM read_csv_auto("
        + literal(str(run / "pitch-status-transition-events.csv"))
        + ") GROUP BY ALL ORDER BY events DESC",
        run / "pitch-status-transition-games.csv",
    )
    coverage_rows = TableDiff(
        adapter=adapter,
        source="coverage_old",
        target="coverage_new",
        on=["game_id", "field"],
        limit=100,
    ).row_diff(temp_schema="sqlmesh_temp")
    save_json(run / "native-coverage.json", {"stats": coverage_rows.stats})
    cast(CsvWriter, coverage_rows.column_stats).to_csv(
        run / "native-coverage-columns.csv"
    )
    cast(CsvWriter, coverage_rows.s_sample).to_csv(
        run / "native-coverage-removed.csv", index=False
    )
    cast(CsvWriter, coverage_rows.t_sample).to_csv(
        run / "native-coverage-added.csv", index=False
    )
    cast(CsvWriter, coverage_rows.joined_sample).to_csv(
        run / "native-coverage-changed.csv", index=False
    )
    for schema, name in connection.execute(
        "SELECT table_schema,table_name FROM information_schema.tables WHERE table_catalog='baseline' AND table_schema IN ('event','game','box_score') ORDER BY 1,2"
    ).fetchall():
        diff = TableDiff(
            adapter=adapter,
            source=f"baseline.{schema}.{name}",
            target=f"fresh.{name}",
            on=["game_id"] if name != "event_flags" else ["event_key"],
        ).schema_diff()
        save_json(
            run / f"schema-{name}.json",
            {
                "old": {k: str(v) for k, v in diff.source_schema.items()},
                "new": {k: str(v) for k, v in diff.target_schema.items()},
            },
        )


def compare(baseline: Path, output: Path, game_limit: int | None) -> dict[str, object]:
    if game_limit is not None and game_limit < 1:
        raise ValueError("game-limit must be positive")
    validate_output(baseline, output)
    validate_frozen(output)
    before = stat_identity(baseline)
    run = output / (f"smoke-{game_limit}" if game_limit else "full")
    run.mkdir(parents=True, exist_ok=True)
    database = run / "comparison.duckdb"
    if database.resolve() == baseline.resolve():
        raise ValueError("Comparison database aliases baseline")
    if database.exists():
        database.unlink()
    c = duckdb.connect(str(database))
    c.execute("SET threads=2")
    c.execute("SET preserve_insertion_order=false")
    c.execute("SET memory_limit='4GB'")
    c.execute(f"ATTACH {literal(str(baseline.resolve()))} AS baseline (READ_ONLY)")
    tables = cast(
        list[tuple[str, str]],
        c.execute(
            "SELECT table_schema,table_name FROM information_schema.tables WHERE table_catalog='baseline' AND table_schema IN ('event','game','box_score') ORDER BY 1,2"
        ).fetchall(),
    )
    c.execute("CREATE SCHEMA fresh")
    inventory: list[dict[str, object]] = []
    for schema, name in tables:
        source = f"baseline.{schema}.{name}"
        fresh = f"fresh.{name}"
        c.execute(
            f"CREATE VIEW {fresh} AS SELECT * FROM read_parquet({literal(str(output / 'new' / (name + '.parquet')))})"
        )
        old_schema = c.execute(f"DESCRIBE {source}").fetchall()
        new_schema = c.execute(f"DESCRIBE {fresh}").fetchall()
        columns = [str(row[0]) for row in old_schema if row[0] not in EXCLUDED]
        new_columns = {str(row[0]) for row in new_schema}
        common = [field for field in columns if field in new_columns]
        args = ",".join(f"CAST({identifier(field)} AS VARCHAR)" for field in common)
        old_count, old_hash = c.execute(
            f"SELECT count(*),bit_xor(hash({args})) FROM {source}"
        ).fetchone() or (0, None)
        new_count, new_hash = c.execute(
            f"SELECT count(*),bit_xor(hash({args})) FROM {fresh}"
        ).fetchone() or (0, None)
        inventory.append(
            {
                "table": f"{schema}.{name}",
                "old_rows": old_count,
                "new_rows": new_count,
                "old_content_fingerprint": old_hash,
                "new_content_fingerprint": new_hash,
                "old_schema": old_schema,
                "new_schema": new_schema,
            }
        )
        LOGGER.info("Inventory %s: %s -> %s", name, old_count, new_count)
    save_json(run / "table-inventory.json", inventory)
    limit = f"LIMIT {game_limit}" if game_limit else ""
    c.execute(
        f"CREATE TABLE selected AS SELECT game_id FROM baseline.game.games WHERE account_type='PlayByPlay' ORDER BY game_id {limit}"
    )
    c.execute(
        "CREATE TABLE game_changes AS SELECT coalesce(o.game_id,n.game_id) game_id,year(coalesce(o.date,n.date)) AS year,o.account_type::VARCHAR old_account,n.account_type::VARCHAR new_account,o.game_type::VARCHAR old_type,n.game_type::VARCHAR new_type FROM baseline.game.games o FULL JOIN fresh.games n USING(game_id)"
    )
    export(c, "SELECT * FROM game_changes ORDER BY game_id", run / "game-changes.csv")
    export(
        c,
        "SELECT year,old_account,new_account,old_type,new_type,count(*) games FROM game_changes GROUP BY ALL ORDER BY ALL",
        run / "game-year-inventory.csv",
    )
    for side, source in [("old", "baseline.event.events"), ("new", "fresh.events")]:
        c.execute(
            f"CREATE TABLE {side}_events AS SELECT e.* FROM {source} e JOIN selected USING(game_id)"
        )
    build_alignment(c)
    export(c, "SELECT * FROM alignment ORDER BY game_id", run / "event-alignment.csv")
    c.execute(
        "CREATE TABLE paired AS SELECT o.game_id,o.event_id FROM old_events o JOIN new_events n USING(game_id,event_id) JOIN alignment a USING(game_id) WHERE a.aligned"
    )
    fields = [
        "batted_trajectory",
        "batted_to_fielder",
        "batted_location_general",
        "batted_location_depth",
        "batted_location_angle",
        "batted_contact_strength",
        "count_complete",
    ]
    c.execute(
        f"CREATE TABLE result_types AS SELECT * FROM read_csv_auto({literal(str(Path('bc/seeds/misc/seed_plate_appearance_result_types.csv').resolve()))})"
    )
    c.execute(
        "CREATE TABLE coverage (side VARCHAR,game_id VARCHAR,field VARCHAR,eligible BIGINT,known BIGINT)"
    )
    for field in fields:
        eligibility = (
            "e.plate_appearance_result IS NOT NULL AND NOT coalesce(e.no_play_flag,FALSE)"
            if field == "count_complete"
            else "coalesce(r.is_batted,FALSE) AND NOT coalesce(e.no_play_flag,FALSE)"
        )
        known = known_expression(field)
        for side in ["old", "new"]:
            c.execute(
                f"INSERT INTO coverage SELECT '{side}',game_id,{literal(field)},count(*) FILTER(WHERE {eligibility}),count(*) FILTER(WHERE {eligibility} AND ({known})) FROM {side}_events e LEFT JOIN result_types r USING(plate_appearance_result) GROUP BY game_id"
            )
    for field in [
        "start_time",
        "time_of_day",
        "sky",
        "field_condition",
        "precipitation",
        "wind_direction",
        "park_id",
        "temperature_fahrenheit",
        "attendance",
        "wind_speed_mph",
        "time_of_game_minutes",
        "scorer",
        "official_scorer",
        "source_scorer",
        "umpire_home_id",
    ]:
        known = known_expression(field)
        for side, source in [("old", "baseline.game.games"), ("new", "fresh.games")]:
            c.execute(
                f"INSERT INTO coverage SELECT '{side}',game_id,{literal('context.' + field)},1,CAST(coalesce(({known}),FALSE) AS INTEGER) FROM {source} JOIN selected USING(game_id)"
            )
    for side, prefix in [("old", "baseline.event"), ("new", "fresh")]:
        for table, field, condition in [
            (
                "event_fielding_play",
                "fielding_known_position",
                "fielding_position BETWEEN 1 AND 9",
            ),
            (
                "event_baserunners",
                "baserunner_known_outcome",
                "is_out=TRUE OR run_scored_flag=TRUE OR base_end IS NOT NULL",
            ),
            ("event_pitch_sequence_status", "pitch.Resolved", "status='Resolved'"),
            (
                "event_pitch_sequence_status",
                "pitch.Unavailable",
                "status='Unavailable'",
            ),
            ("event_pitch_sequence_status", "pitch.Unresolved", "status='Unresolved'"),
        ]:
            c.execute(
                f"INSERT INTO coverage SELECT '{side}',game_id,{literal(field)},count(*),count(*) FILTER(WHERE {condition}) FROM {prefix}.{table} JOIN selected USING(game_id) GROUP BY game_id"
            )
    c.execute(
        "CREATE TABLE coverage_game AS SELECT coalesce(o.game_id,n.game_id) game_id,coalesce(o.field,n.field) field,o.eligible old_eligible,n.eligible new_eligible,o.known old_known,n.known new_known,n.known-o.known known_delta,a.aligned FROM (SELECT * FROM coverage WHERE side='old') o FULL JOIN (SELECT * FROM coverage WHERE side='new') n USING(game_id,field) LEFT JOIN alignment a ON a.game_id=coalesce(o.game_id,n.game_id)"
    )
    export(
        c,
        "SELECT * FROM coverage_game ORDER BY game_id,field",
        run / "coverage-game.csv",
    )
    export(
        c,
        "SELECT year(g.date) AS year,field,aligned,sum(old_eligible) old_eligible,sum(new_eligible) new_eligible,sum(old_known) old_known,sum(new_known) new_known FROM coverage_game JOIN baseline.game.games g USING(game_id) GROUP BY ALL ORDER BY ALL",
        run / "coverage-year.csv",
    )
    export(
        c,
        "SELECT o.game_id,o.event_id,o.raw_play old_raw_play,n.raw_play new_raw_play,a.aligned FROM baseline.event.event_audit o JOIN fresh.event_audit n USING(game_id,event_id) JOIN selected USING(game_id) LEFT JOIN alignment a USING(game_id) WHERE a.aligned AND o.raw_play IS DISTINCT FROM n.raw_play",
        run / "raw-play-changes.csv",
    )
    alignment = c.execute(
        "SELECT aligned,count(*) FROM alignment GROUP BY aligned"
    ).fetchall()
    adapter = DuckDBEngineAdapter(lambda: c)
    c.execute("CREATE SCHEMA sqlmesh_temp")
    native: list[dict[str, object]] = []

    def native_diff(label: str, source: str, target: str, keys: list[str]) -> None:
        LOGGER.info("SQLMesh TableDiff %s", label)
        diff = TableDiff(
            adapter=adapter, source=source, target=target, on=keys, limit=100
        )
        schema = diff.schema_diff()
        rows = diff.row_diff(temp_schema="sqlmesh_temp")
        save_json(
            run / f"native-{label}.json",
            {
                "stats": rows.stats,
                "source_schema": {k: str(v) for k, v in schema.source_schema.items()},
                "target_schema": {k: str(v) for k, v in schema.target_schema.items()},
            },
        )
        cast(CsvWriter, rows.column_stats).to_csv(run / f"native-{label}-columns.csv")
        cast(CsvWriter, rows.joined_sample).to_csv(
            run / f"native-{label}-changed.csv", index=False
        )
        cast(CsvWriter, rows.s_sample).to_csv(
            run / f"native-{label}-removed.csv", index=False
        )
        cast(CsvWriter, rows.t_sample).to_csv(
            run / f"native-{label}-added.csv", index=False
        )
        native.append({"label": label, "keys": keys, "stats": rows.stats})

    for side in ["old", "new"]:
        c.execute(
            f"CREATE VIEW coverage_{side} AS SELECT game_id,field,eligible,known FROM coverage WHERE side='{side}'"
        )
    native_diff("coverage", "coverage_old", "coverage_new", ["game_id", "field"])
    for side in ["old", "new"]:
        columns = [
            str(r[0])
            for r in c.execute(f"DESCRIBE {side}_events").fetchall()
            if r[0] not in EXCLUDED
        ]
        projection = normalized_columns(columns)
        projection += "," + ",".join(
            f"CAST(({known_expression(field)}) AS BOOLEAN) AS {identifier(field + '_known')}"
            for field in fields
        )
        c.execute(
            f"CREATE VIEW aligned_{side} AS SELECT {projection} FROM {side}_events e JOIN alignment a USING(game_id) WHERE a.aligned"
        )
    native_diff("aligned-events", "aligned_old", "aligned_new", ["game_id", "event_id"])
    for schema, name in tables:
        old = f"baseline.{schema}.{name}"
        new = f"fresh.{name}"
        original_diff = TableDiff(
            adapter=adapter,
            source=old,
            target=new,
            on=["game_id"] if name != "event_flags" else ["event_key"],
        ).schema_diff()
        save_json(
            run / f"schema-{name}.json",
            {
                "old": {k: str(v) for k, v in original_diff.source_schema.items()},
                "new": {k: str(v) for k, v in original_diff.target_schema.items()},
            },
        )
        for side, source in [("old", old), ("new", new)]:
            columns = sorted(
                str(r[0])
                for r in c.execute(f"DESCRIBE {source}").fetchall()
                if r[0] not in EXCLUDED
            )
            if "game_id" not in columns:
                columns = sorted(["game_id", "event_id"] + columns)
                events = "baseline.event.events" if side == "old" else "fresh.events"
                source = f"(SELECT e.game_id,e.event_id,f.* EXCLUDE(event_key) FROM {source} f JOIN {events} e USING(event_key))"
            payload = (
                "to_json(struct_pack("
                + ",".join(
                    identifier(col) + ":=CAST(" + identifier(col) + " AS VARCHAR)"
                    for col in columns
                )
                + "))::VARCHAR"
            )
            c.execute(
                f"CREATE VIEW raw_{side} AS SELECT game_id,sha256({payload}) row_identity,count(*) multiplicity FROM {source} JOIN selected USING(game_id) GROUP BY ALL"
            )
        native_diff(name, "raw_old", "raw_new", ["game_id", "row_identity"])
        c.execute("DROP VIEW raw_old")
        c.execute("DROP VIEW raw_new")
    supplemental_reports(c, run)
    for item in native:
        artifact = run / ("native-" + str(item["label"]) + ".json")
        item["stats"] = json.loads(artifact.read_text())["stats"]
    c.close()
    validate_frozen(output)
    after = stat_identity(baseline)
    if before != after:
        raise RuntimeError("Baseline database identity changed during comparison")
    result: dict[str, object] = {
        "baseline_db": str(baseline.resolve()),
        "baseline_stat_before": before,
        "baseline_stat_after": after,
        "game_limit": game_limit,
        "alignment": alignment,
        "native_diffs": native,
        "seed_sha256": digest(
            Path("bc/seeds/misc/seed_plate_appearance_result_types.csv")
        ),
    }
    save_json(run / "summary.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-db", type=Path, required=True)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--source-version", default="")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--game-limit", type=int)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--supplement-existing", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    start = time.monotonic()
    validate_output(args.baseline_db, args.output_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    baseline = duckdb.connect(str(args.baseline_db), read_only=True)
    tables = cast(
        list[tuple[str, str]],
        baseline.execute(
            "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema IN ('event','game','box_score') ORDER BY 1,2"
        ).fetchall(),
    )
    baseline.close()
    freeze_sources(
        tables, args.source_root, args.source_version, args.output_dir, args.offline
    )
    if args.supplement_existing:
        before = stat_identity(args.baseline_db)
        run = args.output_dir / (
            f"smoke-{args.game_limit}" if args.game_limit else "full"
        )
        summary = validate_completed(
            args.baseline_db,
            run,
            Path("bc/seeds/misc/seed_plate_appearance_result_types.csv"),
        )
        connection = duckdb.connect(str(run / "comparison.duckdb"))
        connection.execute("SET threads=2")
        connection.execute("SET memory_limit='4GB'")
        connection.execute(
            f"ATTACH {literal(str(args.baseline_db.resolve()))} AS baseline (READ_ONLY)"
        )
        supplemental_reports(connection, run)
        connection.close()
        validate_frozen(args.output_dir)
        if before != stat_identity(args.baseline_db):
            raise RuntimeError("Baseline changed during supplemental report")
        native_diffs = cast(list[dict[str, object]], summary["native_diffs"])
        for item in native_diffs:
            artifact = run / ("native-" + str(item["label"]) + ".json")
            item["stats"] = json.loads(artifact.read_text())["stats"]
        summary["seed_sha256"] = digest(
            Path("bc/seeds/misc/seed_plate_appearance_result_types.csv")
        )
        summary["supplemental_integrity"] = {
            "baseline_stat": stat_identity(args.baseline_db),
            "source_manifest_sha256": digest(args.output_dir / "source-manifest.json"),
            "script_sha256": digest(Path(__file__)),
            "seed_sha256": digest(
                Path("bc/seeds/misc/seed_plate_appearance_result_types.csv")
            ),
            "runtime_seconds": time.monotonic() - start,
        }
        save_json(run / "summary.json", summary)
    else:
        compare(args.baseline_db, args.output_dir, args.game_limit)
    LOGGER.info("Completed in %.1f seconds", time.monotonic() - start)


if __name__ == "__main__":
    main()
