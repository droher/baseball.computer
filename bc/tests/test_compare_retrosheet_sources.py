import json
from pathlib import Path

import duckdb
import pytest
from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter
from sqlmesh.core.table_diff import TableDiff

from scripts.compare_retrosheet_sources import (
    build_alignment,
    digest,
    freeze_sources,
    known_expression,
    normalized_columns,
    stat_identity,
    validate_output,
    validate_completed,
)


def test_enum_and_string_events_align_but_changed_detail_is_detected() -> None:
    connection = duckdb.connect()
    connection.execute("CREATE TYPE frame_type AS ENUM('Top','Bottom')")
    connection.execute("CREATE TYPE trajectory AS ENUM('Unknown','Fly')")
    connection.execute(
        "CREATE TABLE old_events AS SELECT 'A' game_id,1 event_id,1 inning,'Top'::frame_type frame,'b' batter_id,'p' pitcher_id,'Single' plate_appearance_result,FALSE no_play_flag,0 outs,0 base_state,'Unknown'::trajectory batted_trajectory"
    )
    connection.execute(
        "CREATE TABLE new_events AS SELECT * REPLACE('Top'::VARCHAR AS frame,'Fly'::VARCHAR AS batted_trajectory) FROM old_events"
    )
    build_alignment(connection)
    assert connection.execute("SELECT aligned FROM alignment").fetchone() == (True,)
    for side in ["old", "new"]:
        connection.execute(
            f"CREATE VIEW normalized_{side} AS SELECT {normalized_columns(['game_id', 'event_id', 'batted_trajectory'])} FROM {side}_events e"
        )
    connection.execute("CREATE SCHEMA temp_diff")
    rows = TableDiff(
        adapter=DuckDBEngineAdapter(lambda: connection),
        source="normalized_old",
        target="normalized_new",
        on=["game_id", "event_id"],
    ).row_diff(temp_schema="temp_diff")
    assert rows.stats["join_count"] == 1
    assert rows.stats["batted_trajectory_matches"] == 0
    connection.close()


def test_edited_core_and_duplicate_events_do_not_align() -> None:
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE old_events AS SELECT 'A' game_id,1 event_id,1 inning,'Top' frame,'b' batter_id,'p' pitcher_id,'Single' plate_appearance_result,FALSE no_play_flag,0 outs,0 base_state"
    )
    connection.execute(
        "CREATE TABLE new_events AS SELECT * REPLACE('q' AS pitcher_id) FROM old_events UNION ALL SELECT * REPLACE('q' AS pitcher_id) FROM old_events"
    )
    build_alignment(connection)
    assert connection.execute("SELECT aligned FROM alignment").fetchone() == (False,)
    connection.close()


@pytest.mark.parametrize(
    "field,value,expected",
    [
        ("batted_location_depth", "Default", False),
        ("batted_location_depth", "Unknown", False),
        ("batted_location_depth", "Deep", True),
        ("attendance", 0, False),
        ("batted_to_fielder", 0, False),
        ("batted_to_fielder", 9, True),
    ],
)
def test_known_boundaries(field: str, value: str | int, expected: bool) -> None:
    connection = duckdb.connect()
    assert connection.execute(
        f'SELECT {known_expression(field)} FROM (SELECT ? AS "{field}")', [value]
    ).fetchone() == (expected,)
    connection.close()


def test_offline_cache_rejects_changed_bytes(tmp_path: Path) -> None:
    cache = tmp_path / "new"
    cache.mkdir()
    parquet = cache / "games.parquet"
    parquet.write_bytes(b"first")
    (cache / "games.headers").write_text("etag: frozen\n")
    (tmp_path / "source-manifest.json").write_text(
        json.dumps(
            {
                "source_root": "https://example.test/event",
                "source_version": "v1",
                "files": {"game.games": {"sha256": digest(parquet)}},
            }
        )
    )
    parquet.write_bytes(b"changed")
    with pytest.raises(ValueError, match="Frozen cache altered"):
        freeze_sources(
            [("game", "games")], "https://example.test/event", "v1", tmp_path, True
        )


def test_baseline_cannot_be_inside_output_or_symlink_destination(
    tmp_path: Path,
) -> None:
    baseline = tmp_path / "full" / "summary.json"
    with pytest.raises(ValueError, match="outside"):
        validate_output(baseline, tmp_path)
    output = tmp_path / "output"
    output.mkdir()
    (output / "summary.json").symlink_to(tmp_path / "baseline.db")
    with pytest.raises(ValueError, match="symlinks"):
        validate_output(tmp_path / "baseline.db", output)


def test_output_hardlink_cannot_alias_baseline(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.db"
    baseline.write_bytes(b"baseline")
    output = tmp_path / "output"
    output.mkdir()
    (output / "summary.json").hardlink_to(baseline)
    with pytest.raises(ValueError, match="hardlink"):
        validate_output(baseline, output)
    assert baseline.read_bytes() == b"baseline"


@pytest.mark.parametrize(
    "is_out,base_end,expected",
    [(True, None, True), (False, None, False), (False, "Second", True)],
)
def test_outs_do_not_need_safe_destination(
    is_out: bool, base_end: str | None, expected: bool
) -> None:
    connection = duckdb.connect()
    assert connection.execute(
        f"SELECT {known_expression('baserunner_known_outcome')} FROM (SELECT ?::BOOLEAN is_out,FALSE run_scored_flag,?::VARCHAR base_end)",
        [is_out, base_end],
    ).fetchone() == (expected,)
    connection.close()


def test_scored_run_has_outcome_without_safe_destination() -> None:
    connection = duckdb.connect()
    assert connection.execute(
        f"SELECT {known_expression('baserunner_known_outcome')} FROM (SELECT FALSE is_out,TRUE run_scored_flag,NULL::VARCHAR base_end)"
    ).fetchone() == (True,)
    connection.close()


def test_supplement_requires_matching_completed_baseline(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.db"
    baseline.write_bytes(b"baseline")
    run = tmp_path / "run"
    run.mkdir()
    with pytest.raises(ValueError, match="completed"):
        validate_completed(baseline, run)
    (run / "summary.json").write_text(
        json.dumps(
            {
                "baseline_db": str(baseline.resolve()),
                "baseline_stat_after": {"size": 0, "mtime_ns": 0, "inode": 0},
            }
        )
    )
    with pytest.raises(ValueError, match="differs"):
        validate_completed(baseline, run)


def test_supplement_rejects_changed_seed(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.db"
    baseline.write_bytes(b"baseline")
    run = tmp_path / "run"
    run.mkdir()
    seed = tmp_path / "seed.csv"
    seed.write_bytes(b"initial")
    (run / "summary.json").write_text(
        json.dumps(
            {
                "baseline_db": str(baseline.resolve()),
                "baseline_stat_after": stat_identity(baseline),
                "seed_sha256": digest(seed),
            }
        )
    )
    seed.write_bytes(b"changed")
    with pytest.raises(ValueError, match="seed differs"):
        validate_completed(baseline, run, seed)
