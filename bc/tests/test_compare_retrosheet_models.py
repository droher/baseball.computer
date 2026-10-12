from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import cast

import duckdb
import pytest

from scripts.compare_retrosheet_models import run


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def stat_identity(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "inode": stat.st_ino}


def side(
    path: Path, retrosheet: str, fingerprints: str = "code", seeds: str = "seeds"
) -> dict[str, object]:
    return {
        "db": str(path.resolve()),
        "identity": stat_identity(path),
        "retrosheet_sources_sha256": retrosheet,
        "other_sources_sha256": "other",
        "fingerprints_sha256": fingerprints,
        "seeds_sha256": seeds,
    }


def write_candidates(
    tmp_path: Path, sides: dict[str, dict[str, object]], release: str = "capture"
) -> Path:
    path = tmp_path / "candidates.json"
    path.write_text(json.dumps({"release_manifest_sha256": release, "sides": sides}))
    return path


def compare(
    candidates: Path,
    alignment: Path | None,
    output: Path,
    tables: list[str],
    old: str = "prod",
    new: str = "B",
) -> dict[str, object]:
    return run(candidates, old, new, alignment, output, tables)


def capture(
    tmp_path: Path, coverage_field: tuple[str, str | int | bool] | None = None
) -> tuple[Path, Path, Path, Path]:
    baseline = tmp_path / "old.db"
    candidate = tmp_path / "new.db"
    for path, changed in ((baseline, False), (candidate, True)):
        connection = duckdb.connect()
        connection.execute(f"ATTACH {literal(str(path))} AS bc")
        connection.execute("CREATE SCHEMA bc.main_models")
        connection.execute("CREATE SCHEMA bc.game")
        connection.execute("CREATE SCHEMA bc.event")
        connection.execute("CREATE SCHEMA bc.sqlmesh__main_models")
        connection.execute(
            "CREATE TABLE bc.main_models.game_start_info AS SELECT * FROM (VALUES ('A',2000,FALSE),('B',2000,FALSE),('N',2000,TRUE),('D',2000,FALSE)) t(game_id,season,is_negro_leagues)"
        )
        connection.execute(
            "CREATE TABLE bc.game.games AS SELECT game_id,CASE WHEN game_id='D' THEN 'Deduced' ELSE 'PlayByPlay' END account_type FROM bc.main_models.game_start_info"
        )
        connection.execute(
            "CREATE TABLE bc.event.events AS SELECT game_id FROM bc.main_models.game_start_info"
        )
        key = 100 if changed else 1
        connection.execute(
            f"CREATE TABLE bc.main_models.event_states_full AS SELECT game_id,1 event_id,row_number() OVER()+{key} event_key FROM bc.main_models.game_start_info"
        )
        connection.execute(
            f"CREATE TABLE bc.sqlmesh__main_models.details AS SELECT game_id,event_key,1 fielding_position,'p' player_id, {2 if changed else 1} putouts FROM bc.main_models.event_states_full"
        )
        connection.execute(
            "CREATE VIEW bc.main_models.event_player_fielding_stats AS SELECT * FROM bc.sqlmesh__main_models.details"
        )
        connection.execute(
            f"CREATE TABLE bc.main_models.player_game_offense_stats AS SELECT game_id,'p' player_id,'T' team_id,{2 if changed else 1} hits,{0.5000000000000001 if changed else 0.5} batting_average,CAST({'0.3' if changed else 'NULL'} AS DOUBLE) on_base_percentage FROM bc.main_models.game_start_info"
        )
        connection.execute(
            f"CREATE TABLE bc.main_models.game_data_completeness AS SELECT game_id,{str(changed).upper()} has_count,FALSE has_pitches FROM bc.main_models.game_start_info"
        )
        connection.execute(
            "CREATE TABLE bc.main_models.game_line_scores AS SELECT game_id,CAST(9007199254740993 AS HUGEINT) away_runs_scored,CAST(2 AS HUGEINT) home_runs_scored,[CAST(9007199254740993 AS HUGEINT)] inning_runs,map([1],[CAST(9007199254740993 AS HUGEINT)]) inning_map,struct_pack(v:=CAST(9007199254740993 AS HUGEINT)) inning_struct FROM bc.main_models.game_start_info"
        )
        if coverage_field is not None:
            field, value = coverage_field
            connection.execute(
                f'CREATE OR REPLACE TABLE bc.main_models.game_data_completeness AS SELECT game_id,? AS "{field}" FROM bc.main_models.game_start_info',
                [value],
            )
        if changed:
            connection.execute(
                "ALTER TABLE bc.sqlmesh__main_models.details ADD COLUMN source_annotation VARCHAR DEFAULT 'recovered'"
            )
        connection.close()
    alignment = tmp_path / "event-alignment.csv"
    alignment.write_text("game_id,aligned\nA,true\nB,false\nN,true\nD,true\n")
    identity = stat_identity(baseline)
    (tmp_path / "summary.json").write_text(
        json.dumps(
            {
                "baseline_db": str(baseline.resolve()),
                "baseline_stat_after": identity,
                "supplemental_integrity": {"source_manifest_sha256": "capture"},
            }
        )
    )
    candidates = write_candidates(
        tmp_path,
        {
            "prod": side(baseline, "prod-sources"),
            "A": {"same_as": "prod"},
            "B": side(candidate, "release-sources"),
        },
    )
    return baseline, candidate, alignment, candidates


def test_existing_scope_completeness_gain_and_separate_candidate_views(
    tmp_path: Path,
) -> None:
    _, _, alignment, candidates = capture(tmp_path)
    output = tmp_path / "results"
    summary = compare(
        candidates,
        alignment,
        output,
        ["game_data_completeness", "event_player_fielding_stats"],
    )
    assert summary["cohort"] == [(False, 1), (True, 1)]
    fields = json.loads(
        (output / "game_data_completeness" / "coverage.json").read_text()
    )
    count = next(field for field in fields if field["field"] == "has_count")
    assert count["old_known"] == 0
    assert count["new_known"] == 2
    assert count["gained"] == 2
    native = json.loads(
        (output / "event_player_fielding_stats" / "native.json").read_text()
    )["stats"]
    assert native["join_count"] == 1
    assert native["putouts_matches"] == 0
    assert native["s_only_count"] == native["t_only_count"] == 0
    coverage = json.loads(
        (output / "event_player_fielding_stats" / "coverage.json").read_text()
    )
    assert (
        next(field for field in coverage if field["field"] == "putouts")["old_rows"]
        == 2
    )
    annotation = next(
        field for field in coverage if field["field"] == "source_annotation"
    )
    assert annotation["old_column_present"] is False
    assert annotation["old_known"] == 0
    assert annotation["new_known"] == 2
    assert annotation["gained"] == 1
    assert json.loads((output / "integrity.json").read_text())["unchanged"] is True


@pytest.mark.parametrize(
    "field,value,expected",
    [
        ("has_count", False, False),
        ("has_count", True, True),
        ("trajectory_known", 0, False),
        ("trajectory_known", 1, True),
        ("count_balls", 4, False),
        ("count_strikes", 2, True),
        ("trajectory", "Unknown", False),
        ("trajectory", "Fly", True),
        ("is_out", False, True),
    ],
)
def test_coverage_values(
    tmp_path: Path, field: str, value: str | int | bool, expected: bool
) -> None:
    _, _, alignment, candidates = capture(tmp_path, (field, value))
    output = tmp_path / "results"
    compare(candidates, alignment, output, ["game_data_completeness"])
    fields = json.loads(
        (output / "game_data_completeness" / "coverage.json").read_text()
    )
    coverage = next(item for item in fields if item["field"] == field)
    assert coverage["old_known"] == (2 if expected else 0)
    assert coverage["new_known"] == (2 if expected else 0)


def test_parquet_transport_preserves_hugeint_values(tmp_path: Path) -> None:
    _, _, alignment, candidates = capture(tmp_path)
    output = tmp_path / "results"
    result = compare(candidates, alignment, output, ["game_line_scores"])
    tables = cast(list[dict[str, object]], result["tables"])
    stats = cast(dict[str, object], tables[0]["stats"])
    assert stats["full_match_count"] == 2
    schema = json.loads((output / "game_line_scores" / "schema.json").read_text())
    for field in ("away_runs_scored", "inning_runs", "inning_map", "inning_struct"):
        assert schema["old"][field] == schema["transport_restored_schema"][field]


@pytest.mark.parametrize(
    ("fingerprints", "seeds", "retrosheet", "label"),
    [
        ("code", "seeds", "release-sources", "Source change only"),
        ("other-code", "seeds", "prod-sources", "Code change only"),
        ("code", "other-seeds", "prod-sources", "Code change only"),
        ("other-code", "seeds", "release-sources", "Code and source change"),
    ],
)
def test_attribution_follows_recorded_code_and_source_identity(
    tmp_path: Path, fingerprints: str, seeds: str, retrosheet: str, label: str
) -> None:
    baseline, candidate, alignment, _ = capture(tmp_path)
    candidates = write_candidates(
        tmp_path,
        {
            "prod": side(baseline, "prod-sources"),
            "A": {"same_as": "prod"},
            "B": side(candidate, retrosheet, fingerprints, seeds),
        },
    )
    result = compare(
        candidates, alignment, tmp_path / "results", ["game_data_completeness"]
    )
    assert str(result["attribution"]).startswith(label)


def test_same_sources_need_no_alignment_and_align_every_game(tmp_path: Path) -> None:
    baseline, candidate, _, _ = capture(tmp_path)
    candidates = write_candidates(
        tmp_path,
        {
            "prod": side(baseline, "prod-sources"),
            "A": side(candidate, "prod-sources", "other-code"),
            "B": side(candidate, "release-sources", "other-code"),
        },
    )
    result = compare(
        candidates, None, tmp_path / "results", ["game_data_completeness"], new="A"
    )
    assert result["cohort"] == [(True, 2)]
    assert str(result["attribution"]).startswith("Code change only")


@pytest.mark.parametrize(
    ("summary_change", "message"),
    [
        ({"baseline_stat_after": {"size": 0}}, "Alignment baseline differs"),
        (
            {"supplemental_integrity": {"source_manifest_sha256": "other"}},
            "Alignment release manifest differs",
        ),
    ],
)
def test_alignment_must_match_prod_and_release(
    tmp_path: Path, summary_change: dict[str, object], message: str
) -> None:
    _, _, alignment, candidates = capture(tmp_path)
    summary_path = tmp_path / "summary.json"
    summary = json.loads(summary_path.read_text())
    summary.update(summary_change)
    summary_path.write_text(json.dumps(summary))
    with pytest.raises(ValueError, match=message):
        compare(candidates, alignment, tmp_path / "results", ["game_data_completeness"])


def test_different_sources_require_alignment(tmp_path: Path) -> None:
    _, _, _, candidates = capture(tmp_path)
    with pytest.raises(ValueError, match="pass --alignment"):
        compare(candidates, None, tmp_path / "results", ["game_data_completeness"])


def test_missing_alignment_is_refused_before_output_is_created(
    tmp_path: Path,
) -> None:
    _, _, alignment, candidates = capture(tmp_path)
    alignment.unlink()
    output = tmp_path / "results"
    with pytest.raises(ValueError, match="does not exist"):
        compare(candidates, alignment, output, ["game_data_completeness"])
    assert not output.exists()


def test_old_side_must_read_prod_sources(tmp_path: Path) -> None:
    _, _, alignment, candidates = capture(tmp_path)
    with pytest.raises(ValueError, match="Old side must be prod or A"):
        compare(
            candidates,
            alignment,
            tmp_path / "results",
            ["game_data_completeness"],
            old="B",
            new="A",
        )


def test_duplicate_stable_event_grain_rejected(tmp_path: Path) -> None:
    baseline, candidate, alignment, _ = capture(tmp_path)
    connection = duckdb.connect()
    connection.execute(f"ATTACH {literal(str(candidate))} AS bc")
    connection.execute(
        "INSERT INTO bc.sqlmesh__main_models.details SELECT * FROM bc.sqlmesh__main_models.details WHERE game_id='A'"
    )
    connection.close()
    candidates = write_candidates(
        tmp_path,
        {
            "prod": side(baseline, "prod-sources"),
            "A": {"same_as": "prod"},
            "B": side(candidate, "release-sources"),
        },
    )
    with pytest.raises(ValueError, match="Nonunique or null stable key"):
        compare(
            candidates, alignment, tmp_path / "results", ["event_player_fielding_stats"]
        )


def test_changed_candidate_rejected_against_recorded_identity(tmp_path: Path) -> None:
    _, candidate, alignment, candidates = capture(tmp_path)
    connection = duckdb.connect()
    connection.execute(f"ATTACH {literal(str(candidate))} AS bc")
    connection.execute("UPDATE bc.sqlmesh__main_models.details SET putouts=9")
    connection.close()
    with pytest.raises(ValueError, match="Database changed since the candidates"):
        compare(candidates, alignment, tmp_path / "results", ["game_data_completeness"])


def test_player_season_counts_use_existing_game_cohort_and_exclude_rates(
    tmp_path: Path,
) -> None:
    _, _, alignment, candidates = capture(tmp_path)
    output = tmp_path / "results"
    compare(candidates, alignment, output, ["player_game_offense_stats"])
    with (
        output / "player_game_offense_stats" / "player-season-count-changes.csv"
    ).open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 1
    assert rows[0]["field"] == "hits"
    assert float(rows[0]["old_value"]) == 2
    assert float(rows[0]["new_value"]) == 4
    assert float(rows[0]["delta"]) == 2
    with (
        output / "player_game_offense_stats" / "numeric-magnitude.csv"
    ).open() as stream:
        magnitude = {row["field"]: row for row in csv.DictReader(stream)}
    assert int(magnitude["batting_average"]["changes_at_most_1e_9"]) == 2
    assert int(magnitude["batting_average"]["changes_above_1e_9"]) == 0
    assert int(magnitude["hits"]["changes_above_1e_9"]) == 2
    assert int(magnitude["on_base_percentage"]["null_transitions"]) == 2
    for row in magnitude.values():
        assert int(row["exact_changes"]) == sum(
            int(row[bucket])
            for bucket in (
                "null_transitions",
                "changes_at_most_1e_9",
                "changes_above_1e_9",
            )
        )
