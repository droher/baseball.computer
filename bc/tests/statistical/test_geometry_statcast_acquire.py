from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import duckdb
import polars as pl
import pytest

from python_models.statistical.backtests.geometry_reliability_data import SCHEMA
from python_models.statistical.backtests.geometry_statcast_acquire import (
    run,
    validate_smoke_acceptance,
)
from python_models.statistical.backtests.geometry_statcast_bridge import SelectedGame
from python_models.statistical.backtests.geometry_statcast_bridge_selection import (
    build_selection,
    game_digest,
)
from python_models.statistical.evidence_binding import file_digest

GAMES = ("BOS201605010", "BOS201605020", "BOS201605030")


def geometry_rows(games: tuple[str, ...], *, fold: str = "TRAIN") -> pl.DataFrame:
    rows = [
        {
            "event_key": index * 10 + offset,
            "game_id": game,
            "class": "Fly",
            "raw_value": "Fly",
            "is_observed_class": True,
            "observed_status": "observed",
            "source_family": "retrosheet",
            "source_acquisition_status": "acquired",
            "model_input_eligible": True,
            "data_error_risk": "low",
            "season": int(game[3:7]),
            "park_id": "BOS07",
            "scorer": "scorer",
            "batter_id": f"batter{offset}",
            "pitcher_id": "pitcher",
            "inning_start": 1,
            "frame_start": "Top",
            "base_state_start": 0,
            "outs_start": 0,
            "batter_hand": "R",
            "pitcher_hand": "R",
            "result_family": "out",
            "plate_appearance_result": "InPlayOut",
            "batted_to_fielder": 8,
            "outs_on_play": 1,
            "fielder_chain": "8",
            "primary_fold": fold,
            "source_snapshot_id": "snapshot",
            "geometry_target_contract": "contract",
            "geometry_dimension": "trajectory",
            "game_type": "RegularSeason",
        }
        for index, game in enumerate(games)
        for offset in (1, 2)
    ]
    return pl.DataFrame(rows).with_columns(
        pl.col("event_key").cast(pl.UInt32), pl.col("season").cast(pl.Int16)
    )


def event_rows(games: tuple[str, ...]) -> pl.DataFrame:
    rows = [
        {
            "game_id": game,
            "event_id": offset,
            "event_key": index * 10 + offset,
            "outs": 0,
            "plate_appearance_result": "InPlayOut",
            "outs_on_play": 1,
            "no_play_flag": False,
        }
        for index, game in enumerate(games)
        for offset in (1, 2)
    ]
    return pl.DataFrame(rows).with_columns(pl.col("event_key").cast(pl.UInt32))


def game_rows(games: tuple[str, ...]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "game_id": game,
                "game_key": index,
                "season": int(game[3:7]),
                "park_id": "BOS07",
                "date": f"{game[3:7]}-{game[7:9]}-{game[9:11]}",
                "start_time": None,
                "home_team_id": "BOS",
                "away_team_id": "NYA",
                "doubleheader_status": "SingleGame",
                "game_type": "RegularSeason",
                "account_type": "PlayByPlay",
                "source_type": "PlayByPlay",
                "filename": "2016BOS.EVA",
            }
            for index, game in enumerate(games)
        ]
    ).with_columns(
        pl.col("game_key").cast(pl.UInt32),
        pl.col("season").cast(pl.Int16),
        pl.col("date").str.to_date(),
        pl.col("start_time").cast(pl.Datetime),
    )


def build_database(path: Path, games: tuple[str, ...]) -> None:
    geometry = geometry_rows(games)
    events = event_rows(games)
    game_table = game_rows(games)
    with duckdb.connect(str(path)) as connection:
        _ = connection.register("geometry", geometry)
        _ = connection.register("events", events)
        _ = connection.register("game_table", game_table)
        for statement in (
            f"CREATE SCHEMA {SCHEMA}",
            "CREATE SCHEMA main_models",
            f"CREATE TABLE {SCHEMA}.model_input_geometry AS SELECT * FROM geometry",
            "CREATE TABLE main_models.stg_events AS SELECT * FROM events",
            "CREATE TABLE main_models.stg_games AS SELECT * FROM game_table",
        ):
            _ = connection.execute(statement)


def selection_frame(games: tuple[str, ...]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "game_id": game,
                "season": int(game[3:7]),
                "park_id": "BOS07",
                "date": f"{game[3:7]}-{game[7:9]}-{game[9:11]}",
                "home_team_id": "BOS",
                "away_team_id": "NYA",
                "primary_fold": "TRAIN",
                "acquisition_fold": "fitting",
                "is_smoke": True,
            }
            for game in games
        ]
    )


@dataclass
class Fixture:
    repository: Path
    database: Path
    reserve: Path
    crosswalk_root: Path
    inputs: Path
    inputs_content: dict[str, object]


def build_fixture(tmp_path: Path) -> Fixture:
    repository = tmp_path / "repo"
    docs = repository / "docs"
    docs.mkdir(parents=True)
    amendment = docs / "geometry-statcast-matching-amendment.md"
    amendment.write_text("amendment\n")
    development_protocol = docs / "geometry-statcast-development-protocol.md"
    development_protocol.write_text("development protocol\n")
    protocol = docs / "bridge-protocol.md"
    protocol.write_text("bridge protocol\n")
    selection = repository / "selection"
    selection.mkdir()
    selection_frame(GAMES).write_parquet(selection / "selected_games.parquet")
    (selection / "manifest.json").write_text(
        json.dumps(
            {
                "files_sha256": {
                    "selected_games.parquet": file_digest(
                        selection / "selected_games.parquet"
                    )
                }
            }
        )
    )
    reserve = tmp_path / "reserve.parquet"
    pl.DataFrame({"game_id": ["ZZZ201601010"]}).write_parquet(reserve)
    crosswalk_root = tmp_path / "crosswalk"
    crosswalk_root.mkdir()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "register/data/people-0.csv", "key_retro,key_mlbam\nbatter1,100\n"
        )
    (crosswalk_root / "bc-chadwick-register.zip").write_bytes(buffer.getvalue())
    (crosswalk_root / "bc-chadwick-commit.json").write_text(json.dumps({"sha": "abc"}))
    database = tmp_path / "bc.db"
    build_database(database, GAMES)
    inputs_content: dict[str, object] = {
        "protocol_path": "docs/bridge-protocol.md",
        "protocol_sha256": file_digest(protocol),
        "development_protocol_sha256": file_digest(development_protocol),
        "matching_amendment_sha256": file_digest(amendment),
        "selection_root": "selection",
        "selected_games_sha256": file_digest(selection / "selected_games.parquet"),
        "manifest_sha256": file_digest(selection / "manifest.json"),
        "reserve_file_sha256": file_digest(reserve),
        "chadwick_zip_sha256": file_digest(crosswalk_root / "bc-chadwick-register.zip"),
        "chadwick_commit": "abc",
    }
    inputs = docs / "inputs.json"
    inputs.write_text(json.dumps(inputs_content))
    return Fixture(
        repository, database, reserve, crosswalk_root, inputs, inputs_content
    )


@dataclass
class StubAcquire:
    statuses: dict[str, str]
    calls: list[str] = field(default_factory=list)

    def __call__(
        self,
        game: SelectedGame,
        local: pl.DataFrame,
        crosswalk: dict[str, set[str]],
        output: Path,
        cache: Path | None,
    ) -> dict[str, object]:
        self.calls.append(game.game_id)
        root = output / "games" / game.game_id
        root.mkdir(parents=True, exist_ok=False)
        status = self.statuses[game.game_id]
        report: dict[str, object] = {
            "game": game.model_dump(),
            "status": status,
            "local_events": local.height,
            "matched_angle_available": local.height if status == "paired" else 0,
        }
        if status != "failed":
            local.select("event_key", "game_id").write_parquet(
                root / "paired_events.parquet"
            )
        (root / "report.json").write_text(json.dumps(report))
        return report


def run_fixture(
    fixture: Fixture, output: Path, acquire: StubAcquire, *, inputs: Path | None = None
) -> None:
    run(
        output,
        smoke=True,
        cache=None,
        inputs_path=inputs or fixture.inputs,
        resume=True,
        repository=fixture.repository,
        database=fixture.database,
        reserve=fixture.reserve,
        crosswalk_root=fixture.crosswalk_root,
        acquire=acquire,
    )


def read(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def resumed_stamps(output: Path) -> list[str]:
    stamps = cast(list[str], read(output / "plan.json")["resumed_at"])
    return [str(stamp) for stamp in stamps]


def test_resume_continues_only_unfinished_games_and_keeps_the_plan(
    tmp_path: Path,
) -> None:
    fixture = build_fixture(tmp_path)
    output = tmp_path / "out"
    output.mkdir()
    first = StubAcquire({GAMES[0]: "paired", GAMES[1]: "failed", GAMES[2]: "paired"})
    run_fixture(fixture, output, first)
    assert sorted(first.calls) == list(GAMES)
    report = read(output / "report.json")
    assert (report["games_paired_complete"], report["games_failed"]) == (2, 1)
    plan = read(output / "plan.json")
    assert "resumed_at" not in plan
    assert plan["games"] == 3

    (output / "games" / GAMES[2] / "report.json").write_text("{")
    (output / "games" / GAMES[2] / "paired_events.parquet").unlink()
    foreign = output / "games" / "XXX201601010"
    foreign.mkdir()
    (foreign / "report.json").write_text(json.dumps({"status": "paired"}))
    pl.DataFrame({"event_key": [99], "game_id": ["XXX201601010"]}).write_parquet(
        foreign / "paired_events.parquet"
    )
    second = StubAcquire({GAMES[2]: "paired"})
    run_fixture(fixture, output, second)
    assert second.calls == [GAMES[2]]
    report = read(output / "report.json")
    assert (report["games_paired_complete"], report["games_failed"]) == (2, 1)
    assert report["games_planned"] == 3
    assert len(cast(list[object], report["reports"])) == 3
    resumed = read(output / "plan.json")
    assert resumed.pop("resumed_at") == resumed_stamps(output)
    assert len(resumed_stamps(output)) == 1
    assert resumed == plan
    paired = pl.read_parquet(output / "paired_events.parquet")
    assert set(paired["game_id"].to_list()) == {GAMES[0], GAMES[2]}
    assert read(output / "progress.json")["completed_games"] == 3


def test_resume_refuses_changed_inputs(tmp_path: Path) -> None:
    fixture = build_fixture(tmp_path)
    output = tmp_path / "out"
    run_fixture(fixture, output, StubAcquire(dict.fromkeys(GAMES, "paired")))
    changed = fixture.inputs.with_name("changed.json")
    changed.write_text(json.dumps({**fixture.inputs_content, "note": "changed"}))
    with pytest.raises(ValueError, match="differ from the stored plan"):
        run_fixture(fixture, output, StubAcquire({}), inputs=changed)


def test_protocol_paths_stay_inside_the_repository_and_bind_both_protocols(
    tmp_path: Path,
) -> None:
    fixture = build_fixture(tmp_path)
    escaped = fixture.inputs.with_name("escaped.json")
    escaped.write_text(
        json.dumps(
            {**fixture.inputs_content, "protocol_path": str(fixture.inputs.parent)}
        )
    )
    with pytest.raises(ValueError, match="inside the repository"):
        run_fixture(fixture, tmp_path / "escaped", StubAcquire({}), inputs=escaped)
    development = fixture.repository / "docs/geometry-statcast-development-protocol.md"
    development.write_text("edited\n")
    with pytest.raises(ValueError, match="development protocol changed"):
        run_fixture(fixture, tmp_path / "edited", StubAcquire({}))


def smoke_artifact(root: Path, games: tuple[str, ...], *, angles: int) -> None:
    root.mkdir()
    reports = [
        {
            "game": {"game_id": game},
            "game_pk": index + 1,
            "status": "paired",
            "local_events": 10,
            "matched_angle_available": angles,
        }
        for index, game in enumerate(games)
    ]
    (root / "report.json").write_text(
        json.dumps({"reports": reports, "modern_angle_evaluation_acquired": False})
    )
    (root / "manifest.json").write_text(
        json.dumps({"files_sha256": {"report.json": file_digest(root / "report.json")}})
    )


def test_smoke_acceptance_checks_games_and_gates(tmp_path: Path) -> None:
    gates: dict[str, object] = {
        "angle_availability_overall_minimum": 0.95,
        "complete_one_to_one_batted_ball_matches": True,
        "resolved_games_required": 3,
    }
    accepted = tmp_path / "accepted"
    smoke_artifact(accepted, GAMES, angles=10)
    digest = file_digest(accepted / "manifest.json")
    validate_smoke_acceptance(
        accepted,
        expected_manifest_sha256=digest,
        smoke_games=set(GAMES),
        gates=gates,
    )
    with pytest.raises(ValueError, match="smoke acceptance failed"):
        validate_smoke_acceptance(
            accepted,
            expected_manifest_sha256=digest,
            smoke_games=set(GAMES[:2]),
            gates=gates,
        )
    with pytest.raises(ValueError, match="manifest differs"):
        validate_smoke_acceptance(
            accepted,
            expected_manifest_sha256="other",
            smoke_games=set(GAMES),
            gates=gates,
        )
    sparse = tmp_path / "sparse"
    smoke_artifact(sparse, GAMES, angles=9)
    with pytest.raises(ValueError, match="smoke acceptance failed"):
        validate_smoke_acceptance(
            sparse,
            expected_manifest_sha256=file_digest(sparse / "manifest.json"),
            smoke_games=set(GAMES),
            gates=gates,
        )


def test_build_selection_excludes_the_reserve_and_writes_bound_files(
    tmp_path: Path,
) -> None:
    kept = tuple(
        f"BOS{season}05{day:02d}0" for season in (2016, 2017, 2018) for day in (1, 2, 3)
    )
    games = (*kept, "BOS201605040")
    database = tmp_path / "bc.db"
    build_database(database, games)
    reserve = tmp_path / "reserve.parquet"
    pl.DataFrame({"game_id": [games[-1]]}).write_parquet(reserve)
    output = tmp_path / "selection"
    build_selection(
        output,
        database=database,
        reserve=reserve,
        reserve_file_sha256=file_digest(reserve),
        reserve_game_digest=game_digest([games[-1]]),
    )
    selected = pl.read_parquet(output / "selected_games.parquet")
    assert set(selected["game_id"].to_list()) == set(kept)
    assert selected.filter(pl.col("is_smoke")).group_by("season").len()[
        "len"
    ].to_list() == [2, 2, 2]
    report = read(output / "report.json")
    assert report["reserve_overlap_before_exclusion"] == 1
    assert report["selected_games"] == len(kept)
    assert report["selected_game_ids_sha256_length_prefixed_utf8"] == game_digest(
        list(kept)
    )
    files = cast(dict[str, str], read(output / "manifest.json")["files_sha256"])
    assert set(files) == {"selected_games.parquet", "selection.sql", "report.json"}
    assert (
        files["report.json"]
        == hashlib.sha256((output / "report.json").read_bytes()).hexdigest()
    )
    with pytest.raises(ValueError, match="reserve file digest"):
        build_selection(
            tmp_path / "other",
            database=database,
            reserve=reserve,
            reserve_file_sha256="stale",
            reserve_game_digest=game_digest([games[-1]]),
        )
