from __future__ import annotations

from pathlib import Path

import polars as pl

from python_models.imputation.pitch_validation import (
    TOKEN_COUNTERS,
    derive_token_counters,
    load_pitch_taxonomy,
    validate_pitch_artifact,
)

SEED_PATH = Path("bc/seeds/misc/seed_pitch_types.csv")


def _row(
    *,
    event_key: int,
    game_id: str,
    sequence: str,
    result: str | None,
    balls: int | None,
    strikes: int | None,
    status: str,
) -> dict[str, object]:
    counters, unknown = derive_token_counters(sequence, load_pitch_taxonomy(SEED_PATH))
    assert not unknown
    return {
        "event_key": event_key,
        "game_id": game_id,
        "event_id": 1,
        "season": 2020,
        "appearance_start_event_id": 1,
        "plate_appearance_result": result,
        "count_balls": balls,
        "count_strikes": strikes,
        "completed_pitch_sequence": sequence,
        "constraint_status": status,
        **{f"completed_{counter}": counters[counter] for counter in TOKEN_COUNTERS},
    }


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    pl.DataFrame(rows).write_parquet(path)


def test_all_seventeen_counters_and_legal_walk_pass(tmp_path: Path) -> None:
    artifact = tmp_path / "pitches.parquet"
    _write(
        artifact,
        [
            _row(
                event_key=1,
                game_id="GOOD",
                sequence="Ball|CalledStrike|Ball|Foul|Ball|Ball",
                result="Walk",
                balls=3,
                strikes=2,
                status="constructed_legal_transition_sequence",
            )
        ],
    )
    report = validate_pitch_artifact(artifact, SEED_PATH, batch_size=1)
    assert report.passed
    assert tuple(counter.counter for counter in report.counters) == TOKEN_COUNTERS
    assert all(counter.checked_rows == 1 for counter in report.counters)
    assert not report.unflagged_appearance_violations


def test_explicit_transition_conflict_is_reported_without_failing(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "pitches.parquet"
    _write(
        artifact,
        [
            _row(
                event_key=2,
                game_id="FLAGGED",
                sequence="InPlay|Ball",
                result="InPlayOut",
                balls=0,
                strikes=0,
                status="completed_appearance_transition_conflict",
            )
        ],
    )
    report = validate_pitch_artifact(artifact, SEED_PATH)
    assert report.passed
    assert report.explicit_conflict_appearance_violations == {"pitch_after_terminal": 1}
    assert not report.unflagged_appearance_violations


def test_explicit_counter_mismatch_fails(tmp_path: Path) -> None:
    artifact = tmp_path / "pitches.parquet"
    row = _row(
        event_key=2,
        game_id="FLAGGED_COUNTER",
        sequence="InPlay|Ball",
        result="InPlayOut",
        balls=0,
        strikes=0,
        status="completed_appearance_transition_conflict",
    )
    row["completed_pitches"] = 1
    _write(artifact, [row])
    report = validate_pitch_artifact(artifact, SEED_PATH)
    assert not report.passed
    pitches = next(
        counter for counter in report.counters if counter.counter == "pitches"
    )
    assert pitches.explicit_conflict_violations == 1


def test_unflagged_boundary_and_counter_violations_fail(tmp_path: Path) -> None:
    artifact = tmp_path / "pitches.parquet"
    row = _row(
        event_key=3,
        game_id="BAD",
        sequence="CalledStrike",
        result=None,
        balls=1,
        strikes=0,
        status="source_preserved_no_detected_transition_conflict",
    )
    row["completed_balls"] = 1
    _write(artifact, [row])
    report = validate_pitch_artifact(artifact, SEED_PATH)
    assert not report.passed
    assert report.unflagged_appearance_violations == {"ball_boundary": 1}
    balls = next(counter for counter in report.counters if counter.counter == "balls")
    assert balls.unflagged_violations == 1


def test_streams_unsorted_rows_and_writes_capped_report_artifact(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "pitches.parquet"
    rows = [
        _row(
            event_key=5,
            game_id="SECOND",
            sequence="InPlay",
            result="InPlayOut",
            balls=0,
            strikes=0,
            status="constructed_legal_transition_sequence",
        ),
        _row(
            event_key=4,
            game_id="FIRST",
            sequence="HitBatter",
            result="HitByPitch",
            balls=0,
            strikes=0,
            status="source_preserved_no_detected_transition_conflict",
        ),
    ]
    _write(artifact, rows)
    output = tmp_path / "report"
    report = validate_pitch_artifact(
        artifact,
        SEED_PATH,
        output_directory=output,
        batch_size=1,
        checkpoint_every=1,
        example_limit=1,
    )
    assert report.passed
    assert report.games == 2
    assert report.appearances == 2
    assert {path.name for path in output.iterdir()} == {
        "checkpoint.json",
        "manifest.json",
        "pitch_validation.json",
    }


def test_unknown_taxonomy_token_fails_closed(tmp_path: Path) -> None:
    artifact = tmp_path / "pitches.parquet"
    row = _row(
        event_key=6,
        game_id="UNKNOWN",
        sequence="Ball",
        result=None,
        balls=0,
        strikes=0,
        status="constructed_legal_transition_sequence",
    )
    row["completed_pitch_sequence"] = "NewToken"
    row["completed_balls"] = 0
    row["completed_pitches"] = 0
    _write(artifact, [row])
    report = validate_pitch_artifact(artifact, SEED_PATH)
    assert not report.passed
    assert report.unknown_tokens == {"NewToken": 1}
