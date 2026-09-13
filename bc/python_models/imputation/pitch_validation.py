from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar, Final, Iterator, Protocol, Self, TypedDict, cast

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

logger = logging.getLogger(__name__)


class ArrowBatch(Protocol):
    def to_pylist(self) -> list[dict[str, object]]: ...


class ArrowReader(Protocol):
    def __iter__(self) -> Iterator[ArrowBatch]: ...


class ArrowReaderConnection(Protocol):
    def to_arrow_reader(self, batch_size: int) -> ArrowReader: ...


TOKEN_COUNTERS: Final = (
    "pitches",
    "swings",
    "swings_with_contact",
    "strikes",
    "strikes_called",
    "strikes_swinging",
    "strikes_foul",
    "strikes_foul_tip",
    "strikes_in_play",
    "strikes_unknown",
    "balls",
    "balls_called",
    "balls_intentional",
    "balls_automatic",
    "unknown_pitches",
    "pitchouts",
    "pitcher_pickoff_attempts",
)

UNFLAGGED_STATUSES: Final = frozenset(
    {
        "source_preserved_no_detected_transition_conflict",
        "constructed_legal_transition_sequence",
    }
)

EXPLICIT_CONFLICT_STATUSES: Final = frozenset(
    {
        "source_conflict_quarantined",
        "count_progression_contradiction",
        "terminal_count_contradiction",
        "completed_appearance_transition_conflict",
        "completed_count_boundary_conflict",
        "completed_appearance_terminal_conflict",
    }
)

REQUIRED_COLUMNS: Final = (
    "event_key",
    "game_id",
    "event_id",
    "season",
    "appearance_start_event_id",
    "plate_appearance_result",
    "count_balls",
    "count_strikes",
    "completed_pitch_sequence",
    "constraint_status",
    *(f"completed_{counter}" for counter in TOKEN_COUNTERS),
)


class PitchToken(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    sequence_item: str
    is_pitch: bool
    is_strike: bool
    is_in_play: bool
    can_be_strike_three: bool
    is_swing: bool
    is_contact: bool
    category: str


class ViolationExample(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    scope: str
    violation: str
    game_id: str
    appearance_start_event_id: int
    event_key: int | None
    constraint_statuses: tuple[str, ...]
    recorded: int | str | None = None
    recomputed: int | str | None = None


class CounterValidation(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    counter: str
    checked_rows: int = Field(ge=0)
    unflagged_violations: int = Field(ge=0)
    explicit_conflict_violations: int = Field(ge=0)


class PitchValidationReport(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    input_path: str
    input_bytes: int
    input_sha256: str
    seed_path: str
    seed_sha256: str
    rows: int = Field(ge=0)
    games: int = Field(ge=0)
    appearances: int = Field(ge=0)
    season_start: int | None
    season_end: int | None
    counters: tuple[CounterValidation, ...]
    unflagged_appearance_violations: Mapping[str, int]
    explicit_conflict_appearance_violations: Mapping[str, int]
    constraint_statuses: Mapping[str, int]
    unclassified_constraint_statuses: Mapping[str, int]
    unknown_tokens: Mapping[str, int]
    examples: tuple[ViolationExample, ...]
    passed: bool

    @model_validator(mode="after")
    def validate_report(self) -> Self:
        if tuple(counter.counter for counter in self.counters) != TOKEN_COUNTERS:
            raise ValueError("report must validate all token-derived counters")
        if any(counter.checked_rows != self.rows for counter in self.counters):
            raise ValueError("every counter must be checked for every row")
        has_counter_violations = any(
            counter.unflagged_violations or counter.explicit_conflict_violations
            for counter in self.counters
        )
        expected_passed = not (
            has_counter_violations
            or any(self.unflagged_appearance_violations.values())
            or self.unclassified_constraint_statuses
            or self.unknown_tokens
            or self.rows == 0
        )
        if self.passed != expected_passed:
            raise ValueError("passed does not match validation counts")
        return self


class PitchRow(TypedDict):
    event_key: int
    game_id: str
    event_id: int
    season: int
    appearance_start_event_id: int
    plate_appearance_result: str | None
    count_balls: int | None
    count_strikes: int | None
    completed_pitch_sequence: str
    constraint_status: str


@dataclass(slots=True)
class AppearanceState:
    game_id: str
    appearance_start_event_id: int
    balls: int = 0
    strikes: int = 0
    terminal: str | None = None
    final_result: str | None = None
    season: int = 0
    pitch_count: int = 0
    statuses: set[str] = field(default_factory=set)
    violations: Counter[str] = field(default_factory=Counter)


def load_pitch_taxonomy(path: Path) -> Mapping[str, PitchToken]:
    with path.open(newline="") as handle:
        rows = tuple(csv.DictReader(handle))
    tokens = tuple(PitchToken.model_validate(row) for row in rows)
    taxonomy = {token.sequence_item: token for token in tokens}
    if len(taxonomy) != len(tokens):
        raise ValueError("pitch taxonomy contains duplicate sequence items")
    return taxonomy


def derive_token_counters(
    sequence: str, taxonomy: Mapping[str, PitchToken]
) -> tuple[Mapping[str, int], tuple[str, ...]]:
    counts: Counter[str] = Counter()
    unknown: list[str] = []
    for item in (item for item in sequence.split("|") if item):
        token = taxonomy.get(item)
        if token is None:
            unknown.append(item)
            continue
        conditions = {
            "pitches": token.is_pitch,
            "swings": token.is_swing,
            "swings_with_contact": token.is_contact,
            "strikes": token.is_strike,
            "strikes_called": token.is_strike and not token.is_swing,
            "strikes_swinging": token.is_swing and not token.is_contact,
            "strikes_foul": (
                token.is_swing
                and token.is_contact
                and not token.is_in_play
                and not token.can_be_strike_three
            ),
            "strikes_foul_tip": item.startswith("FoulTip"),
            "strikes_in_play": token.is_in_play,
            "strikes_unknown": item == "StrikeUnknownType",
            "balls": token.category == "Ball",
            "balls_called": item == "Ball",
            "balls_intentional": item == "IntentionalBall",
            "balls_automatic": item == "AutomaticBall",
            "unknown_pitches": token.category == "Unknown",
            "pitchouts": item.endswith("Pitchout"),
            "pitcher_pickoff_attempts": item.startswith("Pickoff"),
        }
        counts.update(name for name, applies in conditions.items() if applies)
    return {counter: counts[counter] for counter in TOKEN_COUNTERS}, tuple(unknown)


def _is_explicit_conflict(statuses: Sequence[str]) -> bool:
    return any(status in EXPLICIT_CONFLICT_STATUSES for status in statuses)


def _row_value(row: Mapping[str, object], name: str) -> object:
    if name not in row:
        raise ValueError(f"pitch artifact is missing {name}")
    return row[name]


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    return value


def _optional_integer(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _integer(value, name)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def _pitch_row(row: Mapping[str, object]) -> PitchRow:
    result = _row_value(row, "plate_appearance_result")
    if result is not None and not isinstance(result, str):
        raise ValueError("plate_appearance_result must be a string or null")
    return {
        "event_key": _integer(_row_value(row, "event_key"), "event_key"),
        "game_id": _string(_row_value(row, "game_id"), "game_id"),
        "event_id": _integer(_row_value(row, "event_id"), "event_id"),
        "season": _integer(_row_value(row, "season"), "season"),
        "appearance_start_event_id": _integer(
            _row_value(row, "appearance_start_event_id"),
            "appearance_start_event_id",
        ),
        "plate_appearance_result": result,
        "count_balls": _optional_integer(_row_value(row, "count_balls"), "count_balls"),
        "count_strikes": _optional_integer(
            _row_value(row, "count_strikes"), "count_strikes"
        ),
        "completed_pitch_sequence": _string(
            _row_value(row, "completed_pitch_sequence"),
            "completed_pitch_sequence",
        ),
        "constraint_status": _string(
            _row_value(row, "constraint_status"), "constraint_status"
        ),
    }


def _add_example(
    examples: list[ViolationExample],
    limit: int,
    *,
    scope: str,
    violation: str,
    state: AppearanceState,
    event_key: int | None,
    recorded: int | str | None = None,
    recomputed: int | str | None = None,
) -> None:
    if len(examples) >= limit:
        return
    examples.append(
        ViolationExample(
            scope=scope,
            violation=violation,
            game_id=state.game_id,
            appearance_start_event_id=state.appearance_start_event_id,
            event_key=event_key,
            constraint_statuses=tuple(sorted(state.statuses)),
            recorded=recorded,
            recomputed=recomputed,
        )
    )


def _consume_transitions(
    state: AppearanceState,
    row: PitchRow,
    taxonomy: Mapping[str, PitchToken],
    unknown_tokens: Counter[str],
) -> None:
    state.final_result = row["plate_appearance_result"]
    state.season = row["season"]
    pitch_tokens: list[PitchToken] = []
    for item in (item for item in row["completed_pitch_sequence"].split("|") if item):
        token = taxonomy.get(item)
        if token is None:
            unknown_tokens[item] += 1
        elif token.is_pitch:
            pitch_tokens.append(token)
    for index, token in enumerate(pitch_tokens):
        if state.terminal is not None:
            state.violations["pitch_after_terminal"] += 1
        if index == len(pitch_tokens) - 1:
            if row["count_balls"] is not None and row["count_balls"] != state.balls:
                state.violations["ball_boundary"] += 1
            if (
                row["count_strikes"] is not None
                and row["count_strikes"] != state.strikes
            ):
                state.violations["strike_boundary"] += 1
        state.pitch_count += 1
        if token.is_in_play:
            state.terminal = "InPlay"
        elif token.sequence_item == "HitBatter":
            state.terminal = "HitByPitch"
        elif token.category == "Ball":
            state.balls += 1
            if state.balls >= 4:
                state.terminal = "Walk"
        elif token.is_strike:
            if state.strikes >= 2 and token.can_be_strike_three:
                state.terminal = "StrikeOut"
            state.strikes = min(state.strikes + 1, 2)


def _finalize_appearance(state: AppearanceState) -> None:
    result = state.final_result
    if result is None or result == "Interference":
        return
    if result == "IntentionalWalk" and state.season >= 2017 and state.pitch_count == 0:
        return
    if result in {"Walk", "IntentionalWalk"}:
        expected = "Walk"
    elif result in {"StrikeOut", "HitByPitch"}:
        expected = result
    else:
        expected = "InPlay"
    if state.terminal != expected:
        state.violations["terminal_outcome"] += 1


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def validate_pitch_artifact(
    input_path: Path,
    seed_path: Path,
    *,
    output_directory: Path | None = None,
    batch_size: int = 32_768,
    checkpoint_every: int = 1_000_000,
    example_limit: int = 50,
    threads: int = 1,
    memory_limit: str = "4GB",
) -> PitchValidationReport:
    if batch_size < 1 or checkpoint_every < 1 or example_limit < 0:
        raise ValueError("batch, checkpoint, and example limits are invalid")
    if not input_path.is_file() or not seed_path.is_file():
        raise FileNotFoundError("pitch artifact and seed taxonomy must exist")
    taxonomy = load_pitch_taxonomy(seed_path)
    input_stat = input_path.stat()
    checkpoint_path: Path | None = None
    if output_directory is not None:
        output_directory.mkdir(parents=True, exist_ok=False)
        checkpoint_path = output_directory / "checkpoint.json"
    selected = ", ".join(f'"{column}"' for column in REQUIRED_COLUMNS)
    query = (
        f"SELECT {selected} FROM read_parquet(?) "
        "ORDER BY game_id, appearance_start_event_id, event_id, event_key"
    )
    counter_unflagged: Counter[str] = Counter()
    counter_flagged: Counter[str] = Counter()
    appearance_unflagged: Counter[str] = Counter()
    appearance_flagged: Counter[str] = Counter()
    statuses: Counter[str] = Counter()
    unexpected_statuses: Counter[str] = Counter()
    unknown_tokens: Counter[str] = Counter()
    examples: list[ViolationExample] = []
    rows_seen = 0
    games = 0
    appearances = 0
    season_start: int | None = None
    season_end: int | None = None
    last_game: str | None = None
    state: AppearanceState | None = None

    def flush_appearance() -> None:
        nonlocal state, appearances
        if state is None:
            return
        _finalize_appearance(state)
        appearances += 1
        destination = (
            appearance_flagged
            if _is_explicit_conflict(tuple(state.statuses))
            else appearance_unflagged
        )
        destination.update(state.violations)
        for violation in state.violations:
            _add_example(
                examples,
                example_limit,
                scope="appearance",
                violation=violation,
                state=state,
                event_key=None,
                recorded=state.final_result,
                recomputed=state.terminal,
            )

    with duckdb.connect(
        ":memory:", config={"threads": threads, "memory_limit": memory_limit}
    ) as connection:
        _ = connection.execute(query, [str(input_path.resolve())])
        reader = cast(ArrowReaderConnection, connection).to_arrow_reader(batch_size)
        for batch in reader:
            raw_rows = batch.to_pylist()
            for raw_row in raw_rows:
                row = _pitch_row(raw_row)
                appearance_key = (
                    row["game_id"],
                    row["appearance_start_event_id"],
                )
                if state is None or appearance_key != (
                    state.game_id,
                    state.appearance_start_event_id,
                ):
                    flush_appearance()
                    state = AppearanceState(*appearance_key)
                if row["game_id"] != last_game:
                    games += 1
                    last_game = row["game_id"]
                rows_seen += 1
                season_start = (
                    row["season"]
                    if season_start is None
                    else min(season_start, row["season"])
                )
                season_end = (
                    row["season"]
                    if season_end is None
                    else max(season_end, row["season"])
                )
                status = row["constraint_status"]
                statuses[status] += 1
                state.statuses.add(status)
                if status not in UNFLAGGED_STATUSES | EXPLICIT_CONFLICT_STATUSES:
                    unexpected_statuses[status] += 1
                recomputed, row_unknown = derive_token_counters(
                    row["completed_pitch_sequence"], taxonomy
                )
                unknown_tokens.update(row_unknown)
                explicit = status in EXPLICIT_CONFLICT_STATUSES
                for counter in TOKEN_COUNTERS:
                    recorded = _integer(
                        _row_value(raw_row, f"completed_{counter}"),
                        f"completed_{counter}",
                    )
                    if recorded == recomputed[counter]:
                        continue
                    destination = counter_flagged if explicit else counter_unflagged
                    destination[counter] += 1
                    _add_example(
                        examples,
                        example_limit,
                        scope="counter",
                        violation=counter,
                        state=state,
                        event_key=row["event_key"],
                        recorded=recorded,
                        recomputed=recomputed[counter],
                    )
                _consume_transitions(state, row, taxonomy, Counter())
                if rows_seen % checkpoint_every == 0:
                    checkpoint = {
                        "rows": rows_seen,
                        "games": games,
                        "appearances_complete": appearances,
                        "last_game_id": row["game_id"],
                        "last_appearance_start_event_id": row[
                            "appearance_start_event_id"
                        ],
                        "unflagged_counter_violations": sum(counter_unflagged.values()),
                        "unflagged_appearance_violations": sum(
                            appearance_unflagged.values()
                        ),
                    }
                    logger.info("Pitch validation checkpoint %s", checkpoint)
                    if checkpoint_path is not None:
                        _write_json(checkpoint_path, checkpoint)
        flush_appearance()
    counters = tuple(
        CounterValidation(
            counter=counter,
            checked_rows=rows_seen,
            unflagged_violations=counter_unflagged[counter],
            explicit_conflict_violations=counter_flagged[counter],
        )
        for counter in TOKEN_COUNTERS
    )
    passed = not (
        any(
            counter.unflagged_violations or counter.explicit_conflict_violations
            for counter in counters
        )
        or appearance_unflagged
        or unexpected_statuses
        or unknown_tokens
        or rows_seen == 0
    )
    report = PitchValidationReport(
        input_path=str(input_path.resolve()),
        input_bytes=input_stat.st_size,
        input_sha256=_sha256(input_path),
        seed_path=str(seed_path.resolve()),
        seed_sha256=_sha256(seed_path),
        rows=rows_seen,
        games=games,
        appearances=appearances,
        season_start=season_start,
        season_end=season_end,
        counters=counters,
        unflagged_appearance_violations=dict(appearance_unflagged),
        explicit_conflict_appearance_violations=dict(appearance_flagged),
        constraint_statuses=dict(statuses),
        unclassified_constraint_statuses=dict(unexpected_statuses),
        unknown_tokens=dict(unknown_tokens),
        examples=tuple(examples),
        passed=passed,
    )
    if output_directory is not None:
        report_path = output_directory / "pitch_validation.json"
        _write_json(report_path, report.model_dump(mode="json"))
        _write_json(
            output_directory / "manifest.json",
            {
                "created_at": datetime.now(UTC).isoformat(),
                "status": "passed" if report.passed else "failed",
                "input_path": report.input_path,
                "input_bytes": report.input_bytes,
                "input_sha256": report.input_sha256,
                "seed_path": report.seed_path,
                "seed_sha256": report.seed_sha256,
                "report": report_path.name,
                "report_sha256": _sha256(report_path),
                "implementation_sha256": _sha256(Path(__file__)),
            },
        )
        _write_json(
            output_directory / "checkpoint.json",
            {
                "rows": report.rows,
                "games": report.games,
                "appearances_complete": report.appearances,
                "status": "complete",
            },
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stream-validate completed pitch counters and appearances."
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--seed",
        type=Path,
        default=Path("bc/seeds/misc/seed_pitch_types.csv"),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=32_768)
    parser.add_argument("--checkpoint-every", type=int, default=1_000_000)
    parser.add_argument("--example-limit", type=int, default=50)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--memory-limit", default="4GB")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    report = validate_pitch_artifact(
        args.input,
        args.seed,
        output_directory=args.output,
        batch_size=args.batch_size,
        checkpoint_every=args.checkpoint_every,
        example_limit=args.example_limit,
        threads=args.threads,
        memory_limit=args.memory_limit,
    )
    logger.info(
        "Pitch validation %s: %d rows, %d games, %d appearances",
        "passed" if report.passed else "failed",
        report.rows,
        report.games,
        report.appearances,
    )
    if not report.passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
