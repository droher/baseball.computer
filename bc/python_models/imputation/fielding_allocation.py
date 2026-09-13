"""Deterministic game-level allocation for compatible fielding residuals."""

from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final, TypedDict, cast

import polars as pl

PATCH_OUTPUT_SCHEMA: Final = {
    "event_key": pl.UInt32,
    "sequence_id": pl.UInt8,
    "completed_fielding_position": pl.UInt8,
    "player_id": pl.String,
    "completion_method": pl.String,
    "constraint_disposition": pl.String,
    "aggregate_constraint_target": pl.Float64,
    "aggregate_constraint_assigned": pl.Int64,
    "aggregate_constraint_delta": pl.Float64,
    "sampled_probability": pl.Float64,
    "expected_credit": pl.Float64,
    "allocation_applied": pl.Boolean,
}

_REQUIRED_COLUMNS: Final = frozenset(
    {
        "event_key",
        "sequence_id",
        "game_id",
        "credit_type",
        "raw_fielding_position",
        "completed_fielding_position",
        "player_id",
        "completion_method",
        "aggregate_constraint_complete",
        "candidate_positions",
        "candidate_player_ids",
        "candidate_probabilities",
        "candidate_aggregate_capacities",
        "sampled_probability",
        "expected_credit",
    }
)


@dataclass(frozen=True, slots=True)
class Candidate:
    position: int
    player_id: str
    probability: float
    capacity: int


class PatchRow(TypedDict):
    event_key: int
    sequence_id: int
    completed_fielding_position: int
    player_id: str | None
    completion_method: str
    constraint_disposition: str
    aggregate_constraint_target: float | None
    aggregate_constraint_assigned: int | None
    aggregate_constraint_delta: float | None
    sampled_probability: float
    expected_credit: float
    allocation_applied: bool


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    return value


def _floating(value: object, name: str) -> float:
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise ValueError(f"{name} must be numeric")
    return float(value)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def _sequence(value: object, name: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise ValueError(f"{name} must be a sequence")
    return cast(Sequence[object], value)


def _candidates(row: Mapping[str, object]) -> tuple[Candidate, ...]:
    positions = _sequence(row["candidate_positions"], "candidate_positions")
    players = _sequence(row["candidate_player_ids"], "candidate_player_ids")
    probabilities = _sequence(row["candidate_probabilities"], "candidate_probabilities")
    capacities = _sequence(
        row["candidate_aggregate_capacities"], "candidate_aggregate_capacities"
    )
    if not (len(positions) == len(players) == len(probabilities) == len(capacities)):
        raise ValueError("candidate arrays must have equal lengths")
    parsed: list[Candidate] = []
    for position, player, probability, capacity in zip(
        positions, players, probabilities, capacities, strict=True
    ):
        capacity_number = _floating(capacity, "candidate aggregate capacity")
        if capacity_number < 0 or not capacity_number.is_integer():
            raise ValueError(
                "candidate aggregate capacities must be nonnegative integers"
            )
        parsed.append(
            Candidate(
                position=_integer(position, "candidate position"),
                player_id=_string(player, "candidate player_id"),
                probability=_floating(probability, "candidate probability"),
                capacity=int(capacity_number),
            )
        )
    return tuple(parsed)


def _preference_key(
    event_key: int, sequence_id: int, candidate: Candidate
) -> tuple[float, str]:
    identity = f"{event_key}:{sequence_id}:{candidate.position}:{candidate.player_id}"
    return -candidate.probability, hashlib.sha256(identity.encode()).hexdigest()


def _matching(
    rows: Sequence[Mapping[str, object]],
    parsed: Sequence[tuple[Candidate, ...]],
) -> list[Candidate] | None:
    capacity_by_identity: dict[tuple[int, str], int] = {}
    candidate_by_identity: dict[tuple[int, str], Candidate] = {}
    for candidates in parsed:
        for candidate in candidates:
            identity = candidate.position, candidate.player_id
            prior_capacity = capacity_by_identity.setdefault(
                identity, candidate.capacity
            )
            if prior_capacity != candidate.capacity:
                return None
            candidate_by_identity[identity] = candidate
    if sum(capacity_by_identity.values()) != len(rows):
        return None
    slots = [
        identity
        for identity in sorted(capacity_by_identity)
        for _ in range(capacity_by_identity[identity])
    ]
    slot_indices: dict[tuple[int, str], list[int]] = defaultdict(list)
    for slot_index, identity in enumerate(slots):
        slot_indices[identity].append(slot_index)
    preferences: list[list[int]] = []
    for row, candidates in zip(rows, parsed, strict=True):
        event_key = _integer(row["event_key"], "event_key")
        sequence_id = _integer(row["sequence_id"], "sequence_id")
        ordered = sorted(
            (candidate for candidate in candidates if candidate.capacity > 0),
            key=lambda candidate: _preference_key(event_key, sequence_id, candidate),
        )
        preferences.append(
            [
                slot_index
                for candidate in ordered
                for slot_index in slot_indices[
                    (candidate.position, candidate.player_id)
                ]
            ]
        )
    slot_owner: list[int | None] = [None] * len(slots)

    def assign(row_index: int, visited: set[int]) -> bool:
        for slot_index in preferences[row_index]:
            if slot_index in visited:
                continue
            visited.add(slot_index)
            owner = slot_owner[slot_index]
            if owner is None or assign(owner, visited):
                slot_owner[slot_index] = row_index
                return True
        return False

    for row_index in range(len(rows)):
        if not assign(row_index, set()):
            return None
    row_slot: dict[int, int] = {
        row_index: slot_index
        for slot_index, row_index in enumerate(slot_owner)
        if row_index is not None
    }
    if len(row_slot) != len(rows):
        return None
    return [candidate_by_identity[slots[row_slot[index]]] for index in range(len(rows))]


def _base_patch(row: Mapping[str, object]) -> PatchRow:
    player = row["player_id"]
    if player is not None and not isinstance(player, str):
        raise ValueError("player_id must be a string or null")
    return {
        "event_key": _integer(row["event_key"], "event_key"),
        "sequence_id": _integer(row["sequence_id"], "sequence_id"),
        "completed_fielding_position": _integer(
            row["completed_fielding_position"], "completed_fielding_position"
        ),
        "player_id": player,
        "completion_method": _string(row["completion_method"], "completion_method"),
        "constraint_disposition": "aggregate_capacity_assignment_incompatible",
        "aggregate_constraint_target": None,
        "aggregate_constraint_assigned": None,
        "aggregate_constraint_delta": None,
        "sampled_probability": _floating(
            row["sampled_probability"], "sampled_probability"
        ),
        "expected_credit": _floating(row["expected_credit"], "expected_credit"),
        "allocation_applied": False,
    }


def _successful_patches(
    rows: Sequence[Mapping[str, object]],
    parsed: Sequence[tuple[Candidate, ...]],
    assignments: Sequence[Candidate],
) -> list[PatchRow]:
    assigned_counts = Counter(
        (candidate.position, candidate.player_id) for candidate in assignments
    )
    patches: list[PatchRow] = []
    for row, candidates, assigned in zip(rows, parsed, assignments, strict=True):
        probability = next(
            candidate.probability
            for candidate in candidates
            if candidate.position == assigned.position
            and candidate.player_id == assigned.player_id
        )
        assigned_count = assigned_counts[(assigned.position, assigned.player_id)]
        patches.append(
            {
                "event_key": _integer(row["event_key"], "event_key"),
                "sequence_id": _integer(row["sequence_id"], "sequence_id"),
                "completed_fielding_position": assigned.position,
                "player_id": assigned.player_id,
                "completion_method": "aggregate_capacity_matching",
                "constraint_disposition": "aggregate_capacity_assignment_satisfied",
                "aggregate_constraint_target": float(assigned.capacity),
                "aggregate_constraint_assigned": assigned_count,
                "aggregate_constraint_delta": float(assigned_count - assigned.capacity),
                "sampled_probability": probability,
                "expected_credit": probability,
                "allocation_applied": True,
            }
        )
    return patches


def build_compatible_assignment_patch(frame: pl.DataFrame) -> pl.DataFrame:
    missing = _REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"missing fielding allocation columns: {sorted(missing)}")
    raw_rows = cast(list[dict[str, object]], frame.to_dicts())
    groups: dict[tuple[str, str], list[Mapping[str, object]]] = defaultdict(list)
    for row in raw_rows:
        if _integer(row["raw_fielding_position"], "raw_fielding_position") != 0:
            continue
        if row["aggregate_constraint_complete"] is not True:
            continue
        groups[
            (
                _string(row["game_id"], "game_id"),
                _string(row["credit_type"], "credit_type"),
            )
        ].append(row)
    patches: list[PatchRow] = []
    for rows in groups.values():
        ordered_rows = sorted(
            rows,
            key=lambda row: (
                _integer(row["event_key"], "event_key"),
                _integer(row["sequence_id"], "sequence_id"),
            ),
        )
        try:
            parsed = [_candidates(row) for row in ordered_rows]
        except ValueError:
            patches.extend(_base_patch(row) for row in ordered_rows)
            continue
        assignments = _matching(ordered_rows, parsed)
        if assignments is None:
            patches.extend(_base_patch(row) for row in ordered_rows)
            continue
        patches.extend(_successful_patches(ordered_rows, parsed, assignments))
    return pl.DataFrame(patches, schema=PATCH_OUTPUT_SCHEMA)
