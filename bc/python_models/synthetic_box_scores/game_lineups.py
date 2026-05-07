from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import logging
import math
import os
import time
from typing import Any, cast

import numpy as np
import polars as pl
from scipy.optimize import Bounds, LinearConstraint, milp
from scipy.sparse import coo_matrix, csr_matrix
from scipy.sparse.csgraph import maximum_bipartite_matching

GAME_INPUT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "date",
    "season",
    "use_dh",
    "home_team_id",
    "away_team_id",
    "home_starting_pitcher_id",
    "away_starting_pitcher_id",
)

LINEUP_INPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "team_id",
    "lineup_position",
    "fielding_position",
    "player_id",
)

CANDIDATE_INPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "team_id",
    "player_id",
    "stint",
    "fielding_position",
    "games_at_position",
    "games_total",
    "plate_appearances",
    "games_played",
    "outs_played",
)

ASSIGNMENT_INPUT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "season",
    "team_id",
    "player_id",
    "stint",
    "side",
    "lineup_position",
    "fielding_position",
)

REPORT_OUTPUT_COLUMNS: tuple[str, ...] = (
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
    "signed_pct_error",
    "error_rank",
)

_FIELDING_POSITIONS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7, 8, 9)
_NON_PITCHER_POSITIONS: tuple[int, ...] = (2, 3, 4, 5, 6, 7, 8, 9)


def _effective_pitcher_id(
    game_side: "_GameSide",
    lineup: "list[_LineupSlot] | None",
) -> str | None:
    """Player to keep out of non-P slots for this side.

    Prefer the gamelog's listed starting pitcher; if absent (common pre-1900),
    fall back to the modal lineup's pitcher so the post-hoc pitcher row does
    not collide with an optimizer-assigned non-P slot for the same player.
    """
    if game_side.starting_pitcher_id is not None:
        return game_side.starting_pitcher_id
    if lineup is None:
        return None
    for slot in lineup:
        if slot.fielding_position == 1:
            return slot.player_id
    return None
_SIDE_SPECS: tuple[tuple[str, str, str], ...] = (
    ("Away", "away_team_id", "away_starting_pitcher_id"),
    ("Home", "home_team_id", "home_starting_pitcher_id"),
)
_MILP_TOLERANCE = 1e-7
_ASSIGNMENT_TIEBREAK_EPSILON = 1e-6
_MODAL_DEFAULT_SLOT_BONUS = 0.01
_MIN_OUTS_FOR_REQUIRED_APPEARANCE = 23
_TOTAL_OVER_PENALTY = 100.0


@dataclass(frozen=True)
class _GameSide:
    index: int
    game_id: str
    date_key: tuple[str, str]
    season: int
    team_id: str
    side: str
    starting_pitcher_id: str | None
    use_dh: bool


@dataclass(frozen=True)
class _LineupSlot:
    lineup_position: int
    fielding_position: int
    player_id: str


@dataclass(frozen=True)
class _Candidate:
    season: int
    team_id: str
    player_id: str
    stint: int
    fielding_position: int
    games_at_position: float
    games_total: float
    plate_appearances: int
    games_played: int
    outs_played: int


@dataclass(frozen=True)
class _AssignmentVariable:
    side_index: int
    game_id: str
    side: str
    team_id: str
    lineup_position: int
    candidate: _Candidate
    game_order: tuple[str, str]


@dataclass(frozen=True)
class _StintWindow:
    start_index: int
    end_index: int


@dataclass
class _MilpProblem:
    variables: list[_AssignmentVariable]
    costs: list[float]
    integrality: list[int]
    lower_bounds: list[float]
    upper_bounds: list[float]
    row_indexes: list[int]
    col_indexes: list[int]
    coefficients: list[float]
    constraint_lower: list[float]
    constraint_upper: list[float]
    random_costs: list[float]
    next_row: int = 0

    def add_variable(
        self,
        *,
        cost: float,
        random_cost: float,
        integrality: int,
        lower_bound: float,
        upper_bound: float,
    ) -> int:
        index = len(self.costs)
        self.costs.append(cost)
        self.random_costs.append(random_cost)
        self.integrality.append(integrality)
        self.lower_bounds.append(lower_bound)
        self.upper_bounds.append(upper_bound)
        return index

    def add_constraint(
        self,
        terms: dict[int, float],
        *,
        lower_bound: float,
        upper_bound: float,
    ) -> None:
        row = self.next_row
        self.next_row += 1
        for column, coefficient in terms.items():
            self.row_indexes.append(row)
            self.col_indexes.append(column)
            self.coefficients.append(coefficient)
        self.constraint_lower.append(lower_bound)
        self.constraint_upper.append(upper_bound)

    def scipy_constraints(self) -> LinearConstraint:
        matrix = coo_matrix(
            (self.coefficients, (self.row_indexes, self.col_indexes)),
            shape=(self.next_row, len(self.costs)),
        )
        return LinearConstraint(
            matrix,
            cast(Any, np.asarray(self.constraint_lower, dtype=float)),
            cast(Any, np.asarray(self.constraint_upper, dtype=float)),
        )


def build_synthetic_batting_core(
    games: pl.DataFrame,
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
) -> pl.DataFrame:
    games = _with_required_game_columns(games)
    non_dh_games = games.filter(~pl.col("use_dh").fill_null(False))
    dh_games = games.filter(pl.col("use_dh").fill_null(False))

    rows: list[dict[str, object]] = []
    assignments = build_synthetic_lineup_assignments(
        non_dh_games,
        lineups,
        candidates,
    )
    rows.extend(
        {
            "game_id": row["game_id"],
            "batter_id": row["player_id"],
            "side": row["side"],
            "lineup_position": row["lineup_position"],
        }
        for row in assignments.to_dicts()
    )

    for lineup in _iter_fallback_game_side_lineups(
        dh_games,
        lineups,
        candidates,
        insert_pitcher=False,
    ):
        ordered = sorted(
            lineup,
            key=lambda row: (
                -_pa_per_game(row),
                -_as_int(row["plate_appearances"]),
                str(row["player_id"]),
            ),
        )
        for lineup_position, row in enumerate(ordered, start=1):
            rows.append(
                {
                    "game_id": row["game_id"],
                    "batter_id": row["player_id"],
                    "side": row["side"],
                    "lineup_position": lineup_position,
                }
            )

    if not rows:
        return _empty_batting_core()

    return pl.DataFrame(rows).select(
        pl.col("game_id"),
        pl.col("batter_id"),
        pl.col("side"),
        pl.col("lineup_position").cast(pl.UInt8),
    )


def build_synthetic_fielding_core(
    games: pl.DataFrame,
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    assignments = build_synthetic_lineup_assignments(
        games,
        lineups,
        candidates,
    )
    for row in sorted(
        assignments.to_dicts(),
        key=lambda item: (
            str(item["game_id"]),
            str(item["side"]),
            int(item["fielding_position"]),
        ),
    ):
        rows.append(
            {
                "game_id": row["game_id"],
                "fielder_id": row["player_id"],
                "side": row["side"],
                "fielding_position": row["fielding_position"],
            }
        )

    if not rows:
        return _empty_fielding_core()

    return pl.DataFrame(rows).select(
        pl.col("game_id"),
        pl.col("fielder_id"),
        pl.col("side"),
        pl.col("fielding_position").cast(pl.UInt8),
    )


def build_synthetic_lineup_assignments(
    games: pl.DataFrame,
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
    *,
    transaction_windows: Mapping[tuple[int, str, str, int], tuple[int, int]] | None = None,
) -> pl.DataFrame:
    games = _with_required_game_columns(games)
    _validate_columns(games, GAME_INPUT_COLUMNS)
    if games.is_empty():
        return _empty_assignment_core()

    _validate_columns(lineups, LINEUP_INPUT_COLUMNS)
    _validate_columns(candidates, CANDIDATE_INPUT_COLUMNS)

    lineup_lookup = _lineup_lookup(lineups, candidates)
    candidate_rows = _candidate_rows(candidates)
    if not lineup_lookup or not candidate_rows:
        return _fallback_assignments(games, lineups, candidates)

    all_game_sides = list(_game_sides(games))
    sides_count_by_team: dict[tuple[int, str], int] = defaultdict(int)
    for side in all_game_sides:
        sides_count_by_team[(side.season, side.team_id)] += 1

    candidate_rows = _allocate_games_total_per_stint(candidate_rows)
    candidate_rows = _scale_position_targets(candidate_rows, sides_count_by_team)

    candidates_by_team: dict[tuple[int, str], list[_Candidate]] = defaultdict(list)
    for candidate in candidate_rows:
        candidates_by_team[(candidate.season, candidate.team_id)].append(candidate)

    sides_by_team: dict[tuple[int, str], list[_GameSide]] = defaultdict(list)
    for game_side in all_game_sides:
        if (game_side.season, game_side.team_id) in lineup_lookup:
            sides_by_team[(game_side.season, game_side.team_id)].append(game_side)

    season_date_index, stint_windows = _build_stint_windows(
        all_game_sides, candidate_rows, transaction_windows=transaction_windows
    )

    rows: list[dict[str, object]] = []
    fallback_sides: list[_GameSide] = []
    fallback_seasons: set[int] = set()
    work_items: list[tuple[int, str, list[_GameSide], list[_Candidate]]] = []
    for (season, team_id), team_sides in sorted(sides_by_team.items()):
        team_candidates = candidates_by_team.get((season, team_id), [])
        solvable_sides, infeasible_sides = _partition_game_sides(
            team_sides,
            lineup_lookup,
            {(season, team_id): team_candidates},
            stint_windows=stint_windows,
            season_date_index=season_date_index,
        )
        if infeasible_sides:
            fallback_sides.extend(infeasible_sides)
            fallback_seasons.add(season)
        if not solvable_sides:
            continue
        work_items.append((season, team_id, solvable_sides, team_candidates))

    def _solve_one(
        item: tuple[int, str, list[_GameSide], list[_Candidate]],
    ) -> tuple[
        int, str, list[dict[str, object]], list[_GameSide]
    ]:
        season, team_id, solvable_sides, team_candidates = item
        solved_rows, sides_to_fallback = _solve_team_season(
            season=season,
            team_id=team_id,
            game_sides=solvable_sides,
            lineup_lookup=lineup_lookup,
            candidates=team_candidates,
            stint_windows=stint_windows,
            season_date_index=season_date_index,
        )
        return season, team_id, solved_rows, sides_to_fallback

    workers = _resolve_worker_count(len(work_items))
    if workers <= 1 or len(work_items) <= 1:
        results: list[
            tuple[int, str, list[dict[str, object]], list[_GameSide]]
        ] = [_solve_one(item) for item in work_items]
    else:
        _logger().info(
            "solving %d team-seasons in parallel with %d workers",
            len(work_items),
            workers,
        )
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(_solve_one, work_items))

    for season, team_id, solved_rows, sides_to_fallback in results:
        if solved_rows:
            rows.extend(solved_rows)
        if sides_to_fallback:
            fallback_sides.extend(sides_to_fallback)
            fallback_seasons.add(season)

    if fallback_sides:
        rows.extend(
            _fallback_assignments_for_sides(
                games.filter(pl.col("season").is_in(list(fallback_seasons))),
                lineups,
                candidates,
                fallback_sides,
            )
        )

    if not rows:
        return _empty_assignment_core()

    return pl.DataFrame(rows).select(
        pl.col("game_id"),
        pl.col("season").cast(pl.Int16),
        pl.col("team_id"),
        pl.col("player_id"),
        pl.col("stint").cast(pl.Int16),
        pl.col("side"),
        pl.col("lineup_position").cast(pl.UInt8),
        pl.col("fielding_position").cast(pl.UInt8),
    )


def build_synthetic_lineup_report(
    games: pl.DataFrame,
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
) -> pl.DataFrame:
    games = _with_required_game_columns(games)
    _validate_columns(games, GAME_INPUT_COLUMNS)
    _validate_columns(lineups, LINEUP_INPUT_COLUMNS)
    _validate_columns(candidates, CANDIDATE_INPUT_COLUMNS)

    candidate_rows = _candidate_rows(candidates)
    if games.is_empty() or not candidate_rows:
        return _empty_lineup_report()

    assignments = build_synthetic_lineup_assignments(games, lineups, candidates)
    return build_synthetic_lineup_report_from_assignments(assignments, candidates)


def build_synthetic_lineup_report_from_assignments(
    assignments: pl.DataFrame,
    candidates: pl.DataFrame,
) -> pl.DataFrame:
    _validate_columns(assignments, ASSIGNMENT_INPUT_COLUMNS)
    _validate_columns(candidates, CANDIDATE_INPUT_COLUMNS)
    candidate_rows = _candidate_rows(candidates)
    if not candidate_rows:
        return _empty_lineup_report()

    if assignments.is_empty():
        return _empty_lineup_report()

    sides_count_by_team: dict[tuple[int, str], int] = defaultdict(int)
    for row in (
        assignments.select(["season", "team_id", "game_id", "side"])
        .unique()
        .to_dicts()
    ):
        sides_count_by_team[(_as_int(row["season"]), str(row["team_id"]))] += 1
    candidate_rows = _allocate_games_total_per_stint(candidate_rows)
    candidate_rows = _scale_position_targets(candidate_rows, sides_count_by_team)

    actual_totals, actual_positions = _actual_target_counts(candidate_rows)
    realized_totals, realized_positions = _realized_target_counts(assignments)

    rows: list[dict[str, object]] = []
    for key in sorted(set(actual_totals) | set(realized_totals)):
        rows.append(
            _report_row(
                key,
                "Total",
                0,
                actual_totals.get(key, 0.0),
                realized_totals.get(key, 0),
            )
        )

    for key in sorted(set(actual_positions) | set(realized_positions)):
        rows.append(
            _report_row(
                key[:4],
                "Position",
                key[4],
                actual_positions.get(key, 0.0),
                realized_positions.get(key, 0),
            )
        )

    if not rows:
        return _empty_lineup_report()

    return (
        pl.DataFrame(rows, schema_overrides={"fielding_position": pl.UInt8})
        .sort(
            [
                "pct_error",
                "abs_error",
                "season",
                "player_id",
                "team_id",
                "stint",
                "metric_type",
                "fielding_position",
            ],
            descending=[True, True, False, False, False, False, False, False],
            nulls_last=True,
        )
        .with_row_index("error_rank", offset=1)
        .select(
            pl.col("season").cast(pl.Int16),
            pl.col("team_id"),
            pl.col("player_id"),
            pl.col("stint").cast(pl.Int16),
            pl.col("metric_type"),
            pl.col("fielding_position").cast(pl.UInt8),
            pl.col("actual_games").cast(pl.Float64),
            pl.col("realized_games").cast(pl.Int32),
            pl.col("signed_error").cast(pl.Float64),
            pl.col("abs_error").cast(pl.Float64),
            pl.col("pct_error").cast(pl.Float64),
            pl.col("signed_pct_error").cast(pl.Float64),
            pl.col("error_rank").cast(pl.UInt32),
        )
    )


def _partition_game_sides(
    game_sides: list[_GameSide],
    lineup_lookup: dict[tuple[int, str], list[_LineupSlot]],
    candidates_by_team: dict[tuple[int, str], list[_Candidate]],
    *,
    stint_windows: dict[tuple[int, str, str, int], _StintWindow],
    season_date_index: dict[int, dict[str, int]],
) -> tuple[list[_GameSide], list[_GameSide]]:
    solvable: list[_GameSide] = []
    fallback: list[_GameSide] = []
    for game_side in game_sides:
        if _game_side_has_optimizer_inputs(
            game_side,
            lineup_lookup,
            candidates_by_team,
            stint_windows=stint_windows,
            season_date_index=season_date_index,
        ):
            solvable.append(game_side)
        else:
            fallback.append(game_side)
    return solvable, fallback


def _game_side_has_optimizer_inputs(
    game_side: _GameSide,
    lineup_lookup: dict[tuple[int, str], list[_LineupSlot]],
    candidates_by_team: dict[tuple[int, str], list[_Candidate]],
    *,
    stint_windows: dict[tuple[int, str, str, int], _StintWindow],
    season_date_index: dict[int, dict[str, int]],
) -> bool:
    lineup = lineup_lookup.get((game_side.season, game_side.team_id))
    if lineup is None:
        return False

    non_pitcher_slots = [
        slot for slot in lineup if slot.fielding_position in _NON_PITCHER_POSITIONS
    ]
    if len(non_pitcher_slots) != len(_NON_PITCHER_POSITIONS):
        return False

    default_slot_by_player = {
        slot.player_id: slot.lineup_position for slot in non_pitcher_slots
    }
    excluded_pitcher_id = _effective_pitcher_id(game_side, lineup)
    positions: set[int] = set()
    lineup_positions: set[int] = set()
    for slot in non_pitcher_slots:
        for candidate in candidates_by_team.get(
            (game_side.season, game_side.team_id), []
        ):
            if candidate.fielding_position not in _NON_PITCHER_POSITIONS:
                continue
            if (
                excluded_pitcher_id is not None
                and candidate.player_id == excluded_pitcher_id
            ):
                continue
            default_slot = default_slot_by_player.get(candidate.player_id)
            if default_slot is None:
                if candidate.fielding_position != slot.fielding_position:
                    continue
            elif (
                default_slot != slot.lineup_position
                and candidate.fielding_position != slot.fielding_position
            ):
                continue
            if not _candidate_eligible_for_side(
                candidate, game_side, stint_windows, season_date_index
            ):
                continue
            positions.add(candidate.fielding_position)
            lineup_positions.add(slot.lineup_position)

    return positions == set(_NON_PITCHER_POSITIONS) and lineup_positions == {
        slot.lineup_position for slot in non_pitcher_slots
    }


def _candidate_eligible_for_side(
    candidate: _Candidate,
    game_side: _GameSide,
    stint_windows: dict[tuple[int, str, str, int], _StintWindow],
    season_date_index: dict[int, dict[str, int]],
) -> bool:
    window = stint_windows.get(
        (
            candidate.season,
            candidate.team_id,
            candidate.player_id,
            candidate.stint,
        )
    )
    if window is None:
        return True
    index = season_date_index.get(candidate.season, {}).get(game_side.date_key[0])
    if index is None:
        return True
    return window.start_index <= index <= window.end_index


def _solve_team_season(
    *,
    season: int,
    team_id: str,
    game_sides: list[_GameSide],
    lineup_lookup: dict[tuple[int, str], list[_LineupSlot]],
    candidates: list[_Candidate],
    stint_windows: dict[tuple[int, str, str, int], _StintWindow],
    season_date_index: dict[int, dict[str, int]],
    enforce_must_appear: bool = True,
    presolve_done: bool = False,
) -> tuple[list[dict[str, object]], list[_GameSide]]:
    """Solve a team-season MILP. Returns (assigned_rows, sides_needing_fallback).

    On infeasibility we try, in order: drop the must-appear constraints; then
    drop game-sides that fail a per-side bipartite matching check (presolve);
    then give up and return the remaining sides for modal-lineup fallback."""
    started_build = time.monotonic()
    problem = _build_milp_problem(
        season=season,
        team_id=team_id,
        game_sides=game_sides,
        lineup_lookup=lineup_lookup,
        candidates=candidates,
        stint_windows=stint_windows,
        season_date_index=season_date_index,
        enforce_must_appear=enforce_must_appear,
    )
    if problem is None or not problem.variables:
        return [], list(game_sides)
    build_seconds = time.monotonic() - started_build
    num_assignment_vars = len(problem.variables)
    num_total_vars = len(problem.costs)
    num_slack_vars = num_total_vars - num_assignment_vars
    num_constraints = problem.next_row

    started_primary = time.monotonic()
    result = milp(
        np.asarray(problem.costs, dtype=float),
        integrality=np.asarray(problem.integrality, dtype=int),
        bounds=Bounds(
            cast(Any, np.asarray(problem.lower_bounds, dtype=float)),
            cast(Any, np.asarray(problem.upper_bounds, dtype=float)),
        ),
        constraints=problem.scipy_constraints(),
        options={"mip_rel_gap": 1e-2, "time_limit": 60.0},
    )
    primary_seconds = time.monotonic() - started_primary
    if result.x is None or result.fun is None:
        if enforce_must_appear:
            _logger().warning(
                "season %s team %s infeasible (status=%s); retrying without "
                "must-appear constraints",
                season,
                team_id,
                result.status,
            )
            return _solve_team_season(
                season=season,
                team_id=team_id,
                game_sides=game_sides,
                lineup_lookup=lineup_lookup,
                candidates=candidates,
                stint_windows=stint_windows,
                season_date_index=season_date_index,
                enforce_must_appear=False,
                presolve_done=presolve_done,
            )
        if not presolve_done:
            feasible_sides, infeasible_sides = _presolve_filter_matchable_sides(
                game_sides,
                lineup_lookup,
                candidates,
                stint_windows,
                season_date_index,
            )
            if infeasible_sides and feasible_sides:
                _logger().warning(
                    "season %s team %s presolve dropped %d/%d sides as "
                    "individually infeasible; retrying with %d sides",
                    season,
                    team_id,
                    len(infeasible_sides),
                    len(game_sides),
                    len(feasible_sides),
                )
                rows, fallback_sides = _solve_team_season(
                    season=season,
                    team_id=team_id,
                    game_sides=feasible_sides,
                    lineup_lookup=lineup_lookup,
                    candidates=candidates,
                    stint_windows=stint_windows,
                    season_date_index=season_date_index,
                    enforce_must_appear=False,
                    presolve_done=True,
                )
                return rows, fallback_sides + infeasible_sides
        _logger().warning(
            "season %s team %s unsolvable after presolve (status=%s); falling "
            "back %d sides to modal lineup",
            season,
            team_id,
            result.status,
            len(game_sides),
        )
        return [], list(game_sides)

    tiebreak_lower = list(problem.lower_bounds)
    tiebreak_upper = list(problem.upper_bounds)
    for index in range(num_assignment_vars, num_total_vars):
        pinned = float(result.x[index])
        tiebreak_lower[index] = max(tiebreak_lower[index], pinned - _MILP_TOLERANCE)
        tiebreak_upper[index] = min(tiebreak_upper[index], pinned + _MILP_TOLERANCE)
    started_tiebreak = time.monotonic()
    tied = milp(
        np.asarray(problem.costs, dtype=float),
        integrality=np.asarray(problem.integrality, dtype=int),
        bounds=Bounds(
            cast(Any, np.asarray(tiebreak_lower, dtype=float)),
            cast(Any, np.asarray(tiebreak_upper, dtype=float)),
        ),
        constraints=problem.scipy_constraints(),
        options={"mip_rel_gap": 0.0},
    )
    tiebreak_seconds = time.monotonic() - started_tiebreak
    solution = tied.x if tied.success and tied.x is not None else result.x

    total_slack = sum(
        float(solution[index])
        for index in range(num_assignment_vars, num_total_vars)
    )
    _logger().info(
        "season %s team %s: %d sides, %d asn-vars, %d slack-vars, "
        "%d constraints, build %.2fs, primary %.2fs, tiebreak %.2fs, slack %.2f",
        season,
        team_id,
        len(game_sides),
        num_assignment_vars,
        num_slack_vars,
        num_constraints,
        build_seconds,
        primary_seconds,
        tiebreak_seconds,
        total_slack,
    )

    selected = [
        variable
        for index, variable in enumerate(problem.variables)
        if solution[index] > 0.5
    ]
    rows = [_pitcher_assignment_row(side, lineup_lookup) for side in game_sides]
    rows.extend(_assignment_row(variable) for variable in selected)
    return [row for row in rows if row is not None], []


def _presolve_filter_matchable_sides(
    game_sides: list[_GameSide],
    lineup_lookup: dict[tuple[int, str], list[_LineupSlot]],
    candidates: list[_Candidate],
    stint_windows: dict[tuple[int, str, str, int], _StintWindow],
    season_date_index: dict[int, dict[str, int]],
) -> tuple[list[_GameSide], list[_GameSide]]:
    """Per-side bipartite matching feasibility check. A side is feasible if
    we can assign 8 distinct candidates to its 8 non-pitcher slots, where
    each candidate is eligible for the slot's (fielding_position,
    lineup_position) pair (modal slot restriction, starting-pitcher
    exclusion, stint window all applied)."""
    feasible: list[_GameSide] = []
    infeasible: list[_GameSide] = []
    candidates_by_team: dict[tuple[int, str], list[_Candidate]] = defaultdict(list)
    for candidate in candidates:
        candidates_by_team[(candidate.season, candidate.team_id)].append(candidate)

    for game_side in game_sides:
        if _side_has_perfect_matching(
            game_side,
            lineup_lookup,
            candidates_by_team.get((game_side.season, game_side.team_id), []),
            stint_windows,
            season_date_index,
        ):
            feasible.append(game_side)
        else:
            infeasible.append(game_side)
    return feasible, infeasible


def _side_has_perfect_matching(
    game_side: _GameSide,
    lineup_lookup: dict[tuple[int, str], list[_LineupSlot]],
    team_candidates: list[_Candidate],
    stint_windows: dict[tuple[int, str, str, int], _StintWindow],
    season_date_index: dict[int, dict[str, int]],
) -> bool:
    lineup = lineup_lookup.get((game_side.season, game_side.team_id))
    if lineup is None:
        return False
    non_pitcher_slots = [
        slot for slot in lineup if slot.fielding_position in _NON_PITCHER_POSITIONS
    ]
    if len(non_pitcher_slots) != len(_NON_PITCHER_POSITIONS):
        return False

    default_slot_by_player = {
        slot.player_id: slot.lineup_position for slot in non_pitcher_slots
    }
    excluded_pitcher_id = _effective_pitcher_id(game_side, lineup)
    slots_indexed = list(enumerate(non_pitcher_slots))
    player_index: dict[str, int] = {}
    rows: list[int] = []
    cols: list[int] = []
    for slot_idx, slot in slots_indexed:
        for candidate in team_candidates:
            if candidate.fielding_position not in _NON_PITCHER_POSITIONS:
                continue
            if (
                excluded_pitcher_id is not None
                and candidate.player_id == excluded_pitcher_id
            ):
                continue
            default_slot = default_slot_by_player.get(candidate.player_id)
            if default_slot is None:
                if candidate.fielding_position != slot.fielding_position:
                    continue
            elif (
                default_slot != slot.lineup_position
                and candidate.fielding_position != slot.fielding_position
            ):
                continue
            if not _candidate_eligible_for_side(
                candidate, game_side, stint_windows, season_date_index
            ):
                continue
            col = player_index.setdefault(candidate.player_id, len(player_index))
            rows.append(slot_idx)
            cols.append(col)

    if not rows or len(player_index) < len(slots_indexed):
        return False

    matrix = csr_matrix(
        ([1] * len(rows), (rows, cols)),
        shape=(len(slots_indexed), len(player_index)),
    )
    matching = maximum_bipartite_matching(matrix, perm_type="column")
    return bool(np.all(matching != -1))


def _build_milp_problem(
    *,
    season: int,
    team_id: str,
    game_sides: list[_GameSide],
    lineup_lookup: dict[tuple[int, str], list[_LineupSlot]],
    candidates: list[_Candidate],
    stint_windows: dict[tuple[int, str, str, int], _StintWindow],
    season_date_index: dict[int, dict[str, int]],
    enforce_must_appear: bool = True,
) -> _MilpProblem | None:
    del team_id
    variables: list[_AssignmentVariable] = []
    costs: list[float] = []
    integrality: list[int] = []
    lower_bounds: list[float] = []
    upper_bounds: list[float] = []
    random_costs: list[float] = []
    variable_groups: dict[str, dict[object, list[int]]] = {
        "position": defaultdict(list),
        "slot": defaultdict(list),
        "side_player": defaultdict(list),
        "position_target": defaultdict(list),
        "total_target": defaultdict(list),
    }
    position_targets: dict[tuple[int, str, str, int, int], float] = {}
    total_targets: dict[tuple[int, str, str, int], float] = {}

    non_pitcher_fielding: dict[tuple[int, str, str, int], float] = defaultdict(float)
    for candidate in candidates:
        position_targets[_position_key(candidate)] = candidate.games_at_position
        if candidate.fielding_position in _NON_PITCHER_POSITIONS:
            non_pitcher_fielding[
                _player_stint_key(candidate)
            ] += candidate.games_at_position
    for key, total in non_pitcher_fielding.items():
        total_targets[key] = total

    for game_side in game_sides:
        lineup = lineup_lookup.get((game_side.season, game_side.team_id))
        if lineup is None:
            continue

        non_pitcher_slots = [
            slot for slot in lineup if slot.fielding_position in _NON_PITCHER_POSITIONS
        ]
        if len(non_pitcher_slots) != len(_NON_PITCHER_POSITIONS):
            return None

        default_slot_by_player = {
            slot.player_id: slot.lineup_position for slot in non_pitcher_slots
        }
        excluded_pitcher_id = _effective_pitcher_id(game_side, lineup)
        for slot in non_pitcher_slots:
            for candidate in candidates:
                if candidate.fielding_position not in _NON_PITCHER_POSITIONS:
                    continue
                if (
                    excluded_pitcher_id is not None
                    and candidate.player_id == excluded_pitcher_id
                ):
                    continue
                default_slot = default_slot_by_player.get(candidate.player_id)
                if default_slot is None:
                    if candidate.fielding_position != slot.fielding_position:
                        continue
                elif (
                    default_slot != slot.lineup_position
                    and candidate.fielding_position != slot.fielding_position
                ):
                    continue
                if not _candidate_eligible_for_side(
                    candidate, game_side, stint_windows, season_date_index
                ):
                    continue

                variable_index = len(variables)
                variable = _AssignmentVariable(
                    side_index=game_side.index,
                    game_id=game_side.game_id,
                    side=game_side.side,
                    team_id=game_side.team_id,
                    lineup_position=slot.lineup_position,
                    candidate=candidate,
                    game_order=game_side.date_key,
                )
                variables.append(variable)
                tiebreak = _stable_random_cost(variable)
                modal_default_bonus = (
                    -_MODAL_DEFAULT_SLOT_BONUS
                    if default_slot is not None
                    and default_slot == slot.lineup_position
                    else 0.0
                )
                costs.append(
                    _ASSIGNMENT_TIEBREAK_EPSILON * tiebreak + modal_default_bonus
                )
                random_costs.append(tiebreak)
                integrality.append(1)
                lower_bounds.append(0.0)
                upper_bounds.append(1.0)
                variable_groups["position"][
                    (game_side.index, candidate.fielding_position)
                ].append(variable_index)
                variable_groups["slot"][(game_side.index, slot.lineup_position)].append(
                    variable_index
                )
                variable_groups["side_player"][
                    (game_side.index, candidate.player_id)
                ].append(variable_index)
                variable_groups["position_target"][_position_key(candidate)].append(
                    variable_index
                )
                variable_groups["total_target"][_player_stint_key(candidate)].append(
                    variable_index
                )

    if not variables:
        return None

    problem = _MilpProblem(
        variables=variables,
        costs=costs,
        integrality=integrality,
        lower_bounds=lower_bounds,
        upper_bounds=upper_bounds,
        row_indexes=[],
        col_indexes=[],
        coefficients=[],
        constraint_lower=[],
        constraint_upper=[],
        random_costs=random_costs,
    )

    for game_side in game_sides:
        for position in _NON_PITCHER_POSITIONS:
            indexes = variable_groups["position"].get((game_side.index, position), [])
            if not indexes:
                return None
            problem.add_constraint(
                {index: 1.0 for index in indexes},
                lower_bound=1.0,
                upper_bound=1.0,
            )

        lineup = lineup_lookup[(game_side.season, game_side.team_id)]
        for slot in lineup:
            if slot.fielding_position not in _NON_PITCHER_POSITIONS:
                continue
            indexes = variable_groups["slot"].get(
                (game_side.index, slot.lineup_position),
                [],
            )
            if not indexes:
                return None
            problem.add_constraint(
                {index: 1.0 for index in indexes},
                lower_bound=1.0,
                upper_bound=1.0,
            )

    for indexes in variable_groups["side_player"].values():
        problem.add_constraint(
            {index: 1.0 for index in indexes},
            lower_bound=-math.inf,
            upper_bound=1.0,
        )

    _add_error_constraints(
        problem,
        variable_groups["position_target"],
        position_targets,
    )
    _add_capped_error_constraints(
        problem,
        variable_groups["total_target"],
        total_targets,
    )

    if enforce_must_appear:
        player_stint_outs: dict[tuple[int, str, str, int], int] = {}
        for candidate in candidates:
            key = _player_stint_key(candidate)
            player_stint_outs[key] = max(
                player_stint_outs.get(key, 0), candidate.outs_played
            )
        for player_stint_key, outs_played in player_stint_outs.items():
            if outs_played <= _MIN_OUTS_FOR_REQUIRED_APPEARANCE:
                continue
            if total_targets.get(player_stint_key, 0.0) < 1.0:
                continue
            indexes = variable_groups["total_target"].get(player_stint_key, [])
            if not indexes:
                continue
            problem.add_constraint(
                {idx: 1.0 for idx in indexes},
                lower_bound=1.0,
                upper_bound=math.inf,
            )
    return problem


def _add_error_constraints(
    problem: _MilpProblem,
    grouped_variables: Mapping[Any, list[int]],
    targets: Mapping[Any, float],
) -> None:
    for key, indexes in grouped_variables.items():
        target = targets[key]
        weight = 1.0
        over_index = problem.add_variable(
            cost=weight,
            random_cost=0.0,
            integrality=0,
            lower_bound=0.0,
            upper_bound=math.inf,
        )
        under_index = problem.add_variable(
            cost=weight,
            random_cost=0.0,
            integrality=0,
            lower_bound=0.0,
            upper_bound=math.inf,
        )
        terms = {index: 1.0 for index in indexes}
        terms[over_index] = -1.0
        terms[under_index] = 1.0
        problem.add_constraint(
            terms,
            lower_bound=float(target),
            upper_bound=float(target),
        )


def _add_capped_error_constraints(
    problem: _MilpProblem,
    grouped_variables: Mapping[Any, list[int]],
    targets: Mapping[Any, float],
) -> None:
    for key, indexes in grouped_variables.items():
        target = targets[key]
        over_index = problem.add_variable(
            cost=_TOTAL_OVER_PENALTY,
            random_cost=0.0,
            integrality=0,
            lower_bound=0.0,
            upper_bound=math.inf,
        )
        under_index = problem.add_variable(
            cost=1.0,
            random_cost=0.0,
            integrality=0,
            lower_bound=0.0,
            upper_bound=math.inf,
        )
        terms = {index: 1.0 for index in indexes}
        terms[over_index] = -1.0
        terms[under_index] = 1.0
        problem.add_constraint(
            terms,
            lower_bound=float(target),
            upper_bound=float(target),
        )


def _build_stint_windows(
    game_sides: list[_GameSide],
    candidate_rows: list[_Candidate],
    *,
    transaction_windows: Mapping[tuple[int, str, str, int], tuple[int, int]] | None = None,
) -> tuple[
    dict[int, dict[str, int]],
    dict[tuple[int, str, str, int], _StintWindow],
]:
    season_dates: dict[int, set[str]] = defaultdict(set)
    team_dates_set: dict[tuple[int, str], set[str]] = defaultdict(set)
    for side in game_sides:
        season_dates[side.season].add(side.date_key[0])
        team_dates_set[(side.season, side.team_id)].add(side.date_key[0])

    season_index: dict[int, dict[str, int]] = {}
    season_count: dict[int, int] = {}
    for season, dates in season_dates.items():
        ordered = sorted(dates)
        season_index[season] = {key: position for position, key in enumerate(ordered)}
        season_count[season] = len(ordered)

    team_dates_ordered: dict[tuple[int, str], list[str]] = {
        key: sorted(dates) for key, dates in team_dates_set.items()
    }

    txn_overrides: dict[tuple[int, str, str, int], _StintWindow] = {}
    if transaction_windows:
        for key, (start_index, end_index) in transaction_windows.items():
            season = key[0]
            n = season_count.get(season, 0)
            if n == 0:
                continue
            si = max(0, min(n - 1, start_index))
            ei = max(si, min(n - 1, end_index))
            txn_overrides[key] = _StintWindow(start_index=si, end_index=ei)

    share_by_player: dict[tuple[int, str], dict[int, dict[str, float]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for cand in candidate_rows:
        team_map = share_by_player[(cand.season, cand.player_id)][cand.stint]
        existing = team_map.get(cand.team_id, 0.0)
        if cand.games_played > 0:
            team_map[cand.team_id] = max(existing, float(cand.games_played))
        else:
            team_map[cand.team_id] = existing + float(cand.games_at_position)

    windows: dict[tuple[int, str, str, int], _StintWindow] = {}
    for (season, player_id), stints in share_by_player.items():
        n = season_count.get(season, 0)
        if n == 0:
            continue
        full = _StintWindow(start_index=0, end_index=n - 1)
        if len(stints) <= 1:
            for stint, team_map in stints.items():
                for team_id in team_map:
                    windows[(season, team_id, player_id, stint)] = full
            continue

        ordered_stints = sorted(stints.items())
        total = sum(
            share for _, team_map in ordered_stints for share in team_map.values()
        )
        if total <= 0:
            for stint, team_map in ordered_stints:
                for team_id in team_map:
                    windows[(season, team_id, player_id, stint)] = full
            continue

        cursor = 0
        cumulative = 0.0
        for index, (stint, team_map) in enumerate(ordered_stints):
            cumulative += sum(team_map.values())
            is_last = index == len(ordered_stints) - 1
            if is_last:
                end_index = n - 1
            else:
                fraction = cumulative / total
                end_index = max(cursor, min(n - 1, round(fraction * n) - 1))
            for team_id, games in team_map.items():
                window_key = (season, team_id, player_id, stint)
                if window_key in txn_overrides:
                    windows[window_key] = txn_overrides[window_key]
                    continue
                start_index, final_end = _ensure_window_covers_team(
                    cursor,
                    end_index,
                    n,
                    season_index[season],
                    team_dates_ordered.get((season, team_id), []),
                    int(round(games)),
                )
                windows[window_key] = _StintWindow(
                    start_index=start_index,
                    end_index=final_end,
                )
            cursor = min(n - 1, end_index + 1)
    return season_index, windows


def _ensure_window_covers_team(
    proposed_start: int,
    proposed_end: int,
    n: int,
    season_index_for_season: dict[str, int],
    team_dates: list[str],
    target_games: int,
) -> tuple[int, int]:
    """Expand a proposed [start, end] window so it covers at least
    ``target_games`` of the team's actual game-side dates. The proportional
    allocation can place a window in a date range that contains no team
    games at all (e.g., a late-season trade with a tiny share). Without
    this guard the player would be silently ineligible for every team
    game-side."""
    if not team_dates or target_games <= 0:
        return proposed_start, proposed_end
    target_games = min(target_games, len(team_dates))
    team_indexes = [season_index_for_season[d] for d in team_dates]
    inside = [idx for idx in team_indexes if proposed_start <= idx <= proposed_end]
    if len(inside) >= target_games:
        return proposed_start, proposed_end
    midpoint = (proposed_start + proposed_end) // 2
    sorted_by_distance = sorted(team_indexes, key=lambda idx: abs(idx - midpoint))
    chosen = sorted_by_distance[:target_games]
    new_start = min(proposed_start, min(chosen))
    new_end = max(proposed_end, max(chosen))
    return max(0, new_start), min(n - 1, new_end)


def _player_stint_key(candidate: _Candidate) -> tuple[int, str, str, int]:
    return (candidate.season, candidate.team_id, candidate.player_id, candidate.stint)


def _position_key(candidate: _Candidate) -> tuple[int, str, str, int, int]:
    return (
        candidate.season,
        candidate.team_id,
        candidate.player_id,
        candidate.stint,
        candidate.fielding_position,
    )


def _actual_target_counts(
    candidates: list[_Candidate],
) -> tuple[
    dict[tuple[int, str, str, int], float],
    dict[tuple[int, str, str, int, int], float],
]:
    non_pitcher_fielding: dict[tuple[int, str, str, int], float] = defaultdict(float)
    positions: dict[tuple[int, str, str, int, int], float] = defaultdict(float)
    for candidate in candidates:
        if candidate.fielding_position in _NON_PITCHER_POSITIONS:
            player_key = _player_stint_key(candidate)
            non_pitcher_fielding[player_key] += candidate.games_at_position
            positions[_position_key(candidate)] += candidate.games_at_position

    totals = {key: total for key, total in non_pitcher_fielding.items() if total > 0}
    return totals, dict(positions)


def _realized_target_counts(
    assignments: pl.DataFrame,
) -> tuple[
    dict[tuple[int, str, str, int], int],
    dict[tuple[int, str, str, int, int], int],
]:
    non_pitcher = assignments.filter(
        pl.col("fielding_position").is_in([2, 3, 4, 5, 6, 7, 8, 9])
    )
    if non_pitcher.is_empty():
        return {}, {}

    totals = {
        (
            _as_int(row["season"]),
            str(row["team_id"]),
            str(row["player_id"]),
            _as_int(row["stint"]),
        ): _as_int(row["len"])
        for row in non_pitcher.group_by(["season", "team_id", "player_id", "stint"])
        .len()
        .to_dicts()
    }
    positions = {
        (
            _as_int(row["season"]),
            str(row["team_id"]),
            str(row["player_id"]),
            _as_int(row["stint"]),
            _as_int(row["fielding_position"]),
        ): _as_int(row["len"])
        for row in non_pitcher.group_by(
            ["season", "team_id", "player_id", "stint", "fielding_position"]
        )
        .len()
        .to_dicts()
    }
    return totals, positions


def _report_row(
    key: tuple[int, str, str, int],
    metric_type: str,
    fielding_position: int | None,
    actual_games: float,
    realized_games: int,
) -> dict[str, object]:
    signed_error = realized_games - actual_games
    abs_error = abs(signed_error)
    denominator = max(actual_games, 1.0)
    return {
        "season": key[0],
        "team_id": key[1],
        "player_id": key[2],
        "stint": key[3],
        "metric_type": metric_type,
        "fielding_position": fielding_position,
        "actual_games": actual_games,
        "realized_games": realized_games,
        "signed_error": signed_error,
        "abs_error": abs_error,
        "pct_error": abs_error / denominator,
        "signed_pct_error": signed_error / denominator,
    }


def _fallback_assignments(
    games: pl.DataFrame,
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for lineup in _iter_fallback_game_side_lineups(
        games,
        lineups,
        candidates,
        insert_pitcher=True,
    ):
        rows.extend(
            {
                "game_id": row["game_id"],
                "season": row["season"],
                "team_id": row["team_id"],
                "player_id": row["player_id"],
                "stint": row["stint"],
                "side": row["side"],
                "lineup_position": row["lineup_position"],
                "fielding_position": row["fielding_position"],
            }
            for row in lineup
        )

    if not rows:
        return _empty_assignment_core()

    return pl.DataFrame(rows).select(
        pl.col("game_id"),
        pl.col("season").cast(pl.Int16),
        pl.col("team_id"),
        pl.col("player_id"),
        pl.col("stint").cast(pl.Int16),
        pl.col("side"),
        pl.col("lineup_position").cast(pl.UInt8),
        pl.col("fielding_position").cast(pl.UInt8),
    )


def _fallback_assignments_for_sides(
    games: pl.DataFrame,
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
    game_sides: list[_GameSide],
) -> list[dict[str, object]]:
    keys = {(side.game_id, side.side) for side in game_sides}
    return [
        row
        for row in _fallback_assignments(games, lineups, candidates).to_dicts()
        if (str(row["game_id"]), str(row["side"])) in keys
    ]


def _iter_fallback_game_side_lineups(
    games: pl.DataFrame,
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
    *,
    insert_pitcher: bool,
) -> Iterable[list[dict[str, object]]]:
    games = _with_required_game_columns(games)
    _validate_columns(games, GAME_INPUT_COLUMNS)
    _validate_columns(lineups, LINEUP_INPUT_COLUMNS)
    _validate_columns(candidates, CANDIDATE_INPUT_COLUMNS)

    lineup_lookup = _lineup_lookup(lineups, candidates)
    candidate_lookup = _candidate_lookup(candidates)
    metrics_lookup = _metrics_lookup(candidates)

    for game in games.to_dicts():
        for side, team_col, pitcher_col in _SIDE_SPECS:
            team_id = str(game[team_col])
            key = (int(game["season"]), team_id)
            base_lineup = lineup_lookup.get(key)
            if base_lineup is None:
                continue

            lineup = [
                {
                    "season": int(game["season"]),
                    "team_id": team_id,
                    "player_id": row.player_id,
                    "fielding_position": row.fielding_position,
                    "lineup_position": row.lineup_position,
                    "stint": _fallback_stint_for_slot(
                        candidates,
                        key[0],
                        team_id,
                        row.player_id,
                        row.fielding_position,
                    ),
                    "game_id": game["game_id"],
                    "side": side,
                    **metrics_lookup.get(
                        (int(game["season"]), team_id, row.player_id),
                        {"plate_appearances": 0, "games_played": 0},
                    ),
                }
                for row in base_lineup
            ]
            if insert_pitcher:
                lineup = _insert_starting_pitcher(
                    lineup,
                    candidate_lookup=candidate_lookup,
                    metrics_lookup=metrics_lookup,
                    listed_pitcher_id=game[pitcher_col],
                    season=key[0],
                    team_id=key[1],
                )
                if lineup is None:
                    continue

            yield lineup


def _insert_starting_pitcher(
    lineup: list[dict[str, object]],
    *,
    candidate_lookup: dict[tuple[int, str, int], list[dict[str, object]]],
    metrics_lookup: dict[tuple[int, str, str], dict[str, object]],
    listed_pitcher_id: object,
    season: int,
    team_id: str,
) -> list[dict[str, object]] | None:
    if listed_pitcher_id is None:
        return lineup

    pitcher_id = str(listed_pitcher_id)
    pitcher_index = _index_for_position(lineup, 1)
    if pitcher_index is None:
        return None

    modal_pitcher_id = str(lineup[pitcher_index]["player_id"])
    if pitcher_id == modal_pitcher_id:
        return lineup

    existing_index = _index_for_player(lineup, pitcher_id)
    pitcher_row = _lineup_row_for_player(
        lineup[pitcher_index],
        pitcher_id,
        metrics_lookup,
        season,
        team_id,
    )
    lineup[pitcher_index] = {
        **pitcher_row,
        "game_id": lineup[pitcher_index]["game_id"],
        "side": lineup[pitcher_index]["side"],
    }

    if existing_index is None:
        return lineup

    occupied = {
        str(row["player_id"])
        for index, row in enumerate(lineup)
        if index != existing_index
    }
    vacated_position = _as_int(lineup[existing_index]["fielding_position"])
    vacated_lineup_position = _as_int(lineup[existing_index]["lineup_position"])
    replacement = _next_candidate(
        candidate_lookup.get((season, team_id, vacated_position), []),
        occupied,
    )
    if replacement is None:
        return None

    lineup[existing_index] = {
        **replacement,
        "season": lineup[existing_index]["season"],
        "team_id": lineup[existing_index]["team_id"],
        "game_id": lineup[existing_index]["game_id"],
        "side": lineup[existing_index]["side"],
        "fielding_position": vacated_position,
        "lineup_position": vacated_lineup_position,
    }
    return lineup


def _lineup_lookup(
    lineups: pl.DataFrame,
    candidates: pl.DataFrame,
) -> dict[tuple[int, str], list[_LineupSlot]]:
    del candidates
    lookup: dict[tuple[int, str], list[_LineupSlot]] = defaultdict(list)
    for row in lineups.to_dicts():
        lookup[(_as_int(row["season"]), str(row["team_id"]))].append(
            _LineupSlot(
                lineup_position=_as_int(row["lineup_position"]),
                fielding_position=_as_int(row["fielding_position"]),
                player_id=str(row["player_id"]),
            )
        )

    out: dict[tuple[int, str], list[_LineupSlot]] = {}
    for key, rows in lookup.items():
        if len(rows) != len(_FIELDING_POSITIONS):
            continue
        if {row.fielding_position for row in rows} != set(_FIELDING_POSITIONS):
            continue
        if len({row.lineup_position for row in rows}) != len(_FIELDING_POSITIONS):
            continue
        out[key] = sorted(rows, key=lambda row: row.fielding_position)
    return out


def _candidate_rows(candidates: pl.DataFrame) -> list[_Candidate]:
    grouped: dict[tuple[int, str, str, int], list[dict[str, object]]] = defaultdict(
        list
    )
    for row in candidates.to_dicts():
        key = (
            _as_int(row["season"]),
            str(row["team_id"]),
            str(row["player_id"]),
            _as_int(row["stint"]),
        )
        grouped[key].append(row)

    rows: list[_Candidate] = []
    for (season, team_id, player_id, stint), group in grouped.items():
        total_fielding_games = sum(_as_float(row["games_at_position"]) for row in group)
        batting_games = max(_as_int(row["games_played"]) for row in group)
        scale = (
            batting_games / total_fielding_games
            if total_fielding_games > batting_games and total_fielding_games > 0
            else 1.0
        )
        games_total = (
            float(batting_games)
            if scale < 1.0
            else max(_as_float(row["games_total"]) for row in group)
        )
        outs_played = max(_as_int(row.get("outs_played", 0) or 0) for row in group)
        for row in group:
            raw_games_at_position = _as_float(row["games_at_position"])
            games_at_position = raw_games_at_position * scale
            if raw_games_at_position <= 0:
                continue
            rows.append(
                _Candidate(
                    season=season,
                    team_id=team_id,
                    player_id=player_id,
                    stint=stint,
                    fielding_position=_as_int(row["fielding_position"]),
                    games_at_position=games_at_position,
                    games_total=games_total,
                    plate_appearances=_as_int(row["plate_appearances"]),
                    games_played=_as_int(row["games_played"]),
                    outs_played=outs_played,
                )
            )
    return rows


def _allocate_games_total_per_stint(
    candidates: list[_Candidate],
) -> list[_Candidate]:
    """Split team-year ``games_total`` across a player's stints at that team.

    Lahman appearances has ``g_all`` only at the (player, year, team) grain,
    so the upstream model broadcasts the same value to every stint row. That
    over-counts a player who came back to the team for a brief stint:
    e.g., 71 games at CIN stint 1 plus 1 game at CIN stint 3 both report
    games_total=72. For the optimizer's hard cap, we want each stint capped at
    its actual playing time. Allocate proportionally by the stint's share of
    the player's total fielding-position games at that team.
    """
    if not candidates:
        return candidates

    pos_sums: dict[tuple[int, str, str, int], float] = defaultdict(float)
    team_total: dict[tuple[int, str, str], float] = defaultdict(float)
    team_games_total: dict[tuple[int, str, str], float] = {}
    for cand in candidates:
        stint_key = _player_stint_key(cand)
        team_key = (cand.season, cand.team_id, cand.player_id)
        pos_sums[stint_key] += cand.games_at_position
        team_total[team_key] += cand.games_at_position
        team_games_total[team_key] = max(
            team_games_total.get(team_key, 0.0),
            cand.games_total,
        )

    out: list[_Candidate] = []
    for cand in candidates:
        stint_key = _player_stint_key(cand)
        team_key = (cand.season, cand.team_id, cand.player_id)
        total = team_total[team_key]
        if total > 0:
            stint_share = pos_sums[stint_key] / total
            stint_total = team_games_total[team_key] * stint_share
        else:
            stint_total = cand.games_total
        out.append(
            _Candidate(
                season=cand.season,
                team_id=cand.team_id,
                player_id=cand.player_id,
                stint=cand.stint,
                fielding_position=cand.fielding_position,
                games_at_position=cand.games_at_position,
                games_total=stint_total,
                plate_appearances=cand.plate_appearances,
                games_played=cand.games_played,
                outs_played=cand.outs_played,
            )
        )
    return out


def _scale_position_targets(
    candidates: list[_Candidate],
    sides_by_team: Mapping[tuple[int, str], int],
) -> list[_Candidate]:
    """Cap per-(team, position) target sums at the team's game-side count.

    Lahman ``fielding.g`` counts every appearance at a position, including
    relief and defensive substitutions. Each synthetic game-side has exactly
    one starter per position, so when the sum across players exceeds the
    available sides we scale all targets at that position pro-rata. Player
    totals are then re-derived from scaled position sums to stay consistent.
    """
    if not candidates:
        return candidates

    sums_by_position: dict[tuple[int, str, int], float] = defaultdict(float)
    for cand in candidates:
        sums_by_position[
            (cand.season, cand.team_id, cand.fielding_position)
        ] += cand.games_at_position

    position_scale: dict[tuple[int, str, int], float] = {}
    for key, total in sums_by_position.items():
        season, team_id, _ = key
        sides = sides_by_team.get((season, team_id), 0)
        if total > sides > 0:
            position_scale[key] = float(sides) / total
        else:
            position_scale[key] = 1.0

    scaled_position: list[float] = []
    for cand in candidates:
        scale = position_scale[
            (cand.season, cand.team_id, cand.fielding_position)
        ]
        new_value = cand.games_at_position * scale
        scaled_position.append(new_value)

    out: list[_Candidate] = []
    for cand, new_g_at_pos in zip(candidates, scaled_position, strict=True):
        if new_g_at_pos <= 0:
            continue
        new_total = cand.games_total
        out.append(
            _Candidate(
                season=cand.season,
                team_id=cand.team_id,
                player_id=cand.player_id,
                stint=cand.stint,
                fielding_position=cand.fielding_position,
                games_at_position=new_g_at_pos,
                games_total=new_total,
                plate_appearances=cand.plate_appearances,
                games_played=cand.games_played,
                outs_played=cand.outs_played,
            )
        )
    return out


def _candidate_lookup(
    candidates: pl.DataFrame,
) -> dict[tuple[int, str, int], list[dict[str, object]]]:
    by_stint = candidates.group_by(["season", "team_id", "player_id", "stint"]).agg(
        pl.max("games_total").alias("total_games"),
        pl.max("plate_appearances").alias("plate_appearances"),
        pl.max("games_played").alias("games_played"),
    )
    with_total_games = candidates.join(
        by_stint,
        on=["season", "team_id", "player_id", "stint"],
        how="left",
    )
    lookup: dict[tuple[int, str, int], list[dict[str, object]]] = {}
    for row in with_total_games.to_dicts():
        key = (
            _as_int(row["season"]),
            str(row["team_id"]),
            _as_int(row["fielding_position"]),
        )
        lookup.setdefault(key, []).append(
            {
                "player_id": str(row["player_id"]),
                "stint": _as_int(row["stint"]),
                "plate_appearances": _as_int(row["plate_appearances"]),
                "games_played": _as_int(row["games_played"]),
                "games_at_position": _as_int(row["games_at_position"]),
                "total_games": _as_int(row["total_games"]),
            }
        )

    for rows in lookup.values():
        rows.sort(
            key=lambda row: (
                -_as_int(row["games_at_position"]),
                -_as_int(row["total_games"]),
                -_as_int(row["plate_appearances"]),
                str(row["player_id"]),
            )
        )

    return lookup


def _metrics_lookup(
    candidates: pl.DataFrame,
) -> dict[tuple[int, str, str], dict[str, object]]:
    if candidates.is_empty():
        return {}

    by_stint = candidates.group_by(["season", "team_id", "player_id", "stint"]).agg(
        pl.max("plate_appearances").alias("plate_appearances"),
        pl.max("games_played").alias("games_played"),
    )
    metrics = by_stint.group_by(["season", "team_id", "player_id"]).agg(
        pl.sum("plate_appearances").alias("plate_appearances"),
        pl.sum("games_played").alias("games_played"),
    )
    return {
        (_as_int(row["season"]), str(row["team_id"]), str(row["player_id"])): {
            "plate_appearances": _as_int(row["plate_appearances"]),
            "games_played": _as_int(row["games_played"]),
        }
        for row in metrics.to_dicts()
    }


def _lineup_row_for_player(
    row: dict[str, object],
    player_id: str,
    metrics_lookup: dict[tuple[int, str, str], dict[str, object]],
    season: int,
    team_id: str,
) -> dict[str, object]:
    metrics = metrics_lookup.get(
        (season, team_id, player_id),
        {"plate_appearances": 0, "games_played": 0},
    )
    return {
        "season": season,
        "team_id": team_id,
        "player_id": player_id,
        "fielding_position": _as_int(row["fielding_position"]),
        "lineup_position": _as_int(row["lineup_position"]),
        "stint": _as_int(row.get("stint", 1)),
        "plate_appearances": _as_int(metrics["plate_appearances"]),
        "games_played": _as_int(metrics["games_played"]),
    }


def _game_sides(games: pl.DataFrame) -> Iterable[_GameSide]:
    index = 0
    for game in games.sort(["season", "date", "game_id"]).to_dicts():
        for side, team_col, pitcher_col in _SIDE_SPECS:
            pitcher_id = game[pitcher_col]
            yield _GameSide(
                index=index,
                game_id=str(game["game_id"]),
                date_key=(_date_key(game["date"]), str(game["game_id"])),
                season=_as_int(game["season"]),
                team_id=str(game[team_col]),
                side=side,
                starting_pitcher_id=None if pitcher_id is None else str(pitcher_id),
                use_dh=bool(game.get("use_dh", False)),
            )
            index += 1


def _pitcher_assignment_row(
    game_side: _GameSide,
    lineup_lookup: dict[tuple[int, str], list[_LineupSlot]],
) -> dict[str, object] | None:
    lineup = lineup_lookup.get((game_side.season, game_side.team_id))
    if lineup is None:
        return None
    pitcher_slot = next(
        (slot for slot in lineup if slot.fielding_position == 1),
        None,
    )
    if pitcher_slot is None:
        return None
    return {
        "game_id": game_side.game_id,
        "season": game_side.season,
        "team_id": game_side.team_id,
        "player_id": game_side.starting_pitcher_id or pitcher_slot.player_id,
        "stint": 0,
        "side": game_side.side,
        "lineup_position": pitcher_slot.lineup_position,
        "fielding_position": 1,
    }


def _assignment_row(variable: _AssignmentVariable) -> dict[str, object]:
    return {
        "game_id": variable.game_id,
        "season": variable.candidate.season,
        "team_id": variable.team_id,
        "player_id": variable.candidate.player_id,
        "stint": variable.candidate.stint,
        "side": variable.side,
        "lineup_position": variable.lineup_position,
        "fielding_position": variable.candidate.fielding_position,
    }


def _resolve_worker_count(num_items: int) -> int:
    raw = os.environ.get("BC_LINEUP_WORKERS")
    if raw is None:
        default = max(1, (os.cpu_count() or 1) - 1)
        return min(default, num_items)
    try:
        requested = int(raw)
    except ValueError:
        _logger().warning(
            "BC_LINEUP_WORKERS=%r is not an integer, defaulting to 1", raw
        )
        return 1
    return max(1, min(requested, num_items))


def _stable_random_cost(variable: _AssignmentVariable) -> float:
    raw = "|".join(
        [
            variable.game_id,
            variable.side,
            variable.team_id,
            str(variable.lineup_position),
            variable.candidate.player_id,
            str(variable.candidate.stint),
            str(variable.candidate.fielding_position),
        ]
    )
    digest = hashlib.blake2b(raw.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") / 2**64


def _next_candidate(
    candidates: list[dict[str, object]],
    occupied: set[str],
) -> dict[str, object] | None:
    for candidate in candidates:
        if str(candidate["player_id"]) not in occupied:
            return candidate
    return None


def _fallback_stint_for_slot(
    candidates: pl.DataFrame,
    season: int,
    team_id: str,
    player_id: str,
    fielding_position: int,
) -> int:
    matches = candidates.filter(
        (pl.col("season") == season)
        & (pl.col("team_id") == team_id)
        & (pl.col("player_id") == player_id)
        & (pl.col("fielding_position") == fielding_position)
    )
    if matches.is_empty():
        return 1
    row = (
        matches.sort(
            ["games_at_position", "games_total", "stint"],
            descending=[True, True, False],
        )
        .head(1)
        .to_dicts()[0]
    )
    return _as_int(row["stint"])


def _index_for_position(
    lineup: list[dict[str, object]],
    fielding_position: int,
) -> int | None:
    for index, row in enumerate(lineup):
        if _as_int(row["fielding_position"]) == fielding_position:
            return index
    return None


def _index_for_player(
    lineup: list[dict[str, object]],
    player_id: str,
) -> int | None:
    for index, row in enumerate(lineup):
        if str(row["player_id"]) == player_id:
            return index
    return None


def _pa_per_game(row: dict[str, object]) -> float:
    games_played = _as_int(row["games_played"])
    if games_played == 0:
        return 0.0
    return _as_int(row["plate_appearances"]) / games_played


def _with_required_game_columns(games: pl.DataFrame) -> pl.DataFrame:
    out = games
    if "date" not in out.columns:
        out = out.with_columns(
            pl.col("game_id").cast(pl.String).str.slice(3, 8).alias("date")
        )
    if "use_dh" not in out.columns:
        out = out.with_columns(pl.lit(False).alias("use_dh"))
    return out


def _date_key(value: object) -> str:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def _logger() -> logging.Logger:
    return logging.getLogger(__name__)


def _as_int(value: object) -> int:
    return int(cast(Any, value))


def _as_float(value: object) -> float:
    return float(cast(Any, value))


def _validate_columns(frame: pl.DataFrame, columns: tuple[str, ...]) -> None:
    missing = set(columns) - set(frame.columns)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")


def _empty_assignment_core() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "game_id": pl.String,
            "season": pl.Int16,
            "team_id": pl.String,
            "player_id": pl.String,
            "stint": pl.Int16,
            "side": pl.String,
            "lineup_position": pl.UInt8,
            "fielding_position": pl.UInt8,
        }
    )


def _empty_lineup_report() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "season": pl.Int16,
            "team_id": pl.String,
            "player_id": pl.String,
            "stint": pl.Int16,
            "metric_type": pl.String,
            "fielding_position": pl.UInt8,
            "actual_games": pl.Float64,
            "realized_games": pl.Int32,
            "signed_error": pl.Float64,
            "abs_error": pl.Float64,
            "pct_error": pl.Float64,
            "signed_pct_error": pl.Float64,
            "error_rank": pl.UInt32,
        }
    )


def _empty_batting_core() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "game_id": pl.String,
            "batter_id": pl.String,
            "side": pl.String,
            "lineup_position": pl.UInt8,
        }
    )


def _empty_fielding_core() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "game_id": pl.String,
            "fielder_id": pl.String,
            "side": pl.String,
            "fielding_position": pl.UInt8,
        }
    )
