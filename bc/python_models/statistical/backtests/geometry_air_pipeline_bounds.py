from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import numpy.typing as npt
import polars as pl
from scipy.linalg import null_space
from scipy.optimize import linprog

from python_models.statistical.backtests.geometry_air_pipeline_data import (
    CLASSES,
    CLUE_LEVELS,
)

FloatArray = npt.NDArray[np.float64]
SLACK_FACTOR = 2.0
SLACK_FLOOR = 0.01
VARIABLES = len(CLASSES) * len(CLASSES) + len(CLASSES)


def clue_table(frame: pl.DataFrame) -> FloatArray:
    table = np.full((len(CLASSES), len(CLUE_LEVELS)), 0.5 / len(CLUE_LEVELS))
    counts = frame.group_by("target_class", "clue").len()
    for band, clue, n in counts.iter_rows():
        table[CLASSES.index(str(band)), CLUE_LEVELS.index(str(clue))] += int(n)
    return table / table.sum(axis=1, keepdims=True)


def band_given_recorded(frame: pl.DataFrame) -> FloatArray:
    table = np.zeros((len(CLASSES), len(CLASSES)))
    counts = frame.group_by("recorded_air_subtype", "target_class").len()
    for recorded, band, n in counts.iter_rows():
        table[CLASSES.index(str(recorded)), CLASSES.index(str(band))] += int(n)
    if (table.sum(axis=1) == 0).any():
        raise ValueError("every recorded subtype needs reference events")
    return table / table.sum(axis=1, keepdims=True)


def clue_slack(
    first: FloatArray,
    second: FloatArray,
    *,
    factor: float = SLACK_FACTOR,
    floor: float = SLACK_FLOOR,
) -> FloatArray:
    return np.maximum(factor * np.abs(first - second).max(axis=0), floor)


def season_margins(counts: pl.DataFrame) -> tuple[FloatArray, FloatArray, int]:
    labels = np.zeros(len(CLASSES))
    clues = np.zeros(len(CLUE_LEVELS))
    for recorded, clue, n in counts.select(
        "recorded_air_subtype", "clue", "n"
    ).iter_rows():
        labels[CLASSES.index(str(recorded))] += int(n)
        clues[CLUE_LEVELS.index(str(clue))] += int(n)
    total = int(labels.sum())
    if total == 0:
        raise ValueError("season has no recorded airborne labels")
    return labels / total, clues / total, total


@dataclass(frozen=True, slots=True)
class IdentifiedSet:
    label_shares: FloatArray
    clue_margin: FloatArray
    clue_given_band: FloatArray
    slack: FloatArray
    equality: FloatArray
    equality_rhs: FloatArray
    inequality: FloatArray
    inequality_rhs: FloatArray
    feasible: bool
    lower: FloatArray
    upper: FloatArray
    vertices: FloatArray

    def translation_bounds(self) -> tuple[FloatArray, FloatArray]:
        shape = (len(CLASSES), len(CLASSES))
        return self.lower[: shape[0] * shape[1]].reshape(shape), self.upper[
            : shape[0] * shape[1]
        ].reshape(shape)

    def band_mix_bounds(self) -> tuple[FloatArray, FloatArray]:
        start = len(CLASSES) * len(CLASSES)
        return self.lower[start:], self.upper[start:]


def _translation_index(recorded: int, band: int) -> int:
    return recorded * len(CLASSES) + band


@dataclass(frozen=True, slots=True)
class Constraints:
    equality: FloatArray
    equality_rhs: FloatArray
    inequality: FloatArray
    inequality_rhs: FloatArray


def constraints(
    label_shares: FloatArray,
    clue_margin: FloatArray,
    clue_given_band: FloatArray,
    slack: FloatArray,
) -> Constraints:
    classes = len(CLASSES)
    mix_start = classes * classes
    equality: list[FloatArray] = []
    equality_rhs: list[float] = []
    for recorded in range(classes):
        row = np.zeros(VARIABLES)
        for band in range(classes):
            row[_translation_index(recorded, band)] = 1.0
        equality.append(row)
        equality_rhs.append(1.0)
    for band in range(classes):
        row = np.zeros(VARIABLES)
        for recorded in range(classes):
            row[_translation_index(recorded, band)] = label_shares[recorded]
        row[mix_start + band] = -1.0
        equality.append(row)
        equality_rhs.append(0.0)
    inequality: list[FloatArray] = []
    inequality_rhs: list[float] = []
    for level in range(len(CLUE_LEVELS)):
        row = np.zeros(VARIABLES)
        row[mix_start:] = clue_given_band[:, level]
        inequality.append(row)
        inequality_rhs.append(float(clue_margin[level] + slack[level]))
        inequality.append(-row)
        inequality_rhs.append(float(slack[level] - clue_margin[level]))
    for band in range(classes):
        for other in range(classes):
            if other == band:
                continue
            row = np.zeros(VARIABLES)
            row[_translation_index(other, band)] = 1.0
            row[_translation_index(band, band)] = -1.0
            inequality.append(row)
            inequality_rhs.append(0.0)
    return Constraints(
        equality=np.stack(equality),
        equality_rhs=np.array(equality_rhs),
        inequality=np.stack(inequality),
        inequality_rhs=np.array(inequality_rhs),
    )


def _solve(
    system: Constraints, objective: FloatArray
) -> tuple[float, FloatArray] | None:
    solution = linprog(
        objective,
        A_ub=system.inequality,
        b_ub=system.inequality_rhs,
        A_eq=system.equality,
        b_eq=system.equality_rhs,
        bounds=[(0.0, 1.0)] * VARIABLES,
        method="highs",
    )
    objective_value = solution.fun
    if not solution.success or objective_value is None:
        return None
    return float(objective_value), np.asarray(solution.x, dtype=np.float64)


def feasible(
    label_shares: FloatArray,
    clue_margin: FloatArray,
    clue_given_band: FloatArray,
    slack: FloatArray,
) -> bool:
    system = constraints(label_shares, clue_margin, clue_given_band, slack)
    return _solve(system, np.zeros(VARIABLES)) is not None


def identified_set(
    label_shares: FloatArray,
    clue_margin: FloatArray,
    clue_given_band: FloatArray,
    slack: FloatArray,
) -> IdentifiedSet:
    system = constraints(label_shares, clue_margin, clue_given_band, slack)
    lower = np.full(VARIABLES, np.nan)
    upper = np.full(VARIABLES, np.nan)
    vertices: list[FloatArray] = []
    is_feasible = True
    for variable in range(VARIABLES):
        for sign, target in ((1.0, lower), (-1.0, upper)):
            objective = np.zeros(VARIABLES)
            objective[variable] = sign
            solved = _solve(system, objective)
            if solved is None:
                is_feasible = False
                break
            target[variable] = sign * solved[0]
            vertices.append(solved[1])
        if not is_feasible:
            break
    return IdentifiedSet(
        label_shares=label_shares,
        clue_margin=clue_margin,
        clue_given_band=clue_given_band,
        slack=slack,
        equality=system.equality,
        equality_rhs=system.equality_rhs,
        inequality=system.inequality,
        inequality_rhs=system.inequality_rhs,
        feasible=is_feasible,
        lower=lower,
        upper=upper,
        vertices=np.stack(vertices) if vertices else np.empty((0, VARIABLES)),
    )


def sample_identified_set(
    identified: IdentifiedSet,
    *,
    draws: int,
    seed: int,
    burn_in: int = 2000,
    thin: int = 10,
) -> FloatArray:
    if not identified.feasible:
        raise ValueError("cannot sample an infeasible identified set")
    basis: FloatArray = np.asarray(null_space(identified.equality), dtype=np.float64)
    current: FloatArray = identified.vertices.mean(axis=0)
    box = np.concatenate([np.eye(VARIABLES), -np.eye(VARIABLES)])
    box_rhs = np.concatenate([np.ones(VARIABLES), np.zeros(VARIABLES)])
    a_ub = np.concatenate([identified.inequality, box])
    b_ub = np.concatenate([identified.inequality_rhs, box_rhs])
    rng = np.random.default_rng(seed)
    output = np.empty((draws, VARIABLES))
    steps = burn_in + draws * thin
    kept = 0
    for step in range(steps):
        direction: FloatArray = basis @ np.asarray(
            rng.standard_normal(basis.shape[1]), dtype=np.float64
        )
        norm = float(np.linalg.norm(direction))
        if norm == 0.0:
            continue
        direction /= norm
        rates: FloatArray = a_ub @ direction
        room: FloatArray = np.maximum(b_ub - a_ub @ current, 0.0)
        with np.errstate(divide="ignore", invalid="ignore"):
            ratios: FloatArray = room / rates
        upper = float(np.min(ratios[rates > 1e-12], initial=np.inf))
        lower = float(
            np.max(-room[rates < -1e-12] / -rates[rates < -1e-12], initial=-np.inf)
        )
        if not np.isfinite(upper) or not np.isfinite(lower) or upper < lower:
            raise RuntimeError("hit-and-run chord is unbounded or empty")
        current = current + rng.uniform(lower, upper) * direction
        if step >= burn_in and (step - burn_in) % thin == 0:
            output[kept] = current
            kept += 1
            if kept == draws:
                break
    if kept != draws:
        raise RuntimeError("hit-and-run produced too few draws")
    return output


@dataclass(frozen=True, slots=True)
class BoundsValidation:
    truth: FloatArray
    lower: FloatArray
    upper: FloatArray
    inside: npt.NDArray[np.bool_]
    max_violation: float


def validate_bounds(identified: IdentifiedSet, truth: FloatArray) -> BoundsValidation:
    lower, upper = identified.translation_bounds()
    tolerance = 1e-9
    inside = (truth >= lower - tolerance) & (truth <= upper + tolerance)
    violation = np.maximum(np.maximum(lower - truth, truth - upper), 0.0)
    return BoundsValidation(
        truth=truth,
        lower=lower,
        upper=upper,
        inside=inside,
        max_violation=float(violation.max()),
    )
