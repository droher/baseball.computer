"""Marginal linear weights from the run-expectancy posterior.

Given the per-``(season, league, state)`` run-expectancy value ``V`` (Model G's
``run_expectancy_summary`` export, column ``re_value_mean``) and the realized
events (start cell, end cell, ``runs_on_play``, play type), the marginal linear
weight of a play type is the mean over events of that type of

    runs_on_play + V_end - V_start

where ``V`` is the run-expectancy value of the cell and the inning-end end state
contributes ``V_end = 0``. This is the STANDARD marginal linear weight: it pairs
each play with the run-expectancy change it produced and averages within
``(result_family, season, league)``. Park-neutrality is trivial in Model G — the
run-expectancy hierarchy carries no park term, so ``V`` is already park-neutral.

DEFERRED: the spec's context-neutral linear weight (03-hierarchical-models.md
~720) integrates ``V_end`` against the modeled marginal transition distribution
``P_LW(end | start)`` rather than the realized end state. That estimand is
ambiguous at the per-play-type grain — ``P_LW`` is keyed on the start state
alone, so it does not distinguish play types sharing a start state, and the
mapping from play type to a transition law needs a modeling decision before it
can be a published number. Only the marginal weight is computed here.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import polars as pl

_log = logging.getLogger(__name__)

START_KEY_COLUMN: str = "run_expectancy_start_key"
END_KEY_COLUMN: str = "run_expectancy_end_key"
RUNS_COLUMN: str = "runs_on_play"
PLAY_TYPE_COLUMN: str = "result_family"
RE_VALUE_COLUMN: str = "re_value_mean"

INNING_END_OUTS: int = 3
BASE_STATES: int = 8

UNKNOWN_PLAY_TYPE: str = "__unknown__"


def _state_suffix(key: str) -> str:
    outs_str, base_str = key.rsplit("_", 2)[-2:]
    outs = int(outs_str)
    base = int(base_str)
    if outs >= INNING_END_OUTS:
        return "__inning_end__"
    return f"{outs}_{base}"


def _key_prefix(key: str) -> tuple[int, str]:
    parts = key.split("_")
    return int(parts[0]), parts[1]


def _re_lookup(re_value_by_cell: pl.DataFrame) -> dict[tuple[int, str, str], float]:
    required = {"season", "league", "state", RE_VALUE_COLUMN}
    missing = required - set(re_value_by_cell.columns)
    if missing:
        raise ValueError(f"run-expectancy summary missing columns: {sorted(missing)}")
    lookup: dict[tuple[int, str, str], float] = {}
    for season, league, state, value in re_value_by_cell.select(
        ["season", "league", "state", RE_VALUE_COLUMN]
    ).iter_rows():
        lookup[(int(season), str(league), str(state))] = float(value)
    return lookup


def compute_marginal_linear_weights(
    re_value_by_cell: pl.DataFrame,
    events_df: pl.DataFrame,
) -> pl.DataFrame:
    """Compute marginal linear weights per ``(result_family, season, league)``.

    ``re_value_by_cell`` is the run-expectancy summary at ``(state, season,
    league)`` grain with a ``re_value_mean`` column (``state`` is the ``outs_base``
    base-out label). Its ``season`` / ``league`` are the grouped values Model G
    keys cells on (``season_group = GREATEST(season, 1914)``, leagues outside
    AL/NL/FL collapsed to ``Other``), so the season / league are read from the
    ``run_expectancy_*_key`` prefix — which carries the same grouped values — not
    from any raw event columns. ``events_df`` carries one row per event with the
    start key, end key, ``runs_on_play``, and ``result_family``. The inning-end end
    state maps to ``V_end = 0``. Events whose start state has no run-expectancy
    value are dropped (logged); the play value is undefined there. Returns a tidy
    frame ``(result_family, season, league, linear_weight, n_events)`` at the
    grouped season / league grain.
    """
    lookup = _re_lookup(re_value_by_cell)

    required = {
        START_KEY_COLUMN,
        END_KEY_COLUMN,
        RUNS_COLUMN,
        PLAY_TYPE_COLUMN,
    }
    missing = required - set(events_df.columns)
    if missing:
        raise ValueError(f"events frame missing columns: {sorted(missing)}")

    deltas: list[float] = []
    families: list[str] = []
    seasons: list[int] = []
    leagues: list[str] = []
    dropped = 0

    for start_key, end_key, runs, family in events_df.select(
        [
            START_KEY_COLUMN,
            END_KEY_COLUMN,
            RUNS_COLUMN,
            PLAY_TYPE_COLUMN,
        ]
    ).iter_rows():
        if start_key is None or end_key is None or runs is None:
            dropped += 1
            continue
        s, lg = _key_prefix(str(start_key))
        v_start = lookup.get((s, lg, _state_suffix(str(start_key))))
        if v_start is None:
            dropped += 1
            continue
        end_suffix = _state_suffix(str(end_key))
        v_end = 0.0 if end_suffix == "__inning_end__" else lookup.get((s, lg, end_suffix))
        if v_end is None:
            dropped += 1
            continue
        deltas.append(float(runs) + v_end - v_start)
        families.append(str(family) if family is not None else UNKNOWN_PLAY_TYPE)
        seasons.append(s)
        leagues.append(lg)

    if dropped:
        _log.info(
            "compute_marginal_linear_weights dropped %d events with no run-expectancy value",
            dropped,
        )

    long = pl.DataFrame(
        {
            PLAY_TYPE_COLUMN: families,
            "season": seasons,
            "league": leagues,
            "delta": deltas,
        }
    )
    out = (
        long.group_by([PLAY_TYPE_COLUMN, "season", "league"])
        .agg(
            pl.col("delta").mean().alias("linear_weight"),
            pl.len().alias("n_events"),
        )
        .sort([PLAY_TYPE_COLUMN, "season", "league"])
    )
    _log.info(
        "compute_marginal_linear_weights produced %d (result_family, season, league) weights over %d events",
        out.height,
        len(deltas),
    )
    return out
