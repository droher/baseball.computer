"""Team-game-grain prep for the park-factor count model.

Reads the per-event ``model_input_park_factors`` Parquet, aggregates to
team-game grain (one row per ``(game_id, batting_team_id)`` with summed
runs and plate-appearance exposure), and emits frozen Pydantic inputs for
a NegativeBinomial park-factor fit. Each ``(park_id, season, league)``
cell is a published park-effect coord; cells below ``MIN_GAMES_PER_PARK_CELL``
training team-games are dropped as unidentified. A deterministic 10% game
holdout (``game_hash_fold``, fold 0) ships alongside training for OOS
scoring, with levels unseen in training encoded to ``-1``.

``cell_season_league_idx`` maps each park cell to its ``(season, league)``
group so the builder can center the park effect within season-league.
``ar_chain_idx`` / ``ar_step_idx`` map each park cell onto a padded
``(n_ar_chains, n_ar_steps)`` grid keyed by (park, league) chain and the
cell's season rank within that chain, for an AR(1) persistence prior across
seasons; ``home_idx`` flags whether the batting team is the home team for a
home-field-advantage term.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import ClassVar

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict

from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]
FloatArray = npt.NDArray[np.float64]

DEFAULT_SEED: int = 20260513
OUTCOME: str = "team_runs"

HOLDOUT_FOLD_COUNT: int = 10
HOLDOUT_FOLD_ID: int = 0

MIN_GAMES_PER_PARK_CELL: int = 25

SINGLE_SOURCE_LABEL: str = "__single__"


class ParkFactorHeldOutSet(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    team_runs: IntArray
    exposure_pa: FloatArray
    season_league_idx: IntArray
    offense_idx: IntArray
    pitching_idx: IntArray
    park_season_league_idx: IntArray

    @property
    def n_games(self) -> int:
        return int(self.team_runs.shape[0])


class ParkFactorInputs(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    team_runs: IntArray
    exposure_pa: FloatArray
    season_league_idx: IntArray
    offense_idx: IntArray
    pitching_idx: IntArray
    park_season_league_idx: IntArray
    home_idx: IntArray

    park_season_league_labels: list[str]
    park_id_by_cell: list[str]
    season_by_cell: list[int]
    league_by_cell: list[str]

    cell_season_league_idx: IntArray
    ar_chain_idx: IntArray
    ar_step_idx: IntArray
    n_ar_chains: int
    n_ar_steps: int

    outcome: str
    coords: dict[str, list[str]]
    held_out: ParkFactorHeldOutSet

    @property
    def n_events(self) -> int:
        return int(self.team_runs.shape[0])

    @property
    def n_cells(self) -> int:
        return len(self.park_season_league_labels)


def _assert_contiguous(idx: IntArray, labels: list[str], column: str) -> None:
    expected = set(range(len(labels)))
    actual: set[int] = {int(v) for v in np.unique(idx).tolist()}
    if actual != expected:
        raise AssertionError(
            f"{column} indexer contiguity check failed: expected {expected}, got {actual}"
        )


def _category_index(df: pl.DataFrame, column: str) -> tuple[IntArray, list[str]]:
    series = df.get_column(column).cast(pl.Utf8)
    labels = sorted(str(v) for v in series.unique().to_list())
    mapping: dict[str, int] = {c: i for i, c in enumerate(labels)}
    codes = (
        series.replace_strict(mapping, return_dtype=pl.Int64)
        .to_numpy()
        .astype(np.int64)
    )
    _assert_contiguous(codes, labels, column)
    return codes, labels


def _encode_codes_with_vocab(
    df: pl.DataFrame, column: str, labels: Sequence[str]
) -> IntArray:
    series = df.get_column(column).cast(pl.Utf8)
    mapping = {c: i for i, c in enumerate(labels)}
    return np.fromiter(
        (mapping.get(str(v), -1) for v in series.to_list()),
        dtype=np.int64,
        count=series.len(),
    )


def _build_ar_chain_grid(
    park_id_by_cell: list[str],
    season_by_cell: list[int],
    league_by_cell: list[str],
) -> tuple[IntArray, IntArray, int, int]:
    chain_keys = [
        f"{park}|{league}"
        for park, league in zip(park_id_by_cell, league_by_cell, strict=True)
    ]
    chain_labels = sorted(set(chain_keys))
    chain_label_to_id = {label: i for i, label in enumerate(chain_labels)}
    chain_idx = np.fromiter(
        (chain_label_to_id[k] for k in chain_keys),
        dtype=np.int64,
        count=len(chain_keys),
    )
    season = np.asarray(season_by_cell, dtype=np.int64)
    step_idx = np.zeros(len(chain_keys), dtype=np.int64)
    for cid in range(len(chain_labels)):
        members = np.flatnonzero(chain_idx == cid)
        order = members[np.argsort(season[members], kind="stable")]
        step_idx[order] = np.arange(order.shape[0], dtype=np.int64)
    n_chains = len(chain_labels)
    n_steps = int(step_idx.max()) + 1 if step_idx.shape[0] else 1
    return chain_idx, step_idx, n_chains, n_steps


def _aggregate_team_games(parquet_path: Path) -> pl.DataFrame:
    return (
        pl.scan_parquet(parquet_path)
        .filter(
            pl.col("game_id").is_not_null()
            & pl.col("batting_team_id").is_not_null()
            & pl.col("runs_on_play").is_not_null()
            & pl.col("plate_appearances").is_not_null()
        )
        .group_by(["game_id", "batting_team_id"])
        .agg(
            pl.col("runs_on_play").sum().alias("team_runs"),
            pl.col("plate_appearances").sum().alias("exposure_pa"),
            pl.col("park_id").first().alias("park_id"),
            pl.col("season").first().alias("season"),
            pl.col("league").first().alias("league"),
            pl.col("fielding_team_id").first().alias("fielding_team_id"),
            pl.col("home_away").first().alias("home_away"),
        )
        .filter(pl.col("exposure_pa") >= 1)
        .with_columns(
            (
                pl.col("season").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("league").cast(pl.Utf8)
            ).alias("season_league"),
            (
                pl.col("park_id").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("season").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("league").cast(pl.Utf8)
            ).alias("park_season_league"),
            (
                pl.col("batting_team_id").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("season").cast(pl.Utf8)
            ).alias("offense_team_season"),
            (
                pl.col("fielding_team_id").cast(pl.Utf8)
                + pl.lit("|")
                + pl.col("season").cast(pl.Utf8)
            ).alias("pitching_team_season"),
        )
        .collect()
    )


def _smoke_subsample(
    df_train: pl.DataFrame, smoke_limit: int, seed: int
) -> pl.DataFrame:
    games_per_game = (
        df_train.select(["game_id"])
        .group_by("game_id")
        .agg(pl.len().alias("n_team_games"))
        .sort("game_id")
    )
    total = int(games_per_game.get_column("n_team_games").sum())
    if total <= smoke_limit:
        return df_train
    shuffled = games_per_game.sample(
        fraction=1.0, seed=seed, shuffle=True
    ).with_columns(pl.col("n_team_games").cum_sum().alias("cum_team_games"))
    kept_games = shuffled.filter(pl.col("cum_team_games") <= smoke_limit).get_column(
        "game_id"
    )
    if kept_games.len() == 0:
        kept_games = shuffled.head(1).get_column("game_id")
    out = df_train.filter(pl.col("game_id").is_in(kept_games.implode()))
    _log.info(
        "prepare_park_factor_inputs smoke-subsampled to %d games (%d team-games; "
        "budget=%d, seed=%d)",
        kept_games.len(),
        out.height,
        smoke_limit,
        seed,
    )
    return out


def prepare_park_factor_inputs(
    parquet_path: Path,
    *,
    dimension: str | None = None,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
) -> ParkFactorInputs:
    """Aggregate per-event rows to team-game grain and shape count inputs.

    The ``dimension`` kwarg is accepted for interface parity with other
    prep entrypoints and ignored (this dataset has no dimension column).
    """
    _ = dimension

    df = _aggregate_team_games(parquet_path)
    if df.height == 0:
        raise ValueError(f"no team-game rows with exposure_pa >= 1 in {parquet_path}")

    distinct_game_ids = df.get_column("game_id").unique().to_list()
    holdout_game_ids = [
        g
        for g in distinct_game_ids
        if game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
    ]
    held_out_df = df.filter(pl.col("game_id").is_in(holdout_game_ids))
    df_train = df.filter(~pl.col("game_id").is_in(holdout_game_ids))
    _log.info(
        "prepare_park_factor_inputs held-out %d/%d games via fold %d/%d",
        len(holdout_game_ids),
        len(distinct_game_ids),
        HOLDOUT_FOLD_ID,
        HOLDOUT_FOLD_COUNT,
    )
    if df_train.height == 0:
        raise ValueError("every game landed in the held-out fold; nothing to fit")

    cell_counts = df_train.group_by("park_season_league").agg(pl.len().alias("_n"))
    kept_cells = cell_counts.filter(pl.col("_n") >= MIN_GAMES_PER_PARK_CELL).get_column(
        "park_season_league"
    )
    dropped = cell_counts.height - kept_cells.len()
    if dropped:
        _log.info(
            "prepare_park_factor_inputs dropped %d park cells with <%d training team-games",
            dropped,
            MIN_GAMES_PER_PARK_CELL,
        )
    df_train = df_train.filter(pl.col("park_season_league").is_in(kept_cells.implode()))
    if df_train.height == 0:
        raise ValueError(
            f"every park cell fell below MIN_GAMES_PER_PARK_CELL={MIN_GAMES_PER_PARK_CELL}; "
            "nothing to fit"
        )

    if smoke_limit is not None:
        df_train = _smoke_subsample(df_train, smoke_limit, seed)

    train = df_train.sort(["game_id", "batting_team_id"])

    team_runs = train.get_column("team_runs").to_numpy().astype(np.int64)
    exposure_pa = train.get_column("exposure_pa").to_numpy().astype(np.float64)
    home_idx = (
        (train.get_column("home_away").cast(pl.Utf8) == "home")
        .cast(pl.Int64)
        .to_numpy()
        .astype(np.int64)
    )

    season_league_idx, season_league_labels = _category_index(train, "season_league")
    offense_idx, offense_labels = _category_index(train, "offense_team_season")
    pitching_idx, pitching_labels = _category_index(train, "pitching_team_season")
    park_season_league_idx, park_season_league_labels = _category_index(
        train, "park_season_league"
    )

    cell_decomp = (
        train.select(["park_season_league", "park_id", "season", "league"])
        .unique(subset=["park_season_league"])
        .sort("park_season_league")
    )
    decomp_map = {
        str(row["park_season_league"]): (
            str(row["park_id"]),
            int(row["season"]),
            str(row["league"]),
        )
        for row in cell_decomp.iter_rows(named=True)
    }
    park_id_by_cell = [decomp_map[label][0] for label in park_season_league_labels]
    season_by_cell = [decomp_map[label][1] for label in park_season_league_labels]
    league_by_cell = [decomp_map[label][2] for label in park_season_league_labels]

    season_league_pos = {label: i for i, label in enumerate(season_league_labels)}
    cell_season_league_idx = np.fromiter(
        (
            season_league_pos[f"{season}|{league}"]
            for season, league in zip(season_by_cell, league_by_cell, strict=True)
        ),
        dtype=np.int64,
        count=len(park_season_league_labels),
    )

    ar_chain_idx, ar_step_idx, n_ar_chains, n_ar_steps = _build_ar_chain_grid(
        park_id_by_cell, season_by_cell, league_by_cell
    )

    held_out = _build_held_out_set(
        held_out_df,
        season_league_labels=season_league_labels,
        offense_labels=offense_labels,
        pitching_labels=pitching_labels,
        park_season_league_labels=park_season_league_labels,
    )

    coords: dict[str, list[str]] = {
        "source": [SINGLE_SOURCE_LABEL],
        "season_league": list(season_league_labels),
        "offense_team": list(offense_labels),
        "pitching_team": list(pitching_labels),
        "park_season_league": list(park_season_league_labels),
    }

    _log.info(
        "prepare_park_factor_inputs train_team_games=%d held_out_team_games=%d "
        "season_leagues=%d offense=%d pitching=%d park_cells=%d",
        train.height,
        held_out.n_games,
        len(season_league_labels),
        len(offense_labels),
        len(pitching_labels),
        len(park_season_league_labels),
    )

    return ParkFactorInputs(
        team_runs=team_runs,
        exposure_pa=exposure_pa,
        season_league_idx=season_league_idx,
        offense_idx=offense_idx,
        pitching_idx=pitching_idx,
        park_season_league_idx=park_season_league_idx,
        home_idx=home_idx,
        park_season_league_labels=list(park_season_league_labels),
        park_id_by_cell=park_id_by_cell,
        season_by_cell=season_by_cell,
        league_by_cell=league_by_cell,
        cell_season_league_idx=cell_season_league_idx,
        ar_chain_idx=ar_chain_idx,
        ar_step_idx=ar_step_idx,
        n_ar_chains=n_ar_chains,
        n_ar_steps=n_ar_steps,
        outcome=OUTCOME,
        coords=coords,
        held_out=held_out,
    )


def _build_held_out_set(
    df: pl.DataFrame,
    *,
    season_league_labels: list[str],
    offense_labels: list[str],
    pitching_labels: list[str],
    park_season_league_labels: list[str],
) -> ParkFactorHeldOutSet:
    if df.height == 0:
        empty_i = np.zeros(0, dtype=np.int64)
        empty_f = np.zeros(0, dtype=np.float64)
        return ParkFactorHeldOutSet(
            team_runs=empty_i,
            exposure_pa=empty_f,
            season_league_idx=empty_i,
            offense_idx=empty_i,
            pitching_idx=empty_i,
            park_season_league_idx=empty_i,
        )

    per_game = df.sort(["game_id", "batting_team_id"])
    return ParkFactorHeldOutSet(
        team_runs=per_game.get_column("team_runs").to_numpy().astype(np.int64),
        exposure_pa=per_game.get_column("exposure_pa").to_numpy().astype(np.float64),
        season_league_idx=_encode_codes_with_vocab(
            per_game, "season_league", season_league_labels
        ),
        offense_idx=_encode_codes_with_vocab(
            per_game, "offense_team_season", offense_labels
        ),
        pitching_idx=_encode_codes_with_vocab(
            per_game, "pitching_team_season", pitching_labels
        ),
        park_season_league_idx=_encode_codes_with_vocab(
            per_game, "park_season_league", park_season_league_labels
        ),
    )
