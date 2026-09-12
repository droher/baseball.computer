"""Posterior-uncertainty propagation for the estimated linear-weights sibling.

Pure dataframe helpers that propagate Model G's published run-expectancy
posterior draws through the deterministic linear-weights formula. The
deterministic ``main_models.linear_weights`` carries point estimates; this
sibling carries the per-(season, league, play) run value with posterior
uncertainty (mean, sd, 94% HDI) by replacing the point RE values in
``expected_runs_change`` with posterior draws and recentering per draw.
Transition-count finite-sample uncertainty rides alongside the RE posterior:
per (season, league) and per RE draw, the combo-frequency weights are drawn
from a Jeffreys Dirichlet over the cell's transition types rather than fixed at
the observed counts, so sparse cells widen while dense cells are unchanged to
first order.

Posterior cells are keyed on the transition row's own ``season`` and ``league``
columns plus the ``outs_base`` suffix of the run-expectancy key, never on the
key's ``season_group`` / ``league_group`` prefix. Rows whose start state has no
posterior cell are dropped, as the deterministic sibling drops events whose
start key misses the run-expectancy matrix; a missing non-terminal end state
maps to zero, matching its ``COALESCE(RE_end, 0)``. The deterministic
occurrence floor per (season, league, play) is ``DETERMINISTIC_PLAY_FLOOR``,
pinned to the ``QUALIFY COUNT(*) OVER result > N`` clause of
``linear_weights.sql`` by a unit test; cells at or below it publish the
corpus-pooled per-play value with ``is_imputed = True``, exactly as the
deterministic table does.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

import numpy as np
import polars as pl

_log = logging.getLogger(__name__)

HDI_PROB: float = 0.94

JEFFREYS_ALPHA: float = 0.5

DIRICHLET_BASE_SEED: int = 20260713

INNING_END_OUTS: int = 3

DETERMINISTIC_LINEAR_WEIGHTS_SQL: Path = (
    Path(__file__).resolve().parents[2]
    / "models"
    / "intermediate"
    / "expectancy"
    / "linear_weights.sql"
)

DETERMINISTIC_PLAY_FLOOR: int = 100

RUN_VALUE_SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int16(),
    "league": pl.Utf8(),
    "play": pl.Utf8(),
    "play_category": pl.Utf8(),
    "n_events": pl.Int64(),
    "run_value_mean": pl.Float64(),
    "run_value_sd": pl.Float64(),
    "run_value_hdi_lower": pl.Float64(),
    "run_value_hdi_upper": pl.Float64(),
    "is_imputed": pl.Boolean(),
}


def _distinct_keys_with_components(
    counts: pl.DataFrame, key_column: str
) -> pl.DataFrame:
    """Return distinct ``(season, league, key)`` triples with parsed components.

    ``re_state`` is the ``outs_base`` suffix of the key (matching the
    posterior's ``state`` column) and ``re_outs`` its outs component; the
    posterior join uses the row's own ``season`` / ``league`` columns.
    """
    parts = pl.col("re_key").str.split("_")
    return (
        counts.select(
            pl.col("season"),
            pl.col("league"),
            pl.col(key_column).alias("re_key"),
        )
        .unique()
        .with_columns(
            pl.concat_str(
                [parts.list.get(-2), parts.list.get(-1)], separator="_"
            ).alias("re_state"),
            parts.list.get(-2).cast(pl.Int64).alias("re_outs"),
        )
        .with_row_index("key_id")
    )


def _draw_value_matrix(
    keys: pl.DataFrame,
    re_draws: pl.DataFrame,
) -> tuple[np.ndarray, np.ndarray]:
    """Return ``(values, has_cell)`` for the distinct keys.

    ``values`` is an ``(n_keys, n_draws)`` matrix of RE values per key per
    draw and ``has_cell`` a boolean per key that is True when the key's
    ``(season, league, state)`` matched a posterior cell. Unmatched keys
    carry zeros; callers decide whether that is a drop (start keys) or a
    zero run expectancy (end keys past the inning).
    """
    draw_ids = re_draws.get_column("draw_id").unique().sort()
    n_draws = draw_ids.len()
    draw_index = {d: i for i, d in enumerate(draw_ids.to_list())}

    wide = (
        keys.join(
            re_draws,
            left_on=["season", "league", "re_state"],
            right_on=["season", "league", "state"],
            how="left",
        )
        .select(["key_id", "draw_id", "value"])
        .drop_nulls(subset=["draw_id"])
    )

    n_keys = keys.height
    out = np.zeros((n_keys, n_draws), dtype=np.float64)
    has_cell = np.zeros(n_keys, dtype=bool)
    if wide.height > 0:
        key_pos = wide.get_column("key_id").to_numpy()
        draw_pos = np.array(
            [draw_index[d] for d in wide.get_column("draw_id").to_list()],
            dtype=np.int64,
        )
        out[key_pos, draw_pos] = wide.get_column("value").to_numpy()
        has_cell[np.unique(key_pos)] = True
    return out, has_cell


def _cell_generator(season: int, league: str, base_seed: int) -> np.random.Generator:
    """Deterministic per-(season, league) numpy Generator.

    Cells draw independent transition-frequency vectors, so each seeds its own
    generator from a stable BLAKE2b digest of ``(base_seed, season, league)``.
    stdlib ``hash`` is salted per interpreter and cannot be used here.
    """
    key = f"{base_seed}|{season}|{league}".encode()
    seed = int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big")
    return np.random.default_rng(seed)


def _key_positions(
    keys: pl.DataFrame, counts: pl.DataFrame, key_column: str
) -> np.ndarray:
    position = {
        (int(s), str(lg), str(k)): int(i)
        for s, lg, k, i in keys.select(
            ["season", "league", "re_key", "key_id"]
        ).iter_rows()
    }
    return np.array(
        [
            position[(int(s), str(lg), str(k))]
            for s, lg, k in counts.select(["season", "league", key_column]).iter_rows()
        ],
        dtype=np.int64,
    )


def propagate_linear_weights_draws(
    transition_counts: pl.DataFrame,
    re_draws: pl.DataFrame,
    *,
    dirichlet_alpha: float | None = JEFFREYS_ALPHA,
    base_seed: int = DIRICHLET_BASE_SEED,
    min_events_per_play: int | None = None,
) -> pl.DataFrame:
    """Propagate RE posterior draws through the linear-weights formula.

    ``transition_counts`` has columns ``season, league, play, play_category,
    run_expectancy_start_key, run_expectancy_end_key, runs_on_play, n``.
    ``re_draws`` is the posterior long form with columns
    ``state, season, league, value, chain, draw``.

    For each draw ``d`` the per-event ``expected_runs_change`` is
    ``runs_on_play + RE_end[d] - RE_start[d]``; the per-(season, league, play)
    run value for draw ``d`` is the weighted mean of ``expected_runs_change``
    over that play's combos, then centered by subtracting the per-(season,
    league) all-play weighted mean for draw ``d``. Collapsing over draws yields
    ``run_value_{mean, sd, hdi_lower, hdi_upper}`` (94% HDI).

    Rows whose start state has no posterior cell for their ``(season,
    league)`` are dropped. An end state past the inning (``outs >= 3``) or
    with no posterior cell contributes ``RE_end = 0``.

    ``min_events_per_play`` is the occurrence floor per (season, league,
    play); ``None`` uses ``DETERMINISTIC_PLAY_FLOOR``, the deterministic
    sibling's floor.
    Cells whose occurrence count is at or below the floor publish the
    corpus-pooled per-play value (event-weighted over every season and
    league, centered against the pooled all-play mean) with
    ``is_imputed = True``; cells above it carry their own value with
    ``is_imputed = False``.

    When ``dirichlet_alpha`` is a float (default ``JEFFREYS_ALPHA``) the combo
    weights carry finite-sample transition-count uncertainty: per (season,
    league) and per RE-posterior draw, the combo-frequency vector is drawn from
    ``Dirichlet(n + dirichlet_alpha)`` rather than fixed at ``n``, so sparse
    cells widen and dense cells are unchanged to first order. The draws are
    deterministic in ``base_seed``. Passing ``dirichlet_alpha=None`` recovers
    the fixed-``n`` weights (the pre-finite-sample behavior).
    """
    if transition_counts.height == 0 or re_draws.height == 0:
        return pl.DataFrame(schema=RUN_VALUE_SUMMARY_SCHEMA)

    floor = (
        DETERMINISTIC_PLAY_FLOOR
        if min_events_per_play is None
        else int(min_events_per_play)
    )

    re_draws = re_draws.with_columns(
        (
            pl.col("chain").cast(pl.Int64) * 1_000_000 + pl.col("draw").cast(pl.Int64)
        ).alias("draw_id"),
        pl.col("state").cast(pl.Utf8),
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("value").cast(pl.Float64),
    ).select(["state", "season", "league", "value", "draw_id"])

    n_draws = re_draws.get_column("draw_id").n_unique()

    counts = transition_counts.with_columns(
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("play").cast(pl.Utf8),
        pl.col("play_category").cast(pl.Utf8),
        pl.col("runs_on_play").cast(pl.Float64),
        pl.col("n").cast(pl.Float64),
    ).sort(
        [
            "season",
            "league",
            "play",
            "run_expectancy_start_key",
            "run_expectancy_end_key",
            "runs_on_play",
        ]
    )

    start_keys = _distinct_keys_with_components(counts, "run_expectancy_start_key")
    end_keys = _distinct_keys_with_components(counts, "run_expectancy_end_key")

    start_matrix, start_has_cell = _draw_value_matrix(start_keys, re_draws)
    end_matrix, end_has_cell = _draw_value_matrix(end_keys, re_draws)
    end_outs = end_keys.get_column("re_outs").to_numpy()
    end_matrix[end_outs >= INNING_END_OUTS, :] = 0.0

    start_idx = _key_positions(start_keys, counts, "run_expectancy_start_key")
    end_idx = _key_positions(end_keys, counts, "run_expectancy_end_key")

    keep = start_has_cell[start_idx]
    dropped_events = float(counts.get_column("n").to_numpy()[~keep].sum())
    if dropped_events:
        _log.info(
            "linear_weights_estimated: dropped %d transition rows (%d events) "
            "whose start state has no posterior cell",
            int((~keep).sum()),
            int(dropped_events),
        )
    end_zeroed = end_idx[keep]
    missing_end = int(
        counts.get_column("n")
        .to_numpy()[keep][
            ~end_has_cell[end_zeroed] & (end_outs[end_zeroed] < INNING_END_OUTS)
        ]
        .sum()
    )
    if missing_end:
        _log.info(
            "linear_weights_estimated: %d events carry a non-terminal end state "
            "with no posterior cell; RE_end = 0 for them",
            missing_end,
        )
    counts = counts.filter(pl.Series(keep))
    start_idx = start_idx[keep]
    end_idx = end_idx[keep]
    if counts.height == 0:
        _log.info("linear_weights_estimated: no rows survive the start-cell join")
        return pl.DataFrame(schema=RUN_VALUE_SUMMARY_SCHEMA)

    raw_weights = counts.get_column("n").to_numpy().astype(np.float64)
    runs_on_play = counts.get_column("runs_on_play").to_numpy()

    season = counts.get_column("season").to_numpy()
    league = counts.get_column("league").to_list()
    play = counts.get_column("play").to_list()
    play_category = counts.get_column("play_category").to_list()

    sl_keys = list(zip(season.tolist(), league))
    slp_keys = list(zip(season.tolist(), league, play))

    sl_index: dict[tuple[int, str], int] = {}
    for k in sl_keys:
        if k not in sl_index:
            sl_index[k] = len(sl_index)
    slp_index: dict[tuple[int, str, str], int] = {}
    slp_order: list[tuple[int, str, str]] = []
    slp_category: list[str] = []
    for k, category in zip(slp_keys, play_category):
        if k not in slp_index:
            slp_index[k] = len(slp_index)
            slp_order.append(k)
            slp_category.append(category)
    play_index: dict[str, int] = {}
    for p in play:
        if p not in play_index:
            play_index[p] = len(play_index)

    sl_codes = np.array([sl_index[k] for k in sl_keys], dtype=np.int64)
    slp_codes = np.array([slp_index[k] for k in slp_keys], dtype=np.int64)
    play_codes = np.array([play_index[p] for p in play], dtype=np.int64)
    n_sl = len(sl_index)
    n_slp = len(slp_index)
    n_play = len(play_index)
    sl_cells: list[tuple[int, str]] = [(0, "")] * n_sl
    for cell, code in sl_index.items():
        sl_cells[code] = cell

    slp_weighted_sum = np.zeros((n_slp, n_draws), dtype=np.float64)
    slp_weight = np.zeros((n_slp, n_draws), dtype=np.float64)
    sl_weighted_sum = np.zeros((n_sl, n_draws), dtype=np.float64)
    sl_weight = np.zeros((n_sl, n_draws), dtype=np.float64)
    slp_count = np.zeros(n_slp, dtype=np.float64)
    np.add.at(slp_count, slp_codes, raw_weights)
    play_pooled_sum = np.zeros((n_play, n_draws), dtype=np.float64)
    play_pooled_weight = np.zeros(n_play, dtype=np.float64)
    np.add.at(play_pooled_weight, play_codes, raw_weights)
    all_pooled_sum = np.zeros(n_draws, dtype=np.float64)

    for code, (cell_season, cell_league) in enumerate(sl_cells):
        rows = np.flatnonzero(sl_codes == code)
        erc = (
            runs_on_play[rows, None]
            + end_matrix[end_idx[rows], :]
            - start_matrix[start_idx[rows], :]
        )
        if dirichlet_alpha is None:
            weights = np.broadcast_to(raw_weights[rows, None], erc.shape)
        else:
            rng = _cell_generator(cell_season, cell_league, base_seed)
            weights = rng.dirichlet(raw_weights[rows] + dirichlet_alpha, size=n_draws).T
        weighted = erc * weights
        pooled = erc * raw_weights[rows, None]
        group_starts = np.concatenate(
            ([0], np.flatnonzero(np.diff(slp_codes[rows]) != 0) + 1)
        )
        group_slp = slp_codes[rows][group_starts]
        slp_weighted_sum[group_slp, :] += np.add.reduceat(
            weighted, group_starts, axis=0
        )
        slp_weight[group_slp, :] += np.add.reduceat(weights, group_starts, axis=0)
        sl_weighted_sum[code, :] = weighted.sum(axis=0)
        sl_weight[code, :] = weights.sum(axis=0)
        np.add.at(
            play_pooled_sum,
            play_codes[rows][group_starts],
            np.add.reduceat(pooled, group_starts, axis=0),
        )
        all_pooled_sum += pooled.sum(axis=0)

    all_pooled_mean = all_pooled_sum / raw_weights.sum()
    play_pooled_centered = (
        play_pooled_sum / play_pooled_weight[:, None] - all_pooled_mean[None, :]
    )

    slp_mean = slp_weighted_sum / slp_weight
    sl_mean = sl_weighted_sum / sl_weight

    slp_to_sl = np.array(
        [sl_index[(s, lg)] for (s, lg, _p) in slp_order], dtype=np.int64
    )
    centered = slp_mean - sl_mean[slp_to_sl, :]

    is_imputed = slp_count <= floor
    if is_imputed.any():
        slp_to_play = np.array(
            [play_index[p] for (_s, _lg, p) in slp_order], dtype=np.int64
        )
        centered[is_imputed, :] = play_pooled_centered[slp_to_play[is_imputed], :]
        _log.info(
            "linear_weights_estimated: %d of %d (season, league, play) cells at or "
            "below the %d-occurrence floor take the pooled per-play value",
            int(is_imputed.sum()),
            n_slp,
            floor,
        )

    mean = centered.mean(axis=1)
    sd = centered.std(axis=1, ddof=1) if n_draws > 1 else np.zeros(n_slp)
    hdi_lower, hdi_upper = _hdi(centered, HDI_PROB)

    out = pl.DataFrame(
        {
            "season": [s for (s, _lg, _p) in slp_order],
            "league": [lg for (_s, lg, _p) in slp_order],
            "play": [p for (_s, _lg, p) in slp_order],
            "play_category": slp_category,
            "n_events": np.rint(slp_count).astype(np.int64).tolist(),
            "run_value_mean": mean.tolist(),
            "run_value_sd": sd.tolist(),
            "run_value_hdi_lower": hdi_lower.tolist(),
            "run_value_hdi_upper": hdi_upper.tolist(),
            "is_imputed": is_imputed.tolist(),
        }
    ).select(
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("play").cast(pl.Utf8),
        pl.col("play_category").cast(pl.Utf8),
        pl.col("n_events").cast(pl.Int64),
        pl.col("run_value_mean").cast(pl.Float64),
        pl.col("run_value_sd").cast(pl.Float64),
        pl.col("run_value_hdi_lower").cast(pl.Float64),
        pl.col("run_value_hdi_upper").cast(pl.Float64),
        pl.col("is_imputed").cast(pl.Boolean),
    )
    _log.info(
        "linear_weights_estimated: %d (season, league, play) rows over %d draws",
        out.height,
        n_draws,
    )
    return out


def _hdi(samples: np.ndarray, prob: float) -> tuple[np.ndarray, np.ndarray]:
    """Per-row highest-density interval over the draw axis (axis=1)."""
    try:
        import arviz as az

        n_rows = samples.shape[0]
        lower = np.empty(n_rows, dtype=np.float64)
        upper = np.empty(n_rows, dtype=np.float64)
        for i in range(n_rows):
            bounds = az.hdi(samples[i, :], hdi_prob=prob)
            lower[i] = bounds[0]
            upper[i] = bounds[1]
        return lower, upper
    except Exception:
        _log.warning("arviz hdi unavailable; falling back to percentile interval")
        tail = (1.0 - prob) / 2.0
        lower = np.percentile(samples, 100.0 * tail, axis=1)
        upper = np.percentile(samples, 100.0 * (1.0 - tail), axis=1)
        return lower, upper
