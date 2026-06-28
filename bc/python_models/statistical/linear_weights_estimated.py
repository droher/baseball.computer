"""Posterior-uncertainty propagation for the estimated linear-weights sibling.

Pure dataframe helpers that propagate Model G's published run-expectancy
posterior draws through the deterministic linear-weights formula. The
deterministic ``main_models.linear_weights`` carries point estimates; this
sibling carries the per-(season, league, play) run value with posterior
uncertainty (mean, sd, 94% HDI) by replacing the point RE values in
``expected_runs_change`` with posterior draws and recentering per draw.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import logging

import numpy as np
import polars as pl

_log = logging.getLogger(__name__)

HDI_PROB: float = 0.94

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
}


def _distinct_keys_with_components(counts: pl.DataFrame, key_column: str) -> pl.DataFrame:
    """Return distinct RE keys with parsed posterior-join components.

    The key is ``CONCAT_WS('_', season_group, league_group, outs, base_state)``;
    ``re_season`` / ``re_league`` are the first two parts, ``re_state`` is the
    ``outs_base`` suffix (matching the posterior's ``state`` column), and
    ``re_outs`` is the outs component as an integer (for the inning-end zero).
    """
    parts = pl.col("re_key").str.split("_")
    return (
        counts.select(pl.col(key_column).alias("re_key"))
        .unique()
        .with_columns(
            parts.list.get(0).cast(pl.Int16).alias("re_season"),
            parts.list.get(1).cast(pl.Utf8).alias("re_league"),
            pl.concat_str(
                [parts.list.get(2), parts.list.get(3)], separator="_"
            ).alias("re_state"),
            parts.list.get(2).cast(pl.Int64).alias("re_outs"),
        )
        .with_row_index("key_id")
    )


def _draw_value_matrix(
    keys: pl.DataFrame,
    re_draws: pl.DataFrame,
    *,
    zero_when_inning_end: bool,
) -> np.ndarray:
    """Return an ``(n_keys, n_draws)`` matrix of RE values per key per draw.

    ``keys`` carries one row per distinct RE key with parsed
    ``re_season``/``re_league``/``re_state`` columns and (when
    ``zero_when_inning_end``) ``re_outs``. ``re_draws`` is the posterior in
    long form ``(state, season, league, draw_id, value)``. Keys whose parsed
    state matches no posterior cell, or (for the end key) whose ``outs >= 3``,
    map to RE = 0 (mirroring ``COALESCE(RE_end, 0)`` and inning-end
    transitions).
    """
    draw_ids = re_draws.get_column("draw_id").unique().sort()
    n_draws = draw_ids.len()
    draw_index = {d: i for i, d in enumerate(draw_ids.to_list())}

    wide = (
        keys.join(
            re_draws,
            left_on=["re_season", "re_league", "re_state"],
            right_on=["season", "league", "state"],
            how="left",
        )
        .select(["key_id", "draw_id", "value"])
        .drop_nulls(subset=["draw_id"])
    )

    n_keys = keys.height
    out = np.zeros((n_keys, n_draws), dtype=np.float64)
    if wide.height > 0:
        key_pos = wide.get_column("key_id").to_numpy()
        draw_pos = np.array(
            [draw_index[d] for d in wide.get_column("draw_id").to_list()],
            dtype=np.int64,
        )
        out[key_pos, draw_pos] = wide.get_column("value").to_numpy()

    if zero_when_inning_end:
        outs = keys.get_column("re_outs").to_numpy()
        out[outs >= 3, :] = 0.0
    return out


def propagate_linear_weights_draws(
    transition_counts: pl.DataFrame,
    re_draws: pl.DataFrame,
) -> pl.DataFrame:
    """Propagate RE posterior draws through the linear-weights formula.

    ``transition_counts`` has columns ``season, league, play, play_category,
    run_expectancy_start_key, run_expectancy_end_key, runs_on_play, n``.
    ``re_draws`` is the posterior long form with columns
    ``state, season, league, value, chain, draw``.

    For each draw ``d`` the per-event ``expected_runs_change`` is
    ``runs_on_play + RE_end[d] - RE_start[d]``; the per-(season, league, play)
    run value for draw ``d`` is the ``n``-weighted mean of
    ``expected_runs_change`` over that play's combos, then centered by
    subtracting the per-(season, league) all-play ``n``-weighted mean for
    draw ``d``. Collapsing over draws yields ``run_value_{mean, sd,
    hdi_lower, hdi_upper}`` (94% HDI).
    """
    if transition_counts.height == 0 or re_draws.height == 0:
        return pl.DataFrame(schema=RUN_VALUE_SUMMARY_SCHEMA)

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
    )

    start_keys = _distinct_keys_with_components(counts, "run_expectancy_start_key")
    end_keys = _distinct_keys_with_components(counts, "run_expectancy_end_key")

    start_matrix = _draw_value_matrix(
        start_keys, re_draws, zero_when_inning_end=False
    )
    end_matrix = _draw_value_matrix(end_keys, re_draws, zero_when_inning_end=True)

    start_pos = dict(
        zip(
            start_keys.get_column("re_key").to_list(),
            start_keys.get_column("key_id").to_list(),
        )
    )
    end_pos = dict(
        zip(
            end_keys.get_column("re_key").to_list(),
            end_keys.get_column("key_id").to_list(),
        )
    )

    start_idx = np.array(
        [start_pos[k] for k in counts.get_column("run_expectancy_start_key").to_list()],
        dtype=np.int64,
    )
    end_idx = np.array(
        [end_pos[k] for k in counts.get_column("run_expectancy_end_key").to_list()],
        dtype=np.int64,
    )

    weights = counts.get_column("n").to_numpy()
    runs_on_play = counts.get_column("runs_on_play").to_numpy()

    erc = (
        runs_on_play[:, None]
        + end_matrix[end_idx, :]
        - start_matrix[start_idx, :]
    )

    weighted = erc * weights[:, None]

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

    sl_codes = np.array([sl_index[k] for k in sl_keys], dtype=np.int64)
    slp_codes = np.array([slp_index[k] for k in slp_keys], dtype=np.int64)
    n_sl = len(sl_index)
    n_slp = len(slp_index)

    slp_weighted_sum = np.zeros((n_slp, n_draws), dtype=np.float64)
    slp_weight = np.zeros(n_slp, dtype=np.float64)
    np.add.at(slp_weighted_sum, slp_codes, weighted)
    np.add.at(slp_weight, slp_codes, weights)

    sl_weighted_sum = np.zeros((n_sl, n_draws), dtype=np.float64)
    sl_weight = np.zeros(n_sl, dtype=np.float64)
    np.add.at(sl_weighted_sum, sl_codes, weighted)
    np.add.at(sl_weight, sl_codes, weights)

    del erc, weighted

    slp_mean = slp_weighted_sum / slp_weight[:, None]
    sl_mean = sl_weighted_sum / sl_weight[:, None]

    slp_to_sl = np.array(
        [sl_index[(s, lg)] for (s, lg, _p) in slp_order], dtype=np.int64
    )
    centered = slp_mean - sl_mean[slp_to_sl, :]

    mean = centered.mean(axis=1)
    sd = centered.std(axis=1, ddof=1) if n_draws > 1 else np.zeros(n_slp)
    hdi_lower, hdi_upper = _hdi(centered, HDI_PROB)

    out = pl.DataFrame(
        {
            "season": [s for (s, _lg, _p) in slp_order],
            "league": [lg for (_s, lg, _p) in slp_order],
            "play": [p for (_s, _lg, p) in slp_order],
            "play_category": slp_category,
            "n_events": np.rint(slp_weight).astype(np.int64).tolist(),
            "run_value_mean": mean.tolist(),
            "run_value_sd": sd.tolist(),
            "run_value_hdi_lower": hdi_lower.tolist(),
            "run_value_hdi_upper": hdi_upper.tolist(),
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
