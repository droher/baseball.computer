"""Invariant tests for the cell-grain assist-count submodel."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from python_models.statistical.models._assist_count_data import (
    ASSIST_COUNT_CLASSES,
    ASSIST_COUNT_MAX,
    _bucket_assist_count,
    prepare_assist_count_inputs,
)
from python_models.statistical.models.assist_count import build_assist_count_model


def _synthetic_assist_count_dataset(
    tmp_path: Path,
    *,
    n_games: int = 40,
    events_per_game: int = 20,
    season: int = 1925,
    seed: int = 20260601,
) -> Path:
    """Seed assist credit rows with a varied per-event assist count M.

    Each event lands a count drawn from a skewed distribution over
    {0, 1, 2, 3, 4}; the count is spread across distinct fielder
    positions so the per-position assist grid sums to M.
    """
    rng = np.random.default_rng(seed)
    count_probs = np.asarray([0.55, 0.30, 0.10, 0.04, 0.01])
    rows: list[dict[str, object]] = []
    for g in range(n_games):
        for e in range(events_per_game):
            eid = 400_000 + g * events_per_game + e
            m = int(rng.choice(np.arange(5), p=count_probs))
            assist_positions = (
                set(rng.choice(np.arange(9), size=m, replace=False).tolist())
                if m > 0
                else set()
            )
            base_state = int(rng.integers(0, 4))
            outs = int(rng.integers(0, 3))
            for k_pos in range(1, 10):
                for ct in ("putout", "assist", "error"):
                    if ct == "assist" and (k_pos - 1) in assist_positions:
                        known_credit = 1.0
                    elif ct == "putout" and k_pos == 1:
                        known_credit = 1.0
                    else:
                        known_credit = 0.0
                    rows.append(
                        {
                            "event_key": eid,
                            "player_id": f"P{g:02d}_{k_pos:02d}",
                            "fielding_position": k_pos,
                            "credit_type": ct,
                            "known_credit": known_credit,
                            "unknown_credit_need": 0.0,
                            "personnel_hard_mask_available": True,
                            "eligible_for_allocation": False,
                            "game_id": f"G{g:03d}",
                            "season": season,
                            "source_family": "play_by_play",
                            "park_id": "ARL01",
                            "scorer": "scorerA",
                            "fielding_team_id": "TEX",
                            "result_family": "out_in_play",
                            "base_state_start": base_state,
                            "outs_start": outs,
                        }
                    )
    dataset_path = tmp_path / "assist_count_dataset.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)
    return dataset_path


def test_bucket_assist_count_caps_and_drops_zero() -> None:
    assert _bucket_assist_count(0) == -1
    assert _bucket_assist_count(1) == 0
    assert _bucket_assist_count(4) == ASSIST_COUNT_MAX - 1
    assert _bucket_assist_count(7) == ASSIST_COUNT_MAX - 1


def test_inputs_shape_and_class_count(tmp_path: Path) -> None:
    dataset_path = _synthetic_assist_count_dataset(tmp_path)
    inputs = prepare_assist_count_inputs(
        dataset_path,
        min_events_per_season=1,
        min_events_per_cell=1,
        held_out_fold_count=999,
    )
    assert inputs.n_classes == ASSIST_COUNT_MAX == len(ASSIST_COUNT_CLASSES)
    assert inputs.counts.shape == (inputs.n_cells, ASSIST_COUNT_MAX)
    assert inputs.cell_event_class_idx.shape[0] == inputs.n_cells
    assert inputs.coords["assist_count_class"] == list(ASSIST_COUNT_CLASSES)
    assert inputs.coords["assist_count_nonref"] == list(ASSIST_COUNT_CLASSES[1:])


def test_counts_only_cover_M_at_least_one(tmp_path: Path) -> None:
    dataset_path = _synthetic_assist_count_dataset(tmp_path)
    inputs = prepare_assist_count_inputs(
        dataset_path,
        min_events_per_season=1,
        min_events_per_cell=1,
        held_out_fold_count=999,
    )
    n_assist_events_with_m_ge_1 = (
        pl.scan_parquet(dataset_path)
        .filter(
            (pl.col("credit_type") == "assist")
            & (pl.col("personnel_hard_mask_available") == True)  # noqa: E712
        )
        .group_by("event_key")
        .agg(pl.col("known_credit").sum().alias("m"))
        .filter(pl.col("m") >= 1)
        .collect()
        .height
    )
    assert int(inputs.counts.sum()) <= n_assist_events_with_m_ge_1
    assert int(inputs.counts.sum()) > 0


def test_cell_event_class_idx_in_range(tmp_path: Path) -> None:
    dataset_path = _synthetic_assist_count_dataset(tmp_path)
    inputs = prepare_assist_count_inputs(
        dataset_path,
        min_events_per_season=1,
        min_events_per_cell=1,
        held_out_fold_count=999,
    )
    n_event_classes = len(inputs.coords["event_class"])
    assert inputs.cell_event_class_idx.min() >= 0
    assert inputs.cell_event_class_idx.max() < n_event_classes


def test_builder_declares_count_simplex(tmp_path: Path) -> None:
    dataset_path = _synthetic_assist_count_dataset(tmp_path)
    inputs = prepare_assist_count_inputs(
        dataset_path,
        min_events_per_season=1,
        min_events_per_cell=1,
        held_out_fold_count=999,
    )
    model = build_assist_count_model(inputs)
    rv_names = {rv.name for rv in model.unobserved_RVs}
    det_names = {d.name for d in model.deterministics}
    observed_names = {rv.name for rv in model.observed_RVs}
    assert {"beta0_count", "event_class_logodds", "cell_logodds"}.issubset(rv_names)
    assert "cell_class_prob" in det_names
    assert "assist_count_obs" in observed_names


def test_prior_predictive_count_prob_sums_to_one(tmp_path: Path) -> None:
    dataset_path = _synthetic_assist_count_dataset(tmp_path)
    inputs = prepare_assist_count_inputs(
        dataset_path,
        min_events_per_season=1,
        min_events_per_cell=1,
        held_out_fold_count=999,
    )
    import pymc as pm

    model = build_assist_count_model(inputs)
    with model:
        prior = pm.sample_prior_predictive(draws=20, random_seed=0)
    probs = prior.prior["cell_class_prob"].values
    sums = probs.sum(axis=-1)
    np.testing.assert_allclose(sums, 1.0, atol=1e-6)
    assert probs.shape[-1] == ASSIST_COUNT_MAX
