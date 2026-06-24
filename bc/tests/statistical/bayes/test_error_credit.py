"""Invariant tests for the error-credit allocation submodel."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.models._credit_data import N_POSITIONS
from python_models.statistical.models._error_credit_data import (
    prepare_error_credit_inputs,
)
from python_models.statistical.models.error_credit import build_error_credit_model


def _synthetic_error_dataset(
    tmp_path: Path,
    *,
    n_games: int = 8,
    events_per_game: int = 16,
    n_scorers: int = 3,
    season: int = 1925,
    seed: int = 20260602,
) -> Path:
    """Seed error credit rows with a single error per event at a chosen position."""
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    for g in range(n_games):
        for e in range(events_per_game):
            eid = 500_000 + g * events_per_game + e
            error_pos_0 = int(rng.integers(0, N_POSITIONS))
            scorer = f"scorer{(g * events_per_game + e) % n_scorers}"
            for k_pos in range(1, N_POSITIONS + 1):
                for ct in ("putout", "assist", "error"):
                    if ct == "error" and (k_pos - 1) == error_pos_0:
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
                            "scorer": scorer,
                            "fielding_team_id": "TEX",
                            "result_family": "reached_on_error",
                            "base_state_start": 0,
                            "outs_start": 1,
                            "alignment_regime": "shift_growth_era",
                        }
                    )
    dataset_path = tmp_path / "error_dataset.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)
    return dataset_path


def test_inputs_filter_to_error_credit_with_known(tmp_path: Path) -> None:
    dataset_path = _synthetic_error_dataset(tmp_path)
    inputs = prepare_error_credit_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.credit_type == "error"
    assert inputs.n_positions == N_POSITIONS
    assert (inputs.U >= 1).all()
    assert inputs.Y_supervised_counts.shape == (inputs.n_events, N_POSITIONS)
    assert int(inputs.Y_supervised_counts.sum()) == int(inputs.U.sum())


def test_supervised_counts_match_U(tmp_path: Path) -> None:
    dataset_path = _synthetic_error_dataset(tmp_path)
    inputs = prepare_error_credit_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    np.testing.assert_array_equal(
        inputs.Y_supervised_counts.sum(axis=1), inputs.Y_supervised_U
    )


def test_builder_has_position_softmax_and_scorer_interaction(tmp_path: Path) -> None:
    dataset_path = _synthetic_error_dataset(tmp_path, n_scorers=4)
    inputs = prepare_error_credit_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    model = build_error_credit_model(inputs)
    rv_names = {rv.name for rv in model.unobserved_RVs}
    det_names = {d.name for d in model.deterministics}
    observed_names = {rv.name for rv in model.observed_RVs}
    assert "alpha_position" in rv_names
    assert "delta_scorer" in rv_names
    assert "E_supervised" in observed_names
    assert "pi" not in det_names, "pi must not be a Deterministic (blows up posterior)"
    assert {"sigma_scorer", "beta_scorer"}.isdisjoint(rv_names), (
        "scalar-per-event scorer RE cancels in the softmax and must be absent"
    )


def test_scorer_interaction_gate_disables_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset_path = _synthetic_error_dataset(tmp_path, n_scorers=4)
    inputs = prepare_error_credit_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    monkeypatch.setenv("BC_ERROR_CREDIT_DISABLE_SCORER", "1")
    model = build_error_credit_model(inputs)
    rv_names = {rv.name for rv in model.unobserved_RVs}
    assert "delta_scorer" not in rv_names


def test_single_scorer_drops_interaction(tmp_path: Path) -> None:
    dataset_path = _synthetic_error_dataset(tmp_path, n_scorers=1)
    inputs = prepare_error_credit_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    model = build_error_credit_model(inputs)
    rv_names = {rv.name for rv in model.unobserved_RVs}
    assert "delta_scorer" not in rv_names


def test_prior_predictive_position_simplex(tmp_path: Path) -> None:
    dataset_path = _synthetic_error_dataset(tmp_path)
    inputs = prepare_error_credit_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    import pymc as pm
    import pytensor.tensor as pt

    model = build_error_credit_model(inputs)
    with model:
        eta = model["alpha_position"]
        _ = pm.Deterministic("pi_check", pt.special.softmax(eta))
        prior = pm.sample_prior_predictive(draws=10, random_seed=0)
    sums = prior.prior["pi_check"].values.sum(axis=-1)
    np.testing.assert_allclose(sums, 1.0, atol=1e-6)
