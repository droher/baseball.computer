"""Builder test for the ball-handler model (Model D)."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from python_models.statistical.models._ball_handler_data import (
    FIXED_EFFECT_COLUMNS,
    N_POSITIONS,
    prepare_ball_handler_inputs,
)
from python_models.statistical.models.ball_handler import build_ball_handler_model

DIMENSION = "ball_handler_position"


def _synthetic_dataset(
    tmp_path: Path, *, n_games: int = 8, events_per_game: int = 10
) -> Path:
    rng = np.random.default_rng(20260525)
    result_families = ("out_in_play", "hit_in_play")
    alignment_regimes = ("shift_growth_era", "pre_shift_era")
    rows: list[dict[str, object]] = []
    for g in range(n_games):
        for e in range(events_per_game):
            eid = 200_000 + g * events_per_game + e
            handler = int(rng.integers(1, N_POSITIONS + 1))
            rows.append(
                {
                    "event_key": eid,
                    "dimension": DIMENSION,
                    "observed_status": "observed",
                    "raw_value": str(handler),
                    "training_weight": 1.0,
                    "game_id": f"G{g:03d}",
                    "season": 1925,
                    "league": "AL",
                    "source_family": "play_by_play",
                    "park_id": "ARL01",
                    "scorer": "scorerA",
                    "base_state_start": int(rng.integers(0, 4)),
                    "outs_start": int(rng.integers(0, 3)),
                    "result_family": result_families[eid % len(result_families)],
                    "alignment_regime": alignment_regimes[eid % len(alignment_regimes)],
                }
            )
    dataset_path = tmp_path / "dataset.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)
    return dataset_path


def test_builds_valid_graph_with_expected_rvs(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(tmp_path)
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    model = build_ball_handler_model(inputs)

    rv_names = {rv.name for rv in model.unobserved_RVs}
    det_names = {d.name for d in model.deterministics}
    observed_names = {rv.name for rv in model.observed_RVs}

    assert "alpha_position" in rv_names
    assert {
        "sigma_season_league",
        "sigma_scorer",
        "z_season_league",
        "z_scorer",
    }.isdisjoint(rv_names), (
        "scalar-per-event REs cancel in the softmax and must not be in the model"
    )
    assert {"beta_season_league", "beta_scorer"}.isdisjoint(det_names)
    for column in FIXED_EFFECT_COLUMNS:
        assert f"delta_{column}" in rv_names
    assert "H_observed" in observed_names


def test_no_event_sized_deterministic(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(tmp_path)
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    model = build_ball_handler_model(inputs)
    det_names = {d.name for d in model.deterministics}
    assert "pi" not in det_names and "eta" not in det_names, (
        "per-event pi / eta must not be Deterministics — they blow up posterior.nc"
    )
    for det in model.deterministics:
        shape = tuple(model.named_vars_to_dims.get(det.name, ()))
        assert "event" not in shape, f"{det.name} is event-sized"
