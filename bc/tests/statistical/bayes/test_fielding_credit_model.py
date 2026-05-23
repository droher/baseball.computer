"""Builder + supervised-arm inference test for the fielding-credit model."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.models._credit_data import (
    N_POSITIONS,
    REAL_UNKNOWN_RATES_BY_POSITION,
    prepare_event_credit_inputs,
)
from python_models.statistical.models.credit import build_fielding_credit_model
from python_models.statistical.pymc_utils import SamplingConfig, sample_model


def _synthetic_dataset(
    tmp_path: Path,
    *,
    n_games: int = 4,
    events_per_game: int = 8,
    sources: tuple[str, ...] = ("play_by_play",),
    season: int = 1925,
    fielding_team_id: str = "TEX",
    true_pos_0based: int | None = None,
    seed: int = 20260521,
) -> Path:
    """Build a well-attributed fixture; if true_pos_0based is set, all events
    land at that position, otherwise positions sample from the per-position weights.
    """
    rng = np.random.default_rng(seed)
    lineups = {
        f"G{g:03d}": [f"P{g:02d}_{p:02d}" for p in range(N_POSITIONS)]
        for g in range(n_games)
    }
    rows: list[dict[str, object]] = []
    weights = np.asarray(REAL_UNKNOWN_RATES_BY_POSITION, dtype=np.float64)
    weights = weights / weights.sum()
    for g in range(n_games):
        for e in range(events_per_game):
            eid = 200_000 + g * events_per_game + e
            if true_pos_0based is None:
                tp0 = int(rng.choice(N_POSITIONS, p=weights))
            else:
                tp0 = true_pos_0based
            lineup = lineups[f"G{g:03d}"]
            src = sources[(g * events_per_game + e) % len(sources)]
            for k_pos in range(1, N_POSITIONS + 1):
                for ct in ("putout", "assist", "error"):
                    known_credit = (
                        1.0
                        if ct == "putout" and (k_pos - 1) == tp0
                        else 0.0
                    )
                    rows.append(
                        {
                            "event_key": eid,
                            "player_id": lineup[k_pos - 1],
                            "fielding_position": k_pos,
                            "credit_type": ct,
                            "known_credit": known_credit,
                            "unknown_credit_need": 0.0,
                            "fielding_evidence_status": "complete_with_zero_unknowns",
                            "gap_class": "complete",
                            "personnel_hard_mask_available": True,
                            "eligible_for_allocation": False,
                            "game_id": f"G{g:03d}",
                            "season": season,
                            "league": "AL",
                            "game_type": "RegularSeason",
                            "source_type": "pbp",
                            "source_family": src,
                            "park_id": "ARL01",
                            "scorer": "scorerA",
                            "fielding_team_id": fielding_team_id,
                            "result_family": "out_in_play",
                            "base_state_start": 0,
                            "outs_start": 1,
                            "frame_start": "Top",
                            "alignment_regime": "shift_growth_era",
                            "personnel_confidence": "high",
                            "context_confidence": "high",
                            "exposure_status": "complete",
                        }
                    )
    df = pl.DataFrame(rows)
    dataset_path = tmp_path / "dataset.parquet"
    df.write_parquet(dataset_path)
    return dataset_path


_SOURCE_VARS = {"sigma_source", "z_source", "beta_source"}


def test_required_rvs_declared(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(tmp_path)
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    model = build_fielding_credit_model(inputs)
    rv_names = {rv.name for rv in model.unobserved_RVs}
    det_names = {d.name for d in model.deterministics}
    assert {
        "alpha_position",
        "sigma_season",
        "sigma_scorer",
        "sigma_park",
        "beta_season",
        "z_scorer",
        "z_park",
    }.issubset(rv_names)
    assert {"beta_scorer", "beta_park"}.issubset(det_names)
    assert "pi" not in det_names and "T_pred" not in det_names, (
        "pi / T_pred must not be Deterministics — they blow up posterior.nc at scale"
    )
    observed_names = {rv.name for rv in model.observed_RVs}
    assert "Y_supervised" in observed_names, (
        "supervised multinomial arm missing"
    )


def test_single_source_drops_source_block(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(tmp_path, sources=("play_by_play",))
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    model = build_fielding_credit_model(inputs)
    rv_names = {rv.name for rv in model.unobserved_RVs}
    det_names = {d.name for d in model.deterministics}
    assert _SOURCE_VARS.isdisjoint(rv_names | det_names)


def test_multi_source_keeps_source_block(tmp_path: Path) -> None:
    dataset_path = _synthetic_dataset(
        tmp_path, sources=("play_by_play", "box_score")
    )
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    model = build_fielding_credit_model(inputs)
    rv_names = {rv.name for rv in model.unobserved_RVs}
    det_names = {d.name for d in model.deterministics}
    assert _SOURCE_VARS.issubset(rv_names | det_names)


@pytest.mark.slow
def test_supervised_arm_drives_pi_to_true_position(tmp_path: Path) -> None:
    """With every event truly at position 1, NUTS should pile share on position 1."""
    from python_models.statistical.bayes.training import _posterior_event_softmax

    dataset_path = _synthetic_dataset(
        tmp_path,
        n_games=4,
        events_per_game=12,
        true_pos_0based=0,
        season=1944,
    )
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="putout",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.n_supervised_events > 0
    model = build_fielding_credit_model(inputs)
    cfg = SamplingConfig(
        draws=120,
        tune=120,
        chains=2,
        target_accept=0.85,
        random_seed=20260521,
        cores=1,
        max_treedepth=8,
        backend="numpyro",
    )
    idata = sample_model(model, cfg)
    pi_mean = _posterior_event_softmax(idata, inputs, chunk_size=50)
    pos1_share = float(pi_mean[:, 0].mean())
    other_share = float(pi_mean[:, 1:].mean())
    assert pos1_share > other_share + 0.1, (
        f"pos1={pos1_share:.3f} not meaningfully above others={other_share:.3f}"
    )
