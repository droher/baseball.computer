"""Builder + supervised-arm inference test for the fielding-credit model."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.models._credit_data import (
    N_POSITIONS,
    N_POSITIONS_ASSIST,
    NONE_POSITION_LABEL,
    PUTOUT_POSITION_FE_COLUMN,
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
    assert "alpha_position" in rv_names
    assert {
        "sigma_season",
        "sigma_scorer",
        "sigma_park",
        "beta_season",
        "z_season",
        "z_scorer",
        "z_park",
    }.isdisjoint(rv_names), (
        "scalar-per-event REs cancel in the softmax and must not be in the model"
    )
    assert {"beta_scorer", "beta_park"}.isdisjoint(det_names)
    assert not any(name.startswith("gamma_") for name in rv_names), (
        "per-event global FEs cancel in the softmax and must not be in the model"
    )
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


def test_multi_source_has_no_source_block(tmp_path: Path) -> None:
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
    assert _SOURCE_VARS.isdisjoint(rv_names | det_names)


def _synthetic_assist_dataset(
    tmp_path: Path,
    *,
    n_games: int = 4,
    events_per_game: int = 8,
    season: int = 1925,
    fielding_team_id: str = "TEX",
    seed: int = 20260524,
) -> Path:
    """Assist fixture with a putout + an assist at distinct positions per event."""
    rng = np.random.default_rng(seed)
    lineups = {
        f"G{g:03d}": [f"P{g:02d}_{p:02d}" for p in range(N_POSITIONS)]
        for g in range(n_games)
    }
    rows: list[dict[str, object]] = []
    for g in range(n_games):
        for e in range(events_per_game):
            eid = 300_000 + g * events_per_game + e
            putout_pos_0 = int(rng.integers(0, N_POSITIONS))
            assist_pos_0 = int(
                rng.choice([p for p in range(N_POSITIONS) if p != putout_pos_0])
            )
            lineup = lineups[f"G{g:03d}"]
            for k_pos in range(1, N_POSITIONS + 1):
                for ct in ("putout", "assist", "error"):
                    if ct == "putout":
                        known_credit = 1.0 if (k_pos - 1) == putout_pos_0 else 0.0
                    elif ct == "assist":
                        known_credit = 1.0 if (k_pos - 1) == assist_pos_0 else 0.0
                    else:
                        known_credit = 0.0
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
                            "source_family": "play_by_play",
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
    dataset_path = tmp_path / "dataset_assist.parquet"
    df.write_parquet(dataset_path)
    return dataset_path


def test_assist_model_position_coord_has_K10_with_none(tmp_path: Path) -> None:
    dataset_path = _synthetic_assist_dataset(tmp_path)
    inputs = prepare_event_credit_inputs(
        dataset_path,
        dimension="assist",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    model = build_fielding_credit_model(inputs)
    coord_position = inputs.coords["position"]
    assert len(coord_position) == N_POSITIONS_ASSIST == 10
    assert coord_position[-1] == NONE_POSITION_LABEL
    rv_names = {rv.name for rv in model.unobserved_RVs}
    assert f"delta_{PUTOUT_POSITION_FE_COLUMN}" in rv_names, (
        "putout_position FE delta missing from assist model"
    )


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
    pi_mean = _posterior_event_softmax(
        idata, inputs, n_positions=inputs.n_positions, chunk_size=50
    )
    pos1_share = float(pi_mean[:, 0].mean())
    other_share = float(pi_mean[:, 1:].mean())
    assert pos1_share > other_share + 0.1, (
        f"pos1={pos1_share:.3f} not meaningfully above others={other_share:.3f}"
    )


def _synthetic_credit_idata(rng: np.random.Generator, *, k: int = 10):
    import arviz as az

    n_chain, n_draw = 2, 5
    return az.from_dict(
        posterior={
            "alpha_position": rng.normal(size=(n_chain, n_draw, k)),
            "delta_putout_position": rng.normal(size=(n_chain, n_draw, 9, k)),
            "delta_result_family": rng.normal(size=(n_chain, n_draw, 3, k)),
        }
    )


def _carrier_with_putout_at(level_idx: int, *, n: int = 7):
    from python_models.statistical.models._credit_data import (
        FixedEffectDesign,
        HeldOutSet,
    )

    rng = np.random.default_rng(1)
    rf_codes = rng.integers(0, 3, size=n).astype(np.int64)
    rf_codes[0] = -1
    zeros = np.zeros(n, dtype=np.int64)
    return HeldOutSet(
        event_keys=np.arange(n, dtype=np.int64),
        true_position=zeros.copy(),
        U=np.ones(n, dtype=np.int64),
        season_idx=zeros.copy(),
        scorer_idx=zeros.copy(),
        park_idx=zeros.copy(),
        source_idx=zeros.copy(),
        fixed_effects={
            "result_family": FixedEffectDesign(
                levels=("a", "b", "c"), codes=rf_codes
            ),
            "putout_position": FixedEffectDesign(
                levels=tuple(str(p) for p in range(1, 10)),
                codes=np.full(n, level_idx, dtype=np.int64),
            ),
        },
    )


def test_putout_marginalization_reduces_to_observed_under_one_hot() -> None:
    from python_models.statistical.bayes.training import (
        _posterior_event_softmax_putout_marginalized,
        _posterior_held_out_softmax,
    )

    idata = _synthetic_credit_idata(np.random.default_rng(0), k=N_POSITIONS_ASSIST)
    putout_level_idx = 2
    carrier = _carrier_with_putout_at(putout_level_idx)

    observed = _posterior_held_out_softmax(
        idata, carrier, n_positions=N_POSITIONS_ASSIST
    )
    one_hot = np.zeros((carrier.n_events, 9), dtype=np.float64)
    one_hot[:, putout_level_idx] = 1.0
    marginalized = _posterior_event_softmax_putout_marginalized(
        idata, carrier, putout_posterior=one_hot, n_positions=N_POSITIONS_ASSIST
    )

    assert observed.shape == (carrier.n_events, N_POSITIONS_ASSIST)
    assert marginalized.shape == (carrier.n_events, N_POSITIONS_ASSIST)
    np.testing.assert_allclose(observed.sum(axis=1), 1.0, atol=1e-9)
    np.testing.assert_allclose(marginalized.sum(axis=1), 1.0, atol=1e-9)
    np.testing.assert_allclose(observed, marginalized, atol=1e-9)


def test_subsample_held_out_is_deterministic_and_preserves_levels() -> None:
    from python_models.statistical.bayes.training import _subsample_held_out

    carrier = _carrier_with_putout_at(2, n=500)
    a = _subsample_held_out(carrier, limit=50, seed=7)
    b = _subsample_held_out(carrier, limit=50, seed=7)
    assert a.n_events == 50
    np.testing.assert_array_equal(a.event_keys, b.event_keys)
    for column, design in a.fixed_effects.items():
        assert design.levels == carrier.fixed_effects[column].levels
        assert design.codes.shape[0] == 50
    passthrough = _subsample_held_out(carrier, limit=10_000, seed=7)
    assert passthrough.n_events == carrier.n_events


def _softmax_mean(eta: np.ndarray) -> np.ndarray:
    eta = eta - eta.max(axis=-1, keepdims=True)
    exp_eta = np.exp(eta)
    return (exp_eta / exp_eta.sum(axis=-1, keepdims=True)).mean(axis=(0, 1))


def test_event_softmax_gives_unseen_fe_level_zero_effect() -> None:
    from python_models.statistical.bayes.training import _posterior_event_softmax

    idata = _synthetic_credit_idata(np.random.default_rng(3), k=N_POSITIONS_ASSIST)
    carrier = _carrier_with_putout_at(1)
    rf_codes = carrier.fixed_effects["result_family"].codes
    unseen_row = int(np.flatnonzero(rf_codes < 0)[0])
    seen_row = int(np.flatnonzero(rf_codes >= 0)[0])

    shares = _posterior_event_softmax(idata, carrier, n_positions=N_POSITIONS_ASSIST)
    np.testing.assert_allclose(shares.sum(axis=1), 1.0, atol=1e-9)

    alpha = np.asarray(idata.posterior["alpha_position"].values)
    delta_rf = np.asarray(idata.posterior["delta_result_family"].values)
    delta_po = np.asarray(idata.posterior["delta_putout_position"].values)
    po_code = int(carrier.fixed_effects["putout_position"].codes[0])

    without_rf = alpha + delta_po[:, :, po_code, :]
    np.testing.assert_allclose(shares[unseen_row], _softmax_mean(without_rf), atol=1e-9)
    with_rf = without_rf + delta_rf[:, :, int(rf_codes[seen_row]), :]
    np.testing.assert_allclose(shares[seen_row], _softmax_mean(with_rf), atol=1e-9)
    assert not np.allclose(shares[seen_row], shares[unseen_row], atol=1e-6)


def test_putout_posterior_scoring_gives_unseen_fe_level_zero_effect() -> None:
    import arviz as az

    from python_models.statistical.bayes.training import _score_putout_posterior

    rng = np.random.default_rng(4)
    n_chain, n_draw = 2, 5
    alpha = rng.normal(size=(n_chain, n_draw, N_POSITIONS))
    delta_rf = rng.normal(size=(n_chain, n_draw, 3, N_POSITIONS))
    putout_idata = az.from_dict(
        posterior={"alpha_position": alpha, "delta_result_family": delta_rf},
        coords={"result_family_levels": ["a", "b", "c"], "position": list(range(9))},
        dims={
            "alpha_position": ["position"],
            "delta_result_family": ["result_family_levels", "position"],
        },
    )
    carrier = _carrier_with_putout_at(1)
    rf_codes = carrier.fixed_effects["result_family"].codes
    unseen_row = int(np.flatnonzero(rf_codes < 0)[0])
    seen_row = int(np.flatnonzero(rf_codes >= 0)[0])

    weights = _score_putout_posterior(putout_idata, carrier)
    np.testing.assert_allclose(weights.sum(axis=1), 1.0, atol=1e-9)
    np.testing.assert_allclose(weights[unseen_row], _softmax_mean(alpha), atol=1e-9)
    np.testing.assert_allclose(
        weights[seen_row],
        _softmax_mean(alpha + delta_rf[:, :, int(rf_codes[seen_row]), :]),
        atol=1e-9,
    )
