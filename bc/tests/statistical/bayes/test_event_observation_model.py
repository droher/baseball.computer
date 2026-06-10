"""Builder smoke for the event-grain observation model.

Covers single-source vs multi-source RV-set expectations and a small
``numpyro`` sample to confirm the model graph compiles and produces a
valid InferenceData.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.models._event_data import (
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    prepare_event_observation_inputs,
)
from python_models.statistical.models.observation import build_observation_model
from python_models.statistical.pymc_utils import SamplingConfig, sample_model
from python_models.statistical.splits import game_hash_fold

_SOURCE_VARS = {"sigma_source", "z_source", "beta_source"}


def _game_id_pool(*, n_holdout: int, n_train: int) -> list[str]:
    holdout: list[str] = []
    train: list[str] = []
    i = 0
    while len(holdout) < n_holdout or len(train) < n_train:
        gid = f"GAME{i:04d}"
        if game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID:
            if len(holdout) < n_holdout:
                holdout.append(gid)
        elif len(train) < n_train:
            train.append(gid)
        i += 1
    return holdout + train


def _make_dataset(
    tmp_path: Path, *, sources: tuple[str, ...], seed: int = 20260519
) -> Path:
    rng = np.random.default_rng(seed)
    n = 96
    game_pool = _game_id_pool(n_holdout=2, n_train=10)
    game_ids = [game_pool[i % len(game_pool)] for i in range(n)]
    df = pl.DataFrame(
        {
            "event_key": np.arange(n, dtype=np.int64),
            "dimension": ["trajectory"] * n,
            "game_id": pl.Series("game_id", game_ids, dtype=pl.Utf8),
            "is_observed": rng.integers(0, 2, size=n).astype(bool),
            "season": pl.Series(
                "season",
                [str(s) for s in rng.choice(["1995", "2015"], size=n).tolist()],
                dtype=pl.Utf8,
            ),
            "scorer": pl.Series(
                "scorer",
                [str(s) for s in rng.choice(["A", "B", "C"], size=n).tolist()],
                dtype=pl.Utf8,
            ),
            "park_id": pl.Series(
                "park_id",
                [str(s) for s in rng.choice(["P1", "P2"], size=n).tolist()],
                dtype=pl.Utf8,
            ),
            "source_family": pl.Series(
                "source_family",
                [str(s) for s in rng.choice(list(sources), size=n).tolist()],
                dtype=pl.Utf8,
            ),
            "game_type": pl.Series(
                "game_type",
                [str(s) for s in rng.choice(["RegularSeason", "Postseason"], size=n).tolist()],
                dtype=pl.Utf8,
            ),
            "frame_start": pl.Series(
                "frame_start",
                [str(s) for s in rng.choice(["Top", "Bottom"], size=n).tolist()],
                dtype=pl.Utf8,
            ),
            "exposure_status": pl.Series(
                "exposure_status", ["complete"] * n, dtype=pl.Utf8
            ),
            "alignment_regime": pl.Series(
                "alignment_regime", ["pre_shift_era"] * n, dtype=pl.Utf8
            ),
            "league": pl.Series(
                "league",
                [str(s) for s in rng.choice(["AL", "NL"], size=n).tolist()],
                dtype=pl.Utf8,
            ),
            "result_family": pl.Series(
                "result_family",
                [str(s) for s in rng.choice(["hit", "out_in_play"], size=n).tolist()],
                dtype=pl.Utf8,
            ),
            "hit_or_out": rng.integers(0, 2, size=n).astype(bool),
            "leverage_bucket": pl.Series(
                "leverage_bucket",
                [str(s) for s in rng.choice(["low", "medium"], size=n).tolist()],
                dtype=pl.Utf8,
            ),
            "batter_hand": pl.Series(
                "batter_hand",
                [str(s) for s in rng.choice(["L", "R"], size=n).tolist()],
                dtype=pl.Utf8,
            ),
            "pitcher_hand": pl.Series(
                "pitcher_hand",
                [str(s) for s in rng.choice(["L", "R"], size=n).tolist()],
                dtype=pl.Utf8,
            ),
            "personnel_confidence": pl.Series(
                "personnel_confidence", ["high"] * n, dtype=pl.Utf8
            ),
            "context_confidence": pl.Series(
                "context_confidence", ["high"] * n, dtype=pl.Utf8
            ),
            "outs_start": rng.integers(0, 3, size=n).astype(np.int64),
            "inning_start": rng.integers(1, 10, size=n).astype(np.int64),
            "score_margin": rng.integers(-3, 3, size=n).astype(np.int64),
            "leverage_index": rng.uniform(0.1, 2.5, size=n).astype(np.float64),
            "runs_on_play": rng.integers(0, 3, size=n).astype(np.int64),
            "training_weight": np.full(n, 1.0),
        }
    )
    parquet_path = tmp_path / "dataset.parquet"
    df.write_parquet(parquet_path)
    return parquet_path


def test_single_source_drops_source_block(tmp_path: Path) -> None:
    parquet = _make_dataset(tmp_path, sources=("play_by_play",))
    inputs = prepare_event_observation_inputs(parquet, dimension="trajectory")
    model = build_observation_model(inputs)
    rv_names = {rv.name for rv in model.unobserved_RVs}
    det_names = {d.name for d in model.deterministics}
    assert _SOURCE_VARS.isdisjoint(rv_names | det_names)


def test_multi_source_keeps_source_block(tmp_path: Path) -> None:
    parquet = _make_dataset(tmp_path, sources=("play_by_play", "box_score"))
    inputs = prepare_event_observation_inputs(parquet, dimension="trajectory")
    model = build_observation_model(inputs)
    rv_names = {rv.name for rv in model.unobserved_RVs}
    det_names = {d.name for d in model.deterministics}
    assert _SOURCE_VARS.issubset(rv_names | det_names)


def test_random_effect_and_fixed_effect_rvs_declared(tmp_path: Path) -> None:
    parquet = _make_dataset(tmp_path, sources=("play_by_play",))
    inputs = prepare_event_observation_inputs(parquet, dimension="trajectory")
    model = build_observation_model(inputs)
    rv_names = {rv.name for rv in model.unobserved_RVs}
    assert {"alpha", "sigma_season", "sigma_scorer", "sigma_park"}.issubset(rv_names)
    assert {"beta_season", "z_scorer", "z_park"}.issubset(rv_names)
    for column in ("game_type", "league", "result_family"):
        assert f"delta_{column}" in rv_names
    for column in ("outs_start", "leverage_index", "runs_on_play"):
        assert f"gamma_{column}" in rv_names


@pytest.mark.slow
def test_numpyro_smoke_fit_runs(tmp_path: Path) -> None:
    parquet = _make_dataset(tmp_path, sources=("play_by_play", "box_score"))
    inputs = prepare_event_observation_inputs(parquet, dimension="trajectory")
    model = build_observation_model(inputs)
    cfg = SamplingConfig(
        draws=50,
        tune=50,
        chains=2,
        target_accept=0.8,
        random_seed=20260519,
        cores=1,
        max_treedepth=8,
        backend="numpyro",
    )
    idata = sample_model(model, cfg)
    assert "alpha" in idata.posterior
    assert idata.posterior.sizes["draw"] == 50
    assert idata.posterior.sizes["chain"] == 2
