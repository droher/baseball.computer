"""Builder tests for the pitch-summary multinomial + coverage models."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pymc as pm
import pytest
from pytensor.graph.basic import ancestors

from python_models.statistical.models._credit_data import FixedEffectDesign
from python_models.statistical.models._pitch_coverage_data import (
    CONTEXT_FIXED_EFFECT_COLUMNS,
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    UNSEEN_LEVEL_CODE,
    PitchCoverageHeldOutSet,
    PitchCoverageInputs,
    prepare_pitch_coverage_inputs,
)
from python_models.statistical.models._pitch_summary_data import (
    PitchSummaryHeldOutSet,
    PitchSummaryInputs,
    _reachable_mask,
    _reference_by_result,
)
from python_models.statistical.models.pitch_coverage import build_pitch_coverage_model
from python_models.statistical.models.pitch_summary import (
    MASK_LOGIT,
    build_pitch_summary_model,
)
from python_models.statistical.splits import game_hash_fold

N_CLASSES = 12


def _class_axes() -> tuple[list[str], list[int], list[int]]:
    labels: list[str] = []
    balls: list[int] = []
    strikes: list[int] = []
    for b in range(4):
        for s in range(3):
            labels.append(f"b{b}_s{s}")
            balls.append(b)
            strikes.append(s)
    return labels, balls, strikes


def _tiny_inputs() -> PitchSummaryInputs:
    cell_labels = [
        "out_in_play|2023|AL",
        "strikeout|2023|AL",
        "strikeout|2023|NL",
        "walk|2023|AL",
    ]
    result_by_cell = ["out_in_play", "strikeout", "strikeout", "walk"]
    season_by_cell = [2023, 2023, 2023, 2023]
    league_by_cell = ["AL", "AL", "NL", "AL"]
    result_family_labels = ["out_in_play", "strikeout", "walk"]
    result_to_idx = {r: i for i, r in enumerate(result_family_labels)}
    cell_result_idx = np.array(
        [result_to_idx[r] for r in result_by_cell], dtype=np.int64
    )
    class_labels, balls_by_class, strikes_by_class = _class_axes()

    reachable = _reachable_mask(result_family_labels)
    rng = np.random.default_rng(20260513)
    counts = (
        rng.integers(2, 9, size=(len(cell_labels), N_CLASSES))
        * reachable[cell_result_idx]
    ).astype(np.int64)
    ref_class_by_result = _reference_by_result(counts, cell_result_idx, reachable)

    return PitchSummaryInputs(
        counts=counts,
        cell_result_idx=cell_result_idx,
        reachable_mask=reachable,
        ref_class_by_result=ref_class_by_result,
        cell_labels=list(cell_labels),
        result_by_cell=result_by_cell,
        season_by_cell=season_by_cell,
        league_by_cell=league_by_cell,
        class_labels=class_labels,
        balls_by_class=balls_by_class,
        strikes_by_class=strikes_by_class,
        result_family_labels=result_family_labels,
        outcome="final_count",
        coords={
            "source": ["__single__"],
            "class": list(class_labels),
            "result_family": list(result_family_labels),
            "cell": list(cell_labels),
        },
        held_out=PitchSummaryHeldOutSet(
            counts=np.zeros((0, N_CLASSES), dtype=np.int64),
            cell_idx=np.zeros(0, dtype=np.int64),
            cell_result_idx=np.zeros(0, dtype=np.int64),
        ),
    )


def test_builds_model_with_cell_class_prob() -> None:
    inputs = _tiny_inputs()
    model = build_pitch_summary_model(inputs)
    assert isinstance(model, pm.Model)
    assert "cell_class_prob" in model.named_vars
    assert "result_class_prob" in model.named_vars
    assert "result_logodds" in model.named_vars
    assert "cell_logodds" in model.named_vars
    assert "beta0" not in model.named_vars
    assert "final_count_obs" in {rv.name for rv in model.observed_RVs}


def test_free_parameters_cover_only_reachable_nonreference_pairs() -> None:
    inputs = _tiny_inputs()
    model = build_pitch_summary_model(inputs)
    reachable = inputs.reachable_mask
    ref = inputs.ref_class_by_result
    n_result_pairs = sum(
        1
        for r in range(inputs.n_result_families)
        for k in range(N_CLASSES)
        if reachable[r, k] and k != ref[r]
    )
    n_cell_pairs = sum(
        1
        for c in range(inputs.n_cells)
        for k in range(N_CLASSES)
        if reachable[inputs.cell_result_idx[c], k]
        and k != ref[inputs.cell_result_idx[c]]
    )
    assert n_result_pairs < inputs.n_result_families * (N_CLASSES - 1)
    assert tuple(model["result_logodds"].type.shape) == (n_result_pairs,)
    assert tuple(model["cell_logodds"].type.shape) == (n_cell_pairs,)


def test_softmax_sums_to_one_and_masks_unreachable() -> None:
    inputs = _tiny_inputs()
    model = build_pitch_summary_model(inputs)
    cell_prob = np.asarray(
        pm.draw(model["cell_class_prob"], draws=1, random_seed=0), dtype=np.float64
    )
    result_prob = np.asarray(
        pm.draw(model["result_class_prob"], draws=1, random_seed=0), dtype=np.float64
    )
    assert cell_prob.shape == (inputs.n_cells, N_CLASSES)
    assert result_prob.shape == (inputs.n_result_families, N_CLASSES)
    assert np.allclose(cell_prob.sum(axis=1), 1.0, atol=1e-9)
    assert np.allclose(result_prob.sum(axis=1), 1.0, atol=1e-9)
    cell_reachable = inputs.cell_reachable_mask
    assert (~cell_reachable).any()
    assert float(cell_prob[~cell_reachable].max()) < 1e-10
    assert float(cell_prob[cell_reachable].min()) > 0.0
    assert float(result_prob[~inputs.reachable_mask].max()) < 1e-10
    assert float(result_prob[inputs.reachable_mask].min()) > 0.0
    assert float(cell_prob[~cell_reachable].max()) <= np.exp(MASK_LOGIT)


def test_reference_class_carries_zero_logit_under_prior_draw() -> None:
    inputs = _tiny_inputs()
    model = build_pitch_summary_model(inputs)
    cell_prob = np.asarray(
        model["cell_class_prob"].eval(
            {model["cell_logodds"]: np.zeros(model["cell_logodds"].type.shape)}
        ),
        dtype=np.float64,
    )
    for c in range(inputs.n_cells):
        r = int(inputs.cell_result_idx[c])
        n_reachable = int(inputs.reachable_mask[r].sum())
        reachable_probs = cell_prob[c][inputs.reachable_mask[r]]
        assert np.allclose(reachable_probs, 1.0 / n_reachable, atol=1e-9)


def test_observed_count_in_unreachable_entry_raises() -> None:
    inputs = _tiny_inputs()
    bad_counts = inputs.counts.copy()
    strikeout_cell = inputs.cell_labels.index("strikeout|2023|AL")
    bad_counts[strikeout_cell, 0] = 5
    bad = inputs.model_copy(update={"counts": bad_counts})
    with pytest.raises(AssertionError, match="unreachable"):
        build_pitch_summary_model(bad)


def test_cell_class_prob_is_the_only_cell_sized_deterministic() -> None:
    inputs = _tiny_inputs()
    model = build_pitch_summary_model(inputs)

    n_cell = inputs.n_cells
    assert n_cell == len(inputs.coords["cell"])

    prob_dims = tuple(model.named_vars_to_dims.get("cell_class_prob", ()))
    assert prob_dims == ("cell", "class")

    coord_lengths = {name: len(vals) for name, vals in inputs.coords.items()}
    cell_sized = [
        det.name
        for det in model.deterministics
        if n_cell
        in tuple(
            coord_lengths.get(d, -1) for d in model.named_vars_to_dims.get(det.name, ())
        )
    ]
    assert cell_sized == ["cell_class_prob"]

    for det in model.deterministics:
        dims = tuple(model.named_vars_to_dims.get(det.name, ()))
        assert dims, f"{det.name} carries no named dims"


@pytest.mark.slow
def test_prior_predictive_runs() -> None:
    inputs = _tiny_inputs()
    model = build_pitch_summary_model(inputs)
    idata = pm.sample_prior_predictive(draws=2, model=model)
    assert "final_count_obs" in idata.prior_predictive


def _tiny_coverage_inputs() -> PitchCoverageInputs:
    cell_labels = ["2023|AL", "2023|NL", "2024|AL"]
    cell_vocab = {c: i for i, c in enumerate(cell_labels)}
    scorer_labels = ["s0", "s1"]
    scorer_vocab = {s: i for i, s in enumerate(scorer_labels)}

    rows = [
        ("2023|AL", "s0", "strikeout", "pre_shift_era", 1),
        ("2023|NL", "s1", "single", "pre_shift_era", 0),
        ("2024|AL", "s0", "out_in_play", "full_shift_era", 1),
        ("2023|AL", "s1", "single", "full_shift_era", 0),
        ("2024|AL", "s1", "strikeout", "pre_shift_era", 1),
        ("2023|NL", "s0", "out_in_play", "full_shift_era", 0),
    ]
    cell_idx = np.array([cell_vocab[r[0]] for r in rows], dtype=np.int64)
    scorer_idx = np.array([scorer_vocab[r[1]] for r in rows], dtype=np.int64)
    y = np.array([r[4] for r in rows], dtype=np.int64)

    fixed_effects: dict[str, FixedEffectDesign] = {}
    fe_levels: dict[str, list[str]] = {}
    for pos, column in enumerate(CONTEXT_FIXED_EFFECT_COLUMNS):
        levels = sorted({str(r[2 + pos]) for r in rows})
        vocab = {lvl: i for i, lvl in enumerate(levels)}
        codes = np.array([vocab[str(r[2 + pos])] for r in rows], dtype=np.int64)
        fixed_effects[column] = FixedEffectDesign(codes=codes, levels=tuple(levels))
        fe_levels[column] = levels

    coords: dict[str, list[str]] = {
        "source": ["__single__"],
        "season_league": list(cell_labels),
        "scorer": list(scorer_labels),
    }
    for column, levels in fe_levels.items():
        coords[f"{column}_levels"] = levels

    empty = np.zeros(0, dtype=np.int64)
    return PitchCoverageInputs(
        y=y,
        cell_idx=cell_idx,
        scorer_idx=scorer_idx,
        fixed_effects=fixed_effects,
        cell_labels=list(cell_labels),
        scorer_labels=list(scorer_labels),
        coords=coords,
        outcome="has_count",
        dimension="has_count",
        held_out=PitchCoverageHeldOutSet(
            y=empty,
            cell_idx=empty,
            scorer_idx=empty,
            fixed_effect_codes={c: empty for c in CONTEXT_FIXED_EFFECT_COLUMNS},
        ),
    )


def test_coverage_model_builds_with_context_terms() -> None:
    inputs = _tiny_coverage_inputs()
    model = build_pitch_coverage_model(inputs)
    assert isinstance(model, pm.Model)
    assert "count_observed" in {rv.name for rv in model.observed_RVs}
    assert "beta_cell" in model.named_vars
    assert "beta_scorer" in model.named_vars
    for column in CONTEXT_FIXED_EFFECT_COLUMNS:
        assert f"delta_{column}" in model.named_vars, column


def test_coverage_logit_predictor_includes_every_input_term() -> None:
    inputs = _tiny_coverage_inputs()
    model = build_pitch_coverage_model(inputs)
    observed = next(rv for rv in model.observed_RVs if rv.name == "count_observed")
    contributors = {var.name for var in ancestors([observed]) if var.name}
    expected = {"alpha", "beta_cell", "beta_scorer"} | {
        f"delta_{c}" for c in CONTEXT_FIXED_EFFECT_COLUMNS
    }
    missing = expected - contributors
    assert not missing, f"coverage logit missing terms: {missing}"


def test_coverage_outcome_is_bernoulli_observed() -> None:
    inputs = _tiny_coverage_inputs()
    model = build_pitch_coverage_model(inputs)
    observed = next(rv for rv in model.observed_RVs if rv.name == "count_observed")
    assert observed.owner.op.name == "bernoulli"
    obs_values = model.rvs_to_values[observed].eval()
    assert set(np.unique(obs_values)).issubset({0, 1})


def test_coverage_single_level_fixed_effect_omitted() -> None:
    inputs = _tiny_coverage_inputs()
    degenerate = dict(inputs.fixed_effects)
    column = CONTEXT_FIXED_EFFECT_COLUMNS[0]
    degenerate[column] = FixedEffectDesign(
        codes=np.zeros_like(inputs.fixed_effects[column].codes),
        levels=("only",),
    )
    coords = dict(inputs.coords)
    coords[f"{column}_levels"] = ["only"]
    patched = inputs.model_copy(
        update={"fixed_effects": degenerate, "coords": coords}
    )
    model = build_pitch_coverage_model(patched)
    assert f"delta_{column}" not in model.named_vars


def _games_in_fold(prefix: str, count: int, *, held_out: bool) -> list[str]:
    games: list[str] = []
    i = 0
    while len(games) < count:
        gid = f"{prefix}{i:05d}"
        in_holdout = (
            game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
        )
        if in_holdout == held_out:
            games.append(gid)
        i += 1
    return games


def _coverage_dataset(
    tmp_path: Path, *, train_games: int = 30, events_per_game: int = 12
) -> Path:
    rng = np.random.default_rng(20260904)
    seasons = (2023, 2024)
    leagues = ("AL", "NL")
    scorers = ("s0", "s1", "s2")
    families = ("strikeout", "single", "out_in_play")
    regimes = ("pre_shift_era", "full_shift_era")
    rows: list[dict[str, object]] = []
    event_key = 0

    def _append(game_id: str, *, scorer: str, result_family: str) -> None:
        nonlocal event_key
        rows.append(
            {
                "event_key": event_key,
                "game_id": game_id,
                "has_count": bool(rng.random() < 0.6),
                "season": seasons[event_key % len(seasons)],
                "league": leagues[(event_key // 2) % len(leagues)],
                "scorer": scorer,
                "result_family": result_family,
                "alignment_regime": regimes[event_key % len(regimes)],
            }
        )
        event_key += 1

    for gid in _games_in_fold("TRAIN", train_games, held_out=False):
        for e in range(events_per_game):
            _append(gid, scorer=scorers[e % 3], result_family=families[e % 3])
    for gid in _games_in_fold("HELD", 2, held_out=True):
        for e in range(events_per_game):
            _append(gid, scorer=scorers[e % 3], result_family=families[e % 3])
        _append(gid, scorer="unseen_scorer", result_family="unseen_family")
    dataset_path = tmp_path / "pitch_coverage_dataset.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)
    return dataset_path


def test_coverage_prep_populates_inputs_from_synthetic_parquet(tmp_path: Path) -> None:
    smoke_limit = 200
    inputs = prepare_pitch_coverage_inputs(
        _coverage_dataset(tmp_path), smoke_limit=smoke_limit
    )
    assert inputs.outcome == "has_count"
    assert inputs.n_events == smoke_limit
    assert inputs.n_cells == len(inputs.coords["season_league"])
    assert set(np.unique(inputs.y)).issubset({0, 1})
    assert 0.0 < float(inputs.y.mean()) < 1.0
    assert inputs.cell_idx.shape == inputs.y.shape
    assert inputs.scorer_idx.shape == inputs.y.shape
    assert int(inputs.cell_idx.max()) < inputs.n_cells
    assert int(inputs.scorer_idx.max()) < len(inputs.coords["scorer"])
    for column in CONTEXT_FIXED_EFFECT_COLUMNS:
        design = inputs.fixed_effects[column]
        assert design.codes.shape == inputs.y.shape
        assert int(design.codes.max()) < len(design.levels)
        assert list(design.levels) == inputs.coords[f"{column}_levels"]


def test_coverage_held_out_disjoint_and_unseen_levels_sentinel(tmp_path: Path) -> None:
    dataset_path = _coverage_dataset(tmp_path)
    inputs = prepare_pitch_coverage_inputs(dataset_path, smoke_limit=10_000)
    held = inputs.held_out
    held_games = {
        g
        for g in pl.read_parquet(dataset_path).get_column("game_id").unique().to_list()
        if game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
    }
    assert held_games
    assert held.n_events == pl.read_parquet(dataset_path).filter(
        pl.col("game_id").is_in(list(held_games))
    ).height
    assert held.n_events + inputs.n_events == pl.read_parquet(dataset_path).height
    assert set(np.unique(held.y)).issubset({0, 1})
    assert int(held.cell_idx.min()) >= UNSEEN_LEVEL_CODE
    assert int(held.cell_idx.max()) < inputs.n_cells
    assert int(held.scorer_idx.min()) == UNSEEN_LEVEL_CODE
    assert int(held.scorer_idx.max()) < len(inputs.scorer_labels)
    assert "unseen_scorer" not in inputs.scorer_labels
    unseen_scorer_rows = held.scorer_idx == UNSEEN_LEVEL_CODE
    assert int(unseen_scorer_rows.sum()) == len(held_games)
    family_codes = held.fixed_effect_codes["result_family"]
    assert family_codes.shape == held.y.shape
    assert int(family_codes.min()) == UNSEEN_LEVEL_CODE
    np.testing.assert_array_equal(family_codes == UNSEEN_LEVEL_CODE, unseen_scorer_rows)
    assert int(family_codes.max()) < len(inputs.fixed_effects["result_family"].levels)
