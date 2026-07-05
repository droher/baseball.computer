"""Pitch-coverage held-out encoding uses the same unseen-level sentinel as scoring."""

from __future__ import annotations

import polars as pl

from python_models.statistical.models import _pitch_coverage_data as pcd


def _held_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "y": [1, 0],
            "season_league": ["2020|NL", "1899|__unknown__"],
            "scorer": ["seen_scorer", "unseen_scorer"],
            "result_family": ["out", "unseen_family"],
            "alignment_regime": ["standard", "standard"],
        }
    )


def test_held_out_unseen_levels_map_to_the_shared_sentinel() -> None:
    held = _held_frame()
    cell_vocab = {"2020|NL": 0}
    scorer_vocab = {"seen_scorer": 0}
    fe_vocabs = {
        "result_family": {"out": 0},
        "alignment_regime": {"standard": 0},
    }

    result = pcd._build_held_out(
        held,
        cell_vocab=cell_vocab,
        scorer_vocab=scorer_vocab,
        fe_vocabs=fe_vocabs,
    )

    sentinel = pcd.UNSEEN_LEVEL_CODE
    assert result.cell_idx.tolist() == [0, sentinel]
    assert result.scorer_idx.tolist() == [0, sentinel]
    assert result.fixed_effect_codes["result_family"].tolist() == [0, sentinel]
    assert result.fixed_effect_codes["alignment_regime"].tolist() == [0, 0]


def test_held_out_fe_sentinel_matches_cell_and_scorer_sentinel() -> None:
    held = _held_frame()
    result = pcd._build_held_out(
        held,
        cell_vocab={},
        scorer_vocab={},
        fe_vocabs={c: {} for c in pcd.CONTEXT_FIXED_EFFECT_COLUMNS},
    )

    unseen_codes = {
        int(result.cell_idx[0]),
        int(result.scorer_idx[0]),
        *(int(codes[0]) for codes in result.fixed_effect_codes.values()),
    }
    assert unseen_codes == {pcd.UNSEEN_LEVEL_CODE}
