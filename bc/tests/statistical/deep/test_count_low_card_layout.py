from __future__ import annotations

import os

_ = os.environ.setdefault("KERAS_BACKEND", "torch")

import polars as pl
import pytest

from python_models.statistical.deep.pretrain.targets import (
    EVENT_UNIVERSE_LAYOUT,
    EVENT_UNIVERSE_LOW_CARD,
    EVENT_UNIVERSE_NUMERIC,
)
from python_models.statistical.deep.targets.geometry import (
    GEOMETRY_LAYOUT,
    LOW_CARD_COLUMNS as GEOMETRY_LOW_CARD,
    NUMERIC_COLUMNS as GEOMETRY_NUMERIC,
)
from python_models.statistical.deep.pretrain.training import (
    _collect_input_stats,
    _encode_inputs,
)


COUNT_COLS = ("count_balls", "count_strikes")


@pytest.mark.parametrize("col", COUNT_COLS)
def test_count_in_pretrain_low_card(col: str) -> None:
    assert col in EVENT_UNIVERSE_LOW_CARD
    assert col not in EVENT_UNIVERSE_NUMERIC
    assert col in EVENT_UNIVERSE_LAYOUT.low_card_columns
    assert col not in EVENT_UNIVERSE_LAYOUT.numeric_columns


@pytest.mark.parametrize("col", COUNT_COLS)
def test_count_in_geometry_low_card(col: str) -> None:
    assert col in GEOMETRY_LOW_CARD
    assert col not in GEOMETRY_NUMERIC
    assert col in GEOMETRY_LAYOUT.low_card_columns
    assert col not in GEOMETRY_LAYOUT.numeric_columns


def _synth_frame() -> pl.DataFrame:
    n = 8
    cols: dict[str, object] = {}
    for c in EVENT_UNIVERSE_LAYOUT.high_card_columns:
        cols[c] = pl.Series(c, [0] * n, dtype=pl.Int64)
    for c in EVENT_UNIVERSE_LAYOUT.low_card_columns:
        if c in COUNT_COLS:
            continue
        cols[c] = pl.Series(c, ["x"] * n, dtype=pl.Utf8)
    for c in EVENT_UNIVERSE_LAYOUT.numeric_columns:
        cols[c] = pl.Series(c, [0.0] * n, dtype=pl.Float64)
    cols["count_balls"] = pl.Series(
        "count_balls", [0, 1, 2, 3, None, None, 0, 1], dtype=pl.Int64
    )
    cols["count_strikes"] = pl.Series(
        "count_strikes", [0, 1, 2, None, None, 0, 1, 2], dtype=pl.Int64
    )
    return pl.DataFrame(cols)


def test_null_count_encodes_to_oov_distinct_from_zero() -> None:
    df = _synth_frame()
    vocabularies, _means, _vars = _collect_input_stats(df, layout=EVENT_UNIVERSE_LAYOUT)

    bal_vocab = vocabularies["count_balls"]
    assert "0" in bal_vocab.values
    assert "" not in bal_vocab.values

    encoded = _encode_inputs(df, layout=EVENT_UNIVERSE_LAYOUT, vocabularies=vocabularies)
    bal_codes = encoded["count_balls"].reshape(-1).tolist()
    str_codes = encoded["count_strikes"].reshape(-1).tolist()

    real_zero_idxs = [0, 6]
    null_idxs = [4, 5]
    assert bal_codes[real_zero_idxs[0]] == bal_codes[real_zero_idxs[1]]
    assert bal_codes[null_idxs[0]] == bal_codes[null_idxs[1]]
    assert bal_codes[real_zero_idxs[0]] != bal_codes[null_idxs[0]]
    assert bal_codes[null_idxs[0]] == 0

    real_zero_idx = 0
    null_idx = 3
    assert str_codes[null_idx] == 0
    assert str_codes[real_zero_idx] != 0
