"""Per-(event, position) export tests for the responsibility path (Model I)."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.bayes.training import (
    _export_responsibility_probabilities,
)
from python_models.statistical.models._responsibility_data import (
    RESPONSIBILITY_POSITION_LABELS,
)

N_ROW = 11


def _means(rng: np.random.Generator, n_row: int, k: int) -> np.ndarray:
    raw = rng.uniform(0.1, 1.0, size=(n_row, k))
    return raw / raw.sum(axis=1, keepdims=True)


def _call(tmp_path: Path) -> tuple[pl.DataFrame, np.ndarray, list[str]]:
    rng = np.random.default_rng(13)
    k = len(RESPONSIBILITY_POSITION_LABELS)
    means = _means(rng, N_ROW, k)
    event_keys = np.arange(1, N_ROW + 1, dtype=np.int64)
    position_labels = list(RESPONSIBILITY_POSITION_LABELS)
    target = tmp_path / "responsibility_probabilities.parquet"
    df = _export_responsibility_probabilities(
        means,
        event_keys=event_keys,
        position_labels=position_labels,
        target_path=target,
    )
    written = pl.read_parquet(target)
    assert df.equals(written)
    return written, event_keys, position_labels


def test_export_schema(tmp_path: Path) -> None:
    df, _keys, _labels = _call(tmp_path)
    assert df.schema == {
        "event_key": pl.Int64,
        "fielding_position": pl.Int8,
        "expected_share": pl.Float64,
    }


def test_export_row_count_and_grain(tmp_path: Path) -> None:
    df, _keys, _labels = _call(tmp_path)
    k = len(RESPONSIBILITY_POSITION_LABELS)
    assert df.height == N_ROW * k
    grain = df.select("event_key", "fielding_position")
    assert grain.n_unique() == df.height


def test_fielding_position_set_is_actual_positions(tmp_path: Path) -> None:
    df, _keys, labels = _call(tmp_path)
    expected = {int(p) for p in labels}
    assert set(df.get_column("fielding_position").unique().to_list()) == expected


def test_per_event_shares_sum_to_one(tmp_path: Path) -> None:
    df, _keys, _labels = _call(tmp_path)
    summed = (
        df.group_by("event_key")
        .agg(pl.col("expected_share").sum().alias("total"))
        .get_column("total")
        .to_numpy()
    )
    assert summed.shape[0] == N_ROW
    assert np.allclose(summed, 1.0)


def test_position_label_length_mismatch_raises(tmp_path: Path) -> None:
    rng = np.random.default_rng(3)
    k = len(RESPONSIBILITY_POSITION_LABELS)
    means = _means(rng, N_ROW, k)
    event_keys = np.arange(1, N_ROW + 1, dtype=np.int64)
    short_labels = list(RESPONSIBILITY_POSITION_LABELS)[:-1]
    with pytest.raises(AssertionError):
        _ = _export_responsibility_probabilities(
            means,
            event_keys=event_keys,
            position_labels=short_labels,
            target_path=tmp_path / "mismatch.parquet",
        )
