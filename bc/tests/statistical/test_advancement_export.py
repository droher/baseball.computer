"""Per-(event, baserunner, class) export tests for the advancement path (Model H)."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical.bayes.training import _export_advancement_probabilities
from python_models.statistical.models._advancement_data import ADVANCEMENT_CLASS_LABELS

N_ROW = 11
BASERUNNERS = ("First", "Second", "Third")


def _means(rng: np.random.Generator, n_row: int, k: int) -> np.ndarray:
    raw = rng.uniform(0.1, 1.0, size=(n_row, k))
    return raw / raw.sum(axis=1, keepdims=True)


def _call(tmp_path: Path) -> tuple[pl.DataFrame, np.ndarray, list[str]]:
    rng = np.random.default_rng(13)
    k = len(ADVANCEMENT_CLASS_LABELS)
    means = _means(rng, N_ROW, k)
    event_keys = np.arange(1, N_ROW + 1, dtype=np.int64)
    baserunner_labels = [BASERUNNERS[i % len(BASERUNNERS)] for i in range(N_ROW)]
    class_labels = list(ADVANCEMENT_CLASS_LABELS)
    target = tmp_path / "advancement_probabilities.parquet"
    df = _export_advancement_probabilities(
        means,
        event_keys=event_keys,
        baserunner_labels=baserunner_labels,
        class_labels=class_labels,
        target_path=target,
    )
    written = pl.read_parquet(target)
    assert df.equals(written)
    return written, event_keys, baserunner_labels


def test_export_schema(tmp_path: Path) -> None:
    df, _keys, _labels = _call(tmp_path)
    assert df.schema == {
        "event_key": pl.Int64,
        "baserunner": pl.Utf8,
        "advancement_class": pl.Utf8,
        "expected_share": pl.Float64,
    }


def test_export_row_count_and_grain(tmp_path: Path) -> None:
    df, _keys, _labels = _call(tmp_path)
    k = len(ADVANCEMENT_CLASS_LABELS)
    assert df.height == N_ROW * k
    grain = df.select("event_key", "baserunner", "advancement_class")
    assert grain.n_unique() == df.height


def test_class_label_set_matches_vocabulary(tmp_path: Path) -> None:
    df, _keys, _labels = _call(tmp_path)
    assert set(df.get_column("advancement_class").unique().to_list()) == set(
        ADVANCEMENT_CLASS_LABELS
    )


def test_per_event_baserunner_shares_sum_to_one(tmp_path: Path) -> None:
    df, _keys, _labels = _call(tmp_path)
    summed = (
        df.group_by("event_key", "baserunner")
        .agg(pl.col("expected_share").sum().alias("total"))
        .get_column("total")
        .to_numpy()
    )
    assert summed.shape[0] == N_ROW
    assert np.allclose(summed, 1.0)


def test_baserunner_length_mismatch_raises(tmp_path: Path) -> None:
    rng = np.random.default_rng(3)
    k = len(ADVANCEMENT_CLASS_LABELS)
    means = _means(rng, N_ROW, k)
    event_keys = np.arange(1, N_ROW + 1, dtype=np.int64)
    short_labels = ["First"] * (N_ROW - 1)
    with pytest.raises(AssertionError):
        _ = _export_advancement_probabilities(
            means,
            event_keys=event_keys,
            baserunner_labels=short_labels,
            class_labels=list(ADVANCEMENT_CLASS_LABELS),
            target_path=tmp_path / "mismatch.parquet",
        )
