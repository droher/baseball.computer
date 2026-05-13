"""Grouped split registry + leakage detection."""

from __future__ import annotations

import polars as pl

from python_models.statistical.splits import (
    DEFAULT_FOLD_COUNT,
    assign_game_hash_split,
    detect_leakage,
    game_hash_fold,
)


def test_game_hash_fold_deterministic() -> None:
    assert game_hash_fold("NYA197304110") == game_hash_fold("NYA197304110")


def test_game_hash_fold_in_range() -> None:
    for game_id in ("A", "BBB", "NYA197304110", "X" * 32):
        fold = game_hash_fold(game_id)
        assert 0 <= fold < DEFAULT_FOLD_COUNT


def test_game_hash_fold_partition_roughly_balanced() -> None:
    counts = [0] * DEFAULT_FOLD_COUNT
    for i in range(20_000):
        counts[game_hash_fold(f"G{i:06d}")] += 1
    smallest = min(counts)
    largest = max(counts)
    assert smallest > 100
    assert largest < 350


def test_assign_game_hash_split_no_leakage() -> None:
    game_ids = [f"G{i:05d}" for i in range(500)]
    assignments = assign_game_hash_split(game_ids, split_registry_id="reg-1")
    rows = [
        {"unit_id": a.unit_id, "fold_id": a.fold_id}
        for a in assignments
    ]
    df = pl.DataFrame(rows)
    duplicates = detect_leakage(df, unit_col="unit_id")
    assert duplicates == []


def test_detect_leakage_flags_cross_fold_unit() -> None:
    df = pl.DataFrame(
        {
            "unit_id": ["A", "A", "B", "C"],
            "fold_id": [0, 1, 0, 0],
        }
    )
    duplicates = detect_leakage(df, unit_col="unit_id")
    assert duplicates == ["A"]
