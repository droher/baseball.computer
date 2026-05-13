"""Atomic Parquet write semantics."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from python_models.statistical.outputs import write_parquet_atomic


def test_write_parquet_atomic_creates_file(tmp_path: Path) -> None:
    df = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    target = tmp_path / "out.parquet"
    write_parquet_atomic(df, target)
    assert target.exists()
    loaded = pl.read_parquet(target)
    assert loaded.shape == df.shape
    assert loaded["x"].to_list() == [1, 2, 3]


def test_write_parquet_atomic_no_temp_residue(tmp_path: Path) -> None:
    df = pl.DataFrame({"x": [1, 2, 3]})
    target = tmp_path / "out.parquet"
    write_parquet_atomic(df, target)
    residue = [p for p in tmp_path.iterdir() if p.name.startswith(".out.")]
    assert residue == []


def test_write_parquet_atomic_overwrite_replaces_content(tmp_path: Path) -> None:
    target = tmp_path / "out.parquet"
    write_parquet_atomic(pl.DataFrame({"x": [1]}), target)
    write_parquet_atomic(pl.DataFrame({"x": [9, 8]}), target)
    loaded = pl.read_parquet(target)
    assert loaded["x"].to_list() == [9, 8]


def test_write_parquet_atomic_creates_parent_dir(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "child" / "out.parquet"
    write_parquet_atomic(pl.DataFrame({"x": [1]}), target)
    assert target.exists()


def test_write_parquet_atomic_failure_leaves_target_untouched(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "out.parquet"
    write_parquet_atomic(pl.DataFrame({"x": [1]}), target)
    original_bytes = target.read_bytes()

    def boom(self: pl.DataFrame, *args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated write failure")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", boom, raising=True)
    with pytest.raises(RuntimeError):
        write_parquet_atomic(pl.DataFrame({"x": [2]}), target)
    assert target.read_bytes() == original_bytes
    residue = [p for p in tmp_path.iterdir() if p.name.startswith(".out.")]
    assert residue == []
