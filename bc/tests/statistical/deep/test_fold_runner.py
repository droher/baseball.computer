"""Synthetic-fixture fold runner: out-of-fold contract + write-time invariants.

Heavy: imports Keras + PyTorch. Marked ``slow`` so a default ``just test`` stays
fast; CI runs the slow tier explicitly.
"""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("KERAS_BACKEND", "torch")

import numpy as np
import polars as pl
import pytest

from python_models.ml.features import FeatureLayout
from python_models.statistical.deep.io import FOLD_ID_COLUMN, KFOLD_COLUMN
from python_models.statistical.deep.target_spec import DeepTargetSpec
from python_models.statistical.splits import game_hash_fold


pytestmark = pytest.mark.slow

N_ROWS: int = 600
N_GAMES: int = 30
CLASS_LABELS: tuple[str, str, str] = ("A", "B", "C")


def _synthetic_dataset(path: Path) -> None:
    rng = np.random.default_rng(20260514)
    game_ids = [f"GAME{idx:04d}" for idx in range(N_GAMES)]
    rows_per_game = N_ROWS // N_GAMES

    records: list[dict[str, object]] = []
    for g_idx, game_id in enumerate(game_ids):
        for r in range(rows_per_game):
            features_low = rng.choice(["L0", "L1", "L2"])
            features_num = float(rng.normal(loc=g_idx * 0.1, scale=1.0))
            target = CLASS_LABELS[int(rng.integers(0, 3))]
            event_key = g_idx * 1000 + r
            split = "TRAIN" if g_idx < 18 else ("VALIDATE" if g_idx < 24 else "TEST")
            records.append(
                {
                    "event_key": event_key,
                    "game_id": game_id,
                    "feature_low": features_low,
                    "feature_num": features_num,
                    "target_class": target,
                    "weight": 1.0,
                    "split_partition": split,
                }
            )

    df = pl.DataFrame(records)
    df.write_parquet(path)


def _layout() -> FeatureLayout:
    return FeatureLayout(
        high_card_columns=(),
        low_card_columns=("feature_low",),
        numeric_columns=("feature_num",),
        grain_column="event_key",
        split_column="split_partition",
    )


def _spec() -> DeepTargetSpec:
    return DeepTargetSpec(
        name="synthetic_target",
        dataset_name="synthetic_dataset",
        target_column="target_class",
        weight_column="weight",
        kind="multiclass",
        proposal_dimension="synthetic",
        fold_count=3,
    )


def test_fold_runner_produces_oof_and_full_fit_predictions(tmp_path: Path) -> None:
    from python_models.statistical.deep import training

    dataset_path = tmp_path / "dataset.parquet"
    _synthetic_dataset(dataset_path)

    spec = _spec()
    layout = _layout()
    result = training.run_target(
        spec,
        dataset_parquet=dataset_path,
        artifact_id="synthetic-aid-1",
        layout=layout,
        source_snapshot_id="src-test",
        artifact_root=tmp_path / "deep",
        epochs=1,
        keras_batch_size=64,
    )

    assert result.manifest_path.exists()
    assert result.probabilities_path.exists()
    assert result.class_labels_path.exists()

    probs = pl.read_parquet(result.probabilities_path)
    assert set(probs["partition"].unique().to_list()) == {"OOF", "VALIDATE", "TEST"}

    train_keys = set(probs.filter(pl.col("partition") == "OOF")["event_key"].to_list())
    assert len(train_keys) == result.train_rows
    val_keys = set(probs.filter(pl.col("partition") == "VALIDATE")["event_key"].to_list())
    assert len(val_keys) == result.validate_rows
    test_keys = set(probs.filter(pl.col("partition") == "TEST")["event_key"].to_list())
    assert len(test_keys) == result.test_rows

    oof = probs.filter(pl.col("partition") == "OOF")
    assert FOLD_ID_COLUMN in probs.columns
    assert oof[FOLD_ID_COLUMN].null_count() == 0
    assert oof[FOLD_ID_COLUMN].n_unique() >= 2
    non_oof = probs.filter(pl.col("partition") != "OOF")
    assert non_oof[FOLD_ID_COLUMN].null_count() == non_oof.height

    dataset = pl.read_parquet(dataset_path).select("event_key", "game_id")
    joined = oof.join(dataset, on="event_key", how="left")
    for game_id, fold_id in joined.select("game_id", FOLD_ID_COLUMN).iter_rows():
        assert fold_id == game_hash_fold(game_id, fold_count=spec.fold_count)


def test_fold_runner_probabilities_normalize(tmp_path: Path) -> None:
    from python_models.statistical.deep import training

    dataset_path = tmp_path / "dataset.parquet"
    _synthetic_dataset(dataset_path)
    spec = _spec()
    layout = _layout()
    _ = training.run_target(
        spec,
        dataset_parquet=dataset_path,
        artifact_id="synthetic-aid-norm",
        layout=layout,
        source_snapshot_id="src-test",
        artifact_root=tmp_path / "deep",
        epochs=1,
        keras_batch_size=64,
    )
    probs = pl.read_parquet(
        tmp_path / "deep" / spec.name / "synthetic-aid-norm" / "exports" / "probabilities.parquet"
    )
    sums = probs["dl_p_class"].list.sum().to_numpy()
    np.testing.assert_allclose(sums, np.ones(len(sums)), atol=1e-5)


def test_fold_runner_respects_game_group_invariant(tmp_path: Path) -> None:
    from python_models.statistical.deep import io

    dataset_path = tmp_path / "dataset.parquet"
    _synthetic_dataset(dataset_path)
    df = pl.read_parquet(dataset_path).filter(pl.col("split_partition") == "TRAIN")
    annotated = io.add_kfold_id(df, game_id_column="game_id", fold_count=3)
    io.assert_game_group_invariant(
        annotated, game_id_column="game_id", kfold_column=KFOLD_COLUMN
    )

    grouped = annotated.group_by("game_id").agg(
        pl.col(KFOLD_COLUMN).n_unique().alias("n")
    )
    assert grouped.filter(pl.col("n") > 1).height == 0


def test_fold_runner_assert_raises_on_split_game_id() -> None:
    from python_models.statistical.deep import io

    bad = pl.DataFrame(
        {
            "game_id": ["G1", "G1", "G2"],
            KFOLD_COLUMN: [0, 1, 0],
        }
    )
    with pytest.raises(ValueError, match="span multiple kfold_id"):
        io.assert_game_group_invariant(
            bad, game_id_column="game_id", kfold_column=KFOLD_COLUMN
        )


def test_fold_runner_is_idempotent_on_rerun(tmp_path: Path) -> None:
    from python_models.statistical.deep import training

    dataset_path = tmp_path / "dataset.parquet"
    _synthetic_dataset(dataset_path)
    spec = _spec()
    layout = _layout()

    first = training.run_target(
        spec,
        dataset_parquet=dataset_path,
        artifact_id="synthetic-aid-idem",
        layout=layout,
        source_snapshot_id="src-test",
        artifact_root=tmp_path / "deep",
        epochs=1,
        keras_batch_size=64,
    )
    first_mtime = first.manifest_path.stat().st_mtime_ns

    second = training.run_target(
        spec,
        dataset_parquet=dataset_path,
        artifact_id="synthetic-aid-idem",
        layout=layout,
        source_snapshot_id="src-test",
        artifact_root=tmp_path / "deep",
        epochs=1,
        keras_batch_size=64,
    )
    second_mtime = second.manifest_path.stat().st_mtime_ns
    assert first_mtime == second_mtime
    assert second.manifest_path == first.manifest_path
