from __future__ import annotations

import argparse
import hashlib
import json
import logging
import platform
import shutil
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import cast

import arviz as az
import numpy as np
import numpy.typing as npt
import polars as pl

from python_models.statistical.backtests.geometry_reference_evaluation import (
    evaluate_reference,
)
from python_models.statistical.backtests.geometry_reference_model import (
    fit_reference,
    predict_reference,
)
from python_models.statistical.evidence_binding import file_digest
from python_models.statistical.pymc_utils import SamplingConfig
from python_models.statistical.validate import (
    compute_posterior_diagnostics,
    write_json_atomic,
)

_log = logging.getLogger(__name__)
SEED = 20260911
INTERACTION_COLUMN = "era_result"
INTERACTION_FEATURES = (
    INTERACTION_COLUMN,
    "base_state_start",
    "outs_start",
    "alignment_regime",
    "batter_hand",
)
SOURCE_FILES = (
    "data/train.parquet",
    "data/test.parquet",
    "data/full_train_counts.parquet",
    "test_predictions.parquet",
)
BOUND_ADDITIVE_FILES = (
    *SOURCE_FILES,
    "data/lineage.json",
    "diagnostics.json",
    "fit_config.json",
    "posterior.nc",
    "prior_check.json",
    "sampling.log",
)
FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]
BoolArray = npt.NDArray[np.bool_]


def _write_json(path: Path, value: object) -> None:
    write_json_atomic(
        path, json.dumps(value, indent=2, sort_keys=True, allow_nan=False)
    )


def _close_idata(idata: az.InferenceData) -> None:
    for group in idata.groups():
        idata[group].close()


def select_smoke_train(train: pl.DataFrame, *, seed: int) -> pl.DataFrame:
    ranked = sorted(
        range(train.height),
        key=lambda index: (
            hashlib.sha256(
                f"{train.item(index, 'event_key')}:{seed}".encode()
            ).digest(),
            int(train.item(index, "event_key")),
        ),
    )[:3000]
    return train[ranked]


def add_interaction(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        pl.concat_str("era", "result_family", separator="|").alias(INTERACTION_COLUMN)
    )


def verify_report_artifacts(
    source_root: Path, report: dict[str, object]
) -> dict[str, str]:
    expected = cast(dict[str, str], report.get("artifact_files"))
    missing_bindings = sorted(set(BOUND_ADDITIVE_FILES) - set(expected))
    if missing_bindings:
        raise ValueError(
            f"frozen additive report lacks artifact hashes: {missing_bindings}"
        )
    verified: dict[str, str] = {}
    for relative in BOUND_ADDITIVE_FILES:
        path = source_root / relative
        actual = file_digest(path)
        if actual != expected[relative]:
            raise ValueError(f"frozen additive report digest mismatch for {relative}")
        verified[relative] = actual
    return verified


def _validate_sources(source_root: Path) -> dict[str, object]:
    lineage_path = source_root / "data" / "lineage.json"
    report_path = source_root / "report.json"
    fit_config_path = source_root / "fit_config.json"
    required = [
        *(source_root / relative for relative in SOURCE_FILES),
        lineage_path,
        report_path,
        fit_config_path,
        source_root / "posterior.nc",
        source_root / "diagnostics.json",
    ]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"frozen source files missing: {missing}")
    lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
    expected = cast(dict[str, str], lineage["snapshot_files"])
    for name in ("train.parquet", "test.parquet", "full_train_counts.parquet"):
        actual = file_digest(source_root / "data" / name)
        if actual != expected[name]:
            raise ValueError(f"frozen source digest mismatch for {name}")
    report = cast(
        dict[str, object], json.loads(report_path.read_text(encoding="utf-8"))
    )
    verified_artifacts = verify_report_artifacts(source_root, report)
    fit_config = json.loads(fit_config_path.read_text(encoding="utf-8"))
    if lineage.get("target") != "trajectory":
        raise ValueError("frozen source target is not trajectory")
    if lineage.get("uses_validate_partition") is not False:
        raise ValueError("frozen source does not establish VALIDATE exclusion")
    if lineage.get("learned_inputs") != []:
        raise ValueError("frozen source includes learned inputs")
    if fit_config.get("training_rows") != 100000:
        raise ValueError("frozen additive fit does not use 100,000 TRAIN rows")
    if report.get("class_labels") != fit_config.get("class_labels"):
        raise ValueError("frozen additive class labels disagree")
    return {
        "lineage": lineage,
        "class_labels": fit_config["class_labels"],
        "source_file_sha256": {
            str(path.relative_to(source_root)): file_digest(path) for path in required
        },
        "report_bound_artifact_sha256": verified_artifacts,
    }


def validate_alignment(test: pl.DataFrame, old: pl.DataFrame) -> None:
    identity = ("event_key", "game_id", "season", "target_class")
    if old.height != test.height:
        raise ValueError("saved additive predictions do not cover frozen TEST")
    for column in identity:
        if not old.get_column(column).equals(test.get_column(column)):
            raise ValueError(f"saved additive predictions misalign on {column}")


def _event_losses(
    probabilities: FloatArray, labels: IntArray
) -> tuple[FloatArray, FloatArray]:
    rows = np.arange(labels.size)
    log_loss = -np.log(np.clip(probabilities[rows, labels], 1e-15, 1.0))
    outcomes = np.zeros_like(probabilities)
    outcomes[rows, labels] = 1.0
    return log_loss, np.square(probabilities - outcomes).sum(axis=1)


def _matrix(frame: pl.DataFrame, column: str, class_count: int) -> FloatArray:
    matrix = np.asarray(frame.get_column(column).to_list(), dtype=np.float64)
    if matrix.shape != (frame.height, class_count):
        raise ValueError(f"{column} has invalid shape {matrix.shape}")
    if not np.isfinite(matrix).all() or (matrix < 0.0).any():
        raise ValueError(f"{column} contains invalid probabilities")
    if not np.allclose(matrix.sum(axis=1), 1.0, rtol=0.0, atol=1e-6):
        raise ValueError(f"{column} probabilities are not normalized")
    return matrix


def paired_additive_comparison(
    frame: pl.DataFrame,
    class_labels: tuple[str, ...],
    *,
    repetitions: int,
    seed: int,
) -> dict[str, object]:
    if repetitions <= 0:
        raise ValueError("repetitions must be positive")
    label_map = {label: index for index, label in enumerate(class_labels)}
    labels_raw = frame.get_column("target_class").to_list()
    unknown = sorted({str(label) for label in labels_raw if label not in label_map})
    if unknown:
        raise ValueError(f"TEST labels absent from TRAIN vocabulary: {unknown}")
    labels = np.asarray([label_map[str(label)] for label in labels_raw], dtype=np.int64)
    interaction = _matrix(frame, "interaction_probability", len(class_labels))
    additive = _matrix(frame, "additive_probability", len(class_labels))
    interaction_log, interaction_brier = _event_losses(interaction, labels)
    additive_log, additive_brier = _event_losses(additive, labels)
    games = np.asarray(frame.get_column("game_id").to_list(), dtype=np.str_)
    seasons = frame.get_column("season").cast(pl.Int64).to_numpy()
    masks: dict[str, BoolArray] = {
        "overall": np.ones(frame.height, dtype=np.bool_),
        "pre_1988_observed_only": seasons < 1988,
    }
    rng = np.random.default_rng(seed)
    results: dict[str, object] = {}
    for slice_name, mask in masks.items():
        selected_games = games[mask]
        unique_games, inverse = np.unique(selected_games, return_inverse=True)
        if unique_games.size == 0:
            results[slice_name] = {
                "row_count": 0,
                "game_count": 0,
                "interaction_log_loss": None,
                "additive_log_loss": None,
                "log_loss_gain": None,
                "log_loss_gain_ci95": None,
                "interaction_brier": None,
                "additive_brier": None,
                "brier_gain": None,
                "brier_gain_ci95": None,
            }
            continue
        rows_per_game = np.bincount(inverse).astype(np.float64)
        log_gain = additive_log[mask] - interaction_log[mask]
        brier_gain = additive_brier[mask] - interaction_brier[mask]
        game_log = np.bincount(inverse, weights=log_gain)
        game_brier = np.bincount(inverse, weights=brier_gain)
        boot_log = np.empty(repetitions, dtype=np.float64)
        boot_brier = np.empty(repetitions, dtype=np.float64)
        for repetition in range(repetitions):
            sampled = rng.integers(0, unique_games.size, size=unique_games.size)
            denominator = rows_per_game[sampled].sum()
            boot_log[repetition] = game_log[sampled].sum() / denominator
            boot_brier[repetition] = game_brier[sampled].sum() / denominator
        results[slice_name] = {
            "row_count": int(mask.sum()),
            "game_count": int(unique_games.size),
            "interaction_log_loss": float(interaction_log[mask].mean()),
            "additive_log_loss": float(additive_log[mask].mean()),
            "log_loss_gain": float(log_gain.mean()),
            "log_loss_gain_ci95": [
                float(np.quantile(boot_log, 0.025)),
                float(np.quantile(boot_log, 0.975)),
            ],
            "interaction_brier": float(interaction_brier[mask].mean()),
            "additive_brier": float(additive_brier[mask].mean()),
            "brier_gain": float(brier_gain.mean()),
            "brier_gain_ci95": [
                float(np.quantile(boot_brier, 0.025)),
                float(np.quantile(boot_brier, 0.975)),
            ],
        }
    return {
        "positive_gain_favors_interaction": True,
        "cluster_unit": "game_id",
        "repetitions": repetitions,
        "train_fit_during_bootstrap": "fixed",
        "slices": results,
    }


def _copy_sources(source_root: Path, output_dir: Path) -> None:
    for relative in SOURCE_FILES:
        destination = output_dir / "source" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_root / relative, destination)


def _archive_implementation(output_dir: Path, *, repair: bool) -> dict[str, str]:
    source_dir = Path(__file__).resolve().parent
    names = (
        "trajectory_interaction.py",
        "geometry_reference_model.py",
        "geometry_reference_evaluation.py",
    )
    archived: dict[str, str] = {}
    for name in names:
        destination = output_dir / "code" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_dir / name, destination)
        archived[f"code/{name}"] = file_digest(destination)
    if repair:
        repair_protocol = (
            Path(__file__).resolve().parents[4]
            / "docs"
            / "trajectory-interaction-repair-2026-09-11.md"
        )
        destination = output_dir / repair_protocol.name
        shutil.copy2(repair_protocol, destination)
        archived[destination.name] = file_digest(destination)
    return archived


def repair_sampling_config(seed: int) -> SamplingConfig:
    return SamplingConfig(
        draws=4000,
        tune=1000,
        chains=4,
        target_accept=0.95,
        random_seed=seed,
        cores=1,
        max_treedepth=12,
        backend="nutpie",
    )


def run(
    source_root: Path,
    output_dir: Path,
    *,
    smoke: bool,
    seed: int = SEED,
    repair: bool = False,
) -> dict[str, object]:
    if smoke and repair:
        raise ValueError("sampling repair cannot run in smoke mode")
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite {output_dir}")
    source_record = _validate_sources(source_root)
    output_dir.mkdir(parents=True)
    _copy_sources(source_root, output_dir)
    archived_implementation = _archive_implementation(output_dir, repair=repair)
    repository_root = Path(__file__).resolve().parents[4]
    protocol_path = repository_root / "docs" / "trajectory-interaction-protocol.md"
    shutil.copy2(protocol_path, output_dir / "trajectory-interaction-protocol.md")
    train = pl.read_parquet(source_root / "data" / "train.parquet")
    test = pl.read_parquet(source_root / "data" / "test.parquet")
    old = pl.read_parquet(source_root / "test_predictions.parquet")
    validate_alignment(test, old)
    if smoke:
        train = select_smoke_train(train, seed=seed)
        test = test.head(5000)
        old = old.head(5000)
    train = add_interaction(train)
    test = add_interaction(test)
    run_config: dict[str, object] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": (
            "sampling_repair"
            if repair
            else "smoke"
            if smoke
            else "development_evidence"
        ),
        "seed": seed,
        "smoke": smoke,
        "training_rows": train.height,
        "test_rows": test.height,
        "feature_columns": list(INTERACTION_FEATURES),
        "source_root": str(source_root.resolve()),
        "source_record": source_record,
        "protocol_sha256": file_digest(protocol_path),
        "packages": {
            name: version(name)
            for name in ("numpy", "polars", "pymc", "arviz", "nutpie")
        },
        "python": platform.python_version(),
        "implementation_sha256": {
            name: file_digest(Path(__file__).with_name(name))
            for name in (
                "trajectory_interaction.py",
                "geometry_reference_model.py",
                "geometry_reference_evaluation.py",
            )
        },
        "archived_implementation_sha256": archived_implementation,
        "sampling_repair": (
            {
                "predecessor": str((output_dir.parent / "20260911-full-v1").resolve()),
                "predecessor_numerical_pass": False,
                "predecessor_original_code_archive_complete": False,
                "predecessor_limitation": "the first full run stored pre-fit implementation hashes but did not copy its exact code into the artifact",
                "reason": "slow mixing in the correlated alpha_class and missing batter-hand direction with otherwise healthy NUTS geometry",
                "only_change": "saved draws per chain increased from 1000 to 4000",
            }
            if repair
            else None
        ),
    }
    _write_json(output_dir / "run_config.json", run_config)
    idata, levels, labels = fit_reference(
        train,
        output_dir=output_dir / "model",
        smoke=smoke,
        seed=seed,
        feature_columns=INTERACTION_FEATURES,
        sampling_config=repair_sampling_config(seed) if repair else None,
    )
    frozen_labels = tuple(cast(list[str], source_record["class_labels"]))
    if labels != frozen_labels:
        _close_idata(idata)
        raise ValueError("interaction and frozen additive class labels disagree")
    diagnostics = compute_posterior_diagnostics(idata)
    _write_json(output_dir / "diagnostics.json", diagnostics.model_dump())
    probabilities = predict_reference(
        idata,
        test,
        levels,
        labels,
        feature_columns=INTERACTION_FEATURES,
    )
    predictions = old.rename(
        {"model_probability": "additive_probability"}
    ).with_columns(pl.Series("interaction_probability", probabilities))
    predictions.write_parquet(output_dir / "test_predictions.parquet")
    evaluation_frame = predictions.rename(
        {"interaction_probability": "model_probability"}
    )
    evaluation = evaluate_reference(
        evaluation_frame,
        labels,
        repetitions=20 if smoke else 500,
        seed=seed,
    )
    paired = paired_additive_comparison(
        predictions,
        labels,
        repetitions=20 if smoke else 500,
        seed=seed,
    )
    numerical_pass = (
        diagnostics.rhat_max <= 1.05
        and min(diagnostics.ess_bulk_min, diagnostics.ess_tail_min) >= 100
        and diagnostics.divergences == 0
    )
    report: dict[str, object] = {
        "status": (
            "sampling_repair"
            if repair
            else "smoke"
            if smoke
            else "development_evidence"
        ),
        "research_only": True,
        "class_labels": list(labels),
        "feature_levels": {name: list(values) for name, values in levels.items()},
        "unseen_test_feature_rows": {
            name: test.filter(~pl.col(name).is_in(values)).height
            for name, values in levels.items()
        },
        "numerical_pass": numerical_pass,
        "diagnostics": diagnostics.model_dump(),
        "evaluation_against_baselines": evaluation,
        "paired_interaction_vs_additive": paired,
        "interpretation_scope": "recorded TEST trajectory events in the frozen geometry 0.4 snapshot",
        "artifact_files": {
            str(path.relative_to(output_dir)): file_digest(path)
            for path in output_dir.rglob("*")
            if path.is_file()
        },
    }
    _write_json(output_dir / "report.json", report)
    _close_idata(idata)
    _log.info(
        "completed trajectory interaction rows=%d test=%d numerical_pass=%s",
        train.height,
        test.height,
        numerical_pass,
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--sampling-repair", action="store_true")
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    output_dir = cast(Path, args.output_dir).resolve()
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(output_dir.parent / f"{output_dir.name}.log"),
        ],
    )
    run(
        cast(Path, args.source_root).resolve(),
        output_dir,
        smoke=bool(args.smoke),
        seed=int(args.seed),
        repair=bool(args.sampling_repair),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
