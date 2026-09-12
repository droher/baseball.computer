from __future__ import annotations

import argparse
import hashlib
import json
import logging
import platform
from importlib.metadata import version
from pathlib import Path
from typing import cast

import numpy as np
import polars as pl

from python_models.statistical.backtests.geometry_air_development import (
    PREDICTORS,
    PRIMARY_STRENGTH,
    SENSITIVITY_STRENGTHS,
    SplitFamily,
    development_folds,
    eligible_rows,
    evaluate_oof_predictions,
    metadata_smoke_games,
)
from python_models.statistical.backtests.geometry_air_regime import (
    AirRegimeFit,
    AirRegimePrediction,
    fit_air_regime,
    predict_air_regime,
)
from python_models.statistical.backtests.geometry_air_regime_predictive import (
    predictive_counts,
)
from python_models.statistical.backtests.geometry_air_translation import Predictor


EXPERIMENT_ID = "geometry-air-regime-posterior-v1"
TARGET_CONTRACT = "trajectory-air-standard-v1"
SOURCE_FRAME_SHA256 = "cfdf2918180812f6b1333a76a2875f87cd75b581fab07c35bb23dd648c4e3e70"
SOURCE_ROWS = 19_436
SOURCE_GAMES = 373
SOURCE_ELIGIBLE_ROWS = 9_869
PRIMARY_CONCENTRATION = PRIMARY_STRENGTH
FULL_CONCENTRATIONS = (
    SENSITIVITY_STRENGTHS[0],
    PRIMARY_CONCENTRATION,
    SENSITIVITY_STRENGTHS[1],
)
FULL_DRAWS = 4_096
SMOKE_DRAWS = 512
FULL_BOOTSTRAP_REPETITIONS = 500
SMOKE_BOOTSTRAP_REPETITIONS = 20
ROOT_SEED = 20260911
SPLIT_FAMILIES: tuple[SplitFamily, ...] = ("game", "park", "season")
SMOKE_STATUS = "smoke_complete"
FULL_STATUS = "development_scoring_complete"
REDUCED_SCALE_STATUS = "reduced_scale_complete"
FAILED_STATUS = "failed"
SOURCE_DIRECTORY = "source"
SOURCE_MODULES = (
    "geometry_air_development.py",
    "geometry_air_dirichlet.py",
    "geometry_air_regime.py",
    "geometry_air_regime_development.py",
    "geometry_air_regime_predictive.py",
    "geometry_air_regime_report.py",
    "geometry_air_standard_data.py",
    "geometry_air_translation.py",
    "geometry_statcast_targets.py",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def declared_source_paths(
    modules: tuple[str, ...] = SOURCE_MODULES,
) -> dict[str, Path]:
    module_root = Path(__file__).resolve().parent
    package_root = module_root.parents[2]
    paths = {
        str((module_root / name).relative_to(package_root)): module_root / name
        for name in modules
    }
    missing = [name for name, path in paths.items() if not path.is_file()]
    if missing:
        raise RuntimeError(f"declared source modules are missing: {missing}")
    return paths


def runtime_versions() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "numpy": np.__version__,
        "polars": pl.__version__,
        "scipy": version("scipy"),
        "pydantic": version("pydantic"),
    }


def load_bound_coverage_frame(
    source_frame: Path,
    *,
    expected_sha256: str = SOURCE_FRAME_SHA256,
    expected_rows: int = SOURCE_ROWS,
    expected_games: int = SOURCE_GAMES,
    expected_eligible_rows: int = SOURCE_ELIGIBLE_ROWS,
) -> pl.DataFrame:
    if len(expected_sha256) != 64 or sha256_file(source_frame) != expected_sha256:
        raise ValueError("source frame SHA-256 does not match the frozen binding")
    frame = pl.read_parquet(source_frame)
    required = {
        "event_key",
        "game_id",
        "season",
        "park_id",
        "game_header_scorer_proxy",
        "result_family",
        "recorded_air_subtype",
        "recorded_broad_type",
        "target_class",
        "target_status",
        "recorded_bunt",
        "known_air_evaluation_eligible",
        "primary_fold",
        "acquisition_fold",
    }
    if not required <= set(frame.columns):
        raise ValueError("source frame is missing required development columns")
    if (
        frame.height != expected_rows
        or frame["game_id"].n_unique() != expected_games
        or frame["event_key"].null_count()
        or frame["event_key"].n_unique() != expected_rows
        or int(frame["known_air_evaluation_eligible"].sum()) != expected_eligible_rows
    ):
        raise ValueError("source frame population differs from the frozen contract")
    if set(frame["primary_fold"].to_list()) != {"TRAIN"}:
        raise ValueError("source frame must contain only PRIMARY TRAIN games")
    if set(frame["acquisition_fold"].to_list()) != {"fitting"}:
        raise ValueError("source frame must contain only fitting games")
    return frame.sort("game_id", "event_key")


def fit_identity(
    concentration: float, split_family: str, fold: str, predictor: Predictor
) -> str:
    payload = json.dumps(
        [EXPERIMENT_ID, concentration, split_family, fold, predictor],
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _prediction_probabilities(prediction: AirRegimePrediction) -> list[list[float]]:
    values = cast(
        list[list[float] | None], prediction.events["probabilities"].to_list()
    )
    if any(value is None for value in values):
        raise ValueError("eligible evaluation row has unsupported probabilities")
    four_class = np.asarray(values, dtype=np.float64)
    if (
        four_class.shape != (prediction.events.height, 4)
        or not np.equal(four_class[:, 0], 0.0).all()
        or not np.isfinite(four_class).all()
        or (four_class < 0).any()
        or not np.allclose(four_class.sum(axis=1), 1.0, rtol=0.0, atol=1e-12)
    ):
        raise ValueError("eligible prediction is not a normalized airborne vector")
    return cast(list[list[float]], four_class[:, 1:].tolist())


def _assert_prediction_masks(
    fits: dict[Predictor, AirRegimeFit],
    predictions: dict[Predictor, AirRegimePrediction],
    evaluation: pl.DataFrame,
) -> None:
    masked_fine = evaluation.with_columns(
        pl.lit(None, dtype=pl.String).alias("recorded_air_subtype")
    )
    for predictor in PREDICTORS:
        original = predictions[predictor]
        masked = predict_air_regime(fits[predictor], masked_fine)
        if predictor in {"recorded", "recorded_result"}:
            if (
                set(masked.events["prediction_status"].to_list())
                != {"fine_label_missing_unsupported"}
                or masked.events["probabilities"].null_count() != masked.events.height
            ):
                raise ValueError(
                    "fine-label arm did not remain unsupported when masked"
                )
        elif not masked.events.equals(original.events):
            raise ValueError("fine-label masking changed an arm that does not use it")
        source_blocked = evaluation.with_columns(
            pl.lit(None, dtype=pl.String).alias("recorded_broad_type")
        )
        blocked = predict_air_regime(fits[predictor], source_blocked)
        if (
            set(blocked.events["prediction_status"].to_list())
            != {"broad_unknown_unsupported"}
            or blocked.events["probabilities"].null_count() != blocked.events.height
        ):
            raise ValueError("source-blocked prediction did not remain unsupported")


def _assert_ground_preservation(fit: AirRegimeFit, evaluation_all: pl.DataFrame) -> int:
    ground = evaluation_all.filter(pl.col("recorded_broad_type") == "Ground")
    if ground.is_empty():
        return 0
    prediction = predict_air_regime(fit, ground).events
    if (
        set(prediction["prediction_status"].to_list()) != {"ground_preserved"}
        or prediction["probabilities"].to_list()
        != [[1.0, 0.0, 0.0, 0.0]] * ground.height
    ):
        raise ValueError("ground preservation invariant failed")
    return ground.height


def _checkpoint_fit(
    root: Path,
    fit: AirRegimeFit,
    predictive: pl.DataFrame,
    *,
    split_family: str,
    fold: str,
) -> None:
    path = (
        root
        / f"concentration-{fit.concentration:g}"
        / f"arm-{fit.predictor}"
        / split_family
        / f"fold-{fold}"
    )
    path.mkdir(parents=True, exist_ok=True)
    _ = (path / "fit.json").write_text(
        fit.model_dump_json(indent=2) + "\n", encoding="utf-8"
    )
    predictive.with_columns(
        pl.lit(split_family).alias("split_family"),
        pl.lit(fold).alias("fold"),
        pl.lit(fit.concentration).alias("prior_strength"),
    ).write_parquet(path / "predictive-counts.parquet")


def build_oof_predictions(
    frame: pl.DataFrame,
    *,
    concentrations: tuple[float, ...],
    draws: int,
    checkpoint_root: Path | None,
    logger: logging.Logger | None,
) -> tuple[pl.DataFrame, list[dict[str, object]]]:
    eligible_keys = set(eligible_rows(frame)["event_key"].to_list())
    outputs: list[pl.DataFrame] = []
    fit_records: list[dict[str, object]] = []
    for concentration in concentrations:
        for split_family in SPLIT_FAMILIES:
            family_keys: list[int] = []
            for fold, training_all, evaluation_all in development_folds(
                frame, split_family
            ):
                training_games = set(training_all["game_id"].to_list())
                evaluation_games = set(evaluation_all["game_id"].to_list())
                if training_games & evaluation_games:
                    raise ValueError("training and evaluation games overlap")
                training = eligible_rows(training_all)
                evaluation = eligible_rows(evaluation_all)
                fits: dict[Predictor, AirRegimeFit] = {}
                predictions: dict[Predictor, AirRegimePrediction] = {}
                for predictor in PREDICTORS:
                    fit_id = fit_identity(concentration, split_family, fold, predictor)
                    fit = fit_air_regime(
                        training,
                        predictor=predictor,
                        fit_id=fit_id,
                        concentration=concentration,
                        draws=draws,
                        seed=ROOT_SEED,
                    )
                    if set(fit.represented_regimes) != {"early", "late"}:
                        raise ValueError("OOF training does not represent both regimes")
                    prediction = predict_air_regime(fit, evaluation)
                    statuses = set(prediction.events["prediction_status"].to_list())
                    if not statuses <= {"posterior_cell", "prior_only_cell"}:
                        raise ValueError(
                            "eligible evaluation contains unsupported rows"
                        )
                    ground_events = _assert_ground_preservation(fit, evaluation_all)
                    counts = predictive_counts(fit, prediction, evaluation)
                    if checkpoint_root is not None:
                        _checkpoint_fit(
                            checkpoint_root,
                            fit,
                            counts,
                            split_family=split_family,
                            fold=fold,
                        )
                    fits[predictor] = fit
                    predictions[predictor] = prediction
                    fit_records.append(
                        {
                            "fit_id": fit.fit_id,
                            "predictor": predictor,
                            "concentration": concentration,
                            "split_family": split_family,
                            "fold": fold,
                            "training_events": training.height,
                            "evaluation_events": evaluation.height,
                            "ground_events_verified": ground_events,
                            "training_game_ids": sorted(training_games),
                            "evaluation_game_ids": sorted(evaluation_games),
                            "prediction_status_counts": prediction.events.group_by(
                                "prediction_status"
                            )
                            .len()
                            .sort("prediction_status")
                            .to_dicts(),
                            "fit": fit.model_dump(mode="json"),
                        }
                    )
                _assert_prediction_masks(fits, predictions, evaluation)
                base = evaluation.select(
                    "event_key",
                    "game_id",
                    "season",
                    "park_id",
                    "game_header_scorer_proxy",
                    "result_family",
                    "recorded_air_subtype",
                    "target_class",
                ).with_columns(
                    pl.lit(split_family).alias("split_family"),
                    pl.lit(fold).alias("fold"),
                    pl.lit(concentration).alias("prior_strength"),
                )
                prediction_frame = base.with_columns(
                    *[
                        pl.Series(
                            f"probability_{predictor}",
                            _prediction_probabilities(predictions[predictor]),
                            dtype=pl.List(pl.Float64),
                        )
                        for predictor in PREDICTORS
                    ],
                    *[
                        pl.lit(fits[predictor].fit_id).alias(f"fit_id_{predictor}")
                        for predictor in PREDICTORS
                    ],
                    *[
                        predictions[predictor]
                        .events["prediction_status"]
                        .alias(f"status_{predictor}")
                        for predictor in PREDICTORS
                    ],
                )
                outputs.append(prediction_frame)
                family_keys.extend(cast(list[int], evaluation["event_key"].to_list()))
                if logger is not None:
                    logger.info(
                        "completed concentration=%s family=%s fold=%s training=%s evaluation=%s",
                        concentration,
                        split_family,
                        fold,
                        training.height,
                        evaluation.height,
                    )
            if (
                len(family_keys) != len(eligible_keys)
                or set(family_keys) != eligible_keys
            ):
                raise ValueError(
                    f"{split_family} OOF population does not cover each eligible event once"
                )
    return pl.concat(outputs, how="vertical"), fit_records


def write_manifest(
    output_root: Path,
    status: str,
    *,
    experiment: str = EXPERIMENT_ID,
    **extra: object,
) -> None:
    files = {
        str(path.relative_to(output_root)): sha256_file(path)
        for path in sorted(output_root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    }
    _ = (output_root / "manifest.json").write_text(
        json.dumps(
            {
                "experiment": experiment,
                "status": status,
                "files_sha256": files,
                **extra,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _frame_counts(frame: pl.DataFrame) -> dict[str, int]:
    return {
        "rows": frame.height,
        "games": frame["game_id"].n_unique(),
        "eligible_air_rows": int(frame["known_air_evaluation_eligible"].sum()),
    }


def run_status(
    *,
    smoke: bool,
    source_frame_sha256: str,
    concentrations: tuple[float, ...],
    draws: int,
    repetitions: int,
) -> str:
    if smoke:
        return SMOKE_STATUS
    protocol_scale = (
        source_frame_sha256 == SOURCE_FRAME_SHA256
        and tuple(concentrations) == FULL_CONCENTRATIONS
        and draws == FULL_DRAWS
        and repetitions == FULL_BOOTSTRAP_REPETITIONS
    )
    return FULL_STATUS if protocol_scale else REDUCED_SCALE_STATUS


def run_experiment(
    frame: pl.DataFrame,
    *,
    source_frame: Path,
    source_frame_sha256: str,
    output_root: Path,
    protocol_path: Path,
    smoke: bool,
    concentrations: tuple[float, ...],
    draws: int,
    repetitions: int,
    smoke_games: list[str] | None = None,
) -> None:
    if output_root.exists():
        raise ValueError("output root must not already exist")
    if sha256_file(source_frame) != source_frame_sha256:
        raise ValueError("source frame does not match its declared SHA-256")
    if not frame.equals(pl.read_parquet(source_frame).sort("game_id", "event_key")):
        raise ValueError("frame does not match the sorted content of the source file")
    source_paths = {protocol_path.name: protocol_path} | declared_source_paths()
    source_snapshots = {name: path.read_bytes() for name, path in source_paths.items()}
    source_hashes = {
        name: hashlib.sha256(content).hexdigest()
        for name, content in source_snapshots.items()
    }
    analysis_frame = frame
    if not smoke:
        if smoke_games is not None:
            raise ValueError("smoke games are only meaningful for a smoke run")
        smoke_games = []
    else:
        if smoke_games is None:
            smoke_games = metadata_smoke_games(frame)
        known_games = set(frame["game_id"].to_list())
        if not smoke_games or not set(smoke_games) <= known_games:
            raise ValueError("smoke games must be a nonempty subset of the frame")
        smoke_games = sorted(smoke_games)
        analysis_frame = frame.filter(pl.col("game_id").is_in(smoke_games))
    status = run_status(
        smoke=smoke,
        source_frame_sha256=source_frame_sha256,
        concentrations=concentrations,
        draws=draws,
        repetitions=repetitions,
    )
    output_root.mkdir(parents=True)
    logger = logging.getLogger(__name__)
    previous_level = logger.level
    handler = logging.FileHandler(output_root / "run.log")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    try:
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        bindings = {
            "experiment": EXPERIMENT_ID,
            "target_contract": TARGET_CONTRACT,
            "status": "bound_before_scoring",
            "expected_status": status,
            "source_frame": str(source_frame),
            "source_frame_sha256": source_frame_sha256,
            "source_hashes": source_hashes,
            "root_seed": ROOT_SEED,
            "concentrations": concentrations,
            "draws": draws,
            "bootstrap_repetitions": repetitions,
            "smoke": smoke,
            "smoke_game_ids": smoke_games,
            "runtime": runtime_versions(),
        }
        _ = (output_root / "bindings.json").write_text(
            json.dumps(bindings, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        logger.info(
            "starting status=%s concentrations=%s draws=%s repetitions=%s games=%s",
            status,
            concentrations,
            draws,
            repetitions,
            analysis_frame["game_id"].n_unique(),
        )
        predictions, fits = build_oof_predictions(
            analysis_frame,
            concentrations=concentrations,
            draws=draws,
            checkpoint_root=output_root / "checkpoints",
            logger=logger,
        )
        evaluation, metrics, game_scores, descriptive = evaluate_oof_predictions(
            predictions,
            repetitions=repetitions,
            seed=ROOT_SEED,
        )
        analysis_frame.write_parquet(output_root / "coverage_frame.parquet")
        predictions.write_parquet(output_root / "oof_predictions.parquet")
        metrics.write_parquet(output_root / "metrics.parquet")
        game_scores.write_parquet(output_root / "per_game_scores.parquet")
        descriptive.write_parquet(output_root / "descriptive_group_metrics.parquet")
        _ = (output_root / "fits.json").write_text(
            json.dumps(fits, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        _ = (output_root / "report.json").write_text(
            json.dumps(
                {
                    "experiment": EXPERIMENT_ID,
                    "status": "development_scored_pending_report"
                    if status == FULL_STATUS
                    else "operational_no_acceptance",
                    "run_status": status,
                    "smoke": smoke,
                    "smoke_game_ids": smoke_games,
                    "full_frame_counts": _frame_counts(frame),
                    "analysis_frame_counts": _frame_counts(analysis_frame),
                    "evaluation": evaluation,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        changed = [
            name
            for name, path in source_paths.items()
            if sha256_file(path) != source_hashes[name]
        ]
        if sha256_file(source_frame) != source_frame_sha256:
            changed.append(source_frame.name)
        if changed:
            raise ValueError(f"bound source changed during run: {changed}")
        for name, content in source_snapshots.items():
            target = output_root / SOURCE_DIRECTORY / name
            target.parent.mkdir(parents=True, exist_ok=True)
            _ = target.write_bytes(content)
        logger.info("completed artifact=%s status=%s", output_root, status)
        handler.flush()
        write_manifest(output_root, status)
    except Exception:
        logger.exception("regime development run failed")
        handler.flush()
        write_manifest(output_root, FAILED_STATUS)
        raise
    finally:
        logger.removeHandler(handler)
        handler.close()
        logger.setLevel(previous_level)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--source-frame", type=Path, required=True)
    _ = parser.add_argument("--output-root", type=Path, required=True)
    _ = parser.add_argument(
        "--protocol-path",
        type=Path,
        default=Path("docs/geometry-air-regime-posterior-protocol.md"),
    )
    _ = parser.add_argument("--smoke", action="store_true")
    return parser


def main() -> None:
    args = _parser().parse_args()
    source_frame = cast(Path, args.source_frame)
    output_root = cast(Path, args.output_root)
    protocol_path = cast(Path, args.protocol_path)
    smoke = bool(args.smoke)
    if output_root.exists():
        raise ValueError("output root must not already exist")
    frame = load_bound_coverage_frame(source_frame)
    run_experiment(
        frame,
        source_frame=source_frame,
        source_frame_sha256=SOURCE_FRAME_SHA256,
        output_root=output_root,
        protocol_path=protocol_path,
        smoke=smoke,
        concentrations=(PRIMARY_CONCENTRATION,) if smoke else FULL_CONCENTRATIONS,
        draws=SMOKE_DRAWS if smoke else FULL_DRAWS,
        repetitions=SMOKE_BOOTSTRAP_REPETITIONS
        if smoke
        else FULL_BOOTSTRAP_REPETITIONS,
    )


if __name__ == "__main__":
    main()
