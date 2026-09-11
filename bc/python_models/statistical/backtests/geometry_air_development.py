from __future__ import annotations

import argparse
import hashlib
import json
import logging
import platform
from collections.abc import Iterable
from pathlib import Path
from typing import Literal, cast

import numpy as np
import numpy.typing as npt
import polars as pl

from python_models.statistical.backtests.geometry_air_standard_data import (
    DEVELOPMENT_SEASONS,
    FOLD_CONTRACT,
    load_development_frame,
    split_development_fold,
    split_leave_one_season_out,
)
from python_models.statistical.backtests.geometry_air_translation import (
    AIR_CLASSES,
    AirTranslationFit,
    fit_air_translation,
    predict_air_translation,
    predict_preserving_broad_type,
)

FloatArray = npt.NDArray[np.float64]
IntArray = npt.NDArray[np.int64]
SplitFamily = Literal["game", "park", "season"]
Predictor = Literal["marginal", "result", "recorded", "recorded_result"]
PREDICTORS: tuple[Predictor, ...] = (
    "marginal",
    "result",
    "recorded",
    "recorded_result",
)
PRIMARY_STRENGTH = 30.0
SENSITIVITY_STRENGTHS = (3.0, 300.0)
SUPPORTED_SEASON_GAMES = 30
SUPPORTED_SEASON_EVENTS = 500
FULL_BOOTSTRAP_REPETITIONS = 500
FULL_BOOTSTRAP_SEED = 20260911
SMOKE_BOOTSTRAP_REPETITIONS = 20
SMOKE_GAMES_PER_SEASON = 3


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def metadata_smoke_games(frame: pl.DataFrame) -> list[str]:
    games = frame.select("game_id", "season").unique()
    selected: list[str] = []
    for season in sorted(DEVELOPMENT_SEASONS):
        season_games = cast(
            list[str],
            games.filter(pl.col("season") == season)["game_id"].to_list(),
        )
        ordered = sorted(
            season_games,
            key=lambda game_id: (
                hashlib.sha256(f"{FOLD_CONTRACT}:smoke:{game_id}".encode()).hexdigest(),
                game_id,
            ),
        )
        if len(ordered) < SMOKE_GAMES_PER_SEASON:
            raise ValueError(f"insufficient smoke games for {season}")
        selected.extend(ordered[:SMOKE_GAMES_PER_SEASON])
    return sorted(selected)


def eligible_rows(frame: pl.DataFrame) -> pl.DataFrame:
    eligible = frame.filter(pl.col("known_air_evaluation_eligible"))
    if eligible.is_empty() or eligible["target_class"].null_count():
        raise ValueError("eligible airborne frame is empty or has null targets")
    return eligible


def development_folds(
    frame: pl.DataFrame, family: SplitFamily
) -> Iterable[tuple[str, pl.DataFrame, pl.DataFrame]]:
    if family == "game" or family == "park":
        for heldout in range(5):
            training, evaluation = split_development_fold(
                frame, unit=family, heldout_fold=heldout
            )
            yield str(heldout), training, evaluation
        return
    for season in sorted(DEVELOPMENT_SEASONS):
        training, evaluation = split_leave_one_season_out(frame, heldout_season=season)
        yield str(season), training, evaluation


def _masked_information_checks(
    fit: AirTranslationFit, evaluation: pl.DataFrame
) -> None:
    masked_fine = evaluation.with_columns(
        pl.lit(None, dtype=pl.String).alias("recorded_air_subtype")
    )
    candidate = predict_air_translation(fit, masked_fine, predictor="recorded_result")
    result_reference = predict_air_translation(fit, masked_fine, predictor="result")
    if not np.array_equal(candidate, result_reference):
        raise ValueError("masked fine-label candidate differs from result reference")
    source_blocked = evaluation.with_columns(
        pl.lit(None, dtype=pl.String).alias("recorded_air_subtype"),
        pl.lit(None, dtype=pl.String).alias("recorded_broad_type"),
    )
    unsupported = predict_preserving_broad_type(fit, source_blocked)
    if (
        set(unsupported["prediction_status"].to_list()) != {"broad_unknown_unsupported"}
        or unsupported["probabilities"].null_count() != unsupported.height
    ):
        raise ValueError("source-blocked rows must remain unsupported")


def _ground_preservation_check(fit: AirTranslationFit, evaluation: pl.DataFrame) -> int:
    ground = evaluation.filter(pl.col("recorded_broad_type") == "Ground")
    if ground.is_empty():
        return 0
    predictions = predict_preserving_broad_type(fit, ground)
    if set(predictions["prediction_status"].to_list()) != {"ground_preserved"}:
        raise ValueError("ground preservation status is invalid")
    expected = [[1.0, 0.0, 0.0, 0.0]] * ground.height
    if predictions["probabilities"].to_list() != expected:
        raise ValueError("ground preservation probabilities are invalid")
    return ground.height


def build_oof_predictions(
    frame: pl.DataFrame,
    *,
    prior_strengths: tuple[float, ...],
    checkpoint_root: Path | None = None,
    logger: logging.Logger | None = None,
) -> tuple[pl.DataFrame, list[dict[str, object]]]:
    eligible_keys = set(eligible_rows(frame)["event_key"].to_list())
    outputs: list[pl.DataFrame] = []
    fit_records: list[dict[str, object]] = []
    for strength in prior_strengths:
        for family in cast(tuple[SplitFamily, ...], ("game", "park", "season")):
            family_keys: list[int] = []
            for fold, training_all, evaluation_all in development_folds(frame, family):
                training = eligible_rows(training_all)
                evaluation = evaluation_all.filter(
                    pl.col("known_air_evaluation_eligible")
                )
                if evaluation.is_empty():
                    raise ValueError(
                        f"{family} fold {fold} has no eligible evaluation events"
                    )
                if evaluation["target_class"].null_count():
                    raise ValueError(
                        f"{family} fold {fold} has null eligible evaluation targets"
                    )
                fit = fit_air_translation(training, prior_strength=strength)
                _masked_information_checks(fit, evaluation)
                ground_events_verified = _ground_preservation_check(fit, evaluation_all)
                probabilities = {
                    predictor: predict_air_translation(
                        fit, evaluation, predictor=predictor
                    )
                    for predictor in PREDICTORS
                }
                prediction_frame = evaluation.select(
                    "event_key",
                    "game_id",
                    "season",
                    "park_id",
                    "game_header_scorer_proxy",
                    "result_family",
                    "recorded_air_subtype",
                    "target_class",
                ).with_columns(
                    pl.lit(family).alias("split_family"),
                    pl.lit(fold).alias("fold"),
                    pl.lit(strength).alias("prior_strength"),
                    *[
                        pl.Series(
                            f"probability_{predictor}",
                            matrix.tolist(),
                            dtype=pl.List(pl.Float64),
                        )
                        for predictor, matrix in probabilities.items()
                    ],
                )
                outputs.append(prediction_frame)
                evaluation_keys = cast(list[int], evaluation["event_key"].to_list())
                family_keys.extend(evaluation_keys)
                record: dict[str, object] = {
                    "split_family": family,
                    "fold": fold,
                    "prior_strength": strength,
                    "training_events": training.height,
                    "evaluation_events": evaluation.height,
                    "ground_events_verified": ground_events_verified,
                    "masked_fine_events_verified": evaluation.height,
                    "source_blocked_events_verified": evaluation.height,
                    "training_game_ids": sorted(
                        cast(list[str], training["game_id"].unique().to_list())
                    ),
                    "evaluation_game_ids": sorted(
                        cast(list[str], evaluation["game_id"].unique().to_list())
                    ),
                    "fit": fit.model_dump(mode="json"),
                }
                fit_records.append(record)
                if checkpoint_root is not None:
                    checkpoint_path = (
                        checkpoint_root
                        / f"strength-{strength:g}"
                        / family
                        / f"fold-{fold}.json"
                    )
                    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
                    _ = checkpoint_path.write_text(
                        json.dumps(record, indent=2, sort_keys=True) + "\n"
                    )
                if logger is not None:
                    logger.info(
                        "completed strength=%s family=%s fold=%s train=%s evaluation=%s",
                        strength,
                        family,
                        fold,
                        training.height,
                        evaluation.height,
                    )
            if (
                len(family_keys) != len(eligible_keys)
                or set(family_keys) != eligible_keys
            ):
                raise ValueError(
                    f"{family} out-of-fold population does not cover each eligible event once"
                )
    return pl.concat(outputs, how="vertical"), fit_records


def _probabilities(frame: pl.DataFrame, predictor: Predictor) -> FloatArray:
    values = frame[f"probability_{predictor}"].to_list()
    matrix = np.asarray(values, dtype=np.float64)
    if matrix.shape != (frame.height, len(AIR_CLASSES)):
        raise ValueError("probability matrix has an invalid shape")
    if (
        not np.isfinite(matrix).all()
        or (matrix < 0).any()
        or not np.allclose(matrix.sum(axis=1), 1.0, atol=1e-12, rtol=0.0)
    ):
        raise ValueError("probability matrix is invalid")
    return matrix


def _labels(frame: pl.DataFrame) -> IntArray:
    labels = cast(list[str], frame["target_class"].to_list())
    if not set(labels) <= set(AIR_CLASSES):
        raise ValueError("evaluation target is outside the airborne classes")
    return np.asarray([AIR_CLASSES.index(label) for label in labels], dtype=np.int64)


def _event_scores(
    probabilities: FloatArray, labels: IntArray
) -> tuple[FloatArray, FloatArray]:
    rows = np.arange(labels.size)
    log_loss = -np.log(np.clip(probabilities[rows, labels], 1e-15, 1.0))
    outcomes = np.zeros_like(probabilities)
    outcomes[rows, labels] = 1.0
    return log_loss, np.square(probabilities - outcomes).sum(axis=1)


def _ece(probabilities: FloatArray, labels: IntArray) -> float:
    weighted_gap = 0.0
    for index in range(len(AIR_CLASSES)):
        predicted = probabilities[:, index]
        observed = (labels == index).astype(np.float64)
        bins = np.minimum((predicted * 15).astype(np.int64), 14)
        counts = np.bincount(bins, minlength=15)
        predicted_sums = np.bincount(bins, weights=predicted, minlength=15)
        observed_sums = np.bincount(bins, weights=observed, minlength=15)
        populated = counts > 0
        weighted_gap += float(
            np.sum(
                counts[populated]
                * np.abs(
                    predicted_sums[populated] / counts[populated]
                    - observed_sums[populated] / counts[populated]
                )
            )
        )
    return weighted_gap / (probabilities.shape[0] * probabilities.shape[1])


def _metric_row(
    frame: pl.DataFrame,
    predictor: Predictor,
    *,
    slice_type: str,
    slice_value: str,
) -> dict[str, object]:
    labels = _labels(frame)
    probabilities = _probabilities(frame, predictor)
    log_loss, brier = _event_scores(probabilities, labels)
    observed = np.bincount(labels, minlength=len(AIR_CLASSES)) / labels.size
    predicted = probabilities.mean(axis=0)
    return {
        "split_family": str(frame["split_family"].item(0)),
        "prior_strength": float(frame["prior_strength"].item(0)),
        "slice_type": slice_type,
        "slice_value": slice_value,
        "predictor": predictor,
        "events": frame.height,
        "games": frame["game_id"].n_unique(),
        "log_loss": float(log_loss.mean()),
        "brier": float(brier.mean()),
        "classwise_ece_15_bin": _ece(probabilities, labels),
        **{
            f"class_share_bias_{label}": float(predicted[index] - observed[index])
            for index, label in enumerate(AIR_CLASSES)
        },
        "max_absolute_class_share_bias": float(np.max(np.abs(predicted - observed))),
    }


def _per_game_scores(frame: pl.DataFrame) -> pl.DataFrame:
    labels = _labels(frame)
    columns: list[pl.Series] = []
    for predictor in PREDICTORS:
        log_loss, brier = _event_scores(_probabilities(frame, predictor), labels)
        columns.extend(
            [
                pl.Series(f"log_loss_{predictor}", log_loss),
                pl.Series(f"brier_{predictor}", brier),
            ]
        )
    scored = (
        frame.select(
            "event_key",
            "game_id",
            "season",
            "split_family",
            "prior_strength",
            "target_class",
            *[f"probability_{predictor}" for predictor in PREDICTORS],
        )
        .with_columns(*columns)
        .sort("prior_strength", "split_family", "game_id", "event_key")
    )
    return scored.group_by("game_id", "season", "split_family", "prior_strength").agg(
        pl.len().alias("events"),
        *[
            pl.col(f"{metric}_{predictor}").sum()
            for predictor in PREDICTORS
            for metric in ("log_loss", "brier")
        ],
        *[
            pl.col("target_class").eq(label).sum().alias(f"observed_{label}")
            for label in AIR_CLASSES
        ],
        *[
            pl.col(f"probability_{predictor}")
            .list.get(index)
            .sum()
            .alias(f"predicted_{predictor}_{label}")
            for predictor in PREDICTORS
            for index, label in enumerate(AIR_CLASSES)
        ],
    )


def _paired_bootstrap(
    game_scores: pl.DataFrame,
    *,
    repetitions: int,
    seed: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    rng = np.random.default_rng(seed)
    groups = game_scores.partition_by(
        "split_family", "prior_strength", as_dict=True, maintain_order=False
    )
    for key in sorted(groups):
        family_value, strength_value = key
        family = str(family_value)
        strength = float(strength_value)
        group = groups[key].sort("game_id")
        counts = group["events"].to_numpy().astype(np.float64)
        samples = rng.integers(
            0, group.height, size=(repetitions, group.height), endpoint=False
        )
        for comparator in cast(tuple[Predictor, ...], ("result", "recorded")):
            intervals: dict[str, list[float]] = {}
            estimates: dict[str, float] = {}
            for metric in ("log_loss", "brier"):
                candidate = group[f"{metric}_recorded_result"].to_numpy()
                reference = group[f"{metric}_{comparator}"].to_numpy()
                gains = reference - candidate
                estimates[metric] = float(gains.sum() / counts.sum())
                replicates = np.empty(repetitions, dtype=np.float64)
                for repetition in range(repetitions):
                    sample = samples[repetition]
                    replicates[repetition] = gains[sample].sum() / counts[sample].sum()
                intervals[metric] = [
                    float(np.quantile(replicates, 0.025)),
                    float(np.quantile(replicates, 0.975)),
                ]
            rows.append(
                {
                    "split_family": family,
                    "prior_strength": strength,
                    "candidate": "recorded_result",
                    "comparator": comparator,
                    "games": group.height,
                    "events": int(counts.sum()),
                    "log_loss_gain": estimates["log_loss"],
                    "log_loss_gain_ci95": intervals["log_loss"],
                    "brier_gain": estimates["brier"],
                    "brier_gain_ci95": intervals["brier"],
                    "repetitions": repetitions,
                    "seed": seed,
                    "cluster_unit": "game_id",
                    "fits_during_bootstrap": "fixed",
                }
            )
    return rows


def _descriptive_group_metrics(predictions: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for column in ("park_id", "game_header_scorer_proxy"):
        groups = predictions.partition_by(
            "split_family", "prior_strength", column, as_dict=True
        )
        ordered_keys = sorted(
            groups,
            key=lambda key: (
                str(key[0]),
                float(key[1]),
                "" if key[2] is None else str(key[2]),
            ),
        )
        for key in ordered_keys:
            group_value = "missing" if key[2] is None else str(key[2])
            rows.append(
                _metric_row(
                    groups[key],
                    "recorded_result",
                    slice_type=column,
                    slice_value=group_value,
                )
            )
    return pl.DataFrame(rows).sort(
        "prior_strength", "split_family", "slice_type", "slice_value"
    )


def _strength_decision(
    metrics: pl.DataFrame,
    bootstrap: list[dict[str, object]],
    strength: float,
) -> dict[str, object]:
    family_results: dict[str, object] = {}
    for family in cast(tuple[SplitFamily, ...], ("game", "park", "season")):
        candidate = metrics.filter(
            (pl.col("split_family") == family)
            & (pl.col("prior_strength") == strength)
            & (pl.col("predictor") == "recorded_result")
        )
        overall = candidate.filter(pl.col("slice_type") == "overall")
        supported_seasons = candidate.filter(
            (pl.col("slice_type") == "season")
            & (pl.col("games") >= SUPPORTED_SEASON_GAMES)
            & (pl.col("events") >= SUPPORTED_SEASON_EVENTS)
        )
        overall_ece = float(cast(float, overall["classwise_ece_15_bin"].item()))
        overall_bias = float(
            cast(float, overall["max_absolute_class_share_bias"].item())
        )
        season_ece = cast(float | None, supported_seasons["classwise_ece_15_bin"].max())
        season_bias = cast(
            float | None,
            supported_seasons["max_absolute_class_share_bias"].max(),
        )
        calibration_pass = bool(
            overall_ece <= 0.05
            and overall_bias <= 0.02
            and (
                supported_seasons.is_empty()
                or (
                    season_ece is not None
                    and season_bias is not None
                    and season_ece <= 0.05
                    and season_bias <= 0.02
                )
            )
        )
        gains = [
            row
            for row in bootstrap
            if row["split_family"] == family and row["prior_strength"] == strength
        ]
        gain_pass = {row["comparator"] for row in gains} == {
            "result",
            "recorded",
        } and all(
            float(cast(list[float], row["log_loss_gain_ci95"])[0]) > 0
            and float(cast(list[float], row["brier_gain_ci95"])[0]) > 0
            for row in gains
        )
        family_results[family] = {
            "gain_intervals_strictly_positive_vs_result_and_recorded": gain_pass,
            "ece_and_class_share_thresholds": calibration_pass,
            "supported_season_slices": supported_seasons.select(
                "slice_value", "events", "games"
            ).to_dicts(),
            "pass": gain_pass and calibration_pass,
        }
    return {
        "prior_strength": strength,
        "split_families": family_results,
        "pass": all(
            bool(cast(dict[str, object], result)["pass"])
            for result in family_results.values()
        ),
    }


def development_decision(
    metrics: pl.DataFrame, bootstrap: list[dict[str, object]]
) -> dict[str, object]:
    primary = _strength_decision(metrics, bootstrap, PRIMARY_STRENGTH)
    sensitivities = [
        _strength_decision(metrics, bootstrap, strength)
        for strength in SENSITIVITY_STRENGTHS
    ]
    primary_pass = bool(primary["pass"])
    sensitivity_changed = any(
        bool(result["pass"]) != primary_pass for result in sensitivities
    )
    return {
        "primary": primary,
        "sensitivities": sensitivities,
        "sensitivity_verdict_changed": sensitivity_changed,
        "pass": primary_pass and not sensitivity_changed,
    }


def evaluate_oof_predictions(
    predictions: pl.DataFrame,
    *,
    repetitions: int,
    seed: int = FULL_BOOTSTRAP_SEED,
) -> tuple[dict[str, object], pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    metric_rows: list[dict[str, object]] = []
    for key, group in predictions.group_by("split_family", "prior_strength"):
        family, strength = cast(tuple[str, float], key)
        for predictor in PREDICTORS:
            metric_rows.append(
                _metric_row(
                    group,
                    predictor,
                    slice_type="overall",
                    slice_value="all",
                )
            )
            for season in sorted(DEVELOPMENT_SEASONS):
                season_frame = group.filter(pl.col("season") == season)
                metric_rows.append(
                    _metric_row(
                        season_frame,
                        predictor,
                        slice_type="season",
                        slice_value=str(season),
                    )
                )
        if (
            str(group["split_family"].item(0)) != family
            or float(group["prior_strength"].item(0)) != strength
        ):
            raise ValueError("group key mismatch")
    metrics = pl.DataFrame(metric_rows).sort(
        "prior_strength", "split_family", "slice_type", "slice_value", "predictor"
    )
    game_scores = _per_game_scores(predictions).sort(
        "prior_strength", "split_family", "game_id"
    )
    bootstrap = _paired_bootstrap(game_scores, repetitions=repetitions, seed=seed)
    descriptive_groups = _descriptive_group_metrics(predictions)
    report: dict[str, object] = {
        "bootstrap_repetitions": repetitions,
        "bootstrap_seed": seed,
        "bootstrap_scope": "paired game-cluster development intervals with fixed TRAIN fits",
        "paired_bootstrap": bootstrap,
        "metrics": metrics.to_dicts(),
    }
    return report, metrics, game_scores, descriptive_groups


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--source-root", type=Path, required=True)
    _ = parser.add_argument("--output-root", type=Path, required=True)
    _ = parser.add_argument("--acquisition-manifest-sha256", required=True)
    _ = parser.add_argument("--selected-games-sha256", required=True)
    _ = parser.add_argument("--target-module-sha256", required=True)
    _ = parser.add_argument(
        "--protocol-path",
        type=Path,
        default=Path("docs/geometry-air-development-protocol.md"),
    )
    _ = parser.add_argument("--smoke", action="store_true")
    return parser


def main() -> None:
    args = _parser().parse_args()
    source_root = cast(Path, args.source_root)
    output_root = cast(Path, args.output_root)
    protocol_path = cast(Path, args.protocol_path)
    smoke = bool(args.smoke)
    if output_root.exists():
        raise ValueError("output root must not already exist")
    source_paths = {
        protocol_path.name: protocol_path,
        Path(__file__).name: Path(__file__),
        "geometry_air_standard_data.py": Path(__file__).with_name(
            "geometry_air_standard_data.py"
        ),
        "geometry_air_translation.py": Path(__file__).with_name(
            "geometry_air_translation.py"
        ),
        "geometry_statcast_targets.py": Path(__file__).with_name(
            "geometry_statcast_targets.py"
        ),
    }
    source_snapshots = {name: path.read_bytes() for name, path in source_paths.items()}
    source_hashes = {
        name: hashlib.sha256(content).hexdigest()
        for name, content in source_snapshots.items()
    }
    acquisition_manifest = (source_root / "manifest.json").read_bytes()
    selected_games = (source_root / "selected_fitting_games.parquet").read_bytes()
    if hashlib.sha256(acquisition_manifest).hexdigest() != str(
        args.acquisition_manifest_sha256
    ):
        raise ValueError("accepted acquisition manifest changed before run")
    if hashlib.sha256(selected_games).hexdigest() != str(args.selected_games_sha256):
        raise ValueError("accepted selected games changed before run")
    if source_hashes["geometry_statcast_targets.py"] != str(args.target_module_sha256):
        raise ValueError("canonical target module changed before run")
    output_root.mkdir(parents=True)
    log_path = output_root / "run.log"
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_path)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    try:
        frame = load_development_frame(
            source_root,
            acquisition_manifest_sha256=str(args.acquisition_manifest_sha256),
            selected_games_sha256=str(args.selected_games_sha256),
            target_module_sha256=str(args.target_module_sha256),
        )
        full_counts = {
            "rows": frame.height,
            "games": frame["game_id"].n_unique(),
            "eligible_air_rows": int(frame["known_air_evaluation_eligible"].sum()),
            "unresolved_rows": int(frame["target_class"].is_null().sum()),
        }
        smoke_games: list[str] = []
        analysis_frame = frame
        if smoke:
            smoke_games = metadata_smoke_games(frame)
            analysis_frame = frame.filter(pl.col("game_id").is_in(smoke_games))
        strengths = (
            (PRIMARY_STRENGTH,)
            if smoke
            else (
                SENSITIVITY_STRENGTHS[0],
                PRIMARY_STRENGTH,
                SENSITIVITY_STRENGTHS[1],
            )
        )
        repetitions = (
            SMOKE_BOOTSTRAP_REPETITIONS if smoke else FULL_BOOTSTRAP_REPETITIONS
        )
        predictions, fits = build_oof_predictions(
            analysis_frame,
            prior_strengths=strengths,
            checkpoint_root=output_root / "checkpoints",
            logger=logger,
        )
        evaluation, metrics, game_scores, descriptive_groups = evaluate_oof_predictions(
            predictions, repetitions=repetitions
        )
        coverage = (
            frame.group_by("season", "target_status", "recorded_broad_type")
            .agg(
                pl.len().alias("events"),
                pl.col("game_id").n_unique().alias("games"),
                pl.col("known_air_evaluation_eligible")
                .sum()
                .alias("eligible_air_events"),
                pl.col("recorded_bunt").sum().alias("recorded_bunt_events"),
            )
            .sort("season", "target_status", "recorded_broad_type", nulls_last=True)
        )
        frame.write_parquet(output_root / "coverage_frame.parquet")
        coverage.write_parquet(output_root / "coverage_counts.parquet")
        predictions.write_parquet(output_root / "oof_predictions.parquet")
        metrics.write_parquet(output_root / "metrics.parquet")
        game_scores.write_parquet(output_root / "per_game_scores.parquet")
        descriptive_groups.write_parquet(
            output_root / "descriptive_group_metrics.parquet"
        )
        _ = (output_root / "fits.json").write_text(
            json.dumps(fits, indent=2, sort_keys=True) + "\n"
        )
        _ = (output_root / "report.json").write_text(
            json.dumps(
                {
                    "experiment": "geometry-air-development-v1",
                    "status": "smoke_only_no_acceptance_decision"
                    if smoke
                    else "full_development_scoring",
                    "smoke": smoke,
                    "full_frame_counts": full_counts,
                    "analysis_frame_counts": {
                        "rows": analysis_frame.height,
                        "games": analysis_frame["game_id"].n_unique(),
                        "eligible_air_rows": int(
                            analysis_frame["known_air_evaluation_eligible"].sum()
                        ),
                    },
                    "smoke_game_ids": smoke_games,
                    "prior_strengths": strengths,
                    "source_bindings": {
                        "acquisition_manifest_sha256": str(
                            args.acquisition_manifest_sha256
                        ),
                        "selected_games_sha256": str(args.selected_games_sha256),
                        "target_module_sha256": str(args.target_module_sha256),
                        "protocol_sha256": source_hashes[protocol_path.name],
                        "runner_sha256": source_hashes[Path(__file__).name],
                        "frame_module_sha256": source_hashes[
                            "geometry_air_standard_data.py"
                        ],
                        "translation_module_sha256": source_hashes[
                            "geometry_air_translation.py"
                        ],
                    },
                    "runtime": {
                        "python": platform.python_version(),
                        "numpy": np.__version__,
                        "polars": pl.__version__,
                    },
                    "decision": {"status": "smoke_only_no_acceptance_decision"}
                    if smoke
                    else development_decision(
                        metrics,
                        cast(list[dict[str, object]], evaluation["paired_bootstrap"]),
                    ),
                    "evaluation": evaluation,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        changed_sources = [
            name
            for name, path in source_paths.items()
            if _sha256(path) != source_hashes[name]
        ]
        if changed_sources:
            raise ValueError(f"source changed during run: {changed_sources}")
        for name, content in source_snapshots.items():
            _ = (output_root / name).write_bytes(content)
        _ = (output_root / "accepted_acquisition_manifest.json").write_bytes(
            acquisition_manifest
        )
        _ = (output_root / "selected_fitting_games.parquet").write_bytes(selected_games)
        logger.info("completed artifact=%s", output_root)
        handler.flush()
        manifest_files = {
            str(path.relative_to(output_root)): _sha256(path)
            for path in sorted(output_root.rglob("*"))
            if path.is_file() and path.name != "manifest.json"
        }
        _ = (output_root / "manifest.json").write_text(
            json.dumps(
                {
                    "experiment": "geometry-air-development-v1",
                    "status": "smoke_only" if smoke else "full_development",
                    "files_sha256": manifest_files,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
    except Exception:
        logger.exception("development run failed")
        raise
    finally:
        logger.removeHandler(handler)
        handler.close()


if __name__ == "__main__":
    main()
