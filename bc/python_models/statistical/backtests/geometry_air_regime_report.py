from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Literal, cast

import numpy as np
import numpy.typing as npt
import polars as pl

from python_models.statistical.backtests.geometry_air_development import (
    SUPPORTED_SEASON_EVENTS,
    SUPPORTED_SEASON_GAMES,
    development_decision,
    evaluate_oof_predictions,
)
from python_models.statistical.backtests.geometry_air_regime import (
    REGIMES,
    SEASON_REGIME,
)
from python_models.statistical.backtests.geometry_air_regime_development import (
    EXPERIMENT_ID,
    FAILED_STATUS,
    FULL_CONCENTRATIONS,
    FULL_STATUS,
    PRIMARY_CONCENTRATION,
    REDUCED_SCALE_STATUS,
    ROOT_SEED,
    SMOKE_STATUS,
    SOURCE_DIRECTORY,
    SPLIT_FAMILIES,
    declared_source_paths,
    runtime_versions,
    sha256_file,
    write_manifest,
)
from python_models.statistical.backtests.geometry_air_regime_predictive import (
    summarize_counts,
)
from python_models.statistical.backtests.geometry_air_translation import AIR_CLASSES

LOGGER = logging.getLogger(__name__)
FloatArray = npt.NDArray[np.float64]
Metric = Literal["log_loss", "brier"]
METRICS: tuple[Metric, ...] = ("log_loss", "brier")
COMPARATORS = ("result", "recorded")
CANDIDATE = "recorded_result"
REGIME_SEASONS = {
    regime: sorted(season for season, value in SEASON_REGIME.items() if value == regime)
    for regime in REGIMES
}
DEFAULT_PROTOCOL_PATH = Path("docs/geometry-air-regime-posterior-protocol.md")
POOLED_RELATIVE_PATH = (
    "artifacts/statistical/backtests/geometry_reliability/"
    "20260911-air-development-full-v1/oof_predictions.parquet"
)
POOLED_SHA256 = "9187b8dc97adff0eac09dd260844b73a5eee2fbde1df68beeba091fd6570beec"
CLASS_ECE_LIMIT = 0.05
COVERAGE_SCREEN = 0.90
COVERAGE_SCREEN_LEVEL = 0.95
REPORTABLE_STATUSES = {
    SMOKE_STATUS: "operational_smoke_complete",
    FULL_STATUS: "development_report_complete",
    REDUCED_SCALE_STATUS: "reduced_scale_report_no_acceptance",
}
CRITERIA = (
    "score_and_previous_calibration_pass",
    "individual_class_ece_pass",
    "necessary_aggregate_coverage_pass",
)


def _probability_matrix(frame: pl.DataFrame, column: str) -> FloatArray:
    matrix = np.asarray(frame[column].to_list(), dtype=np.float64)
    if (
        matrix.ndim != 2
        or matrix.shape != (frame.height, len(AIR_CLASSES))
        or not np.isfinite(matrix).all()
        or (matrix < 0).any()
        or not np.allclose(matrix.sum(axis=1), 1.0, atol=1e-12, rtol=0.0)
    ):
        raise ValueError(f"{column} is not a normalized airborne probability matrix")
    return matrix


def _labels(frame: pl.DataFrame) -> npt.NDArray[np.int64]:
    labels = cast(list[str], frame["target_class"].to_list())
    if not set(labels) <= set(AIR_CLASSES):
        raise ValueError("target class outside the airborne classes")
    return np.asarray([AIR_CLASSES.index(label) for label in labels], dtype=np.int64)


def _supported(games: int, events: int) -> bool:
    return games >= SUPPORTED_SEASON_GAMES and events >= SUPPORTED_SEASON_EVENTS


def _season_slices(group: pl.DataFrame) -> list[tuple[str, pl.DataFrame]]:
    return [("all", group)] + [
        (str(season), group.filter(pl.col("season") == season))
        for season in sorted(cast(list[int], group["season"].unique().to_list()))
    ]


def class_ece(probability: FloatArray, target: FloatArray, bins: int = 15) -> float:
    binned = np.minimum((probability * bins).astype(np.int64), bins - 1)
    counts = np.bincount(binned, minlength=bins)
    errors = np.bincount(binned, weights=probability - target, minlength=bins)
    return float(np.abs(errors[counts > 0]).sum() / probability.size)


def class_calibration(predictions: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for key, group in predictions.group_by("split_family", "prior_strength"):
        family, concentration = cast(tuple[str, float], key)
        for slice_value, frame in _season_slices(group):
            matrix = _probability_matrix(frame, f"probability_{CANDIDATE}")
            labels = _labels(frame)
            games = frame["game_id"].n_unique()
            for index, label in enumerate(AIR_CLASSES):
                probability = matrix[:, index]
                target = (labels == index).astype(np.float64)
                ece = class_ece(probability, target)
                rows.append(
                    {
                        "split_family": family,
                        "prior_strength": concentration,
                        "predictor": CANDIDATE,
                        "slice_value": slice_value,
                        "class_label": label,
                        "games": games,
                        "events": frame.height,
                        "ece": ece,
                        "class_share_bias": float((probability - target).mean()),
                        "supported": _supported(games, frame.height),
                        "pass": ece <= CLASS_ECE_LIMIT,
                    }
                )
    return pl.DataFrame(rows).sort(
        "prior_strength", "split_family", "slice_value", "class_label"
    )


def gain_matrix(
    candidate: FloatArray, reference: FloatArray, metric: Metric
) -> FloatArray:
    if (
        candidate.shape != reference.shape
        or candidate.ndim != 2
        or candidate.shape[1] != len(AIR_CLASSES)
    ):
        raise ValueError("three-class probability matrices must have identical shapes")
    for matrix in (candidate, reference):
        if (
            not np.isfinite(matrix).all()
            or (matrix < 0).any()
            or not np.allclose(matrix.sum(axis=1), 1, atol=1e-12, rtol=0)
        ):
            raise ValueError("invalid probabilities")
    if metric == "log_loss":
        return np.log(np.clip(candidate, 1e-15, 1)) - np.log(
            np.clip(reference, 1e-15, 1)
        )
    return (np.square(reference).sum(axis=1) - np.square(candidate).sum(axis=1))[
        :, None
    ] + 2 * (candidate - reference)


def tipping_point(
    frame: pl.DataFrame, reference: str, metric: Metric
) -> tuple[dict[str, object], pl.DataFrame]:
    frame = frame.sort("event_key")
    candidate = _probability_matrix(frame, f"probability_{CANDIDATE}")
    comparator = _probability_matrix(frame, f"probability_{reference}")
    gains = gain_matrix(candidate, comparator, metric)
    labels = _labels(frame)
    current = gains[np.arange(frame.height), labels]
    replacements = gains.argmin(axis=1)
    changes = gains.min(axis=1) - current
    order = np.argsort(changes, kind="stable")
    initial = float(current.sum())
    residual = initial
    before = initial
    selected: list[int] = []
    if initial > 0:
        for position in order.tolist():
            if changes[position] >= 0:
                break
            before = residual
            residual += float(changes[position])
            selected.append(int(position))
            if residual <= 0:
                break
    ahead = initial > 0
    erased = residual <= 0 if ahead else None
    selected_rows = (frame[selected] if selected else frame.head(0)).select(
        "event_key", "game_id"
    )
    selected_rows = selected_rows.with_columns(
        pl.Series(
            "replacement_class",
            [AIR_CLASSES[int(replacements[i])] for i in selected],
            dtype=pl.String,
        ),
        pl.Series(
            "gain_change", [float(changes[i]) for i in selected], dtype=pl.Float64
        ),
    )
    return {
        "events": frame.height,
        "comparator": reference,
        "metric": metric,
        "original_average_gain": initial / frame.height,
        "candidate_ahead": ahead,
        "gain_erased": erased,
        "minimum_relabelings": len(selected) if erased else None,
        "fraction": len(selected) / frame.height if erased else None,
        "affected_games": selected_rows["game_id"].n_unique() if erased else None,
        "before_average_gain": before / frame.height,
        "after_average_gain": residual / frame.height,
    }, selected_rows


def reference_sensitivity(
    predictions: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    rows: list[dict[str, object]] = []
    selected: list[pl.DataFrame] = []
    for key, group in predictions.group_by("split_family", "prior_strength"):
        family, concentration = cast(tuple[str, float], key)
        regimes = [("all", group)] + [
            (regime, group.filter(pl.col("season").is_in(seasons)))
            for regime, seasons in REGIME_SEASONS.items()
        ]
        for regime, frame in regimes:
            if frame.is_empty():
                continue
            for reference in COMPARATORS:
                for metric in METRICS:
                    result, chosen = tipping_point(frame, reference, metric)
                    metadata: dict[str, object] = {
                        "split_family": family,
                        "prior_strength": concentration,
                        "regime": regime,
                    }
                    rows.append({**metadata, **result})
                    selected.append(
                        chosen.with_columns(
                            pl.lit(family).alias("split_family"),
                            pl.lit(concentration).alias("prior_strength"),
                            pl.lit(regime).alias("regime"),
                            pl.lit(reference).alias("comparator"),
                            pl.lit(metric).alias("metric"),
                        )
                    )
    return (
        pl.DataFrame(rows).sort(
            "prior_strength", "split_family", "regime", "comparator", "metric"
        ),
        pl.concat(selected, how="vertical"),
    )


def unresolved_bounds(coverage: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    air = coverage.filter(pl.col("recorded_broad_type") == "Air")
    for regime, years in REGIME_SEASONS.items():
        group = air.filter(pl.col("season").is_in(years))
        if group.is_empty():
            raise ValueError(f"coverage frame has no local-Air events for {regime}")
        resolved = group.filter(pl.col("known_air_evaluation_eligible"))
        unresolved = group.height - resolved.height
        for label in AIR_CLASSES:
            count = resolved.filter(pl.col("target_class") == label).height
            rows.append(
                {
                    "regime": regime,
                    "class_label": label,
                    "local_air_events": group.height,
                    "resolved_events": resolved.height,
                    "unresolved_events": unresolved,
                    "lower_share": count / group.height,
                    "upper_share": (count + unresolved) / group.height,
                }
            )
    return pl.DataFrame(rows)


def interval_summaries(count_paths: list[Path]) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for path in count_paths:
        counts = pl.read_parquet(path)
        for name in ("split_family", "fold", "prior_strength"):
            if counts[name].n_unique() != 1:
                raise ValueError(f"{path} mixes fits")
        summaries = summarize_counts(
            counts.drop("split_family", "fold", "prior_strength")
        )
        frames.append(
            summaries.with_columns(
                pl.lit(counts["split_family"].item(0)).alias("split_family"),
                pl.lit(counts["fold"].item(0)).alias("fold"),
                pl.lit(counts["prior_strength"].item(0)).alias("prior_strength"),
            )
        )
    summaries = pl.concat(frames, how="vertical")
    if not (summaries["prior_strength"] == summaries["concentration"]).all():
        raise ValueError("checkpoint prior strength disagrees with fit concentration")
    return summaries


def aggregate_coverage(summary: pl.DataFrame, repetitions: int) -> pl.DataFrame:
    games = summary.filter(pl.col("cohort_type") == "game")
    rows: list[dict[str, object]] = []
    for key, group in games.group_by(
        "predictor", "split_family", "prior_strength", "class_label", "interval_level"
    ):
        predictor, family, concentration, label, level = cast(
            tuple[str, str, float, str, float], key
        )
        for slice_value, frame in _season_slices(group):
            frame = frame.sort("cohort_id")
            if frame["cohort_id"].n_unique() != frame.height:
                raise ValueError(
                    "game count intervals duplicated within a split family"
                )
            flags = frame["covered"].to_numpy().astype(np.float64)
            rng = np.random.default_rng(ROOT_SEED)
            samples = rng.integers(0, frame.height, size=(repetitions, frame.height))
            replicates = flags[samples].mean(axis=1)
            events = int(cast(int, frame["events"].sum()))
            rate = float(flags.mean())
            rows.append(
                {
                    "predictor": predictor,
                    "split_family": family,
                    "prior_strength": concentration,
                    "class_label": label,
                    "interval_level": level,
                    "slice_value": slice_value,
                    "games": frame.height,
                    "events": events,
                    "supported": _supported(frame.height, events),
                    "coverage": rate,
                    "coverage_bootstrap_ci95": cast(
                        list[float], np.quantile(replicates, [0.025, 0.975]).tolist()
                    ),
                    "mean_width": float(cast(float, frame["width"].mean())),
                    "max_mc_lower_span": float(
                        cast(float, frame["mc_lower_span"].max())
                    ),
                    "max_mc_upper_span": float(
                        cast(float, frame["mc_upper_span"].max())
                    ),
                    "mean_mc_coverage_disagreement_fraction": float(
                        cast(float, frame["mc_coverage_disagreement_fraction"].mean())
                    ),
                    "necessary_screen_pass": rate >= COVERAGE_SCREEN
                    if level == COVERAGE_SCREEN_LEVEL
                    else None,
                }
            )
    return pl.DataFrame(rows).sort(
        "prior_strength",
        "predictor",
        "split_family",
        "slice_value",
        "class_label",
        "interval_level",
    )


def season_coverage(summary: pl.DataFrame) -> pl.DataFrame:
    seasons = summary.filter(pl.col("cohort_type") == "season")
    return (
        seasons.group_by(
            "predictor",
            "split_family",
            "prior_strength",
            "class_label",
            "interval_level",
        )
        .agg(
            pl.len().alias("season_cohorts"),
            pl.col("fit_id").n_unique().alias("fits"),
            pl.col("covered").mean().alias("coverage"),
            pl.col("width").mean().alias("mean_width"),
            pl.col("events").sum().alias("events"),
        )
        .sort(
            "prior_strength",
            "predictor",
            "split_family",
            "class_label",
            "interval_level",
        )
    )


def _supported_season_slices(
    frame: pl.DataFrame, concentration: float, family: str
) -> list[str]:
    return sorted(
        cast(
            list[str],
            frame.filter(
                (pl.col("prior_strength") == concentration)
                & (pl.col("split_family") == family)
                & (pl.col("slice_value") != "all")
                & pl.col("supported")
            )["slice_value"]
            .unique()
            .to_list(),
        )
    )


def require_complete_grids(
    calibration: pl.DataFrame,
    coverage: pl.DataFrame,
    concentrations: tuple[float, ...],
) -> dict[str, object]:
    expected = {(family, label) for family in SPLIT_FAMILIES for label in AIR_CLASSES}
    candidate_coverage = coverage.filter(
        (pl.col("predictor") == CANDIDATE)
        & (pl.col("interval_level") == COVERAGE_SCREEN_LEVEL)
    )
    supported_slices: dict[str, dict[str, list[str]]] = {}
    for concentration in concentrations:
        classes = calibration.filter(pl.col("prior_strength") == concentration)
        overall = classes.filter(pl.col("slice_value") == "all")
        if set(overall.select("split_family", "class_label").iter_rows()) != expected:
            raise ValueError(
                f"calibration grid incomplete at concentration {concentration}"
            )
        if not overall["supported"].all():
            raise ValueError("overall calibration slice is below the support threshold")
        aggregates = candidate_coverage.filter(
            (pl.col("prior_strength") == concentration)
            & (pl.col("slice_value") == "all")
        )
        if (
            set(aggregates.select("split_family", "class_label").iter_rows())
            != expected
        ):
            raise ValueError(
                f"coverage grid incomplete at concentration {concentration}"
            )
        if not aggregates["supported"].all():
            raise ValueError("overall coverage slice is below the support threshold")
        calibration_slices = {
            family: _supported_season_slices(classes, concentration, family)
            for family in SPLIT_FAMILIES
        }
        coverage_slices = {
            family: _supported_season_slices(candidate_coverage, concentration, family)
            for family in SPLIT_FAMILIES
        }
        if len({tuple(v) for v in calibration_slices.values()}) != 1:
            raise ValueError("supported season slices differ across split families")
        if calibration_slices != coverage_slices:
            raise ValueError("calibration and coverage support grids disagree")
        supported_slices[f"{concentration:g}"] = calibration_slices
    if len({json.dumps(v, sort_keys=True) for v in supported_slices.values()}) != 1:
        raise ValueError("supported season slices differ across concentrations")
    return {
        "split_families": list(SPLIT_FAMILIES),
        "classes": list(AIR_CLASSES),
        "supported_season_slices": supported_slices[f"{concentrations[0]:g}"],
    }


def combined_decision(
    score_decision: dict[str, object],
    calibration: pl.DataFrame,
    coverage: pl.DataFrame,
) -> dict[str, object]:
    ordered = (PRIMARY_CONCENTRATION,) + tuple(
        value for value in FULL_CONCENTRATIONS if value != PRIMARY_CONCENTRATION
    )
    grid = require_complete_grids(calibration, coverage, ordered)
    sensitivities = {
        float(cast(float, row["prior_strength"])): row
        for row in cast(list[dict[str, object]], score_decision["sensitivities"])
    }
    results: list[dict[str, object]] = []
    for concentration in ordered:
        if concentration == PRIMARY_CONCENTRATION:
            score = cast(dict[str, object], score_decision["primary"])
            if score["prior_strength"] != concentration:
                raise ValueError("score decision primary concentration differs")
        else:
            if concentration not in sensitivities:
                raise ValueError(f"score decision lacks concentration {concentration}")
            score = sensitivities[concentration]
        classes = calibration.filter(
            (pl.col("prior_strength") == concentration) & pl.col("supported")
        )
        aggregates = coverage.filter(
            (pl.col("prior_strength") == concentration)
            & (pl.col("predictor") == CANDIDATE)
            & pl.col("supported")
            & (pl.col("interval_level") == COVERAGE_SCREEN_LEVEL)
        )
        per_class_pass = bool(classes["pass"].all())
        aggregate_pass = bool(aggregates["necessary_screen_pass"].all())
        results.append(
            {
                "concentration": concentration,
                "score_and_previous_calibration_pass": bool(score["pass"]),
                "individual_class_ece_pass": per_class_pass,
                "necessary_aggregate_coverage_pass": aggregate_pass,
                "failed_class_slices": classes.filter(~pl.col("pass"))
                .select("split_family", "slice_value", "class_label", "ece")
                .to_dicts(),
                "failed_coverage_slices": aggregates.filter(
                    ~pl.col("necessary_screen_pass")
                )
                .select("split_family", "slice_value", "class_label", "coverage")
                .to_dicts(),
                "pass": bool(score["pass"]) and per_class_pass and aggregate_pass,
            }
        )
    primary = results[0]
    sensitivity_changes = {
        criterion: [
            row["concentration"]
            for row in results[1:]
            if row[criterion] != primary[criterion]
        ]
        for criterion in (*CRITERIA, "pass")
    }
    sensitivity_changed = any(bool(changed) for changed in sensitivity_changes.values())
    return {
        "support_grid": grid,
        "concentrations": results,
        "sensitivity_changes_by_criterion": sensitivity_changes,
        "sensitivity_verdict_changed": sensitivity_changed,
        "development_screen_pass": bool(primary["pass"]) and not sensitivity_changed,
        "historical_reliability_established": False,
        "publication_ready": False,
    }


def pooled_comparison(
    predictions: pl.DataFrame, pooled: pl.DataFrame, repetitions: int
) -> pl.DataFrame:
    keys = ["event_key", "split_family", "prior_strength"]
    joined = predictions.join(
        pooled.select(
            *keys,
            pl.col(f"probability_{CANDIDATE}").alias("probability_pooled"),
            pl.col("target_class").alias("pooled_target"),
        ),
        on=keys,
        how="left",
        validate="1:1",
    )
    if (
        joined["probability_pooled"].null_count()
        or not (joined["target_class"] == joined["pooled_target"]).all()
    ):
        raise ValueError("pooled comparison does not have identical events and targets")
    rows: list[dict[str, object]] = []
    for key, group in joined.group_by("split_family", "prior_strength"):
        family, concentration = cast(tuple[str, float], key)
        candidate = _probability_matrix(group, f"probability_{CANDIDATE}")
        reference = _probability_matrix(group, "probability_pooled")
        labels = _labels(group)
        for metric in METRICS:
            gains = gain_matrix(candidate, reference, metric)[
                np.arange(group.height), labels
            ]
            games = (
                group.select("game_id")
                .with_columns(pl.Series("gain", gains))
                .group_by("game_id")
                .agg(pl.len().alias("events"), pl.col("gain").sum())
                .sort("game_id")
            )
            values = games["gain"].to_numpy()
            counts = games["events"].to_numpy().astype(np.float64)
            rng = np.random.default_rng(ROOT_SEED)
            indexes = rng.integers(0, games.height, size=(repetitions, games.height))
            replicates = values[indexes].sum(axis=1) / counts[indexes].sum(axis=1)
            rows.append(
                {
                    "split_family": family,
                    "prior_strength": concentration,
                    "metric": metric,
                    "mean_gain_vs_pooled_candidate": float(gains.mean()),
                    "paired_game_bootstrap_ci95": cast(
                        list[float], np.quantile(replicates, [0.025, 0.975]).tolist()
                    ),
                    "games": games.height,
                    "events": group.height,
                }
            )
    return pl.DataFrame(rows).sort("prior_strength", "split_family", "metric")


def verify_run(run_root: Path, protocol_path: Path) -> dict[str, object]:
    manifest = cast(
        dict[str, object], json.loads((run_root / "manifest.json").read_text())
    )
    if manifest.get("experiment") != EXPERIMENT_ID:
        raise ValueError("run manifest belongs to a different experiment")
    status = str(manifest.get("status"))
    if status not in REPORTABLE_STATUSES:
        raise ValueError(f"run status {status!r} is not reportable")
    declared = cast(dict[str, str], manifest["files_sha256"])
    actual = {
        str(path.relative_to(run_root))
        for path in run_root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if actual != set(declared):
        raise ValueError("run root files differ from the manifest")
    for name, digest in declared.items():
        if sha256_file(run_root / name) != digest:
            raise ValueError(f"run file {name} does not match its manifest hash")
    bindings = cast(
        dict[str, object], json.loads((run_root / "bindings.json").read_text())
    )
    if str(bindings["expected_status"]) != status:
        raise ValueError("run status and bindings disagree")
    source_hashes = cast(dict[str, str], bindings["source_hashes"])
    for name, digest in source_hashes.items():
        if sha256_file(run_root / SOURCE_DIRECTORY / name) != digest:
            raise ValueError(f"frozen source {name} does not match the run bindings")
    live = {protocol_path.name: protocol_path} | declared_source_paths()
    if set(live) != set(source_hashes):
        raise ValueError("run bindings do not cover the declared sources and protocol")
    drifted = [
        name for name, path in live.items() if sha256_file(path) != source_hashes[name]
    ]
    if drifted:
        raise ValueError(f"live sources differ from the run's frozen copies: {drifted}")
    return {
        "status": status,
        "smoke": bool(bindings["smoke"]),
        "concentrations": tuple(
            float(value) for value in cast(list[float], bindings["concentrations"])
        ),
        "bootstrap_repetitions": int(cast(int, bindings["bootstrap_repetitions"])),
        "source_hashes": source_hashes,
        "source_frame_sha256": str(bindings["source_frame_sha256"]),
    }


def _write_json(path: Path, payload: object) -> None:
    _ = path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def write_report(
    run_root: Path,
    output_root: Path,
    *,
    smoke: bool,
    protocol_path: Path = DEFAULT_PROTOCOL_PATH,
    reduced_scale: bool = False,
    pooled_path: Path | None = None,
    pooled_sha256: str = POOLED_SHA256,
) -> dict[str, object]:
    if output_root.exists():
        raise ValueError("report output root must not already exist")
    run = verify_run(run_root, protocol_path)
    run_status = str(run["status"])
    expected_status = (
        SMOKE_STATUS
        if smoke
        else REDUCED_SCALE_STATUS
        if reduced_scale
        else FULL_STATUS
    )
    if run_status != expected_status:
        raise ValueError(
            f"run status {run_status!r} disagrees with the requested report mode"
        )
    if pooled_path is None:
        pooled_path = Path(__file__).resolve().parents[4] / POOLED_RELATIVE_PATH
    if sha256_file(pooled_path) != pooled_sha256:
        raise ValueError("bound pooled comparison changed")
    concentrations = cast(tuple[float, ...], run["concentrations"])
    repetitions = int(cast(int, run["bootstrap_repetitions"]))
    inputs = [
        run_root / "oof_predictions.parquet",
        run_root / "coverage_frame.parquet",
        run_root / "metrics.parquet",
        run_root / "report.json",
        run_root / "manifest.json",
        run_root / "bindings.json",
        pooled_path,
    ]
    count_paths = sorted(run_root.rglob("predictive-counts.parquet"))
    if not count_paths:
        raise ValueError("posterior predictive count files are missing")
    inputs.extend(count_paths)
    bindings = {str(path.resolve()): sha256_file(path) for path in inputs}
    output_root.mkdir(parents=True)
    try:
        _write_json(
            output_root / "input-bindings.json",
            {
                "run_root": str(run_root.resolve()),
                "run_status": run_status,
                "inputs_sha256": bindings,
                "reporter_sha256": sha256_file(Path(__file__).resolve()),
                "runtime": runtime_versions(),
            },
        )
        predictions = pl.read_parquet(inputs[0])
        coverage_frame = pl.read_parquet(inputs[1])
        run_metrics = pl.read_parquet(inputs[2])
        run_report = cast(dict[str, object], json.loads(inputs[3].read_text()))
        if set(
            cast(list[float], predictions["prior_strength"].unique().to_list())
        ) != set(concentrations):
            raise ValueError("prediction concentrations differ from the run bindings")
        score_report, metrics, game_scores, descriptive = evaluate_oof_predictions(
            predictions, repetitions=repetitions, seed=ROOT_SEED
        )
        if not metrics.equals(run_metrics):
            raise ValueError("reporter did not reproduce the run's metrics")
        run_bootstrap = cast(dict[str, object], run_report["evaluation"])[
            "paired_bootstrap"
        ]
        if json.loads(json.dumps(score_report["paired_bootstrap"])) != run_bootstrap:
            raise ValueError("reporter did not reproduce the run's paired bootstrap")
        calibration = class_calibration(predictions)
        summaries = interval_summaries(count_paths)
        coverage = aggregate_coverage(summaries, repetitions)
        seasons = season_coverage(summaries)
        sensitivity, selected = reference_sensitivity(predictions)
        bounds = unresolved_bounds(coverage_frame)
        pooled_scores = pooled_comparison(
            predictions, pl.read_parquet(pooled_path), repetitions
        )
        score_decision: dict[str, object] | None = None
        decision: dict[str, object] | None = None
        if not smoke:
            if set(concentrations) != set(FULL_CONCENTRATIONS):
                raise ValueError("a decision requires every declared concentration")
            score_decision = development_decision(
                metrics, cast(list[dict[str, object]], score_report["paired_bootstrap"])
            )
            decision = combined_decision(score_decision, calibration, coverage)
        result: dict[str, object] = {
            "experiment": EXPERIMENT_ID,
            "status": REPORTABLE_STATUSES[run_status],
            "run_status": run_status,
            "acceptance_valid": run_status == FULL_STATUS,
            "operational_smoke": smoke,
            "concentrations": concentrations,
            "bootstrap_repetitions": repetitions,
            "decision": decision,
            "score_decision": score_decision,
            "coverage_scope": "whole-game bootstrap conditional on overlapping OOF fits; season totals retain fit identity",
            "mc_scope": "split-batch endpoint spans and interval-flag disagreements, not formal MC confidence intervals",
            "reference_scope": "unknown measurement origin; adversarial fractions are assumptions, not estimated error rates",
            "historical_reliability_established": False,
            "publication_ready": False,
        }
        tables = {
            "metrics": metrics,
            "game-scores": game_scores,
            "descriptive": descriptive,
            "class-calibration": calibration,
            "predictive-intervals": summaries,
            "aggregate-coverage": coverage,
            "season-coverage": seasons,
            "reference-sensitivity": sensitivity,
            "adversarial-selected-events": selected,
            "unresolved-bounds": bounds,
            "pooled-comparison": pooled_scores,
        }
        for name, table in tables.items():
            table.write_parquet(output_root / f"{name}.parquet")
        _write_json(output_root / "score-report.json", score_report)
        _write_json(output_root / "report.json", result)
        if any(sha256_file(Path(path)) != value for path, value in bindings.items()):
            raise ValueError("report input changed during execution")
        write_manifest(
            output_root,
            str(result["status"]),
            reporter_sha256=sha256_file(Path(__file__).resolve()),
        )
    except Exception:
        LOGGER.exception("report failed: %s", output_root)
        write_manifest(output_root, FAILED_STATUS)
        raise
    LOGGER.info("report complete: %s", output_root)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--run-root", type=Path, required=True)
    _ = parser.add_argument("--output-root", type=Path, required=True)
    _ = parser.add_argument("--protocol-path", type=Path, default=DEFAULT_PROTOCOL_PATH)
    _ = parser.add_argument("--smoke", action="store_true")
    _ = parser.add_argument("--reduced-scale", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    _ = write_report(
        cast(Path, args.run_root),
        cast(Path, args.output_root),
        smoke=bool(args.smoke),
        protocol_path=cast(Path, args.protocol_path),
        reduced_scale=bool(args.reduced_scale),
    )


if __name__ == "__main__":
    main()
