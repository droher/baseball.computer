from __future__ import annotations

import argparse
import logging
import platform
import shutil
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import cast

import polars as pl

from python_models.statistical.backtests.geometry_hierarchical import (
    FEATURE_COLUMNS,
    fit_hierarchical,
    predict_hierarchical,
)
from python_models.statistical.backtests.geometry_reliability_data import (
    BASE,
    REPOSITORY,
    inner_fold,
    read_json,
    source_root,
    write_json,
)
from python_models.statistical.backtests.historical_stress_data import (
    contextual_baseline,
)
from python_models.statistical.backtests.historical_stress_evaluation import (
    evaluate_stress,
)
from python_models.statistical.evidence_binding import file_digest
from python_models.statistical.pymc_utils import SamplingConfig
from python_models.statistical.validate import compute_posterior_diagnostics

LOGGER = logging.getLogger(__name__)
SEED = 20260911


def select_frames(
    selected: pl.DataFrame, full: pl.LazyFrame, scenario: str
) -> tuple[pl.DataFrame, pl.DataFrame]:
    train = selected.filter(pl.col("inner_fold") == "inner_fit")
    evaluation = full.filter(pl.col("inner_fold") == "inner_evaluation")
    if scenario == "backward_1988":
        train = train.filter(pl.col("season") >= 1988)
        evaluation = evaluation.filter(pl.col("season") < 1988)
    elif scenario != "game":
        raise ValueError(f"unknown scenario {scenario}")
    return train.sort("event_key"), evaluation.sort("event_key").collect()


def verify_development_split(
    train: pl.DataFrame, evaluation: pl.DataFrame, reserve: set[str]
) -> dict[str, int]:
    if train.is_empty() or evaluation.is_empty():
        raise ValueError("development split is empty")
    populations: list[set[str]] = []
    for frame, fold in ((train, "inner_fit"), (evaluation, "inner_evaluation")):
        if set(frame["primary_fold"].unique().to_list()) != {"TRAIN"}:
            raise ValueError("development split includes non-TRAIN labels")
        if set(frame["inner_fold"].unique().to_list()) != {fold}:
            raise ValueError("development fold assignment differs")
        if frame["event_key"].n_unique() != frame.height:
            raise ValueError("duplicate development event keys")
        games = set(str(game) for game in frame["game_id"])
        if any(inner_fold(game) != fold for game in games):
            raise ValueError("development games violate the frozen fold rule")
        if games & reserve:
            raise ValueError("sealed reserve entered development")
        populations.append(games)
    if populations[0] & populations[1]:
        raise ValueError("fitting and evaluation share games")
    return {
        "train_events": train.height,
        "train_games": len(populations[0]),
        "evaluation_events": evaluation.height,
        "evaluation_games": len(populations[1]),
        "game_overlap": 0,
        "reserve_overlap": 0,
    }


def cohort_calibration(evaluation: dict[str, object]) -> dict[str, object]:
    slices = cast(dict[str, dict[str, object]], evaluation["slices"])
    results: dict[str, object] = {}
    for name, data in slices.items():
        if name != "pre_1988" and not name.startswith("decade_"):
            continue
        supported = (
            int(str(data["row_count"])) >= 500 and int(str(data["game_count"])) >= 50
        )
        if not supported or not data["predictors"]:
            results[name] = {
                "status": "unsupported",
                "events": data["row_count"],
                "games": data["game_count"],
            }
            continue
        model = cast(dict[str, dict[str, object]], data["predictors"])["model"]
        ece = float(str(model["classwise_ece_15_bin"]))
        bias = float(str(model["max_absolute_class_share_bias"]))
        results[name] = {
            "status": "passed" if ece <= 0.05 and bias <= 0.02 else "failed",
            "classwise_ece": ece,
            "max_class_share_bias": bias,
            "events": data["row_count"],
            "games": data["game_count"],
        }
    return results


def development_decision(
    numerical_pass: bool, metrics: dict[str, object] | None
) -> dict[str, object]:
    if metrics is None:
        return {"status": "unsupported", "reason": "unscored smoke"}
    cohorts = cohort_calibration(metrics)
    statuses = [cast(dict[str, object], value)["status"] for value in cohorts.values()]
    overall = cast(dict[str, object], metrics["decision_flags"])["passed"] is True
    failed = not numerical_pass or not overall or "failed" in statuses
    unsupported = not statuses or "unsupported" in statuses
    return {
        "status": "failed" if failed else "unsupported" if unsupported else "passed",
        "numerical_pass": numerical_pass,
        "overall_predictive_pass": overall,
        "cohort_calibration": cohorts,
        "standardized_trajectory_validation": "unsupported",
        "aggregate_uncertainty_validation": "unsupported",
        "full_reliability_established": False,
    }


def run(target: str, output: Path, scenarios: list[str], *, smoke: bool) -> None:
    inputs = read_json(
        REPOSITORY / "docs/geometry-hierarchical-development-inputs.json"
    )
    if inputs.get("status") != "frozen_before_candidate_development_scoring":
        raise ValueError("development input contract is not frozen")
    declared_order = ["game", "backward_1988"]
    if not scenarios or scenarios != [
        name for name in declared_order if name in scenarios
    ]:
        raise ValueError("scenarios must be a unique ordered subset of the protocol")
    bindings = cast(dict[str, dict[str, object]], inputs["targets"])[target]
    data_root = REPOSITORY / str(bindings["root"])
    if file_digest(data_root / "report.json") != bindings["report_sha256"]:
        raise ValueError("development data report differs from frozen inputs")
    data_report = read_json(data_root / "report.json")
    if (
        data_report["smoke"] is not False
        or data_report["uses_validate_labels"] is not False
        or data_report["uses_test_labels"] is not False
        or data_report["target"] != target
    ):
        raise ValueError("development input is not a full TRAIN-only extract")
    for name, expected in cast(dict[str, str], data_report["artifact_files"]).items():
        if file_digest(data_root / name) != expected:
            raise ValueError(f"development input changed: {name}")
    reserve_path = BASE / "historical_stress/20260911-reserve-v1/reserved_games.parquet"
    if file_digest(reserve_path) != inputs["reserved_games_sha256"]:
        raise ValueError("sealed reserve IDs changed")
    reserve = set(str(game) for game in pl.read_parquet(reserve_path)["game_id"])
    contract_path = source_root(target) / "fit_config.json"
    if file_digest(contract_path) != bindings["domain_contract_sha256"]:
        raise ValueError("fixed domain contract changed")
    contract = read_json(contract_path)
    labels = tuple(str(label) for label in cast(list[str], contract["class_labels"]))
    domains = cast(dict[str, list[str]], contract["feature_levels"])
    result_domain = tuple(domains["result_family"])
    output.mkdir(parents=True, exist_ok=False)
    code = output / "code"
    code.mkdir()
    for name in (
        "geometry_reliability.py",
        "geometry_reliability_data.py",
        "geometry_hierarchical.py",
        "historical_stress_data.py",
        "historical_stress_evaluation.py",
    ):
        shutil.copy2(Path(__file__).with_name(name), code / name)
    for name in ("pymc_utils.py", "validate.py", "evidence_binding.py"):
        shutil.copy2(Path(__file__).parent.parent / name, code / name)
    shutil.copy2(
        REPOSITORY / "docs/geometry-hierarchical-development-protocol.md",
        output / "protocol.md",
    )
    shutil.copy2(
        REPOSITORY / "docs/geometry-development-exposure.md",
        output / "development_exposure.md",
    )
    plan = {
        "target": target,
        "scenarios": scenarios,
        "smoke": smoke,
        "seed": SEED,
        "inputs": inputs,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "runtime": {
            "python": platform.python_version(),
            **{
                name: version(name)
                for name in ("numpy", "polars", "pymc", "arviz", "nutpie", "duckdb")
            },
        },
        "implementation_sha256": {
            path.name: file_digest(path) for path in code.iterdir()
        },
        "protocol_sha256": file_digest(output / "protocol.md"),
        "development_exposure_sha256": file_digest(output / "development_exposure.md"),
        "evaluation_scope": "iterative_primary_train_development",
        "protocol_complete": scenarios == declared_order,
        "estimand": "recorded_label_control",
    }
    write_json(output / "plan.json", plan)
    selected = pl.read_parquet(data_root / "selected_train.parquet")
    full = pl.scan_parquet(data_root / "full_primary_train.parquet")
    decisions: dict[str, object] = {}
    for scenario in scenarios:
        train, evaluation = select_frames(selected, full, scenario)
        if smoke:
            train = train.head(3000)
            evaluation = evaluation.head(5000)
        boundary = verify_development_split(train, evaluation, reserve)
        destination = output / scenario
        destination.mkdir()
        train.write_parquet(destination / "train.parquet")
        evaluation.write_parquet(destination / "evaluation.parquet")
        LOGGER.info("scenario=%s target=%s %s", scenario, target, boundary)
        sampler = SamplingConfig(
            draws=50 if smoke else 4000,
            tune=50 if smoke else 1000,
            chains=2 if smoke else 4,
            cores=1,
            target_accept=0.95,
            max_treedepth=12,
            random_seed=SEED,
            backend="nutpie",
        )
        idata, encoding = fit_hierarchical(
            train,
            output_dir=destination / "model",
            class_labels=labels,
            result_domain=result_domain,
            smoke=smoke,
            seed=SEED,
            sampling_config=sampler,
        )
        try:
            diagnostics = compute_posterior_diagnostics(idata)
            prediction = predict_hierarchical(
                idata, evaluation.select(FEATURE_COLUMNS), encoding, seed=SEED
            )
            baseline = contextual_baseline(train, evaluation, labels)
            scored = evaluation.with_columns(
                pl.Series("model_probability", prediction.probabilities),
                pl.Series("baseline_probability", baseline.probabilities),
                pl.Series("baseline_route", baseline.routes),
                pl.Series("known_era", prediction.known_era),
                pl.Series(
                    "observed_era_result_pair", prediction.observed_era_result_pair
                ),
                pl.Series("unseen_context_count", prediction.unseen_context_count),
                pl.Series(
                    "model_context_supported",
                    prediction.known_era
                    & prediction.observed_era_result_pair
                    & (prediction.unseen_context_count == 0),
                ),
                pl.lit("000000").alias("mask_pattern"),
            )
            scored.write_parquet(destination / "predictions.parquet")
            metrics = (
                None
                if smoke
                else evaluate_stress(scored, labels, repetitions=500, seed=SEED)
            )
            report = {
                "target": target,
                "scenario": scenario,
                "smoke": smoke,
                "boundary": boundary,
                "class_labels": labels,
                "diagnostics": diagnostics.model_dump(),
                "numerical_pass": diagnostics.rhat_max <= 1.05
                and min(diagnostics.ess_bulk_min, diagnostics.ess_tail_min) >= 100
                and diagnostics.divergences == 0,
                "evaluation": metrics,
                "cohort_calibration": None
                if metrics is None
                else cohort_calibration(metrics),
                "research_only": True,
                "confirmation_scored": False,
                "artifact_files": {
                    str(path.relative_to(destination)): file_digest(path)
                    for path in destination.rglob("*")
                    if path.is_file()
                },
            }
            decision = development_decision(bool(report["numerical_pass"]), metrics)
            report["development_decision"] = decision
            write_json(destination / "report.json", report)
            decisions[scenario] = decision
            LOGGER.info(
                "saved %s/%s numerical_pass=%s",
                target,
                scenario,
                report["numerical_pass"],
            )
        finally:
            for group in idata.groups():
                idata[group].close()
    write_json(
        output / "report.json",
        {
            "target": target,
            "estimand": "recorded_label_control",
            "smoke": smoke,
            "protocol_complete": scenarios == declared_order,
            "scenario_decisions": decisions,
            "initial_development_pass": not smoke
            and scenarios == declared_order
            and all(
                cast(dict[str, object], value)["status"] == "passed"
                for value in decisions.values()
            ),
            "full_reliability_established": False,
            "confirmation_scored": False,
            "evaluation_scope": "iterative_primary_train_development",
            "scenario_reports_sha256": {
                name: file_digest(output / name / "report.json") for name in scenarios
            },
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target", choices=("trajectory", "location_side"), required=True
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--scenario", action="append", choices=("game", "backward_1988")
    )
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    run(
        str(args.target),
        cast(Path, args.output),
        cast(list[str], args.scenario or ["game", "backward_1988"]),
        smoke=bool(args.smoke),
    )


if __name__ == "__main__":
    main()
