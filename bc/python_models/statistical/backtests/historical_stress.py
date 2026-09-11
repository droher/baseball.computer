from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import cast

import arviz as az
import polars as pl

from python_models.statistical.backtests.geometry_reference_model import (
    fit_reference,
    predict_reference,
)
from python_models.statistical.backtests.historical_stress_data import (
    apply_joint_masks,
    contextual_baseline,
    feature_frame,
    game_digest,
    support_summary,
    verify_exclusions,
)
from python_models.statistical.backtests.trajectory_interaction import (
    repair_sampling_config,
)
from python_models.statistical.evidence_binding import file_digest
from python_models.statistical.validate import (
    compute_posterior_diagnostics,
    write_json_atomic,
)

LOGGER = logging.getLogger(__name__)
SEED = 20260911
BASE = Path("artifacts/statistical/backtests")


def read_json(path: Path) -> dict[str, object]:
    return cast(dict[str, object], json.loads(path.read_text()))


def write_json(path: Path, value: object) -> None:
    write_json_atomic(
        path, json.dumps(value, indent=2, sort_keys=True, allow_nan=False)
    )


def verify_bound_file(path: Path, report: dict[str, object], relative: str) -> str:
    inventory = cast(dict[str, str], report["artifact_files"])
    digest = file_digest(path)
    if inventory.get(relative) != digest:
        raise ValueError(f"artifact digest mismatch: {path}")
    return digest


def load_source(
    target: str,
) -> tuple[pl.DataFrame, pl.DataFrame, Path, dict[str, object]]:
    if target == "trajectory":
        data_root = BASE / "geometry_reference/20260911-full-v1/trajectory"
        fit_root = BASE / "trajectory_interaction/20260911-full-v2-4000draws"
        model_root = fit_root / "model"
        prefix = "model/"
    elif target == "location_side":
        data_root = (
            BASE / "geometry_reference/20260911-global-side-full-v1/location_side"
        )
        fit_root = model_root = data_root
        prefix = ""
    else:
        raise ValueError(f"unsupported target: {target}")
    data_report = read_json(data_root / "report.json")
    fit_report = read_json(fit_root / "report.json")
    if fit_report.get("numerical_pass") is not True:
        raise ValueError("reference posterior did not pass numerical checks")
    hashes = {
        str(path): verify_bound_file(path, report, relative)
        for path, report, relative in (
            (data_root / "data/train.parquet", data_report, "data/train.parquet"),
            (data_root / "data/test.parquet", data_report, "data/test.parquet"),
            (model_root / "posterior.nc", fit_report, prefix + "posterior.nc"),
            (model_root / "fit_config.json", fit_report, prefix + "fit_config.json"),
        )
    }
    train = pl.read_parquet(data_root / "data/train.parquet")
    test = pl.read_parquet(data_root / "data/test.parquet")
    if train.height != 100000:
        raise ValueError("original TRAIN selection must have exactly 100,000 rows")
    return (
        train,
        test,
        model_root,
        {
            "file_sha256": hashes,
            "report_sha256": file_digest(fit_root / "report.json"),
            "numerical_pass": True,
            "diagnostics": fit_report["diagnostics"],
        },
    )


def attach_metadata(frame: pl.DataFrame, metadata: pl.DataFrame) -> pl.DataFrame:
    required = (
        "game_id",
        "season",
        "primary_fold",
        "scorer_signature",
        "cleaned_scorers",
        "scorer_holdout_selected",
    )
    games = metadata.select(required).rename({"scorer_signature": "scorer"})
    if games["game_id"].n_unique() != games.height:
        raise ValueError("game metadata is not unique")
    joined = frame.join(
        games, on="game_id", how="left", suffix="_metadata", validate="m:1"
    )
    if joined["scorer"].null_count():
        raise ValueError("frozen event has no scorer metadata row")
    if joined.filter(
        (pl.col("season") != pl.col("season_metadata"))
        | (pl.col("primary_fold") != pl.col("primary_fold_metadata"))
    ).height:
        raise ValueError("frozen event and metadata season/fold disagree")
    return joined.drop("season_metadata", "primary_fold_metadata")


def scorer_holdouts(metadata: pl.DataFrame) -> list[str]:
    eligible = set(
        str(value) for value in metadata["eligible_scorers"].explode().drop_nulls()
    )
    selected = sorted(
        scorer
        for scorer in eligible
        if int.from_bytes(
            hashlib.sha256(f"historical-stress-scorer-v1:{scorer}".encode()).digest(),
            "big",
        )
        % 5
        == 0
    )
    expected = metadata.select(
        pl.col("cleaned_scorers")
        .list.eval(pl.element().is_in(selected))
        .list.any()
        .alias("selected")
    )["selected"]
    if not expected.equals(metadata["scorer_holdout_selected"].rename("selected")):
        raise ValueError("scorer holdout flags disagree with the frozen identity rule")
    return selected


def select_scenario(
    train: pl.DataFrame, test: pl.DataFrame, scenario: str, heldout: list[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if scenario == "reference":
        return train, test
    if scenario == "backward":
        return train.filter(pl.col("season") >= 1988), test.filter(
            pl.col("season") < 1988
        )
    if scenario == "scorer":
        retained = train.filter(~pl.col("scorer_holdout_selected"))
        evaluated = test.filter(pl.col("scorer_holdout_selected"))
        if set(retained["cleaned_scorers"].explode().drop_nulls()) & set(heldout):
            raise ValueError("held-out co-scorer leaked into training")
        return retained, evaluated
    raise ValueError(f"unsupported scenario: {scenario}")


def smoke_subset(frame: pl.DataFrame, limit: int) -> pl.DataFrame:
    keys = sorted(
        frame["event_key"].to_list(),
        key=lambda key: hashlib.sha256(
            f"historical-smoke:{SEED}:{key}".encode()
        ).digest(),
    )[:limit]
    return frame.filter(pl.col("event_key").is_in(keys)).sort("event_key")


def load_profile(
    root: Path, target: str, expected_manifest: str
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, object]]:
    if file_digest(root / "manifest.json") != expected_manifest:
        raise ValueError("profile manifest differs from the frozen input contract")
    manifest = read_json(root / "manifest.json")
    hashes = cast(dict[str, str], manifest["files_sha256"])
    for relative, expected in hashes.items():
        if file_digest(root / relative) != expected:
            raise ValueError(f"profile artifact changed: {relative}")
    profile_record = read_json(root / "profile.json")
    metadata = pl.read_parquet(root / "frozen_game_metadata.parquet")
    all_patterns = pl.read_parquet(root / "train_joint_feature_patterns.parquet")
    if set(all_patterns["observed_status"].unique().to_list()) - {
        "derived",
        "unknown_code",
        "missing",
        "default_code",
    }:
        raise ValueError("profile contains an undeclared observation status")
    natural = all_patterns.filter(
        (pl.col("target") == target)
        & pl.col("observed_status").is_in(["unknown_code", "missing", "default_code"])
    )
    profiles = natural.select(
        pl.col("era").cast(pl.String),
        pl.when(pl.col("profile_level") == "era_pool")
        .then(pl.lit("*"))
        .otherwise(pl.col("scorer"))
        .alias("scorer"),
        pl.col("joint_feature_mask").alias("mask_pattern"),
        pl.col("weighted_event_count").alias("rows"),
    )
    return metadata, profiles, profile_record


def run_scenario(
    train: pl.DataFrame,
    test: pl.DataFrame,
    mask_profile: pl.DataFrame,
    *,
    target: str,
    scenario: str,
    reference_root: Path,
    source: dict[str, object],
    output: Path,
    heldout: list[str],
    reserve: set[str],
    smoke: bool,
) -> dict[str, object]:
    from python_models.statistical.backtests.historical_stress_evaluation import (
        evaluate_stress,
    )

    train, test = select_scenario(train, test, scenario, heldout)
    if train.is_empty() or test.is_empty():
        raise ValueError(f"empty scenario: {target}/{scenario}")
    if smoke:
        train = smoke_subset(train, 3000)
        test = smoke_subset(test, 5000)
    exclusions = verify_exclusions(train, test, reserve)
    labels = tuple(sorted(str(label) for label in train["target_class"].unique()))
    if set(test["target_class"]) - set(labels):
        raise ValueError("TEST has classes absent from retained TRAIN")
    output.mkdir(parents=True, exist_ok=False)
    train.write_parquet(output / "train.parquet")
    test.write_parquet(output / "test.parquet")
    prepared_train, columns = feature_frame(train, target)
    LOGGER.info(
        "scenario=%s target=%s train=%s test=%s",
        scenario,
        target,
        train.height,
        test.height,
    )
    if scenario == "reference":
        config = read_json(reference_root / "fit_config.json")
        config_levels = cast(dict[str, list[str]], config["feature_levels"])
        levels = {name: tuple(config_levels[name]) for name in columns}
        labels = tuple(cast(list[str], config["class_labels"]))
        if set(labels) != set(train["target_class"]):
            raise ValueError("reference classes differ from retained TRAIN")
        load_posterior = cast(
            Callable[[str], az.InferenceData], getattr(az, "from_netcdf")
        )
        idata = load_posterior(str(reference_root / "posterior.nc"))
        numerical_pass = True
        diagnostics = source["diagnostics"]
    else:
        idata, levels, labels = fit_reference(
            prepared_train,
            output_dir=output / "model",
            smoke=smoke,
            seed=SEED,
            feature_columns=columns,
            sampling_config=None if smoke else repair_sampling_config(SEED),
        )
        diagnostic_model = compute_posterior_diagnostics(idata)
        diagnostics = diagnostic_model.model_dump()
        numerical_pass = (
            diagnostic_model.rhat_max <= 1.05
            and min(diagnostic_model.ess_bulk_min, diagnostic_model.ess_tail_min) >= 100
            and diagnostic_model.divergences == 0
        )
    write_json(output / "diagnostics.json", diagnostics)
    conditions: dict[str, object] = {}
    try:
        for condition in ("recorded", "joint_mask"):
            scored = (
                apply_joint_masks(test, mask_profile, seed=SEED, target=target)
                if condition == "joint_mask"
                else test.with_columns(
                    pl.lit("000000").alias("mask_pattern"),
                    pl.lit("not_applied").alias("mask_profile_route"),
                )
            )
            prepared_test, _ = feature_frame(scored, target)
            predictions = predict_reference(
                idata, prepared_test, levels, labels, feature_columns=columns
            )
            baseline = contextual_baseline(train, scored, labels)
            support, summary = support_summary(prepared_test, levels)
            frame = scored.with_columns(
                pl.Series("model_probability", predictions),
                pl.Series("baseline_probability", baseline.probabilities),
                pl.Series("baseline_route", baseline.routes),
                support.alias("model_context_supported"),
            )
            frame.write_parquet(output / f"{condition}_predictions.parquet")
            evaluation = (
                None
                if smoke
                else evaluate_stress(frame, labels, repetitions=500, seed=SEED)
            )
            conditions[condition] = {
                "support": summary,
                "mask_counts": frame.group_by("mask_pattern", "mask_profile_route")
                .len()
                .sort("mask_pattern", "mask_profile_route")
                .to_dicts(),
                "baseline_routes": frame.group_by("baseline_route")
                .len()
                .sort("baseline_route")
                .to_dicts(),
                "evaluation": evaluation,
            }
            write_json(output / f"{condition}_metrics.json", conditions[condition])
            LOGGER.info("saved %s/%s/%s", target, scenario, condition)
    finally:
        for group in idata.groups():
            idata[group].close()
    report: dict[str, object] = {
        "target": target,
        "scenario": scenario,
        "smoke": smoke,
        "class_labels": labels,
        "training_rows": train.height,
        "test_rows": test.height,
        "exclusions": exclusions,
        "numerical_pass": numerical_pass,
        "diagnostics": diagnostics,
        "conditions": conditions,
        "source": source,
        "research_only": True,
        "global_confirmation": False,
        "artifact_files": {
            str(path.relative_to(output)): file_digest(path)
            for path in output.rglob("*")
            if path.is_file()
        },
    }
    write_json(output / "report.json", report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile-root", type=Path, required=True)
    parser.add_argument(
        "--reserve-root",
        type=Path,
        default=BASE / "historical_stress/20260911-reserve-v1",
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--target", choices=("trajectory", "location_side"), required=True
    )
    parser.add_argument(
        "--scenario", choices=("reference", "backward", "scorer"), action="append"
    )
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    root = cast(Path, args.output_root)
    root.mkdir(parents=True, exist_ok=False)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(root / "run.log")],
    )
    profile_root = cast(Path, args.profile_root)
    reserve_root = cast(Path, args.reserve_root)
    repository = Path(__file__).resolve().parents[4]
    frozen_inputs = read_json(
        repository / "docs/historical-geometry-stress-inputs.json"
    )
    reservation = read_json(reserve_root / "verification.json")
    reserved_file = reserve_root / "reserved_games.parquet"
    if file_digest(reserved_file) != reservation["reserved_game_file_sha256"]:
        raise ValueError("reserve file changed")
    reserve = set(str(value) for value in pl.read_parquet(reserved_file)["game_id"])
    if game_digest(list(reserve)) != reservation["reserved_digest"]:
        raise ValueError("reserve game digest changed")
    if (
        reservation["reserved_digest"] != frozen_inputs["reserve_game_digest"]
        or reservation["candidate_digest"] != frozen_inputs["candidate_game_digest"]
    ):
        raise ValueError("reserve differs from the frozen input contract")
    metadata, profile, profile_record = load_profile(
        profile_root, str(args.target), str(frozen_inputs["profile_manifest_sha256"])
    )
    if set(metadata["primary_fold"].unique().to_list()) - {"TRAIN", "TEST"}:
        raise ValueError("metadata includes a forbidden fold")
    heldout = scorer_holdouts(metadata)
    if not heldout:
        raise ValueError("no scorer groups selected")
    code = root / "code"
    code.mkdir()
    for name in (
        "historical_stress.py",
        "historical_stress_data.py",
        "historical_stress_evaluation.py",
        "geometry_reference_model.py",
        "trajectory_interaction.py",
    ):
        shutil.copy2(Path(__file__).with_name(name), code / name)
    shutil.copy2(
        repository / "docs/historical-geometry-stress-protocol.md", root / "protocol.md"
    )
    shutil.copy2(reserved_file, root / "reserved_games.parquet")
    write_json(
        root / "plan.json",
        {
            "target": args.target,
            "smoke": bool(args.smoke),
            "seed": SEED,
            "scenarios": args.scenario or ["reference", "backward", "scorer"],
            "heldout_scorers": heldout,
            "reservation": reservation,
            "frozen_inputs": frozen_inputs,
            "profile_files": {
                path.name: file_digest(path) for path in profile_root.glob("*.parquet")
            },
            "implementation": {path.name: file_digest(path) for path in code.iterdir()},
            "protocol_sha256": file_digest(root / "protocol.md"),
        },
    )
    train, test, reference, source = load_source(str(args.target))
    expected_inputs = cast(dict[str, str], profile_record["input_sha256"])
    for path, digest in cast(dict[str, str], source["file_sha256"]).items():
        if path.endswith("/train.parquet") or path.endswith("/test.parquet"):
            key = f"{args.target}_{Path(path).stem}"
            if expected_inputs.get(key) != digest:
                raise ValueError("profile and frozen model inputs disagree")
    train, test = attach_metadata(train, metadata), attach_metadata(test, metadata)
    verify_exclusions(train, test, reserve)
    for scenario in args.scenario or ["reference", "backward", "scorer"]:
        run_scenario(
            train,
            test,
            profile,
            target=str(args.target),
            scenario=str(scenario),
            reference_root=reference,
            source=source,
            output=root / str(scenario),
            heldout=heldout,
            reserve=reserve,
            smoke=bool(args.smoke),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
