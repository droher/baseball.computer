from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

import numpy as np
import polars as pl
import pytest

from python_models.statistical.backtests.geometry_air_regime import AIR_CLASSES
from python_models.statistical.backtests.geometry_air_regime_development import (
    FAILED_STATUS,
    FULL_CONCENTRATIONS,
    SPLIT_FAMILIES,
    run_experiment,
)
from python_models.statistical.backtests.geometry_air_regime_report import (
    CANDIDATE,
    CRITERIA,
    class_calibration,
    combined_decision,
    gain_matrix,
    require_complete_grids,
    tipping_point,
    unresolved_bounds,
    verify_run,
    write_report,
)
from tests.statistical.test_geometry_air_regime_development import (
    fold_covering_games,
    synthetic_frame,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _run(
    tmp_path: Path, *, smoke: bool, concentrations: tuple[float, ...]
) -> tuple[Path, Path]:
    frame = synthetic_frame()
    source = tmp_path / "frame.parquet"
    frame.write_parquet(source)
    protocol = tmp_path / "protocol.md"
    _ = protocol.write_text("protocol\n")
    run_root = tmp_path / "run"
    run_experiment(
        frame,
        source_frame=source,
        source_frame_sha256=_sha256(source),
        output_root=run_root,
        protocol_path=protocol,
        smoke=smoke,
        concentrations=concentrations,
        draws=8,
        repetitions=5,
        smoke_games=fold_covering_games(frame) if smoke else None,
    )
    return run_root, protocol


def _random_predictions(seed: int, events: int = 600) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    for index in range(events):
        candidate = rng.dirichlet(np.ones(3))
        rows.append(
            {
                "event_key": index,
                "game_id": f"G{index % 40:03d}",
                "season": [2015, 2019, 2023, 2025][index % 4],
                "target_class": AIR_CLASSES[int(rng.integers(0, 3))],
                "split_family": "game",
                "prior_strength": 30.0,
                f"probability_{CANDIDATE}": candidate.tolist(),
                "probability_result": rng.dirichlet(np.ones(3)).tolist(),
                "probability_recorded": candidate.tolist(),
            }
        )
    return pl.DataFrame(rows)


def test_gain_matrix_matches_event_score_differences() -> None:
    rng = np.random.default_rng(1)
    candidate = rng.dirichlet(np.ones(3), size=50)
    reference = rng.dirichlet(np.ones(3), size=50)
    for label in range(3):
        outcome = np.zeros(3)
        outcome[label] = 1.0
        log_gain = gain_matrix(candidate, reference, "log_loss")[:, label]
        expected = -np.log(reference[:, label]) + np.log(candidate[:, label])
        np.testing.assert_allclose(log_gain, expected)
        brier_gain = gain_matrix(candidate, reference, "brier")[:, label]
        expected_brier = np.square(reference - outcome).sum(axis=1) - np.square(
            candidate - outcome
        ).sum(axis=1)
        np.testing.assert_allclose(brier_gain, expected_brier)
    with pytest.raises(ValueError):
        _ = gain_matrix(candidate, reference[:10], "brier")


def test_tipping_point_relabelings_erase_positive_gain() -> None:
    frame = _random_predictions(2)
    for metric in ("log_loss", "brier"):
        result, selected = tipping_point(frame, "result", metric)
        assert result["candidate_ahead"] == (
            float(cast(float, result["original_average_gain"])) > 0
        )
        if not result["gain_erased"]:
            continue
        assert result["minimum_relabelings"] == selected.height
        relabeled = frame.join(
            selected.select("event_key", "replacement_class"),
            on="event_key",
            how="left",
        ).with_columns(
            pl.coalesce("replacement_class", "target_class").alias("target_class")
        )
        after, _ = tipping_point(relabeled, "result", metric)
        assert float(cast(float, after["original_average_gain"])) <= 1e-12
        assert (selected["gain_change"] < 0).all()
    identical, chosen = tipping_point(frame, "recorded", "log_loss")
    assert not identical["candidate_ahead"]
    assert identical["gain_erased"] is None
    assert identical["minimum_relabelings"] is None
    assert identical["affected_games"] is None
    assert chosen.is_empty()


def test_class_calibration_is_zero_for_perfect_predictions() -> None:
    frame = _random_predictions(3)
    perfect = frame.with_columns(
        pl.col("target_class")
        .map_elements(
            lambda label: [1.0 if label == c else 0.0 for c in AIR_CLASSES],
            return_dtype=pl.List(pl.Float64),
        )
        .alias(f"probability_{CANDIDATE}")
    )
    calibration = class_calibration(perfect)
    assert (calibration["ece"] == 0.0).all()
    assert (calibration["class_share_bias"].abs() < 1e-12).all()
    assert calibration["pass"].all()
    overall = calibration.filter(pl.col("slice_value") == "all")
    assert overall.height == len(AIR_CLASSES)
    assert overall["supported"].all()
    seasons = calibration.filter(pl.col("slice_value") != "all")
    assert not seasons["supported"].any()
    random_calibration = class_calibration(frame)
    assert random_calibration["ece"].is_between(0.0, 1.0).all()


def test_unresolved_bounds_bracket_resolved_shares() -> None:
    coverage = synthetic_frame()
    unresolved = coverage.with_columns(
        pl.when(pl.col("event_key") % 5 == 0)
        .then(pl.lit(False))
        .otherwise(pl.col("known_air_evaluation_eligible"))
        .alias("known_air_evaluation_eligible")
    )
    bounds = unresolved_bounds(unresolved)
    assert bounds.height == 2 * len(AIR_CLASSES)
    assert (bounds["lower_share"] <= bounds["upper_share"]).all()
    totals = bounds.group_by("regime").agg(
        pl.col("lower_share").sum().alias("lower"),
        pl.col("upper_share").sum().alias("upper"),
    )
    assert (totals["lower"] <= 1.0 + 1e-12).all()
    assert (totals["upper"] >= 1.0 - 1e-12).all()
    widths = bounds["upper_share"] - bounds["lower_share"]
    ratios = bounds["unresolved_events"] / bounds["local_air_events"]
    np.testing.assert_allclose(widths.to_numpy(), ratios.to_numpy())


def _grid_frames(
    concentrations: tuple[float, ...], *, failing: dict[float, str] | None = None
) -> tuple[pl.DataFrame, pl.DataFrame]:
    failing = failing or {}
    calibration: list[dict[str, object]] = []
    coverage: list[dict[str, object]] = []
    for concentration in concentrations:
        for family in SPLIT_FAMILIES:
            for slice_value, supported in (
                ("all", True),
                ("2015", True),
                ("2019", False),
            ):
                for label in AIR_CLASSES:
                    ece = 0.2 if failing.get(concentration) == "ece" else 0.01
                    calibration.append(
                        {
                            "split_family": family,
                            "prior_strength": concentration,
                            "predictor": CANDIDATE,
                            "slice_value": slice_value,
                            "class_label": label,
                            "games": 40,
                            "events": 600,
                            "ece": ece,
                            "class_share_bias": 0.0,
                            "supported": supported,
                            "pass": ece <= 0.05,
                        }
                    )
                    for level in (0.9, 0.95):
                        rate = 0.5 if failing.get(concentration) == "coverage" else 0.95
                        coverage.append(
                            {
                                "predictor": CANDIDATE,
                                "split_family": family,
                                "prior_strength": concentration,
                                "class_label": label,
                                "interval_level": level,
                                "slice_value": slice_value,
                                "games": 40,
                                "events": 600,
                                "supported": supported,
                                "coverage": rate,
                                "necessary_screen_pass": rate >= 0.9,
                            }
                        )
    return pl.DataFrame(calibration), pl.DataFrame(coverage)


def _score_decision(failing: set[float]) -> dict[str, object]:
    return {
        "primary": {"prior_strength": 30.0, "pass": 30.0 not in failing},
        "sensitivities": [
            {"prior_strength": value, "pass": value not in failing}
            for value in (3.0, 300.0)
        ],
    }


def test_require_complete_grids_rejects_missing_or_unsupported_slices() -> None:
    calibration, coverage = _grid_frames(FULL_CONCENTRATIONS)
    grid = require_complete_grids(calibration, coverage, FULL_CONCENTRATIONS)
    assert grid["supported_season_slices"] == {
        family: ["2015"] for family in SPLIT_FAMILIES
    }
    missing = calibration.filter(
        ~(
            (pl.col("split_family") == "park")
            & (pl.col("slice_value") == "all")
            & (pl.col("class_label") == "PopUp")
            & (pl.col("prior_strength") == 300.0)
        )
    )
    with pytest.raises(ValueError, match="calibration grid incomplete"):
        _ = require_complete_grids(missing, coverage, FULL_CONCENTRATIONS)
    unsupported = coverage.with_columns(
        pl.when(pl.col("slice_value") == "all")
        .then(pl.lit(False))
        .otherwise(pl.col("supported"))
        .alias("supported")
    )
    with pytest.raises(ValueError, match="below the support threshold"):
        _ = require_complete_grids(calibration, unsupported, FULL_CONCENTRATIONS)
    uneven = calibration.with_columns(
        pl.when(
            (pl.col("split_family") == "season") & (pl.col("slice_value") == "2019")
        )
        .then(pl.lit(True))
        .otherwise(pl.col("supported"))
        .alias("supported")
    )
    with pytest.raises(ValueError, match="differ across split families"):
        _ = require_complete_grids(uneven, coverage, FULL_CONCENTRATIONS)


def _decision_parts(
    decision: dict[str, object],
) -> tuple[list[dict[str, object]], dict[str, list[float]]]:
    return (
        cast(list[dict[str, object]], decision["concentrations"]),
        cast(dict[str, list[float]], decision["sensitivity_changes_by_criterion"]),
    )


def test_combined_decision_reports_each_criterion_sensitivity() -> None:
    calibration, coverage = _grid_frames(FULL_CONCENTRATIONS)
    passing = combined_decision(_score_decision(set()), calibration, coverage)
    rows, changes = _decision_parts(passing)
    assert passing["development_screen_pass"]
    assert not passing["sensitivity_verdict_changed"]
    assert [row["concentration"] for row in rows] == [30.0, 3.0, 300.0]
    assert set(changes) == {*CRITERIA, "pass"}
    assert not passing["publication_ready"]
    calibration, coverage = _grid_frames(FULL_CONCENTRATIONS, failing={3.0: "ece"})
    ece_changed = combined_decision(_score_decision(set()), calibration, coverage)
    rows, changes = _decision_parts(ece_changed)
    assert changes["individual_class_ece_pass"] == [3.0]
    assert changes["necessary_aggregate_coverage_pass"] == []
    assert not ece_changed["development_screen_pass"]
    assert rows[1]["failed_class_slices"]
    calibration, coverage = _grid_frames(
        FULL_CONCENTRATIONS, failing={300.0: "coverage"}
    )
    coverage_changed = combined_decision(_score_decision({30.0}), calibration, coverage)
    rows, changes = _decision_parts(coverage_changed)
    assert changes["score_and_previous_calibration_pass"] == [3.0, 300.0]
    assert changes["necessary_aggregate_coverage_pass"] == [300.0]
    assert not rows[0]["pass"]


def test_smoke_report_reproduces_run_and_skips_acceptance(tmp_path: Path) -> None:
    run_root, protocol = _run(tmp_path, smoke=True, concentrations=(30.0,))
    run = verify_run(run_root, protocol)
    assert run["smoke"]
    pooled = run_root / "oof_predictions.parquet"
    with pytest.raises(ValueError, match="disagrees with the requested report mode"):
        _ = write_report(
            run_root,
            tmp_path / "wrong",
            smoke=False,
            protocol_path=protocol,
            pooled_path=pooled,
            pooled_sha256=_sha256(pooled),
        )
    assert not (tmp_path / "wrong").exists()
    output = tmp_path / "report"
    result = write_report(
        run_root,
        output,
        smoke=True,
        protocol_path=protocol,
        pooled_path=pooled,
        pooled_sha256=_sha256(pooled),
    )
    assert result["status"] == "operational_smoke_complete"
    assert not result["acceptance_valid"]
    assert result["decision"] is None
    assert result["score_decision"] is None
    manifest = json.loads((output / "manifest.json").read_text())
    for name, digest in manifest["files_sha256"].items():
        assert _sha256(output / name) == digest
    pooled_scores = pl.read_parquet(output / "pooled-comparison.parquet")
    assert (pooled_scores["mean_gain_vs_pooled_candidate"].abs() < 1e-12).all()
    coverage = pl.read_parquet(output / "aggregate-coverage.parquet")
    assert set(coverage["predictor"].to_list()) == {
        "marginal",
        "result",
        "recorded",
        CANDIDATE,
    }
    assert coverage["coverage"].is_between(0.0, 1.0).all()
    ninety = coverage.filter(pl.col("interval_level") == 0.9)
    assert ninety["necessary_screen_pass"].null_count() == ninety.height
    assert (
        coverage.filter(pl.col("interval_level") == 0.95)[
            "necessary_screen_pass"
        ].null_count()
        == 0
    )
    seasons = pl.read_parquet(output / "season-coverage.parquet")
    assert seasons["coverage"].is_between(0.0, 1.0).all()
    assert (seasons["season_cohorts"] >= seasons["fits"]).all()
    calibration = pl.read_parquet(output / "class-calibration.parquet")
    assert set(calibration["class_label"].to_list()) == set(AIR_CLASSES)
    sensitivity = pl.read_parquet(output / "reference-sensitivity.parquet")
    assert set(sensitivity["regime"].to_list()) == {"all", "early", "late"}
    with pytest.raises(ValueError, match="must not already exist"):
        _ = write_report(
            run_root,
            output,
            smoke=True,
            protocol_path=protocol,
            pooled_path=pooled,
            pooled_sha256=_sha256(pooled),
        )
    failed = tmp_path / "failed"
    with pytest.raises(ValueError, match="bound pooled comparison changed"):
        _ = write_report(
            run_root,
            failed,
            smoke=True,
            protocol_path=protocol,
            pooled_path=pooled,
            pooled_sha256="0" * 64,
        )
    assert not failed.exists()
    broken = tmp_path / "broken"
    altered = tmp_path / "altered-pooled.parquet"
    pl.read_parquet(pooled).with_columns(
        pl.lit("Fly").alias("target_class")
    ).write_parquet(altered)
    with pytest.raises(ValueError, match="does not have identical events"):
        _ = write_report(
            run_root,
            broken,
            smoke=True,
            protocol_path=protocol,
            pooled_path=altered,
            pooled_sha256=_sha256(altered),
        )
    assert json.loads((broken / "manifest.json").read_text())["status"] == FAILED_STATUS


def test_verify_run_rejects_tampered_drifted_or_failed_runs(tmp_path: Path) -> None:
    run_root, protocol = _run(tmp_path, smoke=True, concentrations=(30.0,))
    metrics = run_root / "metrics.parquet"
    original = metrics.read_bytes()
    pl.read_parquet(metrics).head(1).write_parquet(metrics)
    with pytest.raises(ValueError, match="manifest hash"):
        _ = verify_run(run_root, protocol)
    _ = metrics.write_bytes(original)
    _ = verify_run(run_root, protocol)
    extra = run_root / "stray.txt"
    _ = extra.write_text("x")
    with pytest.raises(ValueError, match="differ from the manifest"):
        _ = verify_run(run_root, protocol)
    extra.unlink()
    protocol_text = protocol.read_text()
    _ = protocol.write_text(protocol_text + "changed\n")
    with pytest.raises(ValueError, match="differ from the run's frozen copies"):
        _ = verify_run(run_root, protocol)
    _ = protocol.write_text(protocol_text)
    with pytest.raises(ValueError, match="do not cover the declared sources"):
        _ = verify_run(run_root, tmp_path / "frame.parquet")
    manifest_path = run_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["status"] = "failed"
    _ = manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="not reportable"):
        _ = verify_run(run_root, protocol)


def test_reduced_scale_report_decides_without_acceptance(tmp_path: Path) -> None:
    run_root, protocol = _run(tmp_path, smoke=False, concentrations=FULL_CONCENTRATIONS)
    pooled = run_root / "oof_predictions.parquet"
    with pytest.raises(ValueError, match="disagrees with the requested report mode"):
        _ = write_report(
            run_root,
            tmp_path / "full",
            smoke=False,
            protocol_path=protocol,
            pooled_path=pooled,
            pooled_sha256=_sha256(pooled),
        )
    result = write_report(
        run_root,
        tmp_path / "report",
        smoke=False,
        protocol_path=protocol,
        reduced_scale=True,
        pooled_path=pooled,
        pooled_sha256=_sha256(pooled),
    )
    assert result["status"] == "reduced_scale_report_no_acceptance"
    assert not result["acceptance_valid"]
    manifest = json.loads((tmp_path / "report" / "manifest.json").read_text())
    assert manifest["status"] == "reduced_scale_report_no_acceptance"
    assert manifest["reporter_sha256"]
    decision = cast(dict[str, object], result["decision"])
    rows = cast(list[dict[str, object]], decision["concentrations"])
    assert [row["concentration"] for row in rows] == [30.0, 3.0, 300.0]
    for row in rows:
        assert row["pass"] == all(bool(row[criterion]) for criterion in CRITERIA)
    changes = cast(dict[str, list[float]], decision["sensitivity_changes_by_criterion"])
    assert decision["sensitivity_verdict_changed"] == any(changes.values())
    assert decision["development_screen_pass"] == (
        bool(rows[0]["pass"]) and not decision["sensitivity_verdict_changed"]
    )
    assert not decision["historical_reliability_established"]
    grid = cast(dict[str, object], decision["support_grid"])
    assert grid["split_families"] == list(SPLIT_FAMILIES)
