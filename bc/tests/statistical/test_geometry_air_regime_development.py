from __future__ import annotations

import hashlib
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import cast

import numpy as np
import polars as pl
import pytest

from python_models.statistical.backtests.geometry_air_development import PREDICTORS
from python_models.statistical.backtests.geometry_air_regime import (
    AIR_CLASSES,
    fit_air_regime,
    predict_air_regime,
)
from python_models.statistical.backtests.geometry_air_standard_data import (
    game_fold,
    park_fold,
)
from python_models.statistical.backtests.geometry_air_regime_development import (
    EXPERIMENT_ID,
    FULL_BOOTSTRAP_REPETITIONS,
    FULL_CONCENTRATIONS,
    FULL_DRAWS,
    FULL_STATUS,
    PRIMARY_CONCENTRATION,
    REDUCED_SCALE_STATUS,
    ROOT_SEED,
    SMOKE_DRAWS,
    SMOKE_STATUS,
    SOURCE_FRAME_SHA256,
    SOURCE_DIRECTORY,
    SOURCE_MODULES,
    build_oof_predictions,
    declared_source_paths,
    fit_identity,
    load_bound_coverage_frame,
    run_experiment,
    run_status,
)


def synthetic_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    event_key = 1
    for season_index, season in enumerate((2015, 2019, 2023, 2025)):
        for game_index in range(25):
            game_id = f"G{season}{game_index:03d}"
            park_id = f"P{season_index}{game_index:02d}"
            for class_index in range(5):
                target = AIR_CLASSES[class_index % 3]
                rows.append(
                    {
                        "event_key": event_key,
                        "game_id": game_id,
                        "season": season,
                        "park_id": park_id,
                        "game_header_scorer_proxy": f"S{game_index % 7}",
                        "result_family": f"R{game_index % 4}",
                        "recorded_air_subtype": AIR_CLASSES[
                            (class_index + game_index) % 3
                        ],
                        "recorded_broad_type": "Air",
                        "target_class": target,
                        "target_status": "air_angle_standardized",
                        "recorded_bunt": False,
                        "known_air_evaluation_eligible": True,
                        "primary_fold": "TRAIN",
                        "acquisition_fold": "fitting",
                    }
                )
                event_key += 1
            for broad, target, status in (
                ("Ground", "GroundBall", "ground_preserved"),
                (None, None, "broad_source_conflict"),
            ):
                rows.append(
                    {
                        "event_key": event_key,
                        "game_id": game_id,
                        "season": season,
                        "park_id": park_id,
                        "game_header_scorer_proxy": f"S{game_index % 7}",
                        "result_family": "R0",
                        "recorded_air_subtype": None,
                        "recorded_broad_type": broad,
                        "target_class": target,
                        "target_status": status,
                        "recorded_bunt": False,
                        "known_air_evaluation_eligible": False,
                        "primary_fold": "TRAIN",
                        "acquisition_fold": "fitting",
                    }
                )
                event_key += 1
    return pl.DataFrame(rows)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fold_covering_games(frame: pl.DataFrame) -> list[str]:
    games = frame.select("game_id", "park_id", "season").unique().sort("game_id")
    selected: list[str] = []
    covered_game_folds: set[int] = set()
    covered_park_folds: set[int] = set()
    for season in sorted(cast(list[int], games["season"].unique().to_list())):
        season_game_folds: set[int] = set()
        season_park_folds: set[int] = set()
        for game_id, park_id in (
            games.filter(pl.col("season") == season)
            .select("game_id", "park_id")
            .iter_rows()
        ):
            if (
                game_fold(game_id) not in season_game_folds
                or park_fold(park_id) not in season_park_folds
            ):
                selected.append(str(game_id))
                season_game_folds.add(game_fold(game_id))
                season_park_folds.add(park_fold(park_id))
        if len(season_game_folds) < 2 or len(season_park_folds) < 2:
            raise ValueError(f"season {season} cannot leave every fold a training game")
        covered_game_folds |= season_game_folds
        covered_park_folds |= season_park_folds
    if covered_game_folds != set(range(5)) or covered_park_folds != set(range(5)):
        raise ValueError("selected games leave a fold without evaluation games")
    return sorted(selected)


def test_bound_frame_requires_hash_population_and_declared_folds(
    tmp_path: Path,
) -> None:
    frame = synthetic_frame()
    path = tmp_path / "frame.parquet"
    frame.write_parquet(path)
    loaded = load_bound_coverage_frame(
        path,
        expected_sha256=_sha256(path),
        expected_rows=frame.height,
        expected_games=frame["game_id"].n_unique(),
        expected_eligible_rows=int(frame["known_air_evaluation_eligible"].sum()),
    )
    assert loaded.height == frame.height
    with pytest.raises(ValueError, match="SHA-256"):
        _ = load_bound_coverage_frame(
            path,
            expected_sha256="0" * 64,
            expected_rows=frame.height,
            expected_games=frame["game_id"].n_unique(),
            expected_eligible_rows=int(frame["known_air_evaluation_eligible"].sum()),
        )
    altered = frame.with_columns(pl.lit("VALIDATE").alias("primary_fold"))
    altered.write_parquet(path)
    with pytest.raises(ValueError, match="PRIMARY TRAIN"):
        _ = load_bound_coverage_frame(
            path,
            expected_sha256=_sha256(path),
            expected_rows=altered.height,
            expected_games=altered["game_id"].n_unique(),
            expected_eligible_rows=int(altered["known_air_evaluation_eligible"].sum()),
        )


def test_oof_predictions_cover_identical_rows_arms_and_checkpoints(
    tmp_path: Path,
) -> None:
    frame = synthetic_frame()
    predictions, records = build_oof_predictions(
        frame,
        concentrations=(30.0,),
        draws=8,
        checkpoint_root=tmp_path,
        logger=logging.getLogger(__name__),
    )
    eligible = frame.filter(pl.col("known_air_evaluation_eligible"))

    assert predictions.height == eligible.height * 3
    assert len(records) == 14 * len(PREDICTORS)
    assert len(list(tmp_path.rglob("predictive-counts.parquet"))) == len(records)
    for family in ("game", "park", "season"):
        family_frame = predictions.filter(pl.col("split_family") == family)
        assert family_frame.height == eligible.height
        assert family_frame["event_key"].n_unique() == eligible.height
        assert set(family_frame["event_key"].to_list()) == set(
            eligible["event_key"].to_list()
        )
        for predictor in PREDICTORS:
            assert family_frame[f"fit_id_{predictor}"].null_count() == 0
            assert set(family_frame[f"status_{predictor}"].to_list()) <= {
                "posterior_cell",
                "prior_only_cell",
            }
            probabilities = np.asarray(
                family_frame[f"probability_{predictor}"].to_list(), dtype=np.float64
            )
            assert probabilities.shape == (eligible.height, 3)
            assert np.allclose(probabilities.sum(axis=1), 1.0)
    for record in records:
        training = set(cast(list[str], record["training_game_ids"]))
        evaluation = set(cast(list[str], record["evaluation_game_ids"]))
        assert training.isdisjoint(evaluation)


def test_hidden_evaluation_fields_do_not_change_prediction_inputs() -> None:
    frame = synthetic_frame().filter(pl.col("known_air_evaluation_eligible"))
    training = frame.filter(pl.col("season").is_in([2015, 2023]))
    evaluation = frame.filter(pl.col("season").is_in([2019, 2025])).head(20)
    altered = evaluation.with_columns(
        pl.lit("hidden").alias("target_class"),
        pl.lit("hidden").alias("target_status"),
        pl.lit(False).alias("known_air_evaluation_eligible"),
    )
    for predictor in PREDICTORS:
        fit = fit_air_regime(
            training,
            predictor=predictor,
            fit_id=f"hidden-{predictor}",
            draws=8,
        )
        original = predict_air_regime(fit, evaluation)
        hidden = predict_air_regime(fit, altered)
        assert original.events.equals(hidden.events)
        assert len(original.cell_draws) == len(hidden.cell_draws)
        for first, second in zip(original.cell_draws, hidden.cell_draws, strict=True):
            np.testing.assert_array_equal(first.probabilities, second.probabilities)


def test_masked_support_and_unrepresented_regime_fail_closed() -> None:
    frame = synthetic_frame().filter(pl.col("known_air_evaluation_eligible"))
    early = frame.filter(pl.col("season") == 2015)
    late = frame.filter(pl.col("season") == 2025).head(1)
    recorded = fit_air_regime(
        early,
        predictor="recorded_result",
        fit_id="early-recorded",
        draws=8,
    )
    result = fit_air_regime(
        early,
        predictor="result",
        fit_id="early-result",
        draws=8,
    )
    assert predict_air_regime(recorded, late).events["prediction_status"].to_list() == [
        "regime_unsupported"
    ]
    masked = early.head(1).with_columns(
        pl.lit(None, dtype=pl.String).alias("recorded_air_subtype")
    )
    assert predict_air_regime(recorded, masked).events[
        "prediction_status"
    ].to_list() == ["fine_label_missing_unsupported"]
    result_original = predict_air_regime(result, early.head(1)).events
    result_masked = predict_air_regime(result, masked).events
    assert result_original.equals(result_masked)
    blocked = early.head(1).with_columns(
        pl.lit(None, dtype=pl.String).alias("recorded_broad_type")
    )
    assert predict_air_regime(result, blocked).events[
        "prediction_status"
    ].to_list() == ["broad_unknown_unsupported"]


def test_frozen_configuration_and_fit_identity_are_deterministic() -> None:
    assert EXPERIMENT_ID == "geometry-air-regime-posterior-v1"
    assert FULL_CONCENTRATIONS == (3.0, 30.0, 300.0)
    assert PRIMARY_CONCENTRATION == 30.0
    assert FULL_DRAWS == 4096
    assert SMOKE_DRAWS == 512
    assert ROOT_SEED == 20260911
    identities = {
        predictor: fit_identity(30.0, "game", "0", predictor)
        for predictor in PREDICTORS
    }
    assert len(set(identities.values())) == len(PREDICTORS)
    assert identities == {
        predictor: fit_identity(30.0, "game", "0", predictor)
        for predictor in reversed(PREDICTORS)
    }


def test_declared_sources_cover_the_runner_import_chain() -> None:
    paths = declared_source_paths()
    assert {Path(name).name for name in paths} == set(SOURCE_MODULES)
    assert all(name.startswith("python_models/") for name in paths)
    assert all(path.is_file() for path in paths.values())
    listing = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, python_models.statistical.backtests.geometry_air_regime_development as m;"
            "print(' '.join(sorted(n for n in sys.modules if n.startswith('python_models.statistical.backtests.'))))",
        ],
        check=True,
        capture_output=True,
        text=True,
        env={"PYTHONPATH": str(Path(__file__).resolve().parents[2])},
    )
    loaded = {f"{name.rsplit('.', 1)[-1]}.py" for name in listing.stdout.split()}
    assert loaded <= set(SOURCE_MODULES)


def test_run_status_requires_the_frozen_protocol_scale() -> None:
    def status(
        *,
        smoke: bool = False,
        sha: str = SOURCE_FRAME_SHA256,
        concentrations: tuple[float, ...] = FULL_CONCENTRATIONS,
        draws: int = FULL_DRAWS,
        repetitions: int = FULL_BOOTSTRAP_REPETITIONS,
    ) -> str:
        return run_status(
            smoke=smoke,
            source_frame_sha256=sha,
            concentrations=concentrations,
            draws=draws,
            repetitions=repetitions,
        )

    assert status() == FULL_STATUS
    assert status(smoke=True) == SMOKE_STATUS
    assert status(sha="0" * 64) == REDUCED_SCALE_STATUS
    assert status(concentrations=(PRIMARY_CONCENTRATION,)) == REDUCED_SCALE_STATUS
    assert status(draws=SMOKE_DRAWS) == REDUCED_SCALE_STATUS
    assert status(repetitions=5) == REDUCED_SCALE_STATUS


def test_run_experiment_writes_verifiable_smoke_artifact(tmp_path: Path) -> None:
    frame = synthetic_frame()
    source = tmp_path / "frame.parquet"
    frame.write_parquet(source)
    protocol = tmp_path / "protocol.md"
    _ = protocol.write_text("protocol\n")
    run_root = tmp_path / "run"
    smoke_games = fold_covering_games(frame)
    run_experiment(
        frame,
        source_frame=source,
        source_frame_sha256=_sha256(source),
        output_root=run_root,
        protocol_path=protocol,
        smoke=True,
        concentrations=(30.0,),
        draws=8,
        repetitions=5,
        smoke_games=smoke_games,
    )
    manifest = json.loads((run_root / "manifest.json").read_text())
    assert manifest["status"] == SMOKE_STATUS
    bindings = json.loads((run_root / "bindings.json").read_text())
    assert set(bindings["runtime"]) == {
        "python",
        "numpy",
        "polars",
        "scipy",
        "pydantic",
    }
    assert bindings["bootstrap_repetitions"] == 5
    assert bindings["smoke_game_ids"] == smoke_games
    for name, digest in bindings["source_hashes"].items():
        assert _sha256(run_root / SOURCE_DIRECTORY / name) == digest
    predictions = pl.read_parquet(run_root / "oof_predictions.parquet")
    assert set(predictions["game_id"].to_list()) == set(bindings["smoke_game_ids"])
    assert pl.read_parquet(run_root / "coverage_frame.parquet").equals(
        frame.filter(pl.col("game_id").is_in(smoke_games))
    )
    assert manifest["status"] == bindings["expected_status"]
    checkpoints = sorted(run_root.rglob("predictive-counts.parquet"))
    assert len(checkpoints) == 14 * len(PREDICTORS)
    for path in checkpoints:
        counts = pl.read_parquet(path)
        assert counts["split_family"].n_unique() == 1
        assert counts["fold"].n_unique() == 1
        assert (counts["prior_strength"] == 30.0).all()
        games = counts.filter(pl.col("cohort_type") == "game")
        for season_row in counts.filter(pl.col("cohort_type") == "season").iter_rows(
            named=True
        ):
            season_games = games.filter(pl.col("season") == season_row["season"])
            total = np.zeros((8, 3), dtype=np.int64)
            for value in season_games["replicated_counts"]:
                total += np.asarray(value.to_list(), dtype=np.int64)
            np.testing.assert_array_equal(total, season_row["replicated_counts"])
    with pytest.raises(ValueError, match="must not already exist"):
        run_experiment(
            frame,
            source_frame=source,
            source_frame_sha256=_sha256(source),
            output_root=run_root,
            protocol_path=protocol,
            smoke=True,
            concentrations=(30.0,),
            draws=8,
            repetitions=5,
            smoke_games=smoke_games,
        )
    with pytest.raises(ValueError, match="does not match the sorted content"):
        run_experiment(
            frame.head(frame.height - 1),
            source_frame=source,
            source_frame_sha256=_sha256(source),
            output_root=tmp_path / "mismatch",
            protocol_path=protocol,
            smoke=True,
            concentrations=(30.0,),
            draws=8,
            repetitions=5,
            smoke_games=smoke_games,
        )
    assert not (tmp_path / "mismatch").exists()
    with pytest.raises(ValueError, match="only meaningful for a smoke run"):
        run_experiment(
            frame,
            source_frame=source,
            source_frame_sha256=_sha256(source),
            output_root=tmp_path / "full",
            protocol_path=protocol,
            smoke=False,
            concentrations=(30.0,),
            draws=8,
            repetitions=5,
            smoke_games=smoke_games,
        )
