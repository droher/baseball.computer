from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import pytest

from python_models.statistical.geometry_contract import (
    CONTRACT_COLUMN,
    DATASET_VERSIONS,
    GEOMETRY_TARGET_CONTRACT,
    GLOBAL_SIDE_LABELS,
    require_geometry_contract,
    require_geometry_relation,
)
from python_models.statistical.dataset_registry import get_spec
from python_models.statistical.datasets import prepare_dataset
from python_models.statistical.models._geometry_data import (
    GEOMETRY_DIMENSIONS,
    _with_remapped_label_and_season_league,
    prepare_geometry_inputs,
)


def side_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "geometry_dimension": ["location_side"] * len(GLOBAL_SIDE_LABELS),
            "observed_status": ["observed"] * len(GLOBAL_SIDE_LABELS),
            "class": list(GLOBAL_SIDE_LABELS),
            CONTRACT_COLUMN: [GEOMETRY_TARGET_CONTRACT] * len(GLOBAL_SIDE_LABELS),
        }
    )


@pytest.mark.parametrize("bad_contract", [None, "legacy-angle"])
def test_rejects_stale_or_null_contract(bad_contract: str | None) -> None:
    frame = side_frame().with_columns(pl.lit(bad_contract).alias(CONTRACT_COLUMN))
    with pytest.raises(ValueError, match="requires"):
        require_geometry_contract(frame.lazy(), dimension_column="geometry_dimension")


@pytest.mark.parametrize("label", [None, "Default", "Foul", "FoulLine"])
def test_rejects_angle_and_missing_observed_labels(label: str | None) -> None:
    frame = side_frame().with_columns(pl.lit(label).alias("class"))
    with pytest.raises(ValueError, match="mapped domain"):
        require_geometry_contract(
            frame.lazy(), dimension_column="geometry_dimension", check_classes=True
        )


def test_accepts_mapped_side_and_excludes_missing_truth() -> None:
    frame = pl.concat(
        [
            side_frame(),
            side_frame().with_columns(
                pl.lit("derived").alias("observed_status"),
                pl.lit(None).cast(pl.String).alias("class"),
            ),
        ]
    )
    require_geometry_contract(
        frame.lazy(), dimension_column="geometry_dimension", check_classes=True
    )
    assert GEOMETRY_DIMENSIONS["location_side"].class_labels == GLOBAL_SIDE_LABELS
    assert not GEOMETRY_DIMENSIONS["location_side"].dl_active


def test_geometry_uses_mapped_target_without_overwriting_raw_source() -> None:
    frame = side_frame().with_columns(
        pl.lit("Third").alias("raw_value"),
        pl.lit(1990).alias("season"),
        pl.lit("AL").alias("league"),
    )
    result = _with_remapped_label_and_season_league(
        frame.lazy(), remap={}, label_column="class"
    ).collect()
    assert result["_label"].equals(frame["class"].alias("_label"))
    assert result["raw_value"].equals(frame["raw_value"])


def test_legacy_side_fit_fails_before_model_preparation(tmp_path: Path) -> None:
    path = tmp_path / "legacy.parquet"
    side_frame().drop(CONTRACT_COLUMN).write_parquet(path)
    with pytest.raises(ValueError, match="legacy datasets"):
        prepare_geometry_inputs(path, dimension="location_side")


def test_export_cannot_stamp_new_version_on_stale_sql(tmp_path: Path) -> None:
    with duckdb.connect() as con:
        con.execute("CREATE SCHEMA main_models")
        con.execute("CREATE TABLE main_models.model_input_geometry (event_key INT)")
        with pytest.raises(ValueError, match="materialize the corrected SQL"):
            prepare_dataset(
                get_spec("model_input_geometry"),
                artifact_id="new",
                con=con,
                output_root=tmp_path,
            )
    assert not list(tmp_path.rglob("manifest.json"))


def test_relation_guard_checks_values_and_registry_versions() -> None:
    with duckdb.connect() as con:
        con.execute(f"CREATE TABLE target ({CONTRACT_COLUMN} VARCHAR)")
        con.execute("INSERT INTO target VALUES (?)", [GEOMETRY_TARGET_CONTRACT])
        require_geometry_relation(con, "target")
        con.execute("INSERT INTO target VALUES (NULL)")
        with pytest.raises(ValueError, match="stale"):
            require_geometry_relation(con, "target")
    for name, version in DATASET_VERSIONS.items():
        assert get_spec(name).dataset_version == version


@pytest.mark.parametrize("field", ["dl_p_class", "propensity_p_observed"])
def test_stamped_side_still_rejects_legacy_learned_values(field: str) -> None:
    value = (
        pl.lit([0.25] * len(GLOBAL_SIDE_LABELS))
        if field == "dl_p_class"
        else pl.lit(0.5)
    )
    frame = side_frame().with_columns(value.alias(field))
    with pytest.raises(ValueError, match="legacy learned"):
        require_geometry_contract(frame.lazy(), dimension_column="geometry_dimension")
    with duckdb.connect() as con:
        con.register("source", frame.to_arrow())
        with pytest.raises(ValueError, match="legacy learned"):
            require_geometry_relation(
                con, "source", dimension_column="geometry_dimension", check_classes=True
            )


def test_relation_guard_rejects_stamped_angle_classes() -> None:
    frame = side_frame().with_columns(pl.lit("Default").alias("class"))
    with duckdb.connect() as con:
        con.register("source", frame.to_arrow())
        with pytest.raises(ValueError, match="mapped domain"):
            require_geometry_relation(
                con, "source", dimension_column="geometry_dimension", check_classes=True
            )


def test_corrected_side_prepares_with_null_propensities(tmp_path: Path) -> None:
    frame = (
        pl.concat([side_frame()] * 40)
        .with_row_index("event_key")
        .with_columns(
            pl.col("event_key").cast(pl.String).alias("game_id"),
            pl.lit("Third").alias("raw_value"),
            pl.lit(1990).alias("season"),
            pl.lit("AL").alias("league"),
            pl.lit("play_by_play").alias("source_family"),
            pl.lit("park").alias("park_id"),
            pl.lit("scorer").alias("scorer"),
            pl.lit("R").alias("batter_hand"),
            pl.lit(0).alias("base_state_start"),
            pl.lit(1).alias("outs_start"),
            pl.lit("hit").alias("result_family"),
            pl.lit("pre_shift_era").alias("alignment_regime"),
            pl.lit(1.0).alias("training_weight"),
            pl.lit(None).cast(pl.List(pl.Float64)).alias("dl_p_class"),
            pl.lit(None).cast(pl.Float64).alias("propensity_p_observed"),
        )
    )
    path = tmp_path / "side.parquet"
    frame.write_parquet(path)
    inputs = prepare_geometry_inputs(
        path, dimension="location_side", min_events_per_season=1
    )
    assert inputs.class_labels == list(GLOBAL_SIDE_LABELS)
    assert not inputs.propensity_active
    assert not inputs.handler_active
    assert not inputs.dl_active
    assert inputs.counts.sum() + inputs.held_out.n_events == frame.height


def test_side_explicit_learned_flavor_is_rejected_before_fit(tmp_path: Path) -> None:
    from python_models.statistical.bayes.training import run_bayes_model

    with pytest.raises(ValueError, match="zero learned-covariate"):
        run_bayes_model(
            model_name="geometry_location_side",
            dataset_artifact_id="stale",
            artifact_id="new",
            source_snapshot_id="test",
            dataset_root=tmp_path,
            gamma_propensity_flavor="gamma_propensity_class",
        )
