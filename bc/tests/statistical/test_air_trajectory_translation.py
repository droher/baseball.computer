from __future__ import annotations

from pathlib import Path
from typing import cast

import duckdb
import polars as pl
import pytest
import sqlglot

from python_models.statistical.air_trajectory_translation import (
    BASES,
    ESTIMATE_ID,
    MODEL_VERSION,
    PARTIALLY_IDENTIFIED_BASIS,
    RECORDED_AIR_SUBTYPES,
    RESULT_FAMILIES,
    RUN_MANIFEST_SHA256,
    SEED_COLUMNS,
    STANDARDIZED_AIR_SUBTYPES,
    STANDARDIZED_MODEL,
    TRANSLATION_MODEL,
    build_standardized_sql,
    build_translation_sql,
)
from python_models.statistical.backtests.geometry_air_translation_seed import (
    ESTIMATE_PATH,
    SEED_PATH,
    load_report,
    read_seed,
    seed_frame,
    write_seed,
)
from python_models.statistical.bayes.manifest_ingest import (
    ESTIMATED_CONTRACT_COLUMNS,
)
from python_models.statistical.publication_tiers import (
    PublicationTier,
    tier_for,
)

CELL_KEYS = ["season", "recorded_air_subtype", "result_family"]


@pytest.fixture(scope="module")
def report() -> dict[str, object]:
    return load_report(ESTIMATE_PATH)


@pytest.fixture(scope="module")
def seed(report: dict[str, object]) -> pl.DataFrame:
    return seed_frame(report)


def test_report_identity_matches_declared_constants(report: dict[str, object]) -> None:
    assert report["estimate"] == ESTIMATE_ID
    assert report["run_manifest_sha256"] == RUN_MANIFEST_SHA256


def test_seed_frame_covers_every_season_cell_and_band(seed: pl.DataFrame) -> None:
    seasons = sorted(seed.get_column("season").unique().to_list())
    assert seasons == list(range(seasons[0], seasons[-1] + 1))
    expected = len(seasons) * len(RECORDED_AIR_SUBTYPES) * len(RESULT_FAMILIES)
    assert seed.height == expected * len(STANDARDIZED_AIR_SUBTYPES)
    assert seed.columns == list(SEED_COLUMNS)
    assert seed.select(CELL_KEYS + ["standardized_air_subtype"]).is_unique().all()
    per_cell = seed.group_by(CELL_KEYS).agg(
        pl.col("probability_mean").sum().alias("total"),
        pl.len().alias("bands"),
    )
    assert per_cell.height == expected
    assert (per_cell.get_column("bands") == len(STANDARDIZED_AIR_SUBTYPES)).all()
    assert ((per_cell.get_column("total") - 1.0).abs() < 1e-9).all()


def test_seed_intervals_bracket_means_and_flags_follow_basis(
    seed: pl.DataFrame,
) -> None:
    assert (seed["probability_lower_95"] <= seed["probability_mean"]).all()
    assert (seed["probability_mean"] <= seed["probability_upper_95"]).all()
    assert (seed["probability_lower_95"] >= 0.0).all()
    assert (seed["probability_upper_95"] <= 1.0).all()
    assert set(seed["basis"].unique().to_list()) == set(BASES)
    raked = seed["basis"] == PARTIALLY_IDENTIFIED_BASIS
    assert (seed["partially_identified"] == raked).all()
    last_raked = cast(int, seed.filter(raked)["season"].max())
    first_identified = cast(int, seed.filter(~raked)["season"].min())
    assert last_raked < first_identified
    per_season = seed.group_by("season").agg(pl.col("basis").n_unique())
    assert (per_season["basis"] == 1).all()


def test_committed_seed_matches_the_estimate_document(
    seed: pl.DataFrame, tmp_path: Path
) -> None:
    committed = read_seed(SEED_PATH)
    regenerated = tmp_path / "seed.csv"
    assert write_seed(ESTIMATE_PATH, regenerated) == seed.height
    reread = read_seed(regenerated)
    assert committed.equals(reread)
    for column in ("probability_mean", "probability_lower_95", "probability_upper_95"):
        assert ((committed[column] - seed[column]).abs() < 1e-9).all()
    assert committed.drop(
        "probability_mean", "probability_lower_95", "probability_upper_95"
    ).equals(
        seed.drop("probability_mean", "probability_lower_95", "probability_upper_95")
    )


def test_seed_frame_rejects_unknown_basis(report: dict[str, object]) -> None:
    seasons = dict(cast(dict[str, dict[str, object]], report["seasons"]))
    first = next(iter(seasons))
    seasons[first] = {**seasons[first], "basis": "guess"}
    with pytest.raises(ValueError, match="unknown basis"):
        _ = seed_frame({**report, "seasons": seasons})


def test_load_report_rejects_a_different_run(tmp_path: Path) -> None:
    path = tmp_path / "estimate.json"
    _ = path.write_text(
        '{"estimate": "%s", "run_manifest_sha256": "0", "seasons": {}}' % ESTIMATE_ID
    )
    with pytest.raises(ValueError, match="different translation run"):
        _ = load_report(path)


def test_both_models_are_registered_as_estimated() -> None:
    assert tier_for(TRANSLATION_MODEL) is PublicationTier.ESTIMATED
    assert tier_for(STANDARDIZED_MODEL) is PublicationTier.ESTIMATED


def _connection(seed: pl.DataFrame) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    _ = con.execute("CREATE SCHEMA main_seeds")
    _ = con.execute("CREATE SCHEMA main_models")
    _ = con.register("seed_frame", seed)
    _ = con.execute(
        "CREATE TABLE main_seeds.seed_air_trajectory_translation AS "
        "SELECT * FROM seed_frame"
    )
    _ = con.execute(
        "CREATE TABLE main_models.air_trajectory_translation AS "
        + build_translation_sql()
    )
    return con


def test_translation_sql_stamps_the_contract(seed: pl.DataFrame) -> None:
    _ = sqlglot.parse_one(build_translation_sql(), read="duckdb")
    con = _connection(seed)
    table = con.execute("SELECT * FROM main_models.air_trajectory_translation").pl()
    assert table.height == seed.height
    for column in ESTIMATED_CONTRACT_COLUMNS:
        assert table[column].null_count() == 0
    assert set(table["artifact_id"].unique().to_list()) == {ESTIMATE_ID}
    assert set(table["model_name"].unique().to_list()) == {TRANSLATION_MODEL}
    assert set(table["model_version"].unique().to_list()) == {MODEL_VERSION}
    assert set(table["source_snapshot_id"].unique().to_list()) == {RUN_MANIFEST_SHA256}
    assert set(table["observed_status"].unique().to_list()) == {"estimated"}
    assert set(table["confidence_status"].unique().to_list()) == {"exploratory"}
    assert (table["weak_identification_flag"] == table["partially_identified"]).all()
    assert (table["method"] == table["basis"]).all()


def test_standardized_sql_translates_only_covered_airborne_events(
    seed: pl.DataFrame,
) -> None:
    _ = sqlglot.parse_one(build_standardized_sql(), read="duckdb")
    con = _connection(seed)
    raked_season = cast(
        int, seed.filter(pl.col("partially_identified"))["season"].min()
    )
    identified_season = cast(
        int, seed.filter(~pl.col("partially_identified"))["season"].max()
    )
    first_season = cast(int, seed["season"].min())
    geometry = pl.DataFrame(
        {
            "event_key": [1, 2, 3, 4, 5, 6, 7, 8],
            "dimension": ["trajectory"] * 7 + ["location_depth"],
            "observed_status": [
                "observed",
                "observed",
                "observed",
                "derived",
                "observed",
                "observed",
                "observed",
                "observed",
            ],
            "raw_value": [
                "Fly",
                "LineDrive",
                "PopUp",
                "Fly",
                "GroundBall",
                "Fly",
                "PopUpBunt",
                "Fly",
            ],
        }
    )
    context = pl.DataFrame(
        {
            "event_key": [1, 2, 3, 4, 5, 6, 7, 8],
            "season": [
                raked_season,
                identified_season,
                identified_season,
                raked_season,
                raked_season,
                first_season - 1,
                raked_season,
                raked_season,
            ],
            "result_family": [
                "hit",
                "out_in_play",
                "sacrifice",
                "hit",
                "hit",
                "hit",
                "out_in_play",
                "hit",
            ],
        }
    )
    _ = con.register("geometry_frame", geometry)
    _ = con.register("context_frame", context)
    _ = con.execute(
        "CREATE TABLE main_models.event_observation_geometry AS SELECT * FROM geometry_frame"
    )
    _ = con.execute(
        "CREATE TABLE main_models.event_observation_context AS SELECT * FROM context_frame"
    )
    rows = con.execute(build_standardized_sql()).pl()
    assert sorted(rows["event_key"].unique().to_list()) == [1, 2, 3]
    per_event = rows.group_by("event_key").agg(
        pl.col("expected_share").sum().alias("total"), pl.len().alias("bands")
    )
    assert (per_event["bands"] == len(STANDARDIZED_AIR_SUBTYPES)).all()
    assert ((per_event["total"] - 1.0).abs() < 1e-9).all()
    assert set(rows["model_name"].unique().to_list()) == {STANDARDIZED_MODEL}
    for column in ESTIMATED_CONTRACT_COLUMNS:
        assert rows[column].null_count() == 0
    flagged = rows.filter(pl.col("event_key") == 1)
    assert flagged["partially_identified"].all()
    assert flagged["weak_identification_flag"].all()
    assert not rows.filter(pl.col("event_key") == 2)["partially_identified"].any()
    expected = (
        seed.filter(
            (pl.col("season") == raked_season)
            & (pl.col("recorded_air_subtype") == "Fly")
            & (pl.col("result_family") == "hit")
        )
        .sort("standardized_air_subtype")
        .get_column("probability_mean")
    )
    assert (
        (flagged.sort("standardized_air_subtype")["expected_share"] - expected).abs()
        < 1e-12
    ).all()
