from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import duckdb
import pytest

from python_models.imputation.geometry_standardization import (
    GeometryStandardizationConfig,
    build_geometry_standardization_parquet_query,
    build_geometry_standardization_query,
)


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA fixture")
    connection.execute(
        """
        CREATE TABLE fixture.air_translation AS
        SELECT *
        FROM (VALUES
            (1989, 'Fly', 'hit', 'Fly', 0.8),
            (1989, 'Fly', 'hit', 'LineDrive', 0.2),
            (1989, 'Fly', 'hit', 'PopUp', 0.0),
            (1989, 'Fly', 'out', 'Fly', 0.6),
            (1989, 'Fly', 'out', 'LineDrive', 0.3),
            (1989, 'Fly', 'out', 'PopUp', 0.1),
            (1989, 'LineDrive', 'hit', 'Fly', 0.1),
            (1989, 'LineDrive', 'hit', 'LineDrive', 0.9),
            (1989, 'LineDrive', 'hit', 'PopUp', 0.0)
        ) AS t(season, recorded_air_subtype, result_family, standardized_air_subtype, probability_mean)
        """
    )
    return connection


def _source_query() -> str:
    return """
    SELECT *
    FROM (VALUES
        (1, 1903, 'Fly', false, NULL, 'estimated', 'old', 'old_basis', true, 0.0, 0.0, 0.0, 0.0, 'keep'),
        (2, 1989, 'LineDrive', false, 'LineDrive', 'estimated', 'valid', 'valid_basis', false, 0.0, 0.1, 0.9, 0.0, 'keep'),
        (3, 1903, 'PopUp', false, NULL, 'estimated', 'old', 'old_basis', true, 0.0, 0.0, 0.0, 0.0, 'keep'),
        (4, 1903, 'GroundBall', false, NULL, 'unresolved', 'old', 'old_basis', true, 0.0, NULL, NULL, NULL, 'keep'),
        (5, 1903, 'PopUpBunt', true, NULL, 'not_applicable_bunt', 'bunt_excluded', 'not_applicable', false, 0.0, 0.0, 0.0, 0.0, 'keep'),
        (6, 1989, 'Fly', false, 'PopUp', 'estimated', 'bad_support', 'old_basis', false, 0.0, 1.0, 0.0, 0.0, 'keep')
    ) AS t(
        event_key, season, trajectory, is_bunt, standardized_trajectory,
        standardized_trajectory_status, standardized_trajectory_method,
        standardized_trajectory_basis, standardized_trajectory_weakly_identified,
        p_standardized_ground_ball, p_standardized_fly,
        p_standardized_line_drive, p_standardized_pop_up, untouched
    )
    """


def _rows(connection: duckdb.DuckDBPyConnection) -> dict[int, dict[str, Any]]:
    config = GeometryStandardizationConfig(
        air_translation_relation="fixture.air_translation"
    )
    relation = connection.sql(
        build_geometry_standardization_query(_source_query(), config)
    )
    columns = relation.columns
    return {row[0]: dict(zip(columns, row, strict=True)) for row in relation.fetchall()}


def test_repairs_only_invalid_nonbunt_standardization() -> None:
    rows = _rows(_connection())

    assert rows[2]["standardized_trajectory_method"] == "valid"
    assert rows[2]["standardized_trajectory_basis"] == "valid_basis"
    assert rows[2]["p_standardized_line_drive"] == pytest.approx(0.9)
    assert rows[5]["standardized_trajectory_status"] == "not_applicable_bunt"
    assert rows[5]["standardized_trajectory"] is None
    assert rows[5]["p_standardized_fly"] == 0.0
    assert {row["untouched"] for row in rows.values()} == {"keep"}

    repaired_fly = rows[1]
    assert repaired_fly["standardized_trajectory_status"] == "estimated_fallback"
    assert repaired_fly["standardized_trajectory_method"] == (
        "standardization_repair_assumed_transport"
    )
    assert repaired_fly["standardized_trajectory_basis"] == (
        "1989_season_recorded_subtype_pooled_over_result_family_assumed_transport"
    )
    assert repaired_fly["p_standardized_fly"] == pytest.approx(0.7)
    assert repaired_fly["p_standardized_line_drive"] == pytest.approx(0.25)
    assert repaired_fly["p_standardized_pop_up"] == pytest.approx(0.05)

    repaired_identity = rows[3]
    assert repaired_identity["standardized_trajectory"] == "PopUp"
    assert repaired_identity["p_standardized_pop_up"] == 1.0
    assert (
        "recorded_subtype_identity"
        in repaired_identity["standardized_trajectory_basis"]
    )

    repaired_ground = rows[4]
    assert repaired_ground["standardized_trajectory"] == "GroundBall"
    assert repaired_ground["p_standardized_ground_ball"] == 1.0
    assert repaired_ground["p_standardized_fly"] == 0.0
    assert repaired_ground["p_standardized_line_drive"] == 0.0
    assert repaired_ground["p_standardized_pop_up"] == 0.0
    assert repaired_ground["standardized_trajectory_weakly_identified"] is False

    repaired_bad_support = rows[6]
    assert repaired_bad_support["standardized_trajectory_method"] == (
        "standardization_repair_translation_fallback"
    )
    chosen_probability = {
        "Fly": repaired_bad_support["p_standardized_fly"],
        "LineDrive": repaired_bad_support["p_standardized_line_drive"],
        "PopUp": repaired_bad_support["p_standardized_pop_up"],
    }[repaired_bad_support["standardized_trajectory"]]
    assert chosen_probability > 0.0


def test_repaired_vectors_are_finite_normalized_and_reproducible() -> None:
    connection = _connection()
    first = _rows(connection)
    second = _rows(connection)

    for event_key in (1, 3, 4, 6):
        row = first[event_key]
        probabilities = [
            row["p_standardized_ground_ball"],
            row["p_standardized_fly"],
            row["p_standardized_line_drive"],
            row["p_standardized_pop_up"],
        ]
        assert all(math.isfinite(value) and value >= 0.0 for value in probabilities)
        assert sum(probabilities) == pytest.approx(1.0)
        assert (
            row["standardized_trajectory"]
            == second[event_key]["standardized_trajectory"]
        )


def test_parquet_wrapper_quotes_paths(tmp_path: Path) -> None:
    connection = _connection()
    parquet_path = tmp_path / "geometry's sample.parquet"
    connection.execute(
        f"COPY ({_source_query()}) TO ? (FORMAT PARQUET)", [str(parquet_path)]
    )
    config = GeometryStandardizationConfig(
        air_translation_relation="fixture.air_translation"
    )

    count = (
        connection.sql(
            build_geometry_standardization_parquet_query(str(parquet_path), config)
        )
        .count("*")
        .fetchone()
    )

    assert count == (6,)


def test_config_validation() -> None:
    with pytest.raises(ValueError, match="invalid air translation relation"):
        GeometryStandardizationConfig(air_translation_relation="bad; DROP TABLE x")
    with pytest.raises(ValueError, match="probability_tolerance"):
        GeometryStandardizationConfig(probability_tolerance=0.0)
