from __future__ import annotations

from collections.abc import Iterator
from typing import cast

import duckdb
import pytest
import sqlglot

from python_models.imputation.geometry import (
    build_geometry_completion_sql as build_original_geometry_completion_sql,
)
from python_models.imputation.geometry_fast import (
    MODEL_NAME,
    OUTPUT_SCHEMA,
    SOURCE_SIGNATURE,
    GeometryCompletionConfig,
    build_geometry_completion_sql,
)


@pytest.fixture()
def connection() -> Iterator[duckdb.DuckDBPyConnection]:
    con = duckdb.connect(":memory:")
    _ = con.execute("CREATE SCHEMA fixture")
    _ = con.execute(
        """
        CREATE TABLE fixture.events (
            event_key UINTEGER, game_id VARCHAR, season SMALLINT,
            batted_trajectory VARCHAR, batted_location_general VARCHAR,
            batted_location_depth VARCHAR, batted_location_angle VARCHAR,
            batted_contact_strength VARCHAR, batted_to_fielder UTINYINT
        )
        """
    )
    _ = con.execute(
        """
        INSERT INTO fixture.events VALUES
          (1, 'P1903', 1903, 'GroundBall', 'Third', 'Default', 'Default', 'Hard', 5),
          (2, 'P1903', 1903, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Default', 0),
          (3, 'P1989', 1989, 'Fly', 'Left', 'Deep', 'Middle', 'Soft', 7),
          (4, 'P1989', 1989, 'PopUpBunt', 'Center', 'Default', 'Default', 'Unknown', 8),
          (5, 'BOX', 1920, 'Fly', 'Right', 'Deep', 'Default', 'Hard', 9),
          (6, 'P2025', 2025, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0),
          (7, 'P1905', 1905, 'Fly', 'Right', 'Deep', 'Default', 'Hard', 9),
          (8, 'P2025', 2025, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Default', NULL),
          (9, 'P1906', 1906, 'GroundBall', 'Pitcher', 'Default', 'Default', 'Soft', 1)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.context AS
        SELECT * FROM (VALUES
          (1, 'P1903', 1903, 'out_in_play', 'R', 'play_by_play', 'event_level'),
          (2, 'P1903', 1903, 'out_in_play', NULL, 'play_by_play', 'event_level'),
          (3, 'P1989', 1989, 'hit', 'L', 'play_by_play', 'event_level'),
          (4, 'P1989', 1989, 'sacrifice', 'R', 'play_by_play', 'event_level'),
          (5, 'BOX', 1920, 'hit', 'R', 'box_score', 'aggregate_only'),
          (6, 'P2025', 2025, 'hit', 'R', 'play_by_play', 'event_level'),
          (7, 'P1905', 1905, 'hit', 'L', 'play_by_play', 'event_level'),
          (8, 'P2025', 2025, NULL, NULL, 'play_by_play', 'event_level')
        ) t(event_key, game_id, season, result_family, batter_hand, source_family, target_population_status)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.batted AS
        SELECT * FROM (VALUES
          (1, 'GroundBall', 'GroundBall', 'Left', 'Infield'),
          (2, 'GroundBall', 'GroundBall', 'Left', 'Infield'),
          (3, 'Fly', 'AirBall', 'Left', 'Outfield'),
          (4, 'PopUpBunt', 'Bunt', 'Middle', 'Outfield'),
          (5, 'Fly', 'AirBall', 'Right', 'Outfield'),
          (6, 'Fly', 'AirBall', 'Unknown', 'Unknown'),
          (7, 'Fly', 'AirBall', 'Right', 'Outfield'),
          (8, 'Unknown', 'AirBall', 'Unknown', 'Unknown'),
          (9, 'GroundBall', 'GroundBall', 'Middle', 'Infield')
        ) t(event_key, trajectory, trajectory_broad_classification, location_side, location_depth)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.offense AS
        SELECT event_key, 'Batter' AS baserunner,
               CASE WHEN event_key = 4 THEN 1 ELSE 0 END AS bunts
        FROM fixture.events
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.location_categories AS
        SELECT * FROM (VALUES
          ('Catcher', 'Plate', 'All', 'All'),
          ('Pitcher', 'Infield', 'Middle', 'Middle'),
          ('First', 'Infield', 'Right', 'Right'),
          ('Second', 'Infield', 'Middle', 'Middle'),
          ('Shortstop', 'Infield', 'Middle', 'Middle'),
          ('Third', 'Infield', 'Left', 'Left'),
          ('Left', 'Outfield', 'Left', 'Left'),
          ('Center', 'Outfield', 'Middle', 'Middle'),
          ('Right', 'Outfield', 'Right', 'Right')
        ) t(batted_location_general, category_depth, category_side, category_edge)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.location_depths AS
        SELECT * FROM (VALUES
          ('Catcher', 'Default'), ('Pitcher', 'Default'), ('First', 'Default'),
          ('Second', 'Default'), ('Shortstop', 'Default'), ('Third', 'Default'),
          ('Third', 'Deep'), ('Left', 'Default'), ('Left', 'Deep'),
          ('Center', 'Default'), ('Center', 'Deep'), ('Right', 'Default'), ('Right', 'Deep')
        ) t(general_location, depth)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.location_angles AS
        SELECT * FROM (VALUES
          ('Catcher', 'Default'), ('Pitcher', 'Default'), ('First', 'Default'),
          ('Second', 'Default'), ('Shortstop', 'Default'), ('Third', 'Default'),
          ('Left', 'Default'), ('Left', 'Middle'), ('Center', 'Default'),
          ('Center', 'Left'), ('Center', 'Right'), ('Right', 'Default'), ('Right', 'Middle')
        ) t(general_location, angle)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.handler_categories AS
        SELECT * FROM (VALUES
          (1, 'Infield', 'Middle'), (2, 'Plate', 'All'), (3, 'Infield', 'Right'),
          (4, 'Infield', 'Middle'), (5, 'Infield', 'Left'), (6, 'Infield', 'Middle'),
          (7, 'Outfield', 'Left'), (8, 'Outfield', 'Middle'), (9, 'Outfield', 'Right')
        ) t(batted_to_fielder, category_depth, category_side)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.air_translation AS
        SELECT
          season, recorded_air_subtype, result_family, standardized_air_subtype,
          CASE standardized_air_subtype WHEN 'Fly' THEN 0.2 WHEN 'LineDrive' THEN 0.7 ELSE 0.1 END AS probability_mean,
          'fixture_basis' AS basis, FALSE AS partially_identified
        FROM (VALUES (1989), (2025)) seasons(season)
        CROSS JOIN (VALUES ('Fly'), ('LineDrive'), ('PopUp')) labels(recorded_air_subtype)
        CROSS JOIN (VALUES ('hit'), ('out_in_play'), ('sacrifice'), ('missing')) results(result_family)
        CROSS JOIN (VALUES ('Fly'), ('LineDrive'), ('PopUp')) bands(standardized_air_subtype)
        """
    )
    yield con
    con.close()


def _config(
    *,
    start_season: int = 1903,
    end_season: int = 2025,
    sample_games: int | None = None,
    sample_seed: str = "fixture",
) -> GeometryCompletionConfig:
    return GeometryCompletionConfig(
        start_season=start_season,
        end_season=end_season,
        sample_games=sample_games,
        sample_seed=sample_seed,
        events_relation="fixture.events",
        context_relation="fixture.context",
        batted_ball_relation="fixture.batted",
        offense_relation="fixture.offense",
        air_translation_relation="fixture.air_translation",
        location_categories_relation="fixture.location_categories",
        location_depths_relation="fixture.location_depths",
        location_angles_relation="fixture.location_angles",
        handler_categories_relation="fixture.handler_categories",
    )


def _rows(
    connection: duckdb.DuckDBPyConnection,
    config: GeometryCompletionConfig | None = None,
) -> duckdb.DuckDBPyRelation:
    return connection.sql(build_geometry_completion_sql(config or _config()))


def _row(rows: duckdb.DuckDBPyRelation, event_key: int) -> dict[str, object]:
    values = rows.filter(f"event_key = {event_key}").fetchone()
    assert values is not None
    return {name: value for name, value in zip(rows.columns, values, strict=True)}


def test_api_exposes_parseable_query_and_declared_schema(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    sql = build_geometry_completion_sql(_config())
    _ = sqlglot.parse_one(sql, read="duckdb")
    columns = connection.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()
    assert tuple(row[0] for row in columns) == tuple(OUTPUT_SCHEMA)
    assert MODEL_NAME == "pbp_completed_geometry"
    assert SOURCE_SIGNATURE == "pbp-geometry-current-source-v1"


def test_preserves_truth_and_covers_all_pbp_seasons(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    rows = _rows(connection)
    assert rows.aggregate(
        "count(*) AS n, min(season) AS first, max(season) AS last"
    ).fetchone() == (
        8,
        1903,
        2025,
    )
    row = _row(rows, 1)
    assert row["trajectory"] == "GroundBall"
    assert row["general_location"] == "Third"
    assert row["location_side"] == "Left"
    assert row["location_depth"] == "Infield"
    assert row["location_edge"] == "Left"
    assert row["contact_strength"] == "Hard"
    assert row["handler_position"] == 5
    assert row["trajectory_status"] == "observed"


def test_joint_location_taxonomy_and_legacy_side_labels_are_excluded(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    rows = _rows(connection)
    invalid = connection.sql(
        f"""
        SELECT COUNT(*)
        FROM ({rows.sql_query()}) AS r
        LEFT JOIN fixture.location_categories AS c
          ON c.batted_location_general = r.general_location
        WHERE c.batted_location_general IS NULL
           OR r.location_side != c.category_side
           OR r.location_depth != c.category_depth
           OR r.location_edge != c.category_edge
           OR r.location_side IN ('Default', 'Foul', 'FoulLine')
        """
    ).fetchone()
    assert invalid == (0,)


def test_default_sentinels_and_missing_context_fallback_are_explicit(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    rows = _rows(connection)
    defaults = _row(rows, 1)
    assert defaults["location_depth_modifier"] in {"Default", "Deep"}
    assert defaults["location_depth_modifier_status"] == "estimated_default_code"
    assert (
        defaults["location_depth_modifier_method"]
        == "default_sentinel_compatible_empirical_draw"
    )
    assert defaults["location_angle"] == "Default"
    assert defaults["location_angle_status"] == "estimated_default_code"
    missing = _row(rows, 8)
    for field in (
        "trajectory",
        "general_location",
        "location_depth_modifier",
        "location_angle",
        "contact_strength",
        "handler_position",
    ):
        assert missing[field] is not None
    assert missing["result_family"] == "missing"
    assert missing["batter_hand"] == "Unknown"
    assert missing["contact_strength"] == "Default"
    assert missing["contact_strength_status"] == "default_code"
    assert missing["contact_strength_method"] == "source_default_unspecified_or_neutral"
    unknown_strength = _row(rows, 6)
    assert unknown_strength["contact_strength"] == "Default"
    assert unknown_strength["contact_strength_status"] == "assumed"
    assert unknown_strength["contact_strength_method"] == "neutral_unspecified_fallback"
    assert missing["raw_handler_position"] is None
    assert (
        abs(
            sum(cast(list[float], missing["trajectory_distribution_probabilities"]))
            - 1.0
        )
        < 1e-12
    )
    assert (
        abs(
            sum(cast(list[float], missing["location_distribution_probabilities"])) - 1.0
        )
        < 1e-12
    )


def test_air_standardization_normalizes_and_never_spills_to_ground_or_bunts(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    rows = _rows(connection)
    air = rows.filter("event_key IN (3, 6, 7)").project(
        "event_key, p_standardized_ground_ball + p_standardized_fly + "
        "p_standardized_line_drive + p_standardized_pop_up AS total"
    )
    assert all(
        abs(float(cast(float, total)) - 1.0) < 1e-12 for _, total in air.fetchall()
    )
    ground = _row(rows, 1)
    assert ground["standardized_trajectory"] == "GroundBall"
    assert ground["p_standardized_ground_ball"] == 1.0
    assert ground["p_standardized_fly"] == 0.0
    bunt = _row(rows, 4)
    assert bunt["standardized_trajectory"] is None
    assert bunt["standardized_trajectory_status"] == "not_applicable_bunt"
    assert (
        sum(
            float(cast(float, bunt[name]))
            for name in (
                "p_standardized_ground_ball",
                "p_standardized_fly",
                "p_standardized_line_drive",
                "p_standardized_pop_up",
            )
        )
        == 0.0
    )
    historical = _row(rows, 7)
    assert (
        historical["standardized_trajectory_basis"]
        == "earliest_compatible_seed_assumed_transport"
    )
    assert historical["standardized_trajectory_weakly_identified"] is True


def test_sampling_is_reproducible_and_whole_game_limited(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    first = _rows(connection, _config(sample_games=2, sample_seed="same")).order(
        "event_key"
    )
    second = _rows(connection, _config(sample_games=2, sample_seed="same")).order(
        "event_key"
    )
    assert first.fetchall() == second.fetchall()
    assert first.aggregate("count(DISTINCT game_id)").fetchone() == (2,)


def test_donor_priors_are_invariant_to_target_season_slice(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    full = _row(_rows(connection), 8)
    sliced = _row(_rows(connection, _config(start_season=2025, end_season=2025)), 8)
    for field in (
        "trajectory",
        "trajectory_distribution_probabilities",
        "general_location",
        "location_distribution_probabilities",
        "contact_strength",
        "handler_position",
    ):
        assert sliced[field] == full[field]


def test_config_rejects_invalid_ranges_and_relations() -> None:
    with pytest.raises(ValueError, match="start_season"):
        _ = GeometryCompletionConfig(start_season=2025, end_season=1903)
    with pytest.raises(ValueError, match="invalid relation"):
        _ = GeometryCompletionConfig(events_relation="events; DROP TABLE events")


def test_fast_query_matches_original_query(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    config = _config()
    original = connection.sql(build_original_geometry_completion_sql(config)).order(
        "event_key"
    )
    fast = _rows(connection, config).order("event_key")
    assert fast.columns == original.columns
    assert fast.fetchall() == original.fetchall()


def test_fast_query_uses_context_distribution_caches() -> None:
    sql = build_geometry_completion_sql(_config())
    assert "LATERAL" not in sql
    for cache in (
        "trajectory_distributions",
        "location_constraint_distributions",
        "location_fallback_distributions",
        "depth_distributions",
        "angle_distributions",
        "handler_distributions",
        "air_translation_aggregated",
    ):
        assert cache in sql
