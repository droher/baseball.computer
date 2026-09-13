from __future__ import annotations

from pathlib import Path

import duckdb

from python_models.imputation.coverage import (
    CoverageOptions,
    EraDefinition,
    audit_coverage,
)
from python_models.imputation.coverage_breakdown import (
    GROUPING_DIMENSIONS,
    build_coverage_breakdown,
    write_breakdown_artifact,
)
from python_models.imputation.registry import (
    FieldDefinition,
    FieldRegistry,
    FieldScope,
    SourceFamily,
    StatusStrategy,
)


def _field(
    relation: str,
    column: str,
    grain: tuple[str, ...],
    *,
    target: bool = False,
    sentinel: str = "FALSE",
    applicability: str = "TRUE",
) -> FieldDefinition:
    return FieldDefinition(
        name=f"{relation.split('.')[1]}_{column}",
        source_relation=relation,
        source_column=column,
        grain_keys=grain,
        family=SourceFamily.EVENT,
        status_strategy=StatusStrategy.ESTIMATE if target else StatusStrategy.METADATA,
        sentinel_sql=sentinel,
        applicability_sql=applicability,
        complete_value_target=column if target else None,
        fallback_description="Implementation pending: fixture coverage field.",
        scope=FieldScope.TARGET if target else FieldScope.OUT_OF_SCOPE,
        exclusion_reason=None if target else "Fixture support column.",
    )


def _registry() -> FieldRegistry:
    game = "main_models.game_start_info"
    event = "main_models.stg_events"
    play = "main_models.stg_event_fielding_plays"
    fields = [
        _field(game, column, ("game_id",))
        for column in (
            "game_id",
            "season",
            "source_type",
            "home_league",
            "away_league",
            "game_type",
        )
    ]
    fields.extend(
        (
            _field(event, "game_id", ("event_key",)),
            _field(event, "event_key", ("event_key",)),
            _field(event, "no_play_flag", ("event_key",)),
            _field(
                event,
                "detail",
                ("event_key",),
                target=True,
                sentinel="{value} = 'Unknown'",
                applicability="NOT event.no_play_flag",
            ),
            _field(play, "game_id", ("event_key", "sequence_id")),
            _field(play, "event_key", ("event_key", "sequence_id")),
            _field(play, "sequence_id", ("event_key", "sequence_id")),
            _field(
                play,
                "credit",
                ("event_key", "sequence_id"),
                target=True,
                sentinel="{value} = 0",
                applicability="NOT event.no_play_flag",
            ),
        )
    )
    return FieldRegistry(
        fields=tuple(fields),
        selected_relations=(game, event, play),
    )


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE SCHEMA main_models")
    connection.execute(
        """
        CREATE TABLE main_models.game_start_info (
            game_id VARCHAR,
            season INTEGER,
            source_type VARCHAR,
            home_league VARCHAR,
            away_league VARCHAR,
            game_type VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO main_models.game_start_info VALUES
          ('EARLY1',1905,'PlayByPlay','AL','AL','RegularSeason'),
          ('MODERN1',2020,'PlayByPlay','NL','AL','WorldSeries'),
          ('BOX1',2020,'BoxScore','NL','NL','RegularSeason')
        """
    )
    connection.execute(
        """
        CREATE TABLE main_models.stg_events (
            game_id VARCHAR, event_key INTEGER, no_play_flag BOOLEAN, detail VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO main_models.stg_events VALUES
          ('EARLY1',1,FALSE,'Known'),
          ('EARLY1',2,FALSE,'Unknown'),
          ('EARLY1',3,TRUE,NULL),
          ('MODERN1',4,FALSE,NULL),
          ('BOX1',5,FALSE,'Known')
        """
    )
    connection.execute(
        """
        CREATE TABLE main_models.stg_event_fielding_plays (
            game_id VARCHAR, event_key INTEGER, sequence_id INTEGER, credit INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO main_models.stg_event_fielding_plays VALUES
          ('EARLY1',1,1,6), ('EARLY1',2,1,0), ('MODERN1',4,1,NULL), ('BOX1',5,1,8)
        """
    )
    return connection


def _options() -> CoverageOptions:
    return CoverageOptions(
        season_start=1903,
        season_end=2025,
        eras=(
            EraDefinition(name="early", season_start=1903, season_end=2019),
            EraDefinition(name="modern", season_start=2020, season_end=2025),
        ),
    )


def test_breakdown_partitions_every_dimension_and_matches_baseline() -> None:
    connection = _connection()
    registry = _registry()
    options = _options()
    baseline = audit_coverage(connection, registry, options)
    report = build_coverage_breakdown(connection, registry, options, baseline)

    assert report.sampled_games == 2
    assert report.sampled_events == 4
    assert report.source_relation_queries == 2
    assert all(item.baseline_match for item in report.reconciliation)
    assert all(item.grouping_dimensions_match for item in report.reconciliation)
    detail = next(
        field
        for field in report.fields
        if field.field_identifier == "main_models.stg_events.detail"
    )
    assert {group.grouping_dimension for group in detail.groups} == set(
        GROUPING_DIMENSIONS
    )
    by_era = {
        group.grouping_value: group
        for group in detail.groups
        if group.grouping_dimension == "era"
    }
    assert by_era["early"].model_dump() | {} == {
        "grouping_dimension": "era",
        "grouping_value": "early",
        "rows": 3,
        "applicable": 2,
        "observed": 1,
        "missing": 0,
        "sentinel": 1,
    }
    assert by_era["modern"].missing == 1
    league_values = {
        group.grouping_value
        for group in detail.groups
        if group.grouping_dimension == "league"
    }
    assert league_values == {"AL", "CrossLeague"}


def test_sample_limit_keeps_whole_game_and_pre1910_rows() -> None:
    connection = _connection()
    options = _options().model_copy(update={"sample_game_limit": 1})
    report = build_coverage_breakdown(connection, _registry(), options)
    assert report.sampled_games == 1
    assert report.sampled_events == 3
    assert {item.baseline_match for item in report.reconciliation} == {None}


def test_writes_content_bound_artifact_folder(tmp_path: Path) -> None:
    connection = _connection()
    report = build_coverage_breakdown(connection, _registry(), _options())
    database = tmp_path / "source.db"
    database.write_bytes(b"read-only-source-identity")
    output = tmp_path / "artifact"
    write_breakdown_artifact(output, report, _registry(), database)
    assert {path.name for path in output.iterdir()} == {
        "coverage_breakdown.json",
        "field_registry.json",
        "manifest.json",
    }
    manifest = (output / "manifest.json").read_text()
    assert '"status": "complete"' in manifest
    assert '"database_sha256"' in manifest
    assert '"report_sha256"' in manifest
