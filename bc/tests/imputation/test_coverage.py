from __future__ import annotations

import duckdb

from python_models.imputation.coverage import (
    CoverageOptions,
    EraDefinition,
    audit_coverage,
)
from python_models.imputation.registry import (
    FieldDefinition,
    FieldRegistry,
    FieldScope,
    SourceFamily,
    StatusStrategy,
)


def _field(
    column: str,
    *,
    scope: FieldScope = FieldScope.TARGET,
    sentinel_sql: str = "FALSE",
    applicability_sql: str = "TRUE",
) -> FieldDefinition:
    excluded = scope is FieldScope.OUT_OF_SCOPE
    return FieldDefinition(
        name=f"events_{column}",
        source_relation="main_models.stg_events",
        source_column=column,
        grain_keys=("event_key",),
        family=SourceFamily.EVENT,
        status_strategy=(
            StatusStrategy.METADATA if excluded else StatusStrategy.ESTIMATE
        ),
        sentinel_sql=sentinel_sql,
        applicability_sql=applicability_sql,
        complete_value_target=None if excluded else column,
        fallback_description="Fixture field classification.",
        scope=scope,
        exclusion_reason="Fixture join key." if excluded else None,
    )


def _game_field(column: str) -> FieldDefinition:
    return FieldDefinition(
        name=f"games_{column}",
        source_relation="main_models.game_start_info",
        source_column=column,
        grain_keys=("game_id",),
        family=SourceFamily.GAME_CONTEXT,
        status_strategy=StatusStrategy.METADATA,
        complete_value_target=None,
        fallback_description="Fixture game-spine field.",
        scope=FieldScope.OUT_OF_SCOPE,
        exclusion_reason="Fixture game-spine field.",
    )


def _classified_field(
    relation: str,
    column: str,
    grain: tuple[str, ...],
    *,
    target: bool = False,
) -> FieldDefinition:
    return FieldDefinition(
        name=f"{relation.split('.')[1]}_{column}",
        source_relation=relation,
        source_column=column,
        grain_keys=grain,
        family=SourceFamily.PITCH,
        status_strategy=(
            StatusStrategy.ESTIMATE if target else StatusStrategy.METADATA
        ),
        complete_value_target=column if target else None,
        fallback_description="Implementation pending: fixture pitch classification.",
        scope=FieldScope.TARGET if target else FieldScope.OUT_OF_SCOPE,
        exclusion_reason=None if target else "Fixture pitch support field.",
    )


def _fixture_registry() -> FieldRegistry:
    trajectory_applicability = (
        "event.batted_trajectory IS NOT NULL OR event.batted_to_fielder IS NOT NULL"
    )
    return FieldRegistry(
        fields=(
            _field("game_id", scope=FieldScope.OUT_OF_SCOPE),
            _field("event_key", scope=FieldScope.OUT_OF_SCOPE),
            _field("no_play_flag", scope=FieldScope.OUT_OF_SCOPE),
            _field(
                "batted_trajectory",
                sentinel_sql="CAST({value} AS VARCHAR) = 'Unknown'",
                applicability_sql=trajectory_applicability,
            ),
            _field(
                "batted_to_fielder",
                sentinel_sql="{value} = 0",
                applicability_sql=trajectory_applicability,
            ),
            _field("runs_on_play"),
            _game_field("game_id"),
            _game_field("season"),
            _game_field("source_type"),
        ),
        selected_relations=(
            "main_models.stg_events",
            "main_models.game_start_info",
        ),
    )


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA main_models")
    connection.execute(
        """
        CREATE TABLE main_models.game_start_info (
            game_id VARCHAR,
            season INTEGER,
            source_type VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE main_models.stg_events (
            game_id VARCHAR,
            event_key INTEGER,
            no_play_flag BOOLEAN,
            batted_trajectory VARCHAR,
            batted_to_fielder INTEGER,
            runs_on_play INTEGER
        )
        """
    )
    connection.executemany(
        "INSERT INTO main_models.game_start_info VALUES (?, ?, ?)",
        (
            ("AAA190506010", 1905, "PlayByPlay"),
            ("AAA190506020", 1905, "BoxScore"),
            ("AAA202006010", 2020, "PlayByPlay"),
        ),
    )
    connection.executemany(
        "INSERT INTO main_models.stg_events VALUES (?, ?, ?, ?, ?, ?)",
        (
            ("AAA190506010", 1, False, "Unknown", 0, 0),
            ("AAA190506010", 2, False, "GroundBall", 6, 1),
            ("AAA190506010", 3, True, None, None, None),
            ("AAA190506020", 4, False, "FlyBall", 8, 2),
            ("AAA202006010", 5, False, None, 7, 0),
        ),
    )
    return connection


def test_coverage_partitions_null_sentinel_and_observed_for_pbp_only() -> None:
    report = audit_coverage(
        _connection(),
        _fixture_registry(),
        CoverageOptions(
            season_start=1903,
            season_end=2025,
            eras=(
                EraDefinition(name="early", season_start=1903, season_end=2019),
                EraDefinition(name="modern", season_start=2020, season_end=2025),
            ),
        ),
    )
    by_field = {coverage.field.source_column: coverage for coverage in report.fields}
    trajectory = {era.era: era for era in by_field["batted_trajectory"].eras}
    fielder = {era.era: era for era in by_field["batted_to_fielder"].eras}
    runs = {era.era: era for era in by_field["runs_on_play"].eras}

    assert report.sampled_games == 2
    assert trajectory["early"].model_dump(exclude_none=True) == {
        "era": "early",
        "season_start": 1903,
        "season_end": 2019,
        "rows": 3,
        "applicable": 2,
        "observed": 1,
        "missing": 0,
        "sentinel": 1,
    }
    assert trajectory["modern"].missing == 1
    assert fielder["early"].sentinel == 1
    assert fielder["early"].observed == 1
    assert runs["early"].missing == 1
    assert runs["early"].observed == 2
    assert all(
        era.applicable == era.observed + era.missing + era.sentinel
        for coverage in report.fields
        for era in coverage.eras
    )


def test_sample_limit_uses_deterministic_game_order_and_keeps_pre_1910() -> None:
    report = audit_coverage(
        _connection(),
        _fixture_registry(),
        CoverageOptions(
            season_start=1903,
            season_end=2025,
            sample_game_limit=1,
            eras=(EraDefinition(name="all", season_start=1903, season_end=2025),),
        ),
    )

    trajectory = next(
        coverage
        for coverage in report.fields
        if coverage.field.source_column == "batted_trajectory"
    )
    assert report.sampled_games == 1
    assert trajectory.eras[0].rows == 3
    assert trajectory.eras[0].sentinel == 1


def test_coverage_fails_closed_when_selected_schema_has_unclassified_column() -> None:
    connection = _connection()
    connection.execute(
        "ALTER TABLE main_models.stg_events ADD COLUMN new_detail INTEGER"
    )

    try:
        audit_coverage(connection, _fixture_registry())
    except ValueError as error:
        assert "main_models.stg_events.new_detail" in str(error)
    else:
        raise AssertionError("coverage audit accepted an unclassified source field")


def test_pitch_coverage_counts_absent_and_unresolved_parent_appearances() -> None:
    connection = _connection()
    connection.execute(
        """
        CREATE TABLE main_models.stg_event_pitch_sequences (
            game_id VARCHAR,
            event_key INTEGER,
            sequence_id INTEGER,
            sequence_item VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE main_models.stg_event_pitch_sequence_status (
            game_id VARCHAR,
            event_key INTEGER,
            appearance_start_event_id INTEGER,
            pitch_sequence_resolution_status VARCHAR
        )
        """
    )
    connection.execute(
        "INSERT INTO main_models.stg_event_pitch_sequences VALUES "
        "('AAA190506010', 2, 1, 'Ball')"
    )
    connection.executemany(
        "INSERT INTO main_models.stg_event_pitch_sequence_status VALUES (?, ?, ?, ?)",
        (
            ("AAA190506010", 1, 1, "Unavailable"),
            ("AAA190506010", 2, 2, "Resolved"),
            ("AAA190506010", 3, 3, "Unresolved"),
        ),
    )
    base = _fixture_registry()
    pitch_sequence_relation = "main_models.stg_event_pitch_sequences"
    pitch_status_relation = "main_models.stg_event_pitch_sequence_status"
    pitch_fields = tuple(
        _classified_field(
            pitch_sequence_relation,
            column,
            ("event_key", "sequence_id"),
            target=column == "sequence_item",
        )
        for column in ("game_id", "event_key", "sequence_id", "sequence_item")
    ) + tuple(
        _classified_field(
            pitch_status_relation,
            column,
            ("event_key",),
        )
        for column in (
            "game_id",
            "event_key",
            "appearance_start_event_id",
            "pitch_sequence_resolution_status",
        )
    )
    registry = FieldRegistry(
        fields=base.fields + pitch_fields,
        selected_relations=base.selected_relations
        + (pitch_sequence_relation, pitch_status_relation),
    )

    report = audit_coverage(
        connection,
        registry,
        CoverageOptions(
            season_start=1903,
            season_end=1909,
            eras=(EraDefinition(name="early", season_start=1903, season_end=1909),),
        ),
    )

    sequence_coverage = next(
        coverage
        for coverage in report.fields
        if coverage.field.identifier.endswith(".sequence_item")
    ).eras[0]
    assert sequence_coverage.rows == 1
    assert sequence_coverage.parent_rows == 3
    assert sequence_coverage.source_block_missing == 1
    assert sequence_coverage.source_unresolved == 1
