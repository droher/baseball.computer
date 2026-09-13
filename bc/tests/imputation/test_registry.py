from __future__ import annotations

import duckdb
import pytest
from pydantic import ValidationError

from python_models.imputation.registry import (
    FieldDefinition,
    FieldRegistry,
    FieldScope,
    SourceFamily,
    StatusStrategy,
    default_field_registry,
)


EXPECTED_COLUMN_COUNTS = {
    "main_models.stg_events": 34,
    "main_models.game_start_info": 51,
    "main_models.game_results": 28,
    "main_models.stg_event_baserunners": 21,
    "main_models.stg_event_fielding_plays": 6,
    "main_models.stg_event_pitch_sequences": 10,
    "main_models.stg_event_pitch_sequence_status": 8,
    "main_models.event_personnel_lookup": 5,
    "main_models.personnel_lineup_states": 8,
    "main_models.personnel_fielding_states": 8,
}


def test_default_registry_classifies_every_selected_surface_column() -> None:
    registry = default_field_registry()
    counts = {
        relation: sum(field.source_relation == relation for field in registry.fields)
        for relation in registry.selected_relations
    }

    assert counts == EXPECTED_COLUMN_COUNTS
    assert len(registry.fields) == len(registry.classified_columns())
    assert all(
        field.complete_value_target is not None
        for field in registry.fields
        if field.scope is FieldScope.TARGET
    )
    assert all(
        field.exclusion_reason
        for field in registry.fields
        if field.scope is FieldScope.OUT_OF_SCOPE
    )


def test_registry_preserves_unknown_zero_and_source_bookkeeping_semantics() -> None:
    fields = {field.identifier: field for field in default_field_registry().fields}

    assert (
        fields["main_models.stg_events.batted_trajectory"].sentinel_sql
        == "CAST({value} AS VARCHAR) = 'Unknown'"
    )
    assert (
        fields["main_models.stg_events.batted_to_fielder"].sentinel_sql == "{value} = 0"
    )
    assert fields["main_models.game_start_info.attendance"].sentinel_sql == "FALSE"
    assert (
        fields["main_models.game_start_info.source_scorer"].scope
        is FieldScope.OUT_OF_SCOPE
    )
    assert (
        fields["main_models.stg_event_pitch_sequence_status.raw_pitch_sequence"].family
        is SourceFamily.SOURCE_BOOKKEEPING
    )


def test_unclassified_columns_reports_schema_drift() -> None:
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA source")
    connection.execute("CREATE TABLE source.events (event_key INTEGER)")
    field = FieldDefinition(
        name="events_event_key",
        source_relation="source.events",
        source_column="event_key",
        grain_keys=("event_key",),
        family=SourceFamily.EVENT,
        status_strategy=StatusStrategy.OBSERVED,
        complete_value_target="event_key",
        fallback_description="Use the observed key.",
    )
    registry = FieldRegistry(fields=(field,), selected_relations=("source.events",))

    assert registry.unclassified_columns(connection) == frozenset()
    connection.execute("ALTER TABLE source.events ADD COLUMN surprise INTEGER")
    assert registry.unclassified_columns(connection) == frozenset(
        {("source.events", "surprise")}
    )


@pytest.mark.parametrize(
    ("relation", "column"),
    (("main_models.events;drop", "value"), ("main_models.events", "value--bad")),
)
def test_field_definition_rejects_unsafe_identifiers(
    relation: str, column: str
) -> None:
    with pytest.raises(ValidationError):
        FieldDefinition(
            name="unsafe_field",
            source_relation=relation,
            source_column=column,
            grain_keys=("event_key",),
            family=SourceFamily.EVENT,
            status_strategy=StatusStrategy.OBSERVED,
            complete_value_target="value",
            fallback_description="Use the observed value.",
        )


def test_field_definition_rejects_unsafe_sql_expression() -> None:
    with pytest.raises(ValidationError):
        FieldDefinition(
            name="unsafe_expression",
            source_relation="main_models.events",
            source_column="value",
            grain_keys=("event_key",),
            family=SourceFamily.EVENT,
            status_strategy=StatusStrategy.OBSERVED,
            applicability_sql="TRUE; DROP TABLE events",
            complete_value_target="value",
            fallback_description="Use the observed value.",
        )
