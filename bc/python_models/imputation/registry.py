from __future__ import annotations

import re
from enum import StrEnum
from typing import Self

from duckdb import DuckDBPyConnection
from pydantic import BaseModel, ConfigDict, Field, model_validator


_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
_RELATION = re.compile(r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")
_UNSAFE_SQL = re.compile(r";|--|/\*|\*/")


class SourceFamily(StrEnum):
    EVENT = "event"
    GAME_CONTEXT = "game_context"
    BASERUNNING = "baserunning"
    FIELDING = "fielding"
    PITCH = "pitch"
    PERSONNEL = "personnel"
    SOURCE_BOOKKEEPING = "source_bookkeeping"


class StatusStrategy(StrEnum):
    OBSERVED = "observed"
    DERIVED = "derived"
    ESTIMATE = "estimate"
    STRUCTURAL = "structural"
    METADATA = "metadata"


class FieldScope(StrEnum):
    TARGET = "target"
    OUT_OF_SCOPE = "out_of_scope"


class FieldDefinition(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    source_relation: str
    source_column: str
    grain_keys: tuple[str, ...] = Field(min_length=1)
    family: SourceFamily
    status_strategy: StatusStrategy
    missing_sql: str = "{value} IS NULL"
    sentinel_sql: str = "FALSE"
    applicability_sql: str = "TRUE"
    complete_value_target: str | None
    fallback_description: str
    scope: FieldScope = FieldScope.TARGET
    exclusion_reason: str | None = None

    @model_validator(mode="after")
    def validate_definition(self) -> Self:
        if not _IDENTIFIER.fullmatch(self.name):
            raise ValueError(f"unsafe field name: {self.name}")
        if not _RELATION.fullmatch(self.source_relation):
            raise ValueError(f"unsafe source relation: {self.source_relation}")
        identifiers = (self.source_column, *self.grain_keys)
        if any(not _IDENTIFIER.fullmatch(value) for value in identifiers):
            raise ValueError("source columns and grain keys must be safe identifiers")
        expressions = (self.missing_sql, self.sentinel_sql, self.applicability_sql)
        if any(_UNSAFE_SQL.search(expression) for expression in expressions):
            raise ValueError("SQL expressions must be single read-only expressions")
        if self.scope is FieldScope.TARGET:
            if self.complete_value_target is None:
                raise ValueError("target fields require a complete-value target")
            if self.exclusion_reason is not None:
                raise ValueError("target fields cannot have an exclusion reason")
        elif not self.exclusion_reason:
            raise ValueError("out-of-scope fields require an exclusion reason")
        return self

    @property
    def identifier(self) -> str:
        return f"{self.source_relation}.{self.source_column}"


class FieldRegistry(BaseModel):
    model_config = ConfigDict(frozen=True)

    fields: tuple[FieldDefinition, ...]
    selected_relations: tuple[str, ...]

    @model_validator(mode="after")
    def validate_registry(self) -> Self:
        identifiers = [field.identifier for field in self.fields]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("field definitions must be unique by relation and column")
        relations = set(self.selected_relations)
        if any(not _RELATION.fullmatch(relation) for relation in relations):
            raise ValueError("selected relations must be safe identifiers")
        if any(field.source_relation not in relations for field in self.fields):
            raise ValueError("every field relation must be selected")
        return self

    def classified_columns(self) -> frozenset[tuple[str, str]]:
        return frozenset(
            (field.source_relation, field.source_column) for field in self.fields
        )

    def unclassified_columns(
        self, connection: DuckDBPyConnection
    ) -> frozenset[tuple[str, str]]:
        actual: set[tuple[str, str]] = set()
        for relation in self.selected_relations:
            schema, table = relation.split(".")
            rows = connection.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                """,
                [schema, table],
            ).fetchall()
            actual.update((relation, str(row[0])) for row in rows)
        return frozenset(actual - self.classified_columns())


def _field(
    relation: str,
    column: str,
    grain: tuple[str, ...],
    family: SourceFamily,
    strategy: StatusStrategy,
    *,
    sentinel: str = "FALSE",
    applicability: str = "TRUE",
    target: str | None = None,
    fallback: str = "Retain source null and represent uncertainty in the field ledger.",
) -> FieldDefinition:
    pending_fallback = (
        fallback
        if fallback.startswith("Implementation pending:")
        else f"Implementation pending: {fallback}"
    )
    return FieldDefinition(
        name=f"{relation.split('.')[1]}_{column}",
        source_relation=relation,
        source_column=column,
        grain_keys=grain,
        family=family,
        status_strategy=strategy,
        sentinel_sql=sentinel,
        applicability_sql=applicability,
        complete_value_target=target or column,
        fallback_description=pending_fallback,
    )


def _excluded(
    relation: str,
    column: str,
    grain: tuple[str, ...],
    family: SourceFamily,
    strategy: StatusStrategy,
    reason: str,
) -> FieldDefinition:
    return FieldDefinition(
        name=f"{relation.split('.')[1]}_{column}",
        source_relation=relation,
        source_column=column,
        grain_keys=grain,
        family=family,
        status_strategy=strategy,
        complete_value_target=None,
        fallback_description="No fallback is permitted for this classified field.",
        scope=FieldScope.OUT_OF_SCOPE,
        exclusion_reason=reason,
    )


def default_field_registry() -> FieldRegistry:
    event = "main_models.stg_events"
    game = "main_models.game_start_info"
    results = "main_models.game_results"
    runners = "main_models.stg_event_baserunners"
    fielding = "main_models.stg_event_fielding_plays"
    pitches = "main_models.stg_event_pitch_sequences"
    pitch_status = "main_models.stg_event_pitch_sequence_status"
    personnel_lookup = "main_models.event_personnel_lookup"
    lineup_states = "main_models.personnel_lineup_states"
    fielding_states = "main_models.personnel_fielding_states"
    event_grain = ("event_key",)
    game_grain = ("game_id",)
    runner_grain = ("event_key", "baserunner")
    fielding_grain = ("event_key", "sequence_id")
    pitch_grain = ("event_key", "sequence_id")

    fields: list[FieldDefinition] = []
    for column in (
        "game_id",
        "event_id",
        "event_key",
        "batting_side",
        "inning",
        "frame",
        "batter_lineup_position",
        "batter_id",
        "pitcher_id",
        "batting_team_id",
        "fielding_team_id",
        "outs",
        "count_balls",
        "count_strikes",
        "base_state",
        "outs_on_play",
        "runs_on_play",
        "runs_batted_in",
        "team_unearned_runs",
        "no_play_flag",
    ):
        family = (
            SourceFamily.PERSONNEL if column.endswith("_id") else SourceFamily.EVENT
        )
        fields.append(
            _field(event, column, event_grain, family, StatusStrategy.OBSERVED)
        )
    for column in (
        "specified_batter_hand",
        "specified_pitcher_hand",
        "strikeout_responsible_batter_id",
        "walk_responsible_pitcher_id",
    ):
        fields.append(
            _field(
                event,
                column,
                event_grain,
                SourceFamily.PERSONNEL,
                StatusStrategy.STRUCTURAL,
                applicability="{value} IS NOT NULL",
                fallback="Leave null outside the rare substitution case where the field applies.",
            )
        )
    fields.append(
        _field(
            event,
            "plate_appearance_result",
            event_grain,
            SourceFamily.EVENT,
            StatusStrategy.DERIVED,
            applicability="NOT event.no_play_flag AND {value} IS NOT NULL",
            fallback="Derive only from an explicit terminal plate-appearance event rule.",
        )
    )
    batted_applicability = (
        "event.batted_trajectory IS NOT NULL OR event.batted_to_fielder IS NOT NULL"
    )
    fields.append(
        _field(
            event,
            "batted_trajectory",
            event_grain,
            SourceFamily.EVENT,
            StatusStrategy.ESTIMATE,
            sentinel="CAST({value} AS VARCHAR) = 'Unknown'",
            applicability=batted_applicability,
            fallback="Complete with an era-aware posterior or broad declared prior; do not withhold solely for low confidence.",
        )
    )
    fields.append(
        _field(
            event,
            "batted_to_fielder",
            event_grain,
            SourceFamily.FIELDING,
            StatusStrategy.ESTIMATE,
            sentinel="{value} = 0",
            applicability=batted_applicability,
            fallback="Use constrained fielding-credit probabilities; zero remains the unknown code.",
        )
    )
    for column in (
        "batted_location_general",
        "batted_location_depth",
        "batted_location_angle",
        "batted_contact_strength",
    ):
        sentinel = "CAST({value} AS VARCHAR) IN ('Unknown', 'Default')"
        fields.append(
            _field(
                event,
                column,
                event_grain,
                SourceFamily.EVENT,
                StatusStrategy.ESTIMATE,
                sentinel=sentinel,
                applicability=batted_applicability,
                fallback="Use an era-aware geometry posterior without treating source defaults as truth.",
            )
        )
    for column in ("date", "season", "runners_count"):
        fields.append(
            _excluded(
                event,
                column,
                event_grain,
                SourceFamily.EVENT,
                StatusStrategy.DERIVED,
                "Deterministic duplicate used for partitioning or convenience.",
            )
        )

    game_estimates = {
        "start_time": "FALSE",
        "time_of_day": "CAST({value} AS VARCHAR) = 'Unknown'",
        "sky": "CAST({value} AS VARCHAR) = 'Unknown'",
        "field_condition": "CAST({value} AS VARCHAR) = 'Unknown'",
        "precipitation": "CAST({value} AS VARCHAR) = 'Unknown'",
        "wind_direction": "CAST({value} AS VARCHAR) = 'Unknown'",
        "park_id": "FALSE",
        "temperature_fahrenheit": "FALSE",
        "attendance": "FALSE",
        "wind_speed_mph": "FALSE",
        "use_dh": "FALSE",
        "official_scorer": "FALSE",
        "umpire_home_id": "FALSE",
        "umpire_first_id": "FALSE",
        "umpire_second_id": "FALSE",
        "umpire_third_id": "FALSE",
        "umpire_left_id": "FALSE",
        "umpire_right_id": "FALSE",
        "home_starting_pitcher_id": "FALSE",
        "away_starting_pitcher_id": "FALSE",
    }
    for column, sentinel in game_estimates.items():
        family = (
            SourceFamily.PERSONNEL
            if column.endswith("_id") and column != "park_id"
            else SourceFamily.GAME_CONTEXT
        )
        fields.append(
            _field(
                game,
                column,
                game_grain,
                family,
                StatusStrategy.ESTIMATE,
                sentinel=sentinel,
                fallback="Estimate with era and source-aware priors and preserve the original value status.",
            )
        )
    for column in (
        "game_id",
        "date",
        "season",
        "home_team_id",
        "away_team_id",
        "doubleheader_status",
        "game_type",
        "bat_first_side",
    ):
        fields.append(
            _field(
                game,
                column,
                game_grain,
                SourceFamily.GAME_CONTEXT,
                StatusStrategy.OBSERVED,
            )
        )
    for column in (
        "scorer",
        "source_scorer",
        "scoring_method",
        "source_type",
        "filename",
    ):
        fields.append(
            _excluded(
                game,
                column,
                game_grain,
                SourceFamily.SOURCE_BOOKKEEPING,
                StatusStrategy.METADATA,
                "Source bookkeeping is retained verbatim and must not be imputed.",
            )
        )
    for column in (
        "is_regular_season",
        "is_postseason",
        "is_integrated",
        "is_negro_leagues",
        "is_segregated_white",
        "away_franchise_id",
        "home_franchise_id",
        "away_league",
        "home_league",
        "away_division",
        "home_division",
        "away_team_name",
        "home_team_name",
        "is_interleague",
        "lineup_map_away",
        "lineup_map_home",
        "fielding_map_away",
        "fielding_map_home",
    ):
        fields.append(
            _excluded(
                game,
                column,
                game_grain,
                SourceFamily.GAME_CONTEXT,
                StatusStrategy.DERIVED,
                "Derived duplicate or structured downstream lookup, outside scalar imputation.",
            )
        )

    for column in ("game_id", "event_id", "event_key", "baserunner"):
        fields.append(
            _field(
                runners,
                column,
                runner_grain,
                SourceFamily.BASERUNNING,
                StatusStrategy.OBSERVED,
            )
        )
    for column in (
        "runner_lineup_position",
        "runner_id",
        "charge_event_id",
        "reached_on_event_id",
        "explicit_charged_pitcher_id",
        "attempted_advance_to_base",
        "baserunning_play_type",
        "is_out",
        "base_end",
        "advanced_on_error_flag",
        "explicit_out_flag",
        "run_scored_flag",
        "rbi_flag",
    ):
        strategy = (
            StatusStrategy.ESTIMATE
            if column
            in {
                "attempted_advance_to_base",
                "baserunning_play_type",
                "base_end",
            }
            else StatusStrategy.OBSERVED
        )
        fields.append(
            _field(
                runners,
                column,
                runner_grain,
                SourceFamily.BASERUNNING,
                strategy,
                applicability={
                    "reached_on_event_id": "src.baserunner::VARCHAR <> 'Batter'",
                    "explicit_charged_pitcher_id": "{value} IS NOT NULL",
                    "attempted_advance_to_base": "src.is_advance_attempt",
                    "baserunning_play_type": "{value} IS NOT NULL",
                    "base_end": "NOT src.is_out",
                }.get(column, "TRUE"),
                fallback="Constrain any estimate by event state and aggregate game totals.",
            )
        )
    for column in (
        "reached_on_event_key",
        "charge_event_key",
        "baserunner_bit",
        "is_advance_attempt",
    ):
        fields.append(
            _excluded(
                runners,
                column,
                runner_grain,
                SourceFamily.BASERUNNING,
                StatusStrategy.DERIVED,
                "Deterministically derived from another classified baserunner field.",
            )
        )

    for column in ("game_id", "event_id", "event_key", "sequence_id"):
        fields.append(
            _field(
                fielding,
                column,
                fielding_grain,
                SourceFamily.FIELDING,
                StatusStrategy.OBSERVED,
            )
        )
    for column in ("fielding_position", "fielding_play"):
        fields.append(
            _field(
                fielding,
                column,
                fielding_grain,
                SourceFamily.FIELDING,
                StatusStrategy.ESTIMATE,
                sentinel="{value} = 0" if column == "fielding_position" else "FALSE",
                fallback="Allocate only within event and box-score fielding constraints.",
            )
        )

    for column in ("game_id", "event_id", "event_key", "sequence_id"):
        fields.append(
            _field(
                pitches,
                column,
                pitch_grain,
                SourceFamily.PITCH,
                StatusStrategy.OBSERVED,
            )
        )
    fields.append(
        _field(
            pitches,
            "sequence_item",
            pitch_grain,
            SourceFamily.PITCH,
            StatusStrategy.ESTIMATE,
            sentinel="CAST({value} AS VARCHAR) IN ('Unknown', 'Unrecognized', 'StrikeUnknownType')",
            fallback="Estimate only within a resolved appearance and conserve the known count.",
        )
    )
    for column in (
        "runners_going_flag",
        "blocked_by_catcher_flag",
        "catcher_pickoff_attempt_at_base",
    ):
        fields.append(
            _field(
                pitches,
                column,
                pitch_grain,
                SourceFamily.PITCH,
                StatusStrategy.ESTIMATE,
                applicability="{value} IS NOT NULL"
                if column == "catcher_pickoff_attempt_at_base"
                else "TRUE",
                fallback="Treat source silence as unknown rather than proof the action did not occur.",
            )
        )
    for column in ("date", "season"):
        fields.append(
            _excluded(
                pitches,
                column,
                pitch_grain,
                SourceFamily.PITCH,
                StatusStrategy.DERIVED,
                "Deterministic partition duplicate.",
            )
        )

    for column in ("game_id", "event_id", "event_key", "appearance_start_event_id"):
        fields.append(
            _field(
                pitch_status,
                column,
                event_grain,
                SourceFamily.PITCH,
                StatusStrategy.OBSERVED,
            )
        )
    fields.append(
        _excluded(
            pitch_status,
            "pitch_sequence_resolution_status",
            event_grain,
            SourceFamily.SOURCE_BOOKKEEPING,
            StatusStrategy.METADATA,
            "Parser resolution is retained verbatim; completing a sequence cannot change source resolution.",
        )
    )
    fields.append(
        _excluded(
            pitch_status,
            "raw_pitch_sequence",
            event_grain,
            SourceFamily.SOURCE_BOOKKEEPING,
            StatusStrategy.METADATA,
            "Raw parser evidence is retained verbatim and must not be imputed.",
        )
    )
    for column in ("date", "season"):
        fields.append(
            _excluded(
                pitch_status,
                column,
                event_grain,
                SourceFamily.PITCH,
                StatusStrategy.DERIVED,
                "Deterministic partition duplicate.",
            )
        )

    for column in ("game_id", "event_id", "event_key"):
        fields.append(
            _field(
                personnel_lookup,
                column,
                event_grain,
                SourceFamily.PERSONNEL,
                StatusStrategy.OBSERVED,
            )
        )
    for column in ("personnel_lineup_key", "personnel_fielding_key"):
        fields.append(
            _field(
                personnel_lookup,
                column,
                event_grain,
                SourceFamily.PERSONNEL,
                StatusStrategy.DERIVED,
                fallback="Reconstruct from audited substitution ranges before estimating personnel.",
            )
        )

    lineup_grain = ("personnel_lineup_key", "lineup_position")
    for column in (
        "game_id",
        "batting_team_id",
        "batting_side",
        "personnel_lineup_key",
        "player_id",
        "lineup_position",
    ):
        fields.append(
            _field(
                lineup_states,
                column,
                lineup_grain,
                SourceFamily.PERSONNEL,
                StatusStrategy.OBSERVED,
                fallback="Reconcile against starting lineups and substitution appearances.",
            )
        )
    for column in ("start_event_id", "end_event_id"):
        fields.append(
            _excluded(
                lineup_states,
                column,
                lineup_grain,
                SourceFamily.PERSONNEL,
                StatusStrategy.DERIVED,
                "Deterministic state-range boundary.",
            )
        )

    personnel_fielding_grain = ("personnel_fielding_key", "fielding_position")
    for column in (
        "game_id",
        "fielding_team_id",
        "fielding_side",
        "personnel_fielding_key",
        "player_id",
        "fielding_position",
    ):
        fields.append(
            _field(
                fielding_states,
                column,
                personnel_fielding_grain,
                SourceFamily.PERSONNEL,
                StatusStrategy.OBSERVED,
                fallback="Reconcile against starting fielders and substitution appearances.",
            )
        )
    for column in ("start_event_id", "end_event_id"):
        fields.append(
            _excluded(
                fielding_states,
                column,
                personnel_fielding_grain,
                SourceFamily.PERSONNEL,
                StatusStrategy.DERIVED,
                "Deterministic state-range boundary.",
            )
        )

    for column in (
        "game_id",
        "season",
        "game_type",
        "game_finish_date",
        "home_team_id",
        "away_team_id",
        "winning_team_id",
        "losing_team_id",
        "winning_team_score",
        "losing_team_score",
        "winning_side",
        "losing_side",
        "forfeit_flag",
        "suspension_flag",
        "tie_flag",
        "winning_pitcher_id",
        "losing_pitcher_id",
        "save_pitcher_id",
        "game_winning_rbi_player_id",
        "home_runs_scored",
        "away_runs_scored",
        "away_line_score",
        "home_line_score",
        "duration_minutes",
        "duration_outs",
        "is_nine_inning_game",
        "is_extra_inning_game",
        "is_shortened_game",
    ):
        if column == "duration_minutes":
            fields.append(
                _field(
                    results,
                    column,
                    game_grain,
                    SourceFamily.GAME_CONTEXT,
                    StatusStrategy.ESTIMATE,
                    fallback="Use era, park, month, and day-aware empirical duration donors from PBP games.",
                )
            )
        elif column in {"winning_pitcher_id", "losing_pitcher_id"}:
            fields.append(
                _field(
                    results,
                    column,
                    game_grain,
                    SourceFamily.PERSONNEL,
                    StatusStrategy.OBSERVED,
                    applicability="NOT src.tie_flag AND NOT src.forfeit_flag",
                    fallback="Use eligible participant uncertainty when an applicable decision is unrecorded.",
                )
            )
        elif column in {"save_pitcher_id", "game_winning_rbi_player_id"}:
            fields.append(
                _field(
                    results,
                    column,
                    game_grain,
                    SourceFamily.PERSONNEL,
                    StatusStrategy.STRUCTURAL,
                    applicability="{value} IS NOT NULL",
                    fallback="Null denotes no recorded optional award; do not fabricate an award or an identity.",
                )
            )
        else:
            fields.append(
                _excluded(
                    results,
                    column,
                    game_grain,
                    SourceFamily.GAME_CONTEXT,
                    StatusStrategy.DERIVED,
                    "Derived from the classified game/event spine and retained in completed game views.",
                )
            )

    return FieldRegistry(
        fields=tuple(fields),
        selected_relations=(
            event,
            game,
            results,
            runners,
            fielding,
            pitches,
            pitch_status,
            personnel_lookup,
            lineup_states,
            fielding_states,
        ),
    )
