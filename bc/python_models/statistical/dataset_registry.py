"""Per-dataset metadata for the seven ``main_models.model_input_*`` views.

The registry tells the dataset exporter which SQLMesh model to read,
which columns to materialize as stable category maps, what the natural
grain is, and how to count training-eligible truth rows. Every modeling
dataset declared in
``notes/data-coverage-implementation/02-eda-and-modeling-datasets.md``
must appear here before ``prepare-dataset`` will export it.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

_SQLMESH_SCHEMA: str = "main_models"


class DatasetSpec(BaseModel):
    name: str
    sqlmesh_table: str
    dataset_version: str
    grain: tuple[str, ...]
    categorical_columns: tuple[str, ...]
    observed_truth_column: str = Field(
        description=(
            "Boolean-typed column whose TRUE count becomes "
            "``observed_truth_count`` in the dataset metadata. Every "
            "dataset in this registry carries ``training_weight``; the "
            "default predicate is ``training_weight > 0``."
        ),
        default="",
    )
    observed_truth_predicate: str = Field(
        default="training_weight > 0",
        description=(
            "SQL boolean expression evaluated against the dataset view "
            "to derive ``observed_truth_count``."
        ),
    )
    slice_columns: tuple[str, ...] = ("season", "league", "source_family")
    target_columns: tuple[str, ...] = ()

    def qualified_table(self, schema: str = _SQLMESH_SCHEMA) -> str:
        return f"{schema}.{self.sqlmesh_table}"


_COMMON_CATEGORICAL: tuple[str, ...] = (
    "league",
    "game_type",
    "source_type",
    "source_family",
    "target_population_status",
    "park_id",
    "park_episode_status",
    "frame_start",
    "leverage_bucket",
    "batter_hand",
    "pitcher_hand",
    "personnel_confidence",
    "context_confidence",
    "exposure_status",
    "result_family",
    "alignment_regime",
    "primary_fold",
)


def _merge(*parts: tuple[str, ...]) -> tuple[str, ...]:
    seen: dict[str, None] = {}
    for part in parts:
        for col in part:
            seen.setdefault(col, None)
    return tuple(seen)


DATASET_SPECS: dict[str, DatasetSpec] = {
    "model_input_observation_batted_ball": DatasetSpec(
        name="model_input_observation_batted_ball",
        sqlmesh_table="model_input_observation_batted_ball",
        dataset_version="0.2.0",
        grain=("event_key", "dimension"),
        categorical_columns=_merge(
            ("dimension", "observed_status", "sentinel_type", "data_error_risk"),
            _COMMON_CATEGORICAL,
        ),
        slice_columns=(
            "season",
            "league",
            "source_family",
            "sentinel_type",
            "dimension",
        ),
        target_columns=(
            "is_observed",
            "observed_status",
            "raw_value",
            "deduced_value",
        ),
    ),
    "model_input_geometry": DatasetSpec(
        name="model_input_geometry",
        sqlmesh_table="model_input_geometry",
        dataset_version="0.3.0",
        grain=("event_key", "geometry_dimension", "class"),
        categorical_columns=_merge(
            (
                "geometry_dimension",
                "class",
                "observed_status",
                "sentinel_type",
                "data_error_risk",
            ),
            _COMMON_CATEGORICAL,
        ),
        slice_columns=(
            "season",
            "league",
            "source_family",
            "sentinel_type",
            "geometry_dimension",
        ),
        target_columns=("class", "is_observed_class"),
    ),
    "model_input_fielding_credit": DatasetSpec(
        name="model_input_fielding_credit",
        sqlmesh_table="model_input_fielding_credit",
        dataset_version="0.2.0",
        grain=("event_key", "player_id", "fielding_position", "credit_type"),
        categorical_columns=_merge(
            (
                "credit_type",
                "fielding_position",
                "fielding_evidence_status",
                "gap_class",
            ),
            _COMMON_CATEGORICAL,
        ),
        observed_truth_predicate="eligible_for_allocation",
        slice_columns=(
            "season",
            "league",
            "source_family",
            "credit_type",
            "fielding_evidence_status",
        ),
        target_columns=(
            "known_credit",
            "unknown_credit_need",
            "aggregate_residual",
            "eligible_for_allocation",
        ),
    ),
    "model_input_advancement": DatasetSpec(
        name="model_input_advancement",
        sqlmesh_table="model_input_advancement",
        dataset_version="0.3.0",
        grain=("event_key", "baserunner"),
        categorical_columns=_merge(
            (
                "baserunner",
                "base_start",
                "advancement_class",
                "trajectory_class",
                "location_depth_class",
                "ball_handler_position_class",
            ),
            _COMMON_CATEGORICAL,
        ),
        slice_columns=(
            "season",
            "league",
            "source_family",
            "baserunner",
        ),
        target_columns=(
            "advancement_class",
            "trajectory_class",
            "location_depth_class",
            "ball_handler_position_class",
        ),
    ),
    "model_input_pitch_summary": DatasetSpec(
        name="model_input_pitch_summary",
        sqlmesh_table="model_input_pitch_summary",
        dataset_version="0.2.0",
        grain=("event_key",),
        categorical_columns=_merge(
            (
                "count_balls_status",
                "count_strikes_status",
                "pitch_sequence_status",
                "pitch_results_status",
                "strike_types_status",
                "pitch_count_total_status",
            ),
            _COMMON_CATEGORICAL,
        ),
        observed_truth_predicate="has_count",
        slice_columns=(
            "season",
            "league",
            "source_family",
        ),
        target_columns=(
            "count_balls_raw",
            "count_strikes_raw",
            "pitch_sequence_raw",
            "pitch_results_raw",
            "has_count",
            "has_pitch_sequence",
        ),
    ),
    "model_input_responsibility": DatasetSpec(
        name="model_input_responsibility",
        sqlmesh_table="model_input_responsibility",
        dataset_version="0.1.0",
        grain=("event_key",),
        categorical_columns=_merge(
            (
                "ball_handler_position",
                "trajectory_class",
                "location_side_class",
                "location_depth_class",
                "location_edge_class",
                "base_state_start",
                "result_family",
                "alignment_regime",
                "alignment_normal_prior",
                "batter_hand",
            ),
            _COMMON_CATEGORICAL,
        ),
        slice_columns=(
            "season",
            "league",
            "source_family",
            "alignment_regime",
        ),
        target_columns=("ball_handler_position",),
    ),
    "model_input_park_factors": DatasetSpec(
        name="model_input_park_factors",
        sqlmesh_table="model_input_park_factors",
        dataset_version="0.2.0",
        grain=("event_key",),
        categorical_columns=_merge(
            ("home_away",),
            _COMMON_CATEGORICAL,
        ),
        slice_columns=(
            "season",
            "league",
            "source_family",
            "park_id",
            "home_away",
        ),
        target_columns=(
            "plate_appearances",
            "at_bats",
            "hits",
            "singles",
            "doubles",
            "triples",
            "home_runs",
            "walks",
            "intentional_walks",
            "hit_by_pitches",
            "strikeouts",
            "balls_in_play",
            "runs",
        ),
    ),
    "model_input_event_universe": DatasetSpec(
        name="model_input_event_universe",
        sqlmesh_table="model_input_event_universe",
        dataset_version="0.2.0",
        grain=("event_key",),
        categorical_columns=(
            "league",
            "game_type",
            "source_family",
            "park_id",
            "scorer",
            "batter_id",
            "pitcher_id",
            "fielder_pos_2",
            "fielder_pos_3",
            "fielder_pos_4",
            "fielder_pos_5",
            "fielder_pos_6",
            "fielder_pos_7",
            "fielder_pos_8",
            "fielder_pos_9",
            "runner_on_1b_id",
            "runner_on_2b_id",
            "runner_on_3b_id",
            "frame_start",
            "base_state_start",
            "outs_start",
            "alignment_regime",
            "personnel_confidence",
            "context_confidence",
            "time_of_day",
            "doubleheader_status",
            "precipitation",
            "sky",
            "wind_direction",
            "field_condition",
            "result_family",
            "pa_result",
            "trajectory_remapped",
            "r1_advancement",
            "r2_advancement",
            "r3_advancement",
            "batted_location_general",
            "batted_location_depth",
            "batted_location_edge",
            "batted_to_fielder_class",
            "primary_fold",
        ),
        slice_columns=(
            "season",
            "league",
            "source_family",
        ),
        target_columns=(
            "pa_result",
            "result_family",
            "hit_or_out",
            "outs_on_play_capped",
            "runs_on_play_capped",
            "trajectory_remapped",
            "r1_advancement",
            "r2_advancement",
            "r3_advancement",
            "batted_location_general",
            "batted_location_depth",
            "batted_location_edge",
            "batted_to_fielder_class",
        ),
    ),
    "model_input_run_values": DatasetSpec(
        name="model_input_run_values",
        sqlmesh_table="model_input_run_values",
        dataset_version="0.2.0",
        grain=("event_key",),
        categorical_columns=_merge(
            ("denominator_policy",),
            _COMMON_CATEGORICAL,
        ),
        slice_columns=(
            "season",
            "league",
            "source_family",
            "denominator_policy",
        ),
        target_columns=(
            "runs_to_end_of_inning",
            "win_flag",
            "run_expectancy_start_key",
            "run_expectancy_end_key",
            "win_expectancy_start_key",
            "win_expectancy_end_key",
        ),
    ),
}


def get_spec(name: str) -> DatasetSpec:
    try:
        return DATASET_SPECS[name]
    except KeyError as exc:
        known = ", ".join(sorted(DATASET_SPECS))
        raise KeyError(f"unknown dataset {name!r}. known: {known}") from exc


def all_dataset_names() -> tuple[str, ...]:
    return tuple(sorted(DATASET_SPECS))
