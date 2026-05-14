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
        dataset_version="0.1.0",
        grain=("event_key", "dimension"),
        categorical_columns=_merge(
            ("dimension", "observed_status", "sentinel_type", "data_error_risk"),
            _COMMON_CATEGORICAL,
        ),
    ),
    "model_input_geometry": DatasetSpec(
        name="model_input_geometry",
        sqlmesh_table="model_input_geometry",
        dataset_version="0.1.0",
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
    ),
    "model_input_fielding_credit": DatasetSpec(
        name="model_input_fielding_credit",
        sqlmesh_table="model_input_fielding_credit",
        dataset_version="0.1.0",
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
    ),
    "model_input_advancement": DatasetSpec(
        name="model_input_advancement",
        sqlmesh_table="model_input_advancement",
        dataset_version="0.1.0",
        grain=("event_key", "baserunner"),
        categorical_columns=_merge(
            (
                "baserunner",
                "base_start",
                "trajectory_class",
                "location_depth_class",
                "ball_handler_position_class",
            ),
            _COMMON_CATEGORICAL,
        ),
    ),
    "model_input_pitch_summary": DatasetSpec(
        name="model_input_pitch_summary",
        sqlmesh_table="model_input_pitch_summary",
        dataset_version="0.1.0",
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
    ),
    "model_input_park_factors": DatasetSpec(
        name="model_input_park_factors",
        sqlmesh_table="model_input_park_factors",
        dataset_version="0.1.0",
        grain=("event_key",),
        categorical_columns=_merge(
            ("home_away",),
            _COMMON_CATEGORICAL,
        ),
    ),
    "model_input_run_values": DatasetSpec(
        name="model_input_run_values",
        sqlmesh_table="model_input_run_values",
        dataset_version="0.1.0",
        grain=("event_key",),
        categorical_columns=_merge(
            ("denominator_policy",),
            _COMMON_CATEGORICAL,
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
