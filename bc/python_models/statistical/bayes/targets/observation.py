"""Phase-4 observation propensity targets.

One ``BayesTargetSpec`` per ``event_observation_geometry`` dimension that
has both observed and unobserved rows. ``pulled_opposite`` is excluded
upstream — it is purely derived (0% observed). All targets share the
same prep + builder and run at a 10K-row training budget per the
sample-size sweep finding (AUC ceiling hit by 10K; calibration plateau
by 100K).
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._event_data import (
    prepare_event_observation_inputs,
)
from python_models.statistical.models.observation import build_observation_model

DATASET_NAME: str = "model_input_observation_batted_ball"
SAMPLE_SIZE: int = 10_000


def _spec(name: str, dimension: str) -> BayesTargetSpec:
    return BayesTargetSpec(
        name=name,
        dimension=dimension,
        dataset_name=DATASET_NAME,
        dataset_dimension_filter=dimension,
        prep_fn=prepare_event_observation_inputs,
        builder=build_observation_model,
        sample_size=SAMPLE_SIZE,
    )


TRAJECTORY_OBSERVEDNESS = _spec("trajectory_observedness", "trajectory")
LOCATION_SIDE_OBSERVEDNESS = _spec("location_side_observedness", "location_side")
LOCATION_DEPTH_OBSERVEDNESS = _spec("location_depth_observedness", "location_depth")
LOCATION_EDGE_OBSERVEDNESS = _spec("location_edge_observedness", "location_edge")
GENERAL_LOCATION_OBSERVEDNESS = _spec(
    "general_location_observedness", "general_location"
)
BALL_HANDLER_POSITION_OBSERVEDNESS = _spec(
    "ball_handler_position_observedness", "ball_handler_position"
)

OBSERVATION_SPECS: tuple[BayesTargetSpec, ...] = (
    TRAJECTORY_OBSERVEDNESS,
    LOCATION_SIDE_OBSERVEDNESS,
    LOCATION_DEPTH_OBSERVEDNESS,
    LOCATION_EDGE_OBSERVEDNESS,
    GENERAL_LOCATION_OBSERVEDNESS,
    BALL_HANDLER_POSITION_OBSERVEDNESS,
)


def _register() -> None:
    for spec in OBSERVATION_SPECS:
        register_target(spec)


_register()
