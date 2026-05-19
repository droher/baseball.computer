"""Model A observation-propensity targets — 4 dims × 2 gamma_dl flavors."""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models.observation import build_observation_model

DATASET_NAME: str = "model_input_observation_batted_ball"

TRAJECTORY_AIRBALL_CLASSES: tuple[str, ...] = ("Fly", "LineDrive", "PopUp")

TRAJECTORY_OBSERVEDNESS = BayesTargetSpec(
    name="trajectory_observedness",
    dimension="trajectory",
    dataset_name=DATASET_NAME,
    dataset_dimension_filter="trajectory",
    builder=build_observation_model,
    dl_proposal_dimension="trajectory",
)

LOCATION_SIDE_OBSERVEDNESS = BayesTargetSpec(
    name="location_side_observedness",
    dimension="location_side",
    dataset_name=DATASET_NAME,
    dataset_dimension_filter="location_side",
    builder=build_observation_model,
    dl_proposal_dimension="location_side",
)

LOCATION_DEPTH_OBSERVEDNESS = BayesTargetSpec(
    name="location_depth_observedness",
    dimension="location_depth",
    dataset_name=DATASET_NAME,
    dataset_dimension_filter="location_depth",
    builder=build_observation_model,
    dl_proposal_dimension="location_depth",
)

BROAD_CONTACT_OBSERVEDNESS = BayesTargetSpec(
    name="broad_contact_observedness",
    dimension="broad_contact",
    dataset_name=DATASET_NAME,
    dataset_dimension_filter="trajectory",
    builder=build_observation_model,
    dl_proposal_dimension="trajectory",
    dl_class_collapse_positive=TRAJECTORY_AIRBALL_CLASSES,
)

OBSERVATION_SPECS: tuple[BayesTargetSpec, ...] = (
    TRAJECTORY_OBSERVEDNESS,
    LOCATION_SIDE_OBSERVEDNESS,
    LOCATION_DEPTH_OBSERVEDNESS,
    BROAD_CONTACT_OBSERVEDNESS,
)


def _register() -> None:
    for spec in OBSERVATION_SPECS:
        register_target(spec)


_register()
