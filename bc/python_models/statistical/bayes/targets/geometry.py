"""Batted-ball geometry imputation targets (Model E).

Per-dimension categorical softmax over the directly recorded geometry
class, for events whose class is unobserved. Consumes the frozen
``model_input_geometry`` dataset filtered to ``geometry_dimension=<dim>``;
truth is the directly recorded class.

The three DL-backed dimensions (trajectory, location_depth, location_edge) carry a published DL proposal whose per-event probabilities
feed the gamma_dl_shrunk flavor, so they register both flavors.
general_location and corrected global location_side are gamma_dl_zero-only.
Corrected global side also excludes propensity and handler inputs.
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import (
    BayesTargetSpec,
    GammaDlFlavor,
    GammaPropensityFlavor,
)
from python_models.statistical.models._geometry_data import prepare_geometry_inputs
from python_models.statistical.models.geometry import build_geometry_model

DATASET_NAME: str = "model_input_geometry"
SAMPLE_SIZE: int = 10_000

DL_DIMENSIONS: tuple[str, ...] = (
    "trajectory",
    "location_depth",
    "location_edge",
)
ZERO_FLAVOR_DIMENSIONS: tuple[str, ...] = ("general_location", "location_side")

_DL_FLAVORS: tuple[GammaDlFlavor, ...] = ("gamma_dl_zero", "gamma_dl_shrunk")
_ZERO_FLAVORS: tuple[GammaDlFlavor, ...] = ("gamma_dl_zero",)
_PROPENSITY_FLAVORS: tuple[GammaPropensityFlavor, ...] = (
    "gamma_propensity_zero",
    "gamma_propensity_class",
)


def _geometry_spec(
    dimension: str,
    *,
    dl_proposal_dimension: str | None,
    default_flavors: tuple[GammaDlFlavor, ...],
) -> BayesTargetSpec:
    return BayesTargetSpec(
        name=f"geometry_{dimension}",
        dimension=dimension,
        dataset_name=DATASET_NAME,
        dataset_dimension_filter=dimension,
        prep_fn=prepare_geometry_inputs,
        builder=build_geometry_model,
        sample_size=SAMPLE_SIZE,
        outcome_kind="multinomial",
        multinomial_export="geometry",
        dl_proposal_dimension=dl_proposal_dimension,
        default_flavors=default_flavors,
        propensity_dimension=dimension,
        default_propensity_flavors=(
            ("gamma_propensity_zero",)
            if dimension == "location_side"
            else _PROPENSITY_FLAVORS
        ),
    )


GEOMETRY_TARGETS: tuple[BayesTargetSpec, ...] = (
    *(
        _geometry_spec(
            dimension,
            dl_proposal_dimension=dimension,
            default_flavors=_DL_FLAVORS,
        )
        for dimension in DL_DIMENSIONS
    ),
    *(
        _geometry_spec(
            dimension,
            dl_proposal_dimension=None,
            default_flavors=_ZERO_FLAVORS,
        )
        for dimension in ZERO_FLAVOR_DIMENSIONS
    ),
)


def _register() -> None:
    for spec in GEOMETRY_TARGETS:
        register_target(spec)


_register()
