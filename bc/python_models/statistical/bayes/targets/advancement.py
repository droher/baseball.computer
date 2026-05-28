"""Runner-advancement target (Model H).

``advancement`` fits a per-(event, baserunner) categorical softmax over the
7 recorded advancement classes, consuming the frozen
``model_input_advancement`` dataset. It reuses the geometry softmax builder
and posterior reconstruction; the prep returns a ``GeometryInputs`` over the
fixed 7-class advancement vocabulary. Carries no DL covariate
(``gamma_dl_zero`` only).
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._advancement_data import (
    prepare_advancement_inputs,
)
from python_models.statistical.models.geometry import build_geometry_model

ADVANCEMENT = BayesTargetSpec(
    name="advancement",
    dimension="advancement",
    dataset_name="model_input_advancement",
    dataset_dimension_filter="advancement",
    prep_fn=prepare_advancement_inputs,
    builder=build_geometry_model,
    sample_size=10_000,
    outcome_kind="multinomial",
    multinomial_export="advancement",
    dl_proposal_dimension=None,
    default_flavors=("gamma_dl_zero",),
)


def _register() -> None:
    register_target(ADVANCEMENT)


_register()
