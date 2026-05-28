"""Fielding-responsibility target (Model I).

``responsibility`` fits a per-event categorical softmax over the range fielder
position (3..9) that handled a batted ball, used as the responsibility /
opportunity proxy, conditioned on recorded batted-ball geometry and the
era-normal alignment basis. It consumes the frozen ``model_input_responsibility``
dataset and reuses the geometry softmax builder; the prep returns a
``GeometryInputs`` over the fixed 7-position vocabulary. Its designed shift
input (Model K) is unavailable, so it runs on the era-normal alignment fallback
and carries no DL covariate (``gamma_dl_zero`` only).
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._responsibility_data import (
    prepare_responsibility_inputs,
)
from python_models.statistical.models.geometry import build_geometry_model

RESPONSIBILITY = BayesTargetSpec(
    name="responsibility",
    dimension="responsibility",
    dataset_name="model_input_responsibility",
    dataset_dimension_filter="responsibility",
    prep_fn=prepare_responsibility_inputs,
    builder=build_geometry_model,
    sample_size=10_000,
    outcome_kind="multinomial",
    multinomial_export="responsibility",
    dl_proposal_dimension=None,
    default_flavors=("gamma_dl_zero",),
)


def _register() -> None:
    register_target(RESPONSIBILITY)


_register()
