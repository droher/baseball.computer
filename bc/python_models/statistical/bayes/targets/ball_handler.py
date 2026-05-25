"""Ball-handler imputation target (Model D).

K=9 categorical softmax over the fielder position that handled a batted
ball, for events whose handler is unobserved. Consumes the observation
modeling dataset (``model_input_observation_batted_ball``) filtered to
``dimension='ball_handler_position'``; truth is the directly recorded
handler position.

Distinct from the Model A ``ball_handler_position_observedness`` Bernoulli
target, which predicts *whether* the handler is recorded. These share a
``dataset_dimension_filter`` but differ in ``outcome_kind`` / ``prep_fn`` /
``builder``; the registry keys on ``name``.
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._ball_handler_data import (
    prepare_ball_handler_inputs,
)
from python_models.statistical.models.ball_handler import build_ball_handler_model

DATASET_NAME: str = "model_input_observation_batted_ball"
SAMPLE_SIZE: int = 10_000

BALL_HANDLER_IMPUTATION = BayesTargetSpec(
    name="ball_handler_imputation",
    dimension="ball_handler_position",
    dataset_name=DATASET_NAME,
    dataset_dimension_filter="ball_handler_position",
    prep_fn=prepare_ball_handler_inputs,
    builder=build_ball_handler_model,
    sample_size=SAMPLE_SIZE,
    outcome_kind="multinomial",
    multinomial_export="ball_handler",
)


def _register() -> None:
    register_target(BALL_HANDLER_IMPUTATION)


_register()
