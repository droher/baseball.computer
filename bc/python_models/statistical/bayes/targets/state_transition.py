"""Base-out transition multinomial target.

``state_transition`` fits a cell-grain Multinomial on the count vector over the
25 end classes (24 base-out states plus an inning-end sentinel) per
``(season, league, start_state)`` cell. The aggregate likelihood collapses the
full per-event corpus to a few thousand cells, so the sample-size budget sits
above the corpus and never subsamples a full fit.
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._state_transition_data import (
    prepare_state_transition_inputs,
)
from python_models.statistical.models.state_transition import (
    build_state_transition_model,
)

STATE_TRANSITION = BayesTargetSpec(
    name="state_transition",
    dimension="state_transition",
    dataset_name="model_input_run_values",
    dataset_dimension_filter="state_transition",
    prep_fn=prepare_state_transition_inputs,
    builder=build_state_transition_model,
    sample_size=50_000_000,
    outcome_kind="multinomial",
    multinomial_export="state_transition",
    dl_proposal_dimension=None,
    default_flavors=("gamma_dl_zero",),
)


def _register() -> None:
    register_target(STATE_TRANSITION)


_register()
