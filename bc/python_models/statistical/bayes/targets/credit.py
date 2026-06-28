"""Fielding-credit allocation targets (Model C).

Two registered targets share the dual-arm builder + prep_fn:

* ``putout_credit_allocation`` — K=9 softmax over fielder positions
  for events whose putout fielder is unknown. v1.5 operating point.
* ``assist_credit_allocation`` — K=10 softmax with a NONE sentinel
  class, single-assist cut. v3 cut 1.

Both consume ``model_input_fielding_credit`` filtered to their
respective ``credit_type`` (the assist prep_fn also joins the putout
known_credit rows internally to derive ``putout_position``).
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._credit_data import (
    prepare_event_credit_inputs,
)
from python_models.statistical.models.credit import build_fielding_credit_model

DATASET_NAME: str = "model_input_fielding_credit"
SAMPLE_SIZE: int = 10_000

PUTOUT_CREDIT_ALLOCATION = BayesTargetSpec(
    name="putout_credit_allocation",
    dimension="putout",
    dataset_name=DATASET_NAME,
    dataset_dimension_filter="putout",
    prep_fn=prepare_event_credit_inputs,
    builder=build_fielding_credit_model,
    sample_size=SAMPLE_SIZE,
    outcome_kind="multinomial",
    multinomial_export="credit",
)

ASSIST_CREDIT_ALLOCATION = BayesTargetSpec(
    name="assist_credit_allocation",
    dimension="assist",
    dataset_name=DATASET_NAME,
    dataset_dimension_filter="assist",
    prep_fn=prepare_event_credit_inputs,
    builder=build_fielding_credit_model,
    sample_size=SAMPLE_SIZE,
    outcome_kind="multinomial",
    multinomial_export="credit",
)

CREDIT_SPECS: tuple[BayesTargetSpec, ...] = (
    PUTOUT_CREDIT_ALLOCATION,
    ASSIST_CREDIT_ALLOCATION,
)


def _register() -> None:
    for spec in CREDIT_SPECS:
        register_target(spec)


_register()
