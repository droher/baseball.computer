"""Assist-count distribution multinomial target.

``assist_count`` fits a cell-grain Multinomial on the per-event realized assist
count ``M in {1, 2, 3, 4}`` per ``(result_family, base_state_start, outs_start)``
cell, restricted to events with at least one assist. The aggregate likelihood
collapses the per-event corpus to a few hundred cells, so the sample-size budget
sits above the corpus and never subsamples a full fit.
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._assist_count_data import (
    prepare_assist_count_inputs,
)
from python_models.statistical.models.assist_count import build_assist_count_model

ASSIST_COUNT = BayesTargetSpec(
    name="assist_count",
    dimension="assist_count",
    dataset_name="model_input_fielding_credit",
    dataset_dimension_filter="assist_count",
    prep_fn=prepare_assist_count_inputs,
    builder=build_assist_count_model,
    sample_size=50_000_000,
    outcome_kind="multinomial",
    multinomial_export="assist_count",
    dl_proposal_dimension=None,
    default_flavors=("gamma_dl_zero",),
)


def _register() -> None:
    register_target(ASSIST_COUNT)


_register()
