"""Run-value count targets.

``run_expectancy`` fits a cell-grain NegativeBinomial on summed
``runs_to_end_of_inning`` per ``(state, season, league)`` cell. The aggregate
likelihood collapses the full per-event corpus to a few thousand cells, so the
sample-size budget sits above the corpus and never subsamples a full fit.
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._run_values_data import (
    prepare_run_expectancy_inputs,
)
from python_models.statistical.models.run_values import build_run_expectancy_model

RUN_EXPECTANCY = BayesTargetSpec(
    name="run_expectancy",
    dimension="run_values",
    dataset_name="model_input_run_values",
    dataset_dimension_filter="run_values",
    prep_fn=prepare_run_expectancy_inputs,
    builder=build_run_expectancy_model,
    sample_size=50_000_000,
    outcome_kind="count",
    count_export="run_expectancy",
    dl_proposal_dimension=None,
    default_flavors=("gamma_dl_zero",),
)


def _register() -> None:
    register_target(RUN_EXPECTANCY)


_register()
