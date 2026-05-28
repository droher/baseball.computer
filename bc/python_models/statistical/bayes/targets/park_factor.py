"""Park-factor count target.

NegativeBinomial team-runs-per-game model with a sum-to-zero
per-(park, season, league) effect as the published park factor. Consumes
the per-event ``model_input_park_factors`` dataset aggregated to team-game
grain.
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._park_factor_data import (
    prepare_park_factor_inputs,
)
from python_models.statistical.models.park_factor import build_park_factor_model

PARK_FACTOR_RUNS = BayesTargetSpec(
    name="park_factor_runs",
    dimension="park_factors",
    dataset_name="model_input_park_factors",
    dataset_dimension_filter="park_factors",
    prep_fn=prepare_park_factor_inputs,
    builder=build_park_factor_model,
    sample_size=500_000,
    outcome_kind="count",
    count_export="park_factor",
    dl_proposal_dimension=None,
    default_flavors=("gamma_dl_zero",),
)


def _register() -> None:
    register_target(PARK_FACTOR_RUNS)


_register()
