"""Phase-4 fielding-credit allocation targets (Model C).

v1 ships one target: ``putout_credit_allocation``. Reads
``model_input_fielding_credit`` filtered to ``credit_type='putout'``
plus the credit-authority targets parquet (materialized on demand
by ``run_bayes_model`` from ``official_aggregate_availability`` joined
to ``official_credit_authority``).

v2/v3/v4/v5 extensions (player REs, assists, errors, team-residual
fallback) ship as separate targets per the design doc.
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._credit_data import (
    prepare_event_credit_inputs,
)
from python_models.statistical.models.credit import build_fielding_credit_model

DATASET_NAME: str = "model_input_fielding_credit"
SAMPLE_SIZE: int = 50_000

PUTOUT_CREDIT_ALLOCATION = BayesTargetSpec(
    name="putout_credit_allocation",
    dimension="putout",
    dataset_name=DATASET_NAME,
    dataset_dimension_filter="putout",
    prep_fn=prepare_event_credit_inputs,
    builder=build_fielding_credit_model,
    sample_size=SAMPLE_SIZE,
    outcome_kind="multinomial",
)

CREDIT_SPECS: tuple[BayesTargetSpec, ...] = (PUTOUT_CREDIT_ALLOCATION,)


def _register() -> None:
    for spec in CREDIT_SPECS:
        register_target(spec)


_register()
