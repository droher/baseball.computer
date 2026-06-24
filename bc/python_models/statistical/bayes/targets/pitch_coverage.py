"""Phase-4 pitch-summary coverage target.

Registers ``pitch_count_observedness`` — an event-grain Bernoulli over
whether the plate appearance's final ball-strike count was observed
(``has_count``), fit on the full ``model_input_pitch_summary`` population.
Structurally the obs-propensity Bernoulli arm with a ``season|league``
cell and ``scorer`` random effect plus sum-to-zero context FEs; it ships
the same ``event_propensity.parquet`` export, stamped with its own
dimension.
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._pitch_coverage_data import (
    DIMENSION,
    prepare_pitch_coverage_inputs,
)
from python_models.statistical.models.pitch_coverage import build_pitch_coverage_model

DATASET_NAME: str = "model_input_pitch_summary"
SAMPLE_SIZE: int = 10_000


PITCH_COUNT_OBSERVEDNESS = BayesTargetSpec(
    name="pitch_count_observedness",
    dimension=DIMENSION,
    dataset_name=DATASET_NAME,
    dataset_dimension_filter=DIMENSION,
    outcome_kind="bernoulli",
    prep_fn=prepare_pitch_coverage_inputs,
    builder=build_pitch_coverage_model,
    sample_size=SAMPLE_SIZE,
)


def _register() -> None:
    register_target(PITCH_COUNT_OBSERVEDNESS)


_register()
