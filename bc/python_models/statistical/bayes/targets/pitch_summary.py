"""Pitch-summary multinomial target.

``pitch_summary`` fits a cell-grain Multinomial on the count vector over the
plate appearance's 12 final ball-strike classes per ``(result_family, season,
league)`` cell, restricted to the ``has_count`` slice. The aggregate likelihood
collapses the slice to a few thousand cells, so the sample-size budget sits
above the corpus and never subsamples a full fit.
"""

from __future__ import annotations

from python_models.statistical.bayes.registry import register_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.models._pitch_summary_data import (
    prepare_pitch_summary_inputs,
)
from python_models.statistical.models.pitch_summary import build_pitch_summary_model

PITCH_SUMMARY = BayesTargetSpec(
    name="pitch_summary",
    dimension="pitch_summary",
    dataset_name="model_input_pitch_summary",
    dataset_dimension_filter="pitch_summary",
    prep_fn=prepare_pitch_summary_inputs,
    builder=build_pitch_summary_model,
    sample_size=50_000_000,
    outcome_kind="multinomial",
    multinomial_export="pitch_summary",
    dl_proposal_dimension=None,
    default_flavors=("gamma_dl_zero",),
)


def _register() -> None:
    register_target(PITCH_SUMMARY)


_register()
