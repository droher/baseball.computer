"""ArviZ summaries, posterior predictive checks, simulation recovery."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import arviz as az

    from python_models.statistical.schemas import Diagnostic

_log = logging.getLogger(__name__)


def summarize_posterior(idata: "az.InferenceData") -> list["Diagnostic"]:
    raise NotImplementedError("ArviZ summary wiring lands with the first PyMC fit")


def posterior_predictive_check(idata: "az.InferenceData") -> list["Diagnostic"]:
    raise NotImplementedError("PPC wiring lands with the first PyMC fit")


def simulation_recovery(idata: "az.InferenceData") -> list["Diagnostic"]:
    raise NotImplementedError("simulation recovery wiring lands per-model")
