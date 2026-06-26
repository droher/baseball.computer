"""Publication-tier registry for the data-coverage published surfaces.

The declared tier of each published ``main_models.*`` surface. The estimated
contract is currently stamped in ``bayes/manifest_ingest`` and enforced by the
``estimated_contract_complete`` audit directly; wiring those to derive their
target set from this registry is the follow-up that makes it the mechanical
source of truth. Model names are stored unqualified (no ``main_models.``
prefix); ``tier_for`` strips a leading ``main_models.`` so callers may pass
either form.
"""

from __future__ import annotations

from enum import Enum


class PublicationTier(str, Enum):
    OFFICIAL = "official"
    DETERMINISTIC = "deterministic"
    ESTIMATED = "estimated"
    SYNTHETIC = "synthetic"
    WITHHELD = "withheld"


_ESTIMATED_MODELS: tuple[str, ...] = (
    "scorer_observation_propensities",
    "pitch_count_coverage",
    "imputed_advancement_probabilities",
    "imputed_ball_handler_probabilities",
    "imputed_batted_ball_geometry",
    "imputed_fielding_credit",
    "park_factor_summary",
    "run_expectancy_summary",
    "pitch_summary_distribution",
    "state_transition_summary",
    "linear_weights_estimated",
)


_DETERMINISTIC_MODELS: tuple[str, ...] = (
    "linear_weights",
    "park_factors",
    "run_expectancy_matrix",
)


PUBLICATION_TIERS: dict[str, PublicationTier] = {
    **{name: PublicationTier.ESTIMATED for name in _ESTIMATED_MODELS},
    **{name: PublicationTier.DETERMINISTIC for name in _DETERMINISTIC_MODELS},
}


def _unqualify(model_name: str) -> str:
    prefix = "main_models."
    if model_name.startswith(prefix):
        return model_name[len(prefix) :]
    return model_name


def tier_for(model_name: str) -> PublicationTier:
    return PUBLICATION_TIERS[_unqualify(model_name)]


def estimated_model_names() -> tuple[str, ...]:
    return _ESTIMATED_MODELS
