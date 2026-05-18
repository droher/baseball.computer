"""Shared sampler config + prior/posterior predictive helpers.

PyMC is imported eagerly at module level. The module itself is lazy-
imported by every CLI handler so ``bc-stats --help`` stays PyMC-free.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnusedCallResult=false

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import arviz as az
import pymc as pm

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SamplingConfig:
    draws: int
    tune: int
    chains: int
    target_accept: float
    random_seed: int
    cores: int = 1


SMOKE_CONFIG: SamplingConfig = SamplingConfig(
    draws=50,
    tune=50,
    chains=2,
    target_accept=0.8,
    random_seed=20260513,
    cores=1,
)


DEFAULT_CONFIG: SamplingConfig = SamplingConfig(
    draws=1000,
    tune=1000,
    chains=4,
    target_accept=0.9,
    random_seed=20260513,
    cores=1,
)


def sample_model(
    model: pm.Model, config: SamplingConfig, output_path: Path | None = None
) -> az.InferenceData:
    """Run NUTS sampling under the active model context."""
    with model:
        idata: az.InferenceData = pm.sample(
            draws=config.draws,
            tune=config.tune,
            chains=config.chains,
            cores=config.cores,
            target_accept=config.target_accept,
            random_seed=config.random_seed,
            return_inferencedata=True,
            progressbar=False,
        )
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        idata.to_netcdf(str(output_path))
    return idata


def prior_predictive(model: pm.Model, config: SamplingConfig) -> az.InferenceData:
    with model:
        return pm.sample_prior_predictive(
            draws=config.draws,
            random_seed=config.random_seed,
        )


def posterior_predictive(
    model: pm.Model, idata: az.InferenceData, config: SamplingConfig
) -> az.InferenceData:
    with model:
        return pm.sample_posterior_predictive(
            idata,
            random_seed=config.random_seed,
            progressbar=False,
        )
