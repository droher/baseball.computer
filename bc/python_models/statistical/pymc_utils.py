"""Shared sampler config + prior/posterior predictive helpers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SamplingConfig:
    draws: int
    tune: int
    chains: int
    target_accept: float
    random_seed: int


SMOKE_CONFIG: SamplingConfig = SamplingConfig(
    draws=50,
    tune=50,
    chains=2,
    target_accept=0.8,
    random_seed=20260513,
)


DEFAULT_CONFIG: SamplingConfig = SamplingConfig(
    draws=1000,
    tune=1000,
    chains=4,
    target_accept=0.9,
    random_seed=20260513,
)


def sample_model(model: object, config: SamplingConfig, output_path: object) -> object:
    raise NotImplementedError("PyMC sampling wiring lands with the observation model")


def prior_predictive(model: object, config: SamplingConfig) -> object:
    raise NotImplementedError("prior predictive wiring lands with the observation model")


def posterior_predictive(model: object, idata: object, config: SamplingConfig) -> object:
    raise NotImplementedError("posterior predictive wiring lands with the observation model")
