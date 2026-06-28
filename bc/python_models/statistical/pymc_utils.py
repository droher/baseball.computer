"""Shared sampler config + prior/posterior predictive helpers.

PyMC is imported eagerly at module level. The module itself is lazy-
imported by every CLI handler so ``bc-stats --help`` stays PyMC-free.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnusedCallResult=false, reportCallIssue=false, reportArgumentType=false, reportAny=false, reportExplicitAny=false

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import arviz as az
import pymc as pm

_log = logging.getLogger(__name__)

_PROGRESS_RATE_MS: int = 30_000

NutsBackend = Literal["pymc", "numpyro", "nutpie", "blackjax"]


@dataclass(frozen=True)
class SamplingConfig:
    draws: int
    tune: int
    chains: int
    target_accept: float
    random_seed: int
    cores: int = 1
    max_treedepth: int = 10
    backend: NutsBackend = "pymc"


SMOKE_CONFIG: SamplingConfig = SamplingConfig(
    draws=50,
    tune=50,
    chains=2,
    target_accept=0.8,
    random_seed=20260513,
    cores=1,
    max_treedepth=10,
    backend="numpyro",
)


DEFAULT_CONFIG: SamplingConfig = SamplingConfig(
    draws=1000,
    tune=1000,
    chains=4,
    target_accept=0.95,
    random_seed=20260513,
    cores=1,
    max_treedepth=12,
    backend="nutpie",
)


def _build_nutpie_progress_logger(log_path: Path) -> Any:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    start_monotonic = time.monotonic()

    def _callback(chains: Any) -> None:
        try:
            now = time.monotonic() - start_monotonic
            total = sum(int(c.total_draws) for c in chains)
            done = sum(int(c.finished_draws) for c in chains)
            divergent = sum(int(c.divergences) for c in chains)
            tuning = sum(1 for c in chains if c.tuning)
            pct = (done / total * 100.0) if total else 0.0
            stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            line = (
                f"{stamp} elapsed_s={now:.1f} chains_tuning={tuning} "
                f"draws={done}/{total} ({pct:.2f}%) divergences={divergent}\n"
            )
            with log_path.open("a", encoding="utf-8") as fh:
                fh.write(line)
                fh.flush()
        except Exception:
            pass

    return _callback


def sample_model(
    model: pm.Model,
    config: SamplingConfig,
    output_path: Path | None = None,
    *,
    progress_log_path: Path | None = None,
) -> az.InferenceData:
    """Run NUTS sampling under the active model context."""
    nuts_sampler_kwargs: dict[str, Any] = {}
    if progress_log_path is not None and config.backend == "nutpie":
        nuts_sampler_kwargs["progress_callback"] = _build_nutpie_progress_logger(
            progress_log_path
        )
        nuts_sampler_kwargs["progress_rate"] = _PROGRESS_RATE_MS

    with model:
        idata: az.InferenceData = pm.sample(
            draws=config.draws,
            tune=config.tune,
            chains=config.chains,
            cores=config.cores,
            target_accept=config.target_accept,
            max_treedepth=config.max_treedepth,
            random_seed=config.random_seed,
            return_inferencedata=True,
            progressbar=False,
            nuts_sampler=config.backend,
            nuts_sampler_kwargs=nuts_sampler_kwargs or None,
        )
    warmup_groups = [g for g in idata.groups() if str(g).startswith("warmup_")]
    if warmup_groups:
        idata = az.InferenceData(
            **{
                str(g): getattr(idata, str(g))
                for g in idata.groups()
                if not str(g).startswith("warmup_")
            }
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
