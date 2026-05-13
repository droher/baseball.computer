"""Tiny PyMC smoke fit — skipped when pymc isn't importable."""

from __future__ import annotations

import pytest

pymc = pytest.importorskip("pymc")


def test_normal_mu_unit_variance_smoke() -> None:
    import numpy as np

    rng = np.random.default_rng(seed=20260513)
    observed = rng.normal(loc=2.0, scale=1.0, size=50)

    with pymc.Model() as model:
        _ = pymc.Normal("mu", mu=0.0, sigma=5.0)
        _ = pymc.Normal("y", mu=model.named_vars["mu"], sigma=1.0, observed=observed)
        idata = pymc.sample(
            draws=50,
            tune=50,
            chains=2,
            cores=1,
            random_seed=20260513,
            progressbar=False,
            return_inferencedata=True,
        )

    mu_mean = float(idata.posterior["mu"].mean().item())
    assert 1.0 < mu_mean < 3.0
