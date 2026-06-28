"""Constant parameters (e.g. a lone-park season-league ``theta_park``) have
undefined rhat/ess. The summary must carry them as ``None`` and round-trip
through the manifest JSON, where Pydantic serializes a float NaN as ``null``.
"""

from __future__ import annotations

import math

from python_models.statistical.bayes.training import _finite_or_none
from python_models.statistical.schemas import (
    BayesPosteriorRow,
    BayesPosteriorSummary,
)


def test_finite_or_none_maps_nan_and_inf() -> None:
    assert _finite_or_none(1.0297) == 1.0297
    assert _finite_or_none(float("nan")) is None
    assert _finite_or_none(float("inf")) is None


def test_posterior_row_accepts_none_diagnostics() -> None:
    row = BayesPosteriorRow(
        variable="theta_park[ANA01|1966|AL]",
        mean=0.0,
        sd=0.0,
        hdi_lower=0.0,
        hdi_upper=0.0,
        ess_bulk=None,
        ess_tail=None,
        rhat=None,
    )
    assert row.rhat is None


def test_summary_round_trips_none_through_json() -> None:
    summary = BayesPosteriorSummary(
        rows=(
            BayesPosteriorRow(
                variable="theta_park[lone|1885|UA]",
                mean=0.0,
                sd=0.0,
                hdi_lower=0.0,
                hdi_upper=0.0,
                ess_bulk=None,
                ess_tail=None,
                rhat=None,
            ),
            BayesPosteriorRow(
                variable="phi",
                mean=4.2,
                sd=0.1,
                hdi_lower=4.0,
                hdi_upper=4.4,
                ess_bulk=3200.0,
                ess_tail=3100.0,
                rhat=1.0008,
            ),
        )
    )
    payload = summary.model_dump_json()
    assert "null" in payload
    restored = BayesPosteriorSummary.model_validate_json(payload)
    assert restored.rows[0].rhat is None
    assert restored.rows[1].rhat is not None
    assert math.isclose(restored.rows[1].rhat, 1.0008)
