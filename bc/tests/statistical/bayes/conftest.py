"""Pytest fixtures for the bayes test directory.

The production saturated-season filter requires 100 rare-class events
per season AND >=0.5% rare-class share. Synthetic test fixtures are
much smaller than that by design, so loosen both thresholds to keep
all synthetic seasons informative unless a specific test opts back
into the production values.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _loosen_saturation_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    from python_models.statistical.models import _event_data

    monkeypatch.setattr(_event_data, "MIN_RARE_CLASS_COUNT_PER_SEASON", 2)
    monkeypatch.setattr(_event_data, "MIN_RARE_CLASS_RATE_PER_SEASON", 0.0)
