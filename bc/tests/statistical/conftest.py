"""Shared fixtures for the statistical test tree.

The published-pointer resolver falls back to the global published root
(`artifacts/statistical/published/`) when the per-branch root has no
match. On a developer machine that directory holds real pointers, so a
test that only redirects the branch root would still resolve real
artifacts. Neutralize the global root to an empty tmp dir for every test
so the suite stays hermetic and order-independent.
"""

from __future__ import annotations

import pytest

from python_models.statistical import config as cfg


@pytest.fixture(autouse=True)
def _neutralize_global_published_root(
    tmp_path_factory: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    empty = tmp_path_factory.mktemp("global_published_neutral")
    monkeypatch.setenv(cfg.ENV_GLOBAL_PUBLISHED_ROOT, str(empty))
