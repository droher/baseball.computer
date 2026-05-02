"""Metric registry + SQL builder for the 9 metrics_* tables.

Importing this package triggers ``_metric_registrations`` so the global
``METRICS`` dict is populated by the time callers look at it.
"""

from __future__ import annotations

import importlib

# Side effect: populate the registry. Do the import via importlib so the
# resolved name works under both `bc.python_models.metrics` (repo-root
# scripts and the diff harness) and `python_models.metrics` (SQLMesh
# loading from bc/).
importlib.import_module(f"{__name__}._metric_registrations")

from .registry import METRICS, Metric  # noqa: E402

__all__ = ["METRICS", "Metric"]
