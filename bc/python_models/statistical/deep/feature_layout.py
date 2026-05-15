"""FeatureLayout registry for coverage-target deep training.

Re-exports ``FeatureLayout`` from the legacy ML package so deep callers
don't reach into ``python_models.ml`` directly. Provides a per-dataset
registry; PR1 ships the type wiring, PR3+ registers real layouts as
each target ships.
"""

from __future__ import annotations

from python_models.ml.features import FeatureLayout

__all__ = ("FeatureLayout", "coverage_layout_for", "register_coverage_layout")

_REGISTRY: dict[str, FeatureLayout] = {}


def register_coverage_layout(dataset_name: str, layout: FeatureLayout) -> None:
    """Idempotent overwrite — re-registering the same dataset is a no-op."""
    _REGISTRY[dataset_name] = layout


def coverage_layout_for(dataset_name: str) -> FeatureLayout:
    try:
        return _REGISTRY[dataset_name]
    except KeyError as exc:
        known = ", ".join(sorted(_REGISTRY)) or "<empty>"
        raise KeyError(
            f"no FeatureLayout registered for dataset {dataset_name!r}. registered: {known}"
        ) from exc


def registered_datasets() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))
