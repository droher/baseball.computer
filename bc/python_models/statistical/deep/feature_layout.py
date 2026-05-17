"""FeatureLayout registry for coverage-target deep training.

Re-exports ``FeatureLayout`` from the legacy ML package so deep callers
don't reach into ``python_models.ml`` directly. Provides a per-dataset
registry and a ``validate_pre_event`` deny-list check that every
target's ``_register()`` calls before publishing its layout.
"""

from __future__ import annotations

from python_models.ml.features import FeatureLayout

__all__ = (
    "FeatureLayout",
    "coverage_layout_for",
    "register_coverage_layout",
    "registered_datasets",
    "validate_pre_event",
    "PreEventLayoutError",
)

_REGISTRY: dict[str, FeatureLayout] = {}


_LEAK_EXACT: frozenset[str] = frozenset(
    {
        "pa_result",
        "result_family",
        "fielder_chain",
        "gap_class",
        "fielding_evidence_status",
        "outs_end",
        "balls_called",
        "hit_or_out",
    }
)

_LEAK_SUFFIXES: tuple[str, ...] = (
    "_end",
    "_on_play",
)

_LEAK_PREFIXES: tuple[str, ...] = (
    "batted_to_fielder",
    "strikes_",
    "swings",
    "pitches",
)


class PreEventLayoutError(ValueError):
    """Raised when a FeatureLayout contains post-event / outcome-correlated columns."""


def _is_leak_column(col: str) -> str | None:
    if col in _LEAK_EXACT:
        return f"exact match {col!r}"
    for suffix in _LEAK_SUFFIXES:
        if col.endswith(suffix):
            return f"suffix {suffix!r}"
    for prefix in _LEAK_PREFIXES:
        if col.startswith(prefix):
            return f"prefix {prefix!r}"
    return None


def validate_pre_event(layout: FeatureLayout) -> None:
    """Reject layouts that include known post-event / outcome columns.

    Phase-3 v6 invariant: every deep-supplement input layout must contain
    only features available at inference time. Post-event signals
    (`*_on_play`, `*_end`, `pa_result`, `result_family`, `fielder_chain`,
    `gap_class`, `fielding_evidence_status`, `batted_to_fielder*`,
    cumulative AB pitch counters) train the model to a distribution that
    does not exist at scoring time and let it short-circuit entity priors.

    Raises ``PreEventLayoutError`` listing each offending column.
    """
    columns: tuple[str, ...] = (
        tuple(layout.high_card_columns)
        + tuple(layout.low_card_columns)
        + tuple(layout.numeric_columns)
    )
    offenders: list[tuple[str, str]] = []
    for col in columns:
        reason = _is_leak_column(col)
        if reason is not None:
            offenders.append((col, reason))
    if offenders:
        joined = ", ".join(f"{c} ({r})" for c, r in offenders)
        raise PreEventLayoutError(
            f"FeatureLayout contains post-event / outcome columns: {joined}"
        )


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
