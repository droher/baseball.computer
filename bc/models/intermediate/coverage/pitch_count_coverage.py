"""Pitch-summary count-coverage propensities.

Thin gather over the published Bayes pitch-coverage artifact. One row per
(event_key, dimension) carrying ``p_observed_mean`` — the posterior mean
P(final ball-strike count observed) — and the ``bayes_artifact_id`` that
produced it. Streams directly from ``exports/event_propensity.parquet``;
emits a typed empty frame when the target has not published yet.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("event_key"),
    exp.column("dimension"),
)

_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("event_key"),
                    exp.column("dimension"),
                    exp.column("p_observed_mean"),
                ]
            ),
        },
    ),
    (
        "unique_grain",
        {"columns": exp.Tuple(expressions=list(_GRAIN_COLUMNS))},
    ),
]


@model(
    "main_models.pitch_count_coverage",
    kind="FULL",
    columns={
        "event_key": "UINTEGER",
        "dimension": "VARCHAR",
        "p_observed_mean": "DOUBLE",
        "bayes_artifact_id": "VARCHAR",
    },
    grain=["event_key", "dimension"],
    audits=_AUDITS,
    description=(
        "Per-event posterior mean P(final ball-strike count observed) for "
        "the Phase-4 pitch-summary coverage model. Grain (event_key, "
        "dimension). Sourced directly from the published Bayes pitch-coverage "
        "target's exports/event_propensity.parquet."
    ),
)
def execute(
    context: ExecutionContext, **kwargs: t.Any
) -> Iterator[pl.DataFrame]:
    del context, kwargs
    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.manifest_ingest import (
        aggregate_pitch_coverage_frames,
    )

    yield from aggregate_pitch_coverage_frames()
