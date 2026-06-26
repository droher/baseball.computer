"""Scorer / source / event observation propensities.

Thin gather over published Bayes observation artifacts. One row per
(event_key, dimension) carrying ``p_observed_mean`` and the
estimated-metadata contract columns. Streams directly from
``exports/event_propensity.parquet``; emits a typed empty frame when no
target has published yet.
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
    ("estimated_contract_complete", {}),
]


@model(
    "main_models.scorer_observation_propensities",
    kind="FULL",
    columns={
        "event_key": "UINTEGER",
        "dimension": "VARCHAR",
        "p_observed_mean": "DOUBLE",
        "artifact_id": "VARCHAR",
        "model_name": "VARCHAR",
        "model_version": "VARCHAR",
        "source_snapshot_id": "VARCHAR",
        "method": "VARCHAR",
        "observed_status": "VARCHAR",
        "confidence_status": "VARCHAR",
        "weak_identification_flag": "BOOLEAN",
    },
    grain=["event_key", "dimension"],
    audits=_AUDITS,
    description=(
        "Per-event posterior mean P(observed) for the Phase-4 observation "
        "propensity model. Grain (event_key, dimension). Sourced directly "
        "from each published Bayes target's exports/event_propensity.parquet."
    ),
)
def execute(
    context: ExecutionContext, **kwargs: t.Any
) -> Iterator[pl.DataFrame]:
    del context, kwargs
    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.manifest_ingest import (
        aggregate_observation_propensity_frames,
    )

    yield from aggregate_observation_propensity_frames()
