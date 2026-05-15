"""DL proposal manifest — Python @model gating on published deep artifacts.

Aggregates every registered deep target whose `sibling_manifest ==
"dl_proposal_manifest"` (geometry, observation, pitch_summary,
park_factors, run_values per pre-flight decision 4 of the Phase-3
plan), reads each published artifact's `probabilities.parquet`, stamps
the row-level `dimension` value from `DeepTargetSpec.proposal_dimension`,
and emits one (event_key, dimension, dl_artifact_id, dl_p_class) row
per upstream prediction.

If a target has no published pointer (early phases / failed fit), the
target is skipped silently with an info log. If NO target publishes
yet, the model yields a typed empty frame so every `model_input_*`
LEFT JOIN keeps working.
"""

from __future__ import annotations

import logging
import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_log = logging.getLogger(__name__)

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
                    exp.column("dl_artifact_id"),
                    exp.column("dl_p_class"),
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
    "main_models.dl_proposal_manifest",
    kind="FULL",
    columns={
        "event_key": "UINTEGER",
        "dimension": "VARCHAR",
        "dl_artifact_id": "VARCHAR",
        "dl_p_class": "DOUBLE[]",
    },
    grain=["event_key", "dimension"],
    audits=_AUDITS,
    description=(
        "Deep-learning proposal manifest (grain: event_key x dimension). "
        "Aggregates per-dimension published deep artifacts; emits a typed "
        "empty frame until publish-manifest writes the first pointer."
    ),
)
def execute(
    context: ExecutionContext, **kwargs: t.Any
) -> Iterator[pl.DataFrame]:
    del context, kwargs
    from python_models.statistical.deep import targets as _targets  # noqa: F401
    from python_models.statistical.deep.manifest_ingest import (
        aggregate_proposal_manifest_frames,
    )

    yield from aggregate_proposal_manifest_frames("dl_proposal_manifest")
