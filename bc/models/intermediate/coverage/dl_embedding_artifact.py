"""DL embedding artifact — Python @model fed by published deep artifacts.

Aggregates every published deep target whose ``manifest.output_paths``
carries an ``embeddings`` key. PR6 zero-row fallback when no target
publishes embeddings; PR3/PR4/PR5 targets do not yet export them, so
this stays empty until a target's fold runner is updated to emit
``exports/embeddings.parquet``.
"""

from __future__ import annotations

import logging
import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_log = logging.getLogger(__name__)

_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("entity_type"),
                    exp.column("entity_id"),
                    exp.column("embedding_value"),
                ]
            ),
        },
    ),
]


@model(
    "main_models.dl_embedding_artifact",
    kind="FULL",
    columns={
        "entity_type": "VARCHAR",
        "entity_id": "VARCHAR",
        "embedding_value": "DOUBLE[]",
        "dl_artifact_id": "VARCHAR",
        "source_target": "VARCHAR",
    },
    grain=["entity_type", "entity_id", "source_target"],
    audits=_AUDITS,
    description=(
        "DL embedding artifact. Aggregates published embeddings.parquet "
        "exports across all registered deep targets. PR6 ships the gate; "
        "real embeddings populate as targets opt in."
    ),
)
def execute(
    context: ExecutionContext, **kwargs: t.Any
) -> Iterator[pl.DataFrame]:
    del context, kwargs
    from python_models.statistical.deep import targets as _targets  # noqa: F401
    from python_models.statistical.deep.embeddings import (
        aggregate_embedding_frames,
    )

    yield from aggregate_embedding_frames()
