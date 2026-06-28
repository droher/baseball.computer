"""Imputed per-event batted-ball geometry class probabilities (Model E).

Thin gather over published Bayes geometry artifacts. Reads each artifact's
``exports/geometry_probabilities.parquet`` (grain
``event_key x geometry_dimension x class_index``) and emits the typed rows
directly. Geometry class is per-event, not per-player, so unlike the
ball-handler @model there is no personnel join. Materializes a typed empty
frame until at least one geometry Bayes pointer lands.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("event_key"),
    exp.column("geometry_dimension"),
    exp.column("class_index"),
)

_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("event_key"),
                    exp.column("geometry_dimension"),
                    exp.column("class_index"),
                    exp.column("expected_share"),
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
    "main_models.imputed_batted_ball_geometry",
    kind="FULL",
    columns={
        "event_key": "UINTEGER",
        "geometry_dimension": "VARCHAR",
        "class_index": "UTINYINT",
        "class_label": "VARCHAR",
        "expected_share": "DOUBLE",
        "artifact_id": "VARCHAR",
        "model_name": "VARCHAR",
        "model_version": "VARCHAR",
        "source_snapshot_id": "VARCHAR",
        "method": "VARCHAR",
        "observed_status": "VARCHAR",
        "confidence_status": "VARCHAR",
        "weak_identification_flag": "BOOLEAN",
    },
    grain=["event_key", "geometry_dimension", "class_index"],
    audits=_AUDITS,
    description=(
        "Per-(event, geometry dimension, class) expected probability of the "
        "directly recorded batted-ball geometry class, from the Bayes geometry "
        "model (Model E). Grain (event_key, geometry_dimension, class_index). "
        "Sourced from each published Bayes geometry target's "
        "exports/geometry_probabilities.parquet. Per-(event, dimension) shares "
        "over the dimension's classes sum to 1."
    ),
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pl.DataFrame]:
    del context, kwargs
    import logging

    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.manifest_ingest import (
        GEOMETRY_SCHEMA,
        aggregate_geometry_frames,
    )

    log = logging.getLogger(__name__)

    emitted = False
    for geometry_frame in aggregate_geometry_frames():
        if geometry_frame.height == 0:
            log.info("imputed_batted_ball_geometry: empty frame; skipping")
            continue
        emitted = True
        log.info("imputed_batted_ball_geometry: %d rows", geometry_frame.height)
        yield geometry_frame.select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("geometry_dimension").cast(pl.Utf8),
                pl.col("class_index").cast(pl.UInt8),
                pl.col("class_label").cast(pl.Utf8),
                pl.col("expected_share").cast(pl.Float64),
                pl.col("artifact_id").cast(pl.Utf8),
                pl.col("model_name").cast(pl.Utf8),
                pl.col("model_version").cast(pl.Utf8),
                pl.col("source_snapshot_id").cast(pl.Utf8),
                pl.col("method").cast(pl.Utf8),
                pl.col("observed_status").cast(pl.Utf8),
                pl.col("confidence_status").cast(pl.Utf8),
                pl.col("weak_identification_flag").cast(pl.Boolean()),
            ]
        )
    if not emitted:
        log.info(
            "imputed_batted_ball_geometry: no geometry targets published; "
            "yielding empty frame"
        )
        yield pl.DataFrame(schema=GEOMETRY_SCHEMA)
