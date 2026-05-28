"""Fielder-responsibility probabilities (Model I).

Thin gather over published Bayes responsibility artifacts. Reads each artifact's
``exports/responsibility_probabilities.parquet`` (grain ``event_key x
fielding_position``) and emits the typed rows directly. Responsibility is the
analytical opportunity over the range fielder positions (3..9); these
probabilities never rewrite official putouts, assists, errors, or double plays.
Materializes a typed empty frame until at least one responsibility Bayes pointer
lands.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("event_key"),
    exp.column("fielding_position"),
)

_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("event_key"),
                    exp.column("fielding_position"),
                    exp.column("expected_share"),
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
    "main_models.fielder_responsibility_probabilities",
    kind="FULL",
    columns={
        "event_key": "UINTEGER",
        "fielding_position": "UTINYINT",
        "expected_share": "DOUBLE",
        "bayes_artifact_id": "VARCHAR",
    },
    grain=["event_key", "fielding_position"],
    audits=_AUDITS,
    description=(
        "Per-(event, range fielder position 3..9) expected responsibility "
        "probability from the Bayes responsibility model (Model I). Grain "
        "(event_key, fielding_position). Sourced from each published Bayes "
        "responsibility target's exports/responsibility_probabilities.parquet. "
        "Per-event shares over the range positions sum to 1. Analytical "
        "opportunity only — never rewrites official credit."
    ),
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pl.DataFrame]:
    del context, kwargs
    import logging

    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.manifest_ingest import (
        aggregate_responsibility_frames,
    )

    log = logging.getLogger(__name__)

    emitted = False
    for responsibility_frame in aggregate_responsibility_frames():
        if responsibility_frame.height == 0:
            log.info("fielder_responsibility_probabilities: empty frame; skipping")
            continue
        emitted = True
        log.info(
            "fielder_responsibility_probabilities: %d rows",
            responsibility_frame.height,
        )
        yield responsibility_frame.select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("fielding_position").cast(pl.UInt8),
                pl.col("expected_share").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        log.info(
            "fielder_responsibility_probabilities: no responsibility targets "
            "published; yielding empty frame"
        )
        empty_schema: dict[str, pl.DataType] = {
            "event_key": pl.UInt32(),
            "fielding_position": pl.UInt8(),
            "expected_share": pl.Float64(),
            "bayes_artifact_id": pl.Utf8(),
        }
        yield pl.DataFrame(schema=empty_schema)
