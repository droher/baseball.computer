"""Imputed per-(event, baserunner) runner-advancement class probabilities (Model H).

Thin gather over published Bayes advancement artifacts. Reads each artifact's
``exports/advancement_probabilities.parquet`` (grain ``event_key x baserunner x
advancement_class``) and emits the typed rows directly. Advancement is keyed by
the baserunner slot, not by player, so there is no personnel join. Materializes
a typed empty frame until at least one advancement Bayes pointer lands.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("event_key"),
    exp.column("baserunner"),
    exp.column("advancement_class"),
)

_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("event_key"),
                    exp.column("baserunner"),
                    exp.column("advancement_class"),
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
    "main_models.imputed_advancement_probabilities",
    kind="FULL",
    columns={
        "event_key": "UINTEGER",
        "baserunner": "VARCHAR",
        "advancement_class": "VARCHAR",
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
    grain=["event_key", "baserunner", "advancement_class"],
    audits=_AUDITS,
    description=(
        "Per-(event, baserunner, class) expected probability of the runner's "
        "advancement outcome, from the Bayes advancement model (Model H). Grain "
        "(event_key, baserunner, advancement_class). Sourced from each published "
        "Bayes advancement target's exports/advancement_probabilities.parquet. "
        "Per-(event, baserunner) shares over the 7 advancement classes sum to 1."
    ),
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pl.DataFrame]:
    del context, kwargs
    import logging

    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.manifest_ingest import (
        ADVANCEMENT_SCHEMA,
        aggregate_advancement_frames,
    )

    log = logging.getLogger(__name__)

    emitted = False
    for advancement_frame in aggregate_advancement_frames():
        if advancement_frame.height == 0:
            log.info("imputed_advancement_probabilities: empty frame; skipping")
            continue
        emitted = True
        log.info("imputed_advancement_probabilities: %d rows", advancement_frame.height)
        yield advancement_frame.select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("baserunner").cast(pl.Utf8),
                pl.col("advancement_class").cast(pl.Utf8),
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
            "imputed_advancement_probabilities: no advancement targets published; "
            "yielding empty frame"
        )
        yield pl.DataFrame(schema=ADVANCEMENT_SCHEMA)
