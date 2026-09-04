"""Published assist-count posterior distribution (Model C count head).

Thin gather over published Bayes assist-count artifacts. Reads each artifact's
``exports/assist_count_summary.parquet`` (grain
``result_family x base_state_start x outs_start x assist_count_class``) and
stamps the estimated-metadata contract. Materializes a typed empty frame until at
least one assist-count Bayes pointer lands.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("result_family"),
    exp.column("base_state_start"),
    exp.column("outs_start"),
    exp.column("assist_count_class"),
)

_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("result_family"),
                    exp.column("base_state_start"),
                    exp.column("outs_start"),
                    exp.column("assist_count_class"),
                    exp.column("prob_mean"),
                ]
            ),
        },
    ),
    (
        "unique_grain",
        {"columns": exp.Tuple(expressions=list(_GRAIN_COLUMNS))},
    ),
    ("estimated_contract_complete", {}),
    ("min_row_count", {"threshold": 100}),
]


@model(
    "main_models.assist_count_distribution",
    kind="FULL",
    columns={
        "result_family": "VARCHAR",
        "base_state_start": "TINYINT",
        "outs_start": "TINYINT",
        "assist_count_class": "VARCHAR",
        "prob_mean": "DOUBLE",
        "prob_sd": "DOUBLE",
        "prob_hdi_lower": "DOUBLE",
        "prob_hdi_upper": "DOUBLE",
        "artifact_id": "VARCHAR",
        "model_name": "VARCHAR",
        "model_version": "VARCHAR",
        "source_snapshot_id": "VARCHAR",
        "method": "VARCHAR",
        "observed_status": "VARCHAR",
        "confidence_status": "VARCHAR",
        "weak_identification_flag": "BOOLEAN",
    },
    grain=["result_family", "base_state_start", "outs_start", "assist_count_class"],
    audits=_AUDITS,
    description=(
        "Per-(result_family, base_state_start, outs_start, assist_count_class) "
        "posterior summary of the missing-assist-count distribution from the "
        "Bayes assist-count model (Model C count head). Grain (result_family, "
        "base_state_start, outs_start, assist_count_class). Sourced from each "
        "published Bayes assist-count target's "
        "exports/assist_count_summary.parquet. Carries the count-class "
        "probability posterior summary plus the estimated-metadata contract "
        "columns."
    ),
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pl.DataFrame]:
    del context, kwargs
    import logging

    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.manifest_ingest import (
        ASSIST_COUNT_SUMMARY_SCHEMA,
        aggregate_assist_count_frames,
    )

    log = logging.getLogger(__name__)

    emitted = False
    for assist_count_frame in aggregate_assist_count_frames():
        if assist_count_frame.height == 0:
            log.info("assist_count_distribution: empty frame; skipping")
            continue
        emitted = True
        log.info("assist_count_distribution: %d rows", assist_count_frame.height)
        yield assist_count_frame.select(
            [
                pl.col("result_family").cast(pl.Utf8),
                pl.col("base_state_start").cast(pl.Int8),
                pl.col("outs_start").cast(pl.Int8),
                pl.col("assist_count_class").cast(pl.Utf8),
                pl.col("prob_mean").cast(pl.Float64),
                pl.col("prob_sd").cast(pl.Float64),
                pl.col("prob_hdi_lower").cast(pl.Float64),
                pl.col("prob_hdi_upper").cast(pl.Float64),
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
            "assist_count_distribution: no assist-count targets published; "
            "yielding empty frame"
        )
        yield pl.DataFrame(schema=ASSIST_COUNT_SUMMARY_SCHEMA)
