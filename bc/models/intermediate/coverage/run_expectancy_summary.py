"""Published run-expectancy posterior summary (Model G).

Thin gather over published Bayes run-expectancy artifacts. Reads each
artifact's ``exports/run_expectancy_summary.parquet`` (grain
``state x season x league x outcome``) and stamps the estimated-metadata contract.
Materializes a typed empty frame until at least one run-expectancy Bayes
pointer lands.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("state"),
    exp.column("season"),
    exp.column("league"),
    exp.column("outcome"),
)

_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("state"),
                    exp.column("season"),
                    exp.column("league"),
                    exp.column("outcome"),
                    exp.column("re_value_mean"),
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
    "main_models.run_expectancy_summary",
    kind="FULL",
    columns={
        "state": "VARCHAR",
        "base_state": "TINYINT",
        "outs": "TINYINT",
        "season": "SMALLINT",
        "league": "VARCHAR",
        "outcome": "VARCHAR",
        "re_value_mean": "DOUBLE",
        "re_value_sd": "DOUBLE",
        "re_value_hdi_lower": "DOUBLE",
        "re_value_hdi_upper": "DOUBLE",
        "artifact_id": "VARCHAR",
        "model_name": "VARCHAR",
        "model_version": "VARCHAR",
        "source_snapshot_id": "VARCHAR",
        "method": "VARCHAR",
        "observed_status": "VARCHAR",
        "confidence_status": "VARCHAR",
        "weak_identification_flag": "BOOLEAN",
    },
    grain=["state", "season", "league", "outcome"],
    audits=_AUDITS,
    description=(
        "Per-(state, season, league, outcome) posterior summary of the "
        "run-expectancy value from the Bayes run-expectancy model (Model G). "
        "Grain (state, season, league, outcome). Sourced from each "
        "published Bayes run-expectancy target's "
        "exports/run_expectancy_summary.parquet. Carries the re_value "
        "posterior summary plus the base-out decomposition (base_state, outs) "
        "and the estimated-metadata contract columns."
    ),
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pl.DataFrame]:
    del context, kwargs
    import logging

    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.manifest_ingest import (
        RUN_EXPECTANCY_SUMMARY_SCHEMA,
        aggregate_run_expectancy_frames,
    )

    log = logging.getLogger(__name__)

    emitted = False
    for run_expectancy_frame in aggregate_run_expectancy_frames():
        if run_expectancy_frame.height == 0:
            log.info("run_expectancy_summary: empty frame; skipping")
            continue
        emitted = True
        log.info("run_expectancy_summary: %d rows", run_expectancy_frame.height)
        yield run_expectancy_frame.select(
            [
                pl.col("state").cast(pl.Utf8),
                pl.col("base_state").cast(pl.Int8),
                pl.col("outs").cast(pl.Int8),
                pl.col("season").cast(pl.Int16),
                pl.col("league").cast(pl.Utf8),
                pl.col("outcome").cast(pl.Utf8),
                pl.col("re_value_mean").cast(pl.Float64),
                pl.col("re_value_sd").cast(pl.Float64),
                pl.col("re_value_hdi_lower").cast(pl.Float64),
                pl.col("re_value_hdi_upper").cast(pl.Float64),
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
            "run_expectancy_summary: no run-expectancy targets published; "
            "yielding empty frame"
        )
        yield pl.DataFrame(schema=RUN_EXPECTANCY_SUMMARY_SCHEMA)
