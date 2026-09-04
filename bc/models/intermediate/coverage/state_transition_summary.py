"""Published base-out transition posterior distribution (Model G transition arm).

Thin gather over published Bayes state-transition artifacts. Reads each
artifact's ``exports/state_transition_summary.parquet`` (grain
``start_state x season x league x end_class``) and stamps the
estimated-metadata contract. Materializes a typed empty frame until at least one
state-transition Bayes pointer lands.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("start_state"),
    exp.column("season"),
    exp.column("league"),
    exp.column("end_class"),
)

_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("start_state"),
                    exp.column("season"),
                    exp.column("league"),
                    exp.column("end_class"),
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
    ("min_row_count", {"threshold": 10000}),
]


@model(
    "main_models.state_transition_summary",
    kind="FULL",
    columns={
        "start_state": "VARCHAR",
        "season": "SMALLINT",
        "league": "VARCHAR",
        "end_class": "VARCHAR",
        "outcome": "VARCHAR",
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
    grain=["start_state", "season", "league", "end_class"],
    audits=_AUDITS,
    description=(
        "Per-(start_state, season, league, end_class) posterior summary of the "
        "base-out transition distribution from the Bayes state-transition model "
        "(Model G transition arm). Grain (start_state, season, league, "
        "end_class). Sourced from each published Bayes state-transition target's "
        "exports/state_transition_summary.parquet. Carries the cell-class "
        "probability posterior summary plus the estimated-metadata contract "
        "columns."
    ),
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pl.DataFrame]:
    del context, kwargs
    import logging

    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.manifest_ingest import (
        STATE_TRANSITION_SUMMARY_SCHEMA,
        aggregate_state_transition_frames,
    )

    log = logging.getLogger(__name__)

    emitted = False
    for state_transition_frame in aggregate_state_transition_frames():
        if state_transition_frame.height == 0:
            log.info("state_transition_summary: empty frame; skipping")
            continue
        emitted = True
        log.info(
            "state_transition_summary: %d rows", state_transition_frame.height
        )
        yield state_transition_frame.select(
            [
                pl.col("start_state").cast(pl.Utf8),
                pl.col("season").cast(pl.Int16),
                pl.col("league").cast(pl.Utf8),
                pl.col("end_class").cast(pl.Utf8),
                pl.col("outcome").cast(pl.Utf8),
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
            "state_transition_summary: no state-transition targets published; "
            "yielding empty frame"
        )
        yield pl.DataFrame(schema=STATE_TRANSITION_SUMMARY_SCHEMA)
