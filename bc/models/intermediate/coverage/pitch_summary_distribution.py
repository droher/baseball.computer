"""Published pitch-summary posterior distribution (Model J).

Thin gather over published Bayes pitch-summary artifacts. Reads each
artifact's ``exports/pitch_summary_summary.parquet`` (grain
``result_family x season x league x final_count_class``) and stamps
``bayes_artifact_id``. Materializes a typed empty frame until at least one
pitch-summary Bayes pointer lands.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("result_family"),
    exp.column("season"),
    exp.column("league"),
    exp.column("final_count_class"),
)

_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("result_family"),
                    exp.column("season"),
                    exp.column("league"),
                    exp.column("final_count_class"),
                    exp.column("prob_mean"),
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
    "main_models.pitch_summary_distribution",
    kind="FULL",
    columns={
        "result_family": "VARCHAR",
        "season": "SMALLINT",
        "league": "VARCHAR",
        "final_count_class": "VARCHAR",
        "balls": "TINYINT",
        "strikes": "TINYINT",
        "outcome": "VARCHAR",
        "prob_mean": "DOUBLE",
        "prob_sd": "DOUBLE",
        "prob_hdi_lower": "DOUBLE",
        "prob_hdi_upper": "DOUBLE",
        "ess_bulk": "DOUBLE",
        "rhat": "DOUBLE",
        "bayes_artifact_id": "VARCHAR",
    },
    grain=["result_family", "season", "league", "final_count_class"],
    audits=_AUDITS,
    description=(
        "Per-(result_family, season, league, final_count_class) posterior "
        "summary of the final ball-strike count distribution from the Bayes "
        "pitch-summary model (Model J). Grain (result_family, season, league, "
        "final_count_class). Sourced from each published Bayes pitch-summary "
        "target's exports/pitch_summary_summary.parquet. Carries the "
        "cell-class probability posterior summary plus the count decomposition "
        "(balls, strikes) and posterior diagnostics (ess_bulk, rhat)."
    ),
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pl.DataFrame]:
    del context, kwargs
    import logging

    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.manifest_ingest import (
        aggregate_pitch_summary_frames,
    )

    log = logging.getLogger(__name__)

    emitted = False
    for pitch_summary_frame in aggregate_pitch_summary_frames():
        if pitch_summary_frame.height == 0:
            log.info("pitch_summary_distribution: empty frame; skipping")
            continue
        emitted = True
        log.info("pitch_summary_distribution: %d rows", pitch_summary_frame.height)
        yield pitch_summary_frame.select(
            [
                pl.col("result_family").cast(pl.Utf8),
                pl.col("season").cast(pl.Int16),
                pl.col("league").cast(pl.Utf8),
                pl.col("final_count_class").cast(pl.Utf8),
                pl.col("balls").cast(pl.Int8),
                pl.col("strikes").cast(pl.Int8),
                pl.col("outcome").cast(pl.Utf8),
                pl.col("prob_mean").cast(pl.Float64),
                pl.col("prob_sd").cast(pl.Float64),
                pl.col("prob_hdi_lower").cast(pl.Float64),
                pl.col("prob_hdi_upper").cast(pl.Float64),
                pl.col("ess_bulk").cast(pl.Float64),
                pl.col("rhat").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        log.info(
            "pitch_summary_distribution: no pitch-summary targets published; "
            "yielding empty frame"
        )
        empty_schema: dict[str, pl.DataType] = {
            "result_family": pl.Utf8(),
            "season": pl.Int16(),
            "league": pl.Utf8(),
            "final_count_class": pl.Utf8(),
            "balls": pl.Int8(),
            "strikes": pl.Int8(),
            "outcome": pl.Utf8(),
            "prob_mean": pl.Float64(),
            "prob_sd": pl.Float64(),
            "prob_hdi_lower": pl.Float64(),
            "prob_hdi_upper": pl.Float64(),
            "ess_bulk": pl.Float64(),
            "rhat": pl.Float64(),
            "bayes_artifact_id": pl.Utf8(),
        }
        yield pl.DataFrame(schema=empty_schema)
