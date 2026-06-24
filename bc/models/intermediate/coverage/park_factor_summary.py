"""Published park-factor posterior summary (Model F).

Thin gather over published Bayes park-factor artifacts. Reads each
artifact's ``exports/park_factor_summary.parquet`` (grain
``park_id x season x league x outcome``), drops the diagnostic columns
(``ess_bulk`` / ``rhat``), and stamps ``bayes_artifact_id``. Materializes a
typed empty frame until at least one park-factor Bayes pointer lands.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("park_id"),
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
                    exp.column("park_id"),
                    exp.column("season"),
                    exp.column("league"),
                    exp.column("outcome"),
                    exp.column("theta_mean"),
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
    "main_models.park_factor_summary",
    kind="FULL",
    columns={
        "park_id": "VARCHAR",
        "season": "SMALLINT",
        "league": "VARCHAR",
        "outcome": "VARCHAR",
        "theta_mean": "DOUBLE",
        "theta_sd": "DOUBLE",
        "theta_hdi_lower": "DOUBLE",
        "theta_hdi_upper": "DOUBLE",
        "park_factor_mean": "DOUBLE",
        "bayes_artifact_id": "VARCHAR",
    },
    grain=["park_id", "season", "league", "outcome"],
    audits=_AUDITS,
    description=(
        "Per-(park, season, league, outcome) posterior summary of the "
        "park-factor effect from the Bayes park-factor model (Model F). "
        "Grain (park_id, season, league, outcome). Sourced from each "
        "published Bayes park-factor target's "
        "exports/park_factor_summary.parquet. Carries theta posterior "
        "summary plus park_factor_mean = exp(theta_mean); diagnostic "
        "columns (ess_bulk, rhat) are dropped."
    ),
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pl.DataFrame]:
    del context, kwargs
    import logging

    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.manifest_ingest import (
        PARK_FACTOR_SUMMARY_SCHEMA,
        aggregate_park_factor_frames,
    )

    log = logging.getLogger(__name__)

    emitted = False
    for park_factor_frame in aggregate_park_factor_frames():
        if park_factor_frame.height == 0:
            log.info("park_factor_summary: empty frame; skipping")
            continue
        emitted = True
        log.info("park_factor_summary: %d rows", park_factor_frame.height)
        yield park_factor_frame.select(
            [
                pl.col("park_id").cast(pl.Utf8),
                pl.col("season").cast(pl.Int16),
                pl.col("league").cast(pl.Utf8),
                pl.col("outcome").cast(pl.Utf8),
                pl.col("theta_mean").cast(pl.Float64),
                pl.col("theta_sd").cast(pl.Float64),
                pl.col("theta_hdi_lower").cast(pl.Float64),
                pl.col("theta_hdi_upper").cast(pl.Float64),
                pl.col("park_factor_mean").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        log.info(
            "park_factor_summary: no park-factor targets published; "
            "yielding empty frame"
        )
        yield pl.DataFrame(schema=PARK_FACTOR_SUMMARY_SCHEMA)
