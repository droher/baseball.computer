"""SQLMesh wrapper around the has-batting binary scorer.

Streams `ml_features` rows in DuckDB Arrow record batches, runs each
batch through the cached Keras scorer, and yields one pandas DataFrame
per batch. SQLMesh accepts an iterator return from `execute()` and
appends each yielded frame to the materialized table, so working-set
memory stays at `O(_BATCH_ROWS)`.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import pandas as pd
import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

from python_models.ml import artifact_exists

_UPSTREAM = "main_models.ml_features"
_BATCH_ROWS = 500_000

_GRAIN = exp.column("event_key")
_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("event_key"),
                    exp.column("predicted_class_bin"),
                    exp.column("predicted_proba"),
                    exp.column("model_run_id"),
                ]
            ),
        },
    ),
    ("unique_grain", {"columns": exp.Tuple(expressions=[_GRAIN])}),
    (
        "relationships",
        {
            "column": _GRAIN,
            "to_model": exp.to_table("main_models.ml_features"),
            "to_column": _GRAIN,
        },
    ),
    (
        "bounded_range",
        {
            "column": exp.column("predicted_proba"),
            "min_v": exp.convert(0.0),
            "max_v": exp.convert(1.0),
        },
    ),
    (
        "bounded_range",
        {
            "column": exp.column("predicted_class_bin"),
            "min_v": exp.convert(0),
            "max_v": exp.convert(1),
        },
    ),
]


@model(
    "main_models.predictions_has_batting_bin",
    kind="FULL",
    enabled=artifact_exists("has_batting_bin"),
    columns={
        "event_key": "UINTEGER",
        "predicted_class_bin": "UTINYINT",
        "predicted_proba": "DOUBLE",
        "model_run_id": "VARCHAR",
    },
    grain=["event_key"],
    depends_on={_UPSTREAM},
    audits=_AUDITS,
)
def execute(
    context: ExecutionContext, **kwargs: t.Any
) -> Iterator[pd.DataFrame]:
    del kwargs
    import logging

    from python_models.ml.prediction import load_scorer

    log = logging.getLogger(__name__)
    upstream = context.resolve_table(_UPSTREAM)
    scorer = load_scorer("has_batting_bin")
    read_con = context.engine_adapter.cursor.cursor()
    rel = read_con.sql(f"SELECT * FROM {upstream}")
    reader = rel.to_arrow_reader(_BATCH_ROWS)
    total = 0
    try:
        for batch_idx, record_batch in enumerate(reader):
            features = pl.from_arrow(record_batch)
            assert isinstance(features, pl.DataFrame)
            scored = scorer.score(features).to_pandas()
            total += record_batch.num_rows
            log.info(
                "scored batch %d (%d rows, cumulative %d)",
                batch_idx,
                record_batch.num_rows,
                total,
            )
            yield scored
    finally:
        read_con.close()
    log.info("finished scoring: total=%d", total)
