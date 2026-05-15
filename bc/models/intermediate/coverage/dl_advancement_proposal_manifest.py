"""DL advancement-proposal manifest — sibling of dl_proposal_manifest.

Grain ``(event_key, baserunner)``. Consumed by
``main_models.model_input_advancement``'s LEFT JOIN.

PR3 ships this as a zero-row fallback; PR4 (advancement proposals)
replaces the body with the real iteration over published advancement
artifacts.
"""

from __future__ import annotations

import typing as t

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("event_key"),
    exp.column("baserunner"),
)

_AUDITS = [
    (
        "unique_grain",
        {"columns": exp.Tuple(expressions=list(_GRAIN_COLUMNS))},
    ),
]

@model(
    "main_models.dl_advancement_proposal_manifest",
    kind="FULL",
    columns={
        "event_key": "UINTEGER",
        "baserunner": "VARCHAR",
        "dl_artifact_id": "VARCHAR",
        "dl_p_class": "DOUBLE[]",
    },
    grain=["event_key", "baserunner"],
    audits=_AUDITS,
    description=(
        "DL advancement-proposal manifest (grain: event_key x baserunner). "
        "PR3 zero-row fallback; PR4 wires real per-baserunner published "
        "artifacts."
    ),
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> pl.DataFrame:
    del context, kwargs
    import logging

    logging.getLogger(__name__).info(
        "dl_advancement_proposal_manifest: PR3 zero-row fallback"
    )
    return pl.DataFrame(
        schema={
            "event_key": pl.UInt32(),
            "baserunner": pl.Utf8(),
            "dl_artifact_id": pl.Utf8(),
            "dl_p_class": pl.List(pl.Float64()),
        }
    )
