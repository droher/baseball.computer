"""DL advancement-proposal manifest — sibling of dl_proposal_manifest.

Grain ``(event_key, baserunner)``. Consumed by
``main_models.model_input_advancement``'s LEFT JOIN.

Zero-row stub. Advancement specs (`advancement_r1/_r2/_r3` in
`deep/targets/advancement.py`) are defined but not registered on
import because `model_input_advancement` lacks the
`advancement_class` + `time_forward_fold` columns the fits need. The
stub keeps the typed schema visible to downstream LEFT JOINs. See
`notes/followups.md` for the SQL gap.
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
        "Zero-row stub; advancement specs are not yet registered because "
        "model_input_advancement lacks advancement_class + time_forward_fold."
    ),
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> pl.DataFrame:
    del context, kwargs
    import logging

    logging.getLogger(__name__).info(
        "dl_advancement_proposal_manifest: zero-row stub (SQL gap blocks registration)"
    )
    return pl.DataFrame(
        schema={
            "event_key": pl.UInt32(),
            "baserunner": pl.Utf8(),
            "dl_artifact_id": pl.Utf8(),
            "dl_p_class": pl.List(pl.Float64()),
        }
    )
