"""DL credit-proposal manifest — sibling of dl_proposal_manifest.

Grain ``(event_key, player_id, fielding_position, credit_type)``.
Consumed by ``main_models.model_input_fielding_credit``'s LEFT JOIN.

PR3 ships this as a zero-row fallback; PR5 (fielding-credit proposals)
replaces the body with the real iteration over published credit
artifacts.
"""

from __future__ import annotations

import typing as t

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("event_key"),
    exp.column("player_id"),
    exp.column("fielding_position"),
    exp.column("credit_type"),
)

_AUDITS = [
    (
        "unique_grain",
        {"columns": exp.Tuple(expressions=list(_GRAIN_COLUMNS))},
    ),
]

@model(
    "main_models.dl_credit_proposal_manifest",
    kind="FULL",
    columns={
        "event_key": "UINTEGER",
        "player_id": "VARCHAR",
        "fielding_position": "UTINYINT",
        "credit_type": "VARCHAR",
        "dl_artifact_id": "VARCHAR",
        "dl_p_class": "DOUBLE[]",
    },
    grain=["event_key", "player_id", "fielding_position", "credit_type"],
    audits=_AUDITS,
    description=(
        "DL credit-proposal manifest (grain: event_key x player_id x "
        "fielding_position x credit_type). PR3 zero-row fallback; PR5 wires "
        "real per-credit-type published artifacts."
    ),
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> pl.DataFrame:
    del context, kwargs
    import logging

    logging.getLogger(__name__).info(
        "dl_credit_proposal_manifest: PR3 zero-row fallback"
    )
    return pl.DataFrame(
        schema={
            "event_key": pl.UInt32(),
            "player_id": pl.Utf8(),
            "fielding_position": pl.UInt8(),
            "credit_type": pl.Utf8(),
            "dl_artifact_id": pl.Utf8(),
            "dl_p_class": pl.List(pl.Float64()),
        }
    )
