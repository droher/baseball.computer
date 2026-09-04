"""Imputed per-event ball-handler probabilities (Model D).

Thin gather over published Bayes ball-handler artifacts. Reads each
artifact's ``exports/ball_handler_probabilities.parquet`` (grain
``event_key x fielding_position``), joins ``personnel_fielding_states``
through ``event_personnel_lookup`` to stamp ``player_id`` per
``(event_key, fielding_position)``, and emits rows at grain
``(event_key, player_id, fielding_position)``. Keeps the parquet
detached from personnel identity so a personnel-state revision doesn't
invalidate published artifacts. Materializes a typed empty frame until at
least one ball-handler Bayes pointer lands.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("event_key"),
    exp.column("player_id"),
    exp.column("fielding_position"),
)

_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("event_key"),
                    exp.column("player_id"),
                    exp.column("fielding_position"),
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
    ("min_row_count", {"threshold": 100000}),
]


@model(
    "main_models.imputed_ball_handler_probabilities",
    kind="FULL",
    columns={
        "event_key": "UINTEGER",
        "player_id": "VARCHAR",
        "fielding_position": "UTINYINT",
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
    grain=["event_key", "player_id", "fielding_position"],
    audits=_AUDITS,
    description=(
        "Per-(event, player, position) expected probability that the player "
        "handled the batted ball, from the Bayes ball-handler model (Model D). "
        "Grain (event_key, player_id, fielding_position). Sourced from each "
        "published Bayes ball-handler target's "
        "exports/ball_handler_probabilities.parquet, with player_id stamped "
        "via a join to personnel_fielding_states. Per-event shares over the 9 "
        "positions sum to 1."
    ),
    depends_on={
        "main_models.event_personnel_lookup",
        "main_models.personnel_fielding_states",
    },
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pl.DataFrame]:
    del kwargs
    import logging

    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.manifest_ingest import (
        ESTIMATED_CONTRACT_SCHEMA,
        aggregate_ball_handler_frames,
        query_ball_handler_personnel,
        stamp_ball_handler_personnel,
    )

    log = logging.getLogger(__name__)

    epl_table = context.resolve_table("main_models.event_personnel_lookup")
    pfs_table = context.resolve_table("main_models.personnel_fielding_states")
    cursor = context.engine_adapter.cursor

    emitted = False
    for handler_frame in aggregate_ball_handler_frames():
        if handler_frame.height == 0:
            log.info("imputed_ball_handler_probabilities: empty frame; skipping join")
            continue
        emitted = True
        event_keys_df = (
            handler_frame.get_column("event_key").unique().cast(pl.UInt32).to_frame()
        )
        personnel = query_ball_handler_personnel(
            cursor,
            epl_table=epl_table,
            pfs_table=pfs_table,
            event_keys=event_keys_df,
        )
        joined = stamp_ball_handler_personnel(handler_frame, personnel).select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("player_id").cast(pl.Utf8),
                pl.col("fielding_position").cast(pl.UInt8),
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
        log.info(
            "imputed_ball_handler_probabilities: %d rows after personnel join (input %d)",
            joined.height,
            handler_frame.height,
        )
        yield joined
    if not emitted:
        log.info(
            "imputed_ball_handler_probabilities: no ball-handler targets published; "
            "yielding empty frame"
        )
        empty_schema: dict[str, pl.DataType] = {
            "event_key": pl.UInt32(),
            "player_id": pl.Utf8(),
            "fielding_position": pl.UInt8(),
            "expected_share": pl.Float64(),
            **ESTIMATED_CONTRACT_SCHEMA,
        }
        yield pl.DataFrame(schema=empty_schema)
