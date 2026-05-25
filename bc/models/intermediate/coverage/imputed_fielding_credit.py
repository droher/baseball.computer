"""Imputed per-event fielding credit (Model C).

Thin gather over published Bayes fielding-credit artifacts. Reads each
artifact's ``exports/event_credit.parquet`` (grain
``event_key x fielding_position x credit_type``), joins ``personnel_fielding_states``
through ``event_personnel_lookup`` to stamp ``player_id`` per
``(event_key, fielding_position)``, and emits rows at grain
``(event_key, player_id, fielding_position, credit_type)``. Keeps the
parquet detached from personnel identity so a personnel-state revision
doesn't invalidate published artifacts. Materializes a typed empty
frame until at least one fielding-credit Bayes pointer lands.
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
    exp.column("credit_type"),
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
                    exp.column("credit_type"),
                    exp.column("expected_share"),
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
    "main_models.imputed_fielding_credit",
    kind="FULL",
    columns={
        "event_key": "UINTEGER",
        "player_id": "VARCHAR",
        "fielding_position": "UTINYINT",
        "credit_type": "VARCHAR",
        "expected_share": "DOUBLE",
        "none_share": "DOUBLE",
        "bayes_artifact_id": "VARCHAR",
    },
    grain=["event_key", "player_id", "fielding_position", "credit_type"],
    audits=_AUDITS,
    description=(
        "Per-(event, player, position, credit_type) expected fielding-credit "
        "share from the Bayes allocation model. Grain "
        "(event_key, player_id, fielding_position, credit_type). Sourced from "
        "each published Bayes credit target's exports/event_credit.parquet, "
        "with player_id stamped via a join to personnel_fielding_states. "
        "none_share is the per-event P(no credit allocation) from the K=10 "
        "softmax for credit_types that carry a NONE sentinel (assist v3); "
        "NULL for credit_types that don't (putout v1.5)."
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
        aggregate_fielding_credit_frames,
    )

    log = logging.getLogger(__name__)

    epl_table = context.resolve_table("main_models.event_personnel_lookup")
    pfs_table = context.resolve_table("main_models.personnel_fielding_states")
    cursor = context.engine_adapter.cursor

    emitted = False
    for credit_frame in aggregate_fielding_credit_frames():
        if credit_frame.height == 0:
            log.info("imputed_fielding_credit: empty credit frame; skipping join")
            continue
        emitted = True
        event_keys_df = (
            credit_frame.get_column("event_key").unique().cast(pl.UInt32).to_frame()
        )
        cursor.register("event_keys_tbl", event_keys_df)
        try:
            personnel = cursor.sql(
                f"""
                SELECT
                    epl.event_key::UINTEGER AS event_key,
                    pfs.fielding_position::UTINYINT AS fielding_position,
                    pfs.player_id::VARCHAR AS player_id
                FROM {epl_table} AS epl
                INNER JOIN {pfs_table} AS pfs
                    ON pfs.game_id = epl.game_id
                    AND pfs.personnel_fielding_key = epl.personnel_fielding_key
                WHERE epl.event_key IN (SELECT event_key FROM event_keys_tbl)
                    AND pfs.fielding_position BETWEEN 1 AND 9
                """
            ).pl()
        finally:
            cursor.unregister("event_keys_tbl")
        joined = credit_frame.join(
            personnel, on=["event_key", "fielding_position"], how="inner"
        ).select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("player_id").cast(pl.Utf8),
                pl.col("fielding_position").cast(pl.UInt8),
                pl.col("credit_type").cast(pl.Utf8),
                pl.col("expected_share").cast(pl.Float64),
                pl.col("none_share").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
        log.info(
            "imputed_fielding_credit: %d rows after personnel join (input %d)",
            joined.height,
            credit_frame.height,
        )
        yield joined
    if not emitted:
        log.info(
            "imputed_fielding_credit: no credit targets published; yielding empty frame"
        )
        empty_schema: dict[str, pl.DataType] = {
            "event_key": pl.UInt32(),
            "player_id": pl.Utf8(),
            "fielding_position": pl.UInt8(),
            "credit_type": pl.Utf8(),
            "expected_share": pl.Float64(),
            "none_share": pl.Float64(),
            "bayes_artifact_id": pl.Utf8(),
        }
        yield pl.DataFrame(schema=empty_schema)
