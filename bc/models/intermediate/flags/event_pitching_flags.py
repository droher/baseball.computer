"""Polars FSM that derives save / hold / blown-save flags per event."""

from __future__ import annotations

import typing as t

import pandas as pd
import polars as pl
from sqlmesh import ExecutionContext, model

from python_models._doc_lookup import doc
from python_models.event_locality import (
    PITCHING_FLAGS_INPUT_COLUMNS,
    compute_pitching_flags,
)

_UPSTREAM = "main_models.event_states_full"


@model(
    "main_models.event_pitching_flags",
    kind="FULL",
    columns={
        "game_id": "VARCHAR",
        "event_key": "UINTEGER",
        "event_id": "UTINYINT",
        "previous_pitcher_id": "VARCHAR",
        "pitcher_id": "VARCHAR",
        "starting_pitcher_flag": "BOOLEAN",
        "bequeathed_runners": "UTINYINT",
        "inherited_runners": "UTINYINT",
        "new_relief_pitcher_flag": "BOOLEAN",
        "pitcher_exit_flag": "BOOLEAN",
        "pitcher_finish_flag": "BOOLEAN",
        "starting_pitcher_exit_flag": "BOOLEAN",
        "starting_pitcher_early_exit_flag": "BOOLEAN",
        "save_situation_start_flag": "BOOLEAN",
        "hold_flag": "BOOLEAN",
        "save_flag": "BOOLEAN",
        "blown_save_flag": "BOOLEAN",
        "blown_long_save_flag": "BOOLEAN",
    },
    column_descriptions={
        "game_id": doc("game_id"),
        "event_key": doc("event_key"),
        "event_id": doc("event_id"),
        "pitcher_id": doc("pitcher_id"),
        "bequeathed_runners": doc("bequeathed_runners"),
        "inherited_runners": doc("inherited_runners"),
    },
    grain=["event_key"],
    physical_properties={
        "download_parquet": "https://data.baseball.computer/dbt/main_models_event_pitching_flags.parquet",
    },
    depends_on={_UPSTREAM},
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> pd.DataFrame:
    del kwargs
    upstream = context.resolve_table(_UPSTREAM)
    columns = ", ".join(PITCHING_FLAGS_INPUT_COLUMNS)
    events: pl.DataFrame = context.engine_adapter.cursor.sql(
        f"SELECT {columns} FROM {upstream}"
    ).pl()
    return compute_pitching_flags(events).to_pandas()
