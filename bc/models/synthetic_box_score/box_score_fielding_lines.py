"""Synthetic fielding lines for gamelog-only games.

Optimizes non-pitcher starters across each season so generated lineups
approximate Databank player and position appearance totals. Stat columns
stay NULL; we are filling in lineup skeletons, not fabricating outcomes.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import pandas as pd
import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

from python_models._doc_lookup import doc
from python_models._enum_types import GAME_ID, PLAYER_ID, udt

_UPSTREAM_ASSIGNMENTS = "synthetic_box_score.lineup_assignments"


_ASSIGNMENTS_SQL = """
SELECT
    game_id::VARCHAR AS game_id,
    player_id::VARCHAR AS fielder_id,
    side,
    fielding_position
FROM {assignments}
"""


_NULL_FIELDING_STATS: tuple[str, ...] = (
    "outs_played",
    "putouts",
    "assists",
    "errors",
    "double_plays",
    "triple_plays",
    "passed_balls",
)


_GRAIN = exp.Tuple(
    expressions=[
        exp.column("game_id"),
        exp.column("side"),
        exp.column("fielder_id"),
    ]
)
_POSITION_GRAIN = exp.Tuple(
    expressions=[
        exp.column("game_id"),
        exp.column("side"),
        exp.column("fielding_position"),
    ]
)
_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("game_id"),
                    exp.column("side"),
                    exp.column("fielder_id"),
                    exp.column("fielding_position"),
                    exp.column("nth_position_played_by_player"),
                ]
            ),
        },
    ),
    ("unique_grain", {"columns": _GRAIN}),
    ("unique_grain", {"columns": _POSITION_GRAIN}),
    (
        "relationships",
        {
            "column": exp.column("game_id"),
            "to_model": exp.to_table("main_models.game_results"),
            "to_column": exp.column("game_id"),
        },
    ),
]


_COLUMNS: dict[str, t.Any] = {
    "game_id": GAME_ID,
    "fielder_id": PLAYER_ID,
    "side": udt("SIDE"),
    "fielding_position": "UTINYINT",
    "nth_position_played_by_player": "UTINYINT",
}
_COLUMNS.update({col: "UTINYINT" for col in _NULL_FIELDING_STATS})


@model(
    "synthetic_box_score.box_score_fielding_lines",
    kind="FULL",
    description=(
        "One row per (game_id, inferred fielder, fielding position) for "
        "every gamelog-only game. Mirrors box_score.box_score_fielding_lines "
        "but with stat columns NULL — lineups only, no outcomes. "
        "Reads the shared synthetic lineup assignment table so the optimizer "
        "runs once per build."
    ),
    columns=_COLUMNS,
    column_descriptions={
        "game_id": doc("game_id"),
        "fielding_position": doc("fielding_position"),
        "side": doc("side"),
    },
    grain=["game_id", "side", "fielder_id"],
    audits=_AUDITS,
    physical_properties={
        "download_parquet": (
            "https://data.baseball.computer/dbt/"
            "synthetic_box_score_box_score_fielding_lines.parquet"
        ),
    },
    depends_on={
        _UPSTREAM_ASSIGNMENTS,
    },
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pd.DataFrame]:
    del kwargs
    assignments_sql = _ASSIGNMENTS_SQL.format(
        assignments=context.resolve_table(_UPSTREAM_ASSIGNMENTS),
    )
    core: pl.DataFrame = context.engine_adapter.cursor.sql(assignments_sql).pl()
    if core.is_empty():
        return

    out = core.select(
        pl.col("game_id"),
        pl.col("fielder_id"),
        pl.col("side"),
        pl.col("fielding_position"),
        pl.lit(1, dtype=pl.UInt8).alias("nth_position_played_by_player"),
        *(pl.lit(None, dtype=pl.UInt8).alias(col) for col in _NULL_FIELDING_STATS),
    )

    yield out.to_pandas()
