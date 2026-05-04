"""SQLMesh model for the advanced park-factor table."""

from __future__ import annotations

from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models._doc_lookup import doc
from python_models._enum_types import PARK_ID
from python_models.park_factors import (
    build_advanced_park_factor_sql,
)
from python_models.park_factors.advanced import ADVANCED_PARK_FACTOR_RATE_STATS


@model(
    "main_models.calc_park_factors_advanced",
    is_sql=True,
    kind="FULL",
    columns={
        "park_id": PARK_ID,
        "season": "SMALLINT",
        "league": "VARCHAR",
        "sqrt_sample_size": "DOUBLE",
        **{f"{s}_park_factor": "DOUBLE" for s in ADVANCED_PARK_FACTOR_RATE_STATS},
    },
    column_descriptions={
        "park_id": doc("park_id"),
        "season": doc("season"),
        "league": doc("league"),
    },
    grain=["park_id", "season", "league"],
    physical_properties={
        "download_parquet": "https://data.baseball.computer/dbt/main_models_calc_park_factors_advanced.parquet",
    },
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    del evaluator
    return build_advanced_park_factor_sql()
