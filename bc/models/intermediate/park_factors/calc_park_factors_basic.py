"""SQLMesh model for the basic park-factor table."""

from __future__ import annotations

from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models._doc_lookup import doc
from python_models._enum_types import PARK_ID
from python_models.park_factors import build_basic_park_factor_sql


@model(
    "main_models.calc_park_factors_basic",
    is_sql=True,
    kind="FULL",
    columns={
        "park_id": PARK_ID,
        "season": "SMALLINT",
        "league": "VARCHAR",
        "sqrt_sample_size": "DOUBLE",
        "avg_this_runs_per_inning": "DOUBLE",
        "avg_other_runs_per_inning": "DOUBLE",
        "basic_park_factor": "DOUBLE",
    },
    column_descriptions={
        "park_id": doc("park_id"),
        "season": doc("season"),
        "league": doc("league"),
    },
    grain=["park_id", "season", "league"],
    physical_properties={
        "download_parquet": "https://data.baseball.computer/dbt/main_models_calc_park_factors_basic.parquet",
    },
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    del evaluator
    return build_basic_park_factor_sql()
