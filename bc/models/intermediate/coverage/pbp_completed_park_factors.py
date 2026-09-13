from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models.imputation.ingest import completed_schema
from python_models.imputation.values import PARK_FACTORS_OUTPUT_SCHEMA


@model(
    "main_models.pbp_completed_park_factors",
    is_sql=True,
    kind="FULL",
    columns=completed_schema(PARK_FACTORS_OUTPUT_SCHEMA),
    grain=["season", "park_id", "league", "metric"],
    audits=[("estimated_contract_complete", {}), ("min_row_count", {"threshold": 1})],
    description="Long-form park factors for contexts present in actual PBP games, with fixed-population exploratory fallback.",
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    from python_models.imputation.ingest import build_ingestion_sql

    root = evaluator.var("pbp_imputation_root", "")
    if not isinstance(root, str):
        raise TypeError("pbp_imputation_root must be a string")
    return build_ingestion_sql(root, "park_factors", PARK_FACTORS_OUTPUT_SCHEMA)
