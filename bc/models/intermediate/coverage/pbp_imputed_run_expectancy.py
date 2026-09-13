from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models.imputation.ingest import completed_schema
from python_models.imputation.values import RUN_EXPECTANCY_OUTPUT_SCHEMA


@model(
    "main_models.pbp_imputed_run_expectancy",
    is_sql=True,
    kind="FULL",
    columns=completed_schema(RUN_EXPECTANCY_OUTPUT_SCHEMA),
    grain=["season", "league", "base_state", "outs"],
    audits=[("estimated_contract_complete", {}), ("min_row_count", {"threshold": 1})],
    description="Run expectancy for base-out contexts present in actual PBP, with explicitly exploratory transported fallback.",
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    from python_models.imputation.ingest import build_ingestion_sql

    root = evaluator.var("pbp_imputation_root", "")
    if not isinstance(root, str):
        raise TypeError("pbp_imputation_root must be a string")
    return build_ingestion_sql(root, "run_expectancy", RUN_EXPECTANCY_OUTPUT_SCHEMA)
