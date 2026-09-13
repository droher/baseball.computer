from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models.imputation.ingest import completed_schema
from python_models.imputation.values import LINEAR_WEIGHTS_OUTPUT_SCHEMA


@model(
    "main_models.pbp_completed_linear_weights",
    is_sql=True,
    kind="FULL",
    columns=completed_schema(LINEAR_WEIGHTS_OUTPUT_SCHEMA),
    grain=["season", "league", "play"],
    audits=[("estimated_contract_complete", {}), ("min_row_count", {"threshold": 1})],
    description="Linear weights for actual PBP season-league contexts, falling back to the existing deterministic surface.",
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    from python_models.imputation.ingest import build_ingestion_sql

    root = evaluator.var("pbp_imputation_root", "")
    if not isinstance(root, str):
        raise TypeError("pbp_imputation_root must be a string")
    return build_ingestion_sql(root, "linear_weights", LINEAR_WEIGHTS_OUTPUT_SCHEMA)
