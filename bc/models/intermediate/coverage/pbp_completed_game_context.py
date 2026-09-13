from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models.imputation.context import OUTPUT_SCHEMA
from python_models.imputation.ingest import completed_schema


@model(
    "main_models.pbp_completed_game_context",
    is_sql=True,
    kind="FULL",
    columns=completed_schema(OUTPUT_SCHEMA),
    grain=["game_id"],
    audits=[("estimated_contract_complete", {}), ("min_row_count", {"threshold": 1})],
    description="Source-preserving game-context completion for PBP games only. Empty until an explicit full artifact root is selected; values carry field-level methods and exploratory provenance.",
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    from python_models.imputation.ingest import build_ingestion_sql

    root = evaluator.var("pbp_imputation_root", "")
    if not isinstance(root, str):
        raise TypeError("pbp_imputation_root must be a string")
    return build_ingestion_sql(root, "context", OUTPUT_SCHEMA)
