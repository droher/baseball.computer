from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models.imputation.pitches import OUTPUT_SCHEMA
from python_models.imputation.ingest import completed_schema


@model(
    "main_models.pbp_completed_pitches",
    is_sql=True,
    kind="FULL",
    columns=completed_schema(OUTPUT_SCHEMA),
    grain=["event_key"],
    audits=[("estimated_contract_complete", {}), ("min_row_count", {"threshold": 1})],
    description="Additive full-history PBP pitches completion with preserved source evidence and explicit exploratory methods. Empty until a full artifact root is selected.",
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    from python_models.imputation.ingest import build_ingestion_sql

    root = evaluator.var("pbp_imputation_root", "")
    if not isinstance(root, str):
        raise TypeError("pbp_imputation_root must be a string")
    return build_ingestion_sql(root, "pitches", OUTPUT_SCHEMA)
