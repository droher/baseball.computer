from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models.imputation.ingest import completed_schema
from python_models.imputation.values import STATE_TRANSITIONS_OUTPUT_SCHEMA


@model(
    "main_models.pbp_imputed_state_transitions",
    is_sql=True,
    kind="FULL",
    columns=completed_schema(STATE_TRANSITIONS_OUTPUT_SCHEMA),
    grain=["season", "league", "start_state", "end_class"],
    audits=[("estimated_contract_complete", {}), ("min_row_count", {"threshold": 1})],
    description="Normalized transition vectors for actual PBP contexts, preserving posterior vectors and transporting whole donor vectors.",
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    from python_models.imputation.ingest import build_ingestion_sql

    root = evaluator.var("pbp_imputation_root", "")
    if not isinstance(root, str):
        raise TypeError("pbp_imputation_root must be a string")
    return build_ingestion_sql(
        root, "state_transitions", STATE_TRANSITIONS_OUTPUT_SCHEMA
    )
