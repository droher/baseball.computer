from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models.imputation.ingest import completed_schema
from python_models.imputation.values import EVENT_VALUES_OUTPUT_SCHEMA


@model(
    "main_models.pbp_imputed_event_values",
    is_sql=True,
    kind="FULL",
    columns=completed_schema(EVENT_VALUES_OUTPUT_SCHEMA),
    grain=["event_key"],
    audits=[("estimated_contract_complete", {}), ("min_row_count", {"threshold": 1})],
    description="Source-preserving derived values for every actual PBP event, including postseason and other non-regular-season PBP; estimated values remain explicitly identified.",
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    from python_models.imputation.ingest import build_ingestion_sql

    root = evaluator.var("pbp_imputation_root", "")
    if not isinstance(root, str):
        raise TypeError("pbp_imputation_root must be a string")
    return build_ingestion_sql(root, "event_values", EVENT_VALUES_OUTPUT_SCHEMA)
